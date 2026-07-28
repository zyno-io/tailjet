package cdc

import (
	"bytes"
	"context"
	"fmt"
	"io"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"

	"github.com/zyno-io/tailjet/internal/outbox"
)

const (
	// MySQL permits transaction-payload compression settings that need a 128 MiB
	// Zstd window. Keep this ceiling above valid server output while rejecting
	// hostile frames that would otherwise reserve an unbounded decoder window.
	maxTransactionPayloadDecoderMemory = 128 << 20
	transactionPayloadReadChunk        = 32 << 10
)

// rawEventDecoder keeps go-mysql from eagerly expanding TransactionPayloadEvent.
// The wire event is received in raw mode, then regular events are decoded with a
// BinlogParser. Transaction payloads are decompressed as a stream: table maps are
// read so outbox rows can be identified, while row images for every other table are
// discarded without allocating their uncompressed contents.
type rawEventDecoder struct {
	parser        *replication.BinlogParser
	flavor        string
	gtid          gomysql.GTIDSet
	outboxDecoder *Decoder
	gtidMode      bool
	maxRows       int
	maxBytes      int64
	formatEvent   []byte
	tableIDSize   int
	hasChecksum   bool
}

// decodedTransactionPayloadEvent is intentionally compact: message payloads
// are copied by Decoder and the original binlog row bytes are released before
// this event reaches streamProcessor.
type decodedTransactionPayloadEvent struct {
	messages []outbox.Message
	gtid     gomysql.GTIDSet
}

func (e *decodedTransactionPayloadEvent) Decode([]byte) error { return nil }

func (e *decodedTransactionPayloadEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Decoded transaction payload: %d outbox messages\n", len(e.messages))
}

func newRawEventDecoder(flavor, checkpointGTID string, outboxDecoder *Decoder, maxRows int, maxBytes int64) (*rawEventDecoder, error) {
	if outboxDecoder == nil {
		return nil, fmt.Errorf("outbox decoder is required")
	}
	d := &rawEventDecoder{
		parser:        replication.NewBinlogParser(),
		flavor:        flavor,
		outboxDecoder: outboxDecoder,
		gtidMode:      checkpointGTID != "",
		maxRows:       maxRows,
		maxBytes:      maxBytes,
		tableIDSize:   6,
	}
	d.parser.SetFlavor(flavor)
	d.parser.SetVerifyChecksum(true)
	if checkpointGTID == "" {
		return d, nil
	}
	gtid, err := gomysql.ParseGTIDSet(flavor, checkpointGTID)
	if err != nil {
		return nil, err
	}
	d.gtid = gtid
	return d, nil
}

func (d *rawEventDecoder) Decode(ctx context.Context, raw *replication.BinlogEvent) (*replication.BinlogEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if raw == nil || raw.Header == nil {
		return nil, fmt.Errorf("binlog event has no header")
	}
	if raw.Header.EventType == replication.TRANSACTION_PAYLOAD_EVENT {
		return d.decodeTransactionPayload(ctx, raw)
	}

	event, err := d.parser.Parse(raw.RawData)
	if err != nil {
		return nil, err
	}
	if format, ok := event.Event.(*replication.FormatDescriptionEvent); ok {
		d.formatEvent = append(d.formatEvent[:0], raw.RawData...)
		d.hasChecksum = format.ChecksumAlgorithm == replication.BINLOG_CHECKSUM_ALG_CRC32
		if len(format.EventTypeHeaderLengths) > int(replication.TABLE_MAP_EVENT)-1 &&
			format.EventTypeHeaderLengths[replication.TABLE_MAP_EVENT-1] == 6 {
			d.tableIDSize = 4
		} else {
			d.tableIDSize = 6
		}
	}
	if err := d.updateGTID(event.Event); err != nil {
		return nil, err
	}
	d.attachGTID(event.Event)
	return event, nil
}

func (d *rawEventDecoder) updateGTID(event replication.Event) error {
	if !d.gtidMode {
		return nil
	}
	gtidEvent, ok := event.(gomysql.BinlogGTIDEvent)
	if !ok {
		return nil
	}
	next, err := gtidEvent.GTIDNext()
	if err != nil {
		return err
	}
	if d.gtid == nil {
		d.gtid = next
		return nil
	}
	return d.gtid.Update(next.String())
}

func (d *rawEventDecoder) attachGTID(event replication.Event) {
	if !d.gtidMode || d.gtid == nil {
		return
	}
	switch event := event.(type) {
	case *replication.XIDEvent:
		event.GSet = d.gtid.Clone()
	case *replication.QueryEvent:
		event.GSet = d.gtid.Clone()
	}
}

func (d *rawEventDecoder) decodeTransactionPayload(ctx context.Context, raw *replication.BinlogEvent) (*replication.BinlogEvent, error) {
	if len(d.formatEvent) == 0 {
		return nil, fmt.Errorf("transaction payload received before format description event")
	}
	body, err := d.eventBody(raw)
	if err != nil {
		return nil, err
	}
	payload, err := transactionPayload(body)
	if err != nil {
		return nil, err
	}
	decoder, err := zstd.NewReader(
		bytes.NewReader(payload),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(maxTransactionPayloadDecoderMemory),
		zstd.WithDecoderMaxWindow(maxTransactionPayloadDecoderMemory),
	)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	payloadParser, err := d.newPayloadParser()
	if err != nil {
		return nil, err
	}
	messages, err := d.decodePayloadMessages(ctx, decoder, payloadParser)
	if err != nil {
		return nil, err
	}

	return &replication.BinlogEvent{
		Header: raw.Header,
		Event: &decodedTransactionPayloadEvent{
			messages: messages,
			gtid:     cloneGTID(d.gtid),
		},
	}, nil
}

func (d *rawEventDecoder) eventBody(raw *replication.BinlogEvent) ([]byte, error) {
	if len(raw.RawData) < replication.EventHeaderSize || int(raw.Header.EventSize) != len(raw.RawData) {
		return nil, fmt.Errorf("invalid transaction payload event size: header=%d data=%d", raw.Header.EventSize, len(raw.RawData))
	}
	body := raw.RawData[replication.EventHeaderSize:]
	if d.hasChecksum {
		if len(body) < replication.BinlogChecksumLength {
			return nil, fmt.Errorf("transaction payload event is shorter than its checksum")
		}
		body = body[:len(body)-replication.BinlogChecksumLength]
	}
	return body, nil
}

func (d *rawEventDecoder) newPayloadParser() (*replication.BinlogParser, error) {
	// Payload members have no individual checksum. Replaying the outer format
	// description with its checksum algorithm changed to OFF gives go-mysql the
	// correct row/table-map layout without asking it to decode the payload.
	format := append([]byte(nil), d.formatEvent...)
	if len(format) < replication.EventHeaderSize+5 {
		return nil, fmt.Errorf("format description event is too short")
	}
	format[len(format)-5] = byte(replication.BINLOG_CHECKSUM_ALG_OFF)
	parser := replication.NewBinlogParser()
	parser.SetFlavor(d.flavor)
	parser.SetVerifyChecksum(false)
	if _, err := parser.Parse(format); err != nil {
		return nil, fmt.Errorf("initialize transaction payload parser: %w", err)
	}
	return parser, nil
}

func (d *rawEventDecoder) decodePayloadMessages(ctx context.Context, reader io.Reader, parser *replication.BinlogParser) ([]outbox.Message, error) {
	outboxTableIDs := make(map[uint64]bool)
	var messages []outbox.Message
	var rowCount int
	var rowBytes int64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		headerBytes := make([]byte, replication.EventHeaderSize)
		_, err := readFullContext(ctx, reader, headerBytes)
		if err == io.EOF {
			return messages, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read transaction payload event header: %w", err)
		}
		header := new(replication.EventHeader)
		if err := header.Decode(headerBytes); err != nil {
			return nil, err
		}
		if header.EventSize < replication.EventHeaderSize {
			return nil, fmt.Errorf("invalid transaction payload member size %d", header.EventSize)
		}
		bodySize := int(header.EventSize) - replication.EventHeaderSize

		switch {
		case header.EventType == replication.TABLE_MAP_EVENT:
			event, err := readEmbeddedEvent(ctx, reader, headerBytes, bodySize)
			if err != nil {
				return nil, err
			}
			parsed, err := parser.Parse(event)
			if err != nil {
				return nil, fmt.Errorf("parse transaction payload table map: %w", err)
			}
			table, ok := parsed.Event.(*replication.TableMapEvent)
			if !ok {
				return nil, fmt.Errorf("transaction payload table map decoded as %T", parsed.Event)
			}
			outboxTableIDs[table.TableID] = string(table.Schema) == d.outboxDecoder.database && string(table.Table) == d.outboxDecoder.table

		case isRowsEvent(header.EventType):
			if bodySize < d.tableIDSize {
				return nil, fmt.Errorf("transaction payload rows event is shorter than table id")
			}
			tableIDBytes := make([]byte, d.tableIDSize)
			if _, err := readFullContext(ctx, reader, tableIDBytes); err != nil {
				return nil, fmt.Errorf("read transaction payload rows table id: %w", err)
			}
			tableID := fixedLengthInt(tableIDBytes)
			if !outboxTableIDs[tableID] || !isInsertRowsEvent(header.EventType) {
				if err := discardContext(ctx, reader, bodySize-d.tableIDSize); err != nil {
					return nil, err
				}
				continue
			}
			event, err := readEmbeddedEventAfterPrefix(ctx, reader, headerBytes, tableIDBytes, bodySize-d.tableIDSize)
			if err != nil {
				return nil, err
			}
			parsed, err := parser.Parse(event)
			if err != nil {
				return nil, fmt.Errorf("parse transaction payload outbox rows: %w", err)
			}
			decodedRows, ok := parsed.Event.(*replication.RowsEvent)
			if !ok {
				return nil, fmt.Errorf("transaction payload rows decoded as %T", parsed.Event)
			}
			decodedMessages, err := d.outboxDecoder.Decode(decodedRows)
			parsed.RawData = nil
			decodedRows.Rows = nil
			if err != nil {
				return nil, err
			}
			for _, message := range decodedMessages {
				if rowCount+1 > d.maxRows {
					return nil, fmt.Errorf("outbox transaction exceeds TAILJET_MAX_TRANSACTION_ROWS (%d)", d.maxRows)
				}
				if rowBytes+message.Size() > d.maxBytes {
					return nil, fmt.Errorf("outbox transaction exceeds TAILJET_MAX_TRANSACTION_BYTES (%d)", d.maxBytes)
				}
				rowCount++
				rowBytes += message.Size()
				messages = append(messages, message)
			}

		default:
			if err := discardContext(ctx, reader, bodySize); err != nil {
				return nil, err
			}
		}
	}
}

func transactionPayload(data []byte) ([]byte, error) {
	compressionType := uint64(replication.NONE)
	for offset := 0; ; {
		if offset >= len(data) {
			return nil, fmt.Errorf("transaction payload has no header terminator")
		}
		fieldType := data[offset]
		offset++
		if fieldType == 0 {
			if compressionType != replication.ZSTD {
				return nil, fmt.Errorf("transaction payload compression type %d is unsupported", compressionType)
			}
			return data[offset:], nil
		}
		if offset >= len(data) {
			return nil, fmt.Errorf("transaction payload field %d has no length", fieldType)
		}
		fieldLength := int(data[offset])
		offset++
		if fieldLength > len(data)-offset {
			return nil, fmt.Errorf("transaction payload field %d exceeds payload length", fieldType)
		}
		if fieldType == 2 {
			compressionType = fixedLengthInt(data[offset : offset+fieldLength])
		}
		offset += fieldLength
	}
}

func readEmbeddedEvent(ctx context.Context, reader io.Reader, header []byte, bodySize int) ([]byte, error) {
	event := make([]byte, len(header)+bodySize)
	copy(event, header)
	if _, err := readFullContext(ctx, reader, event[len(header):]); err != nil {
		return nil, fmt.Errorf("read transaction payload event body: %w", err)
	}
	return event, nil
}

func readEmbeddedEventAfterPrefix(ctx context.Context, reader io.Reader, header, prefix []byte, remaining int) ([]byte, error) {
	event := make([]byte, len(header)+len(prefix)+remaining)
	copy(event, header)
	copy(event[len(header):], prefix)
	if _, err := readFullContext(ctx, reader, event[len(header)+len(prefix):]); err != nil {
		return nil, fmt.Errorf("read transaction payload event body: %w", err)
	}
	return event, nil
}

func discardContext(ctx context.Context, reader io.Reader, size int) error {
	buffer := make([]byte, min(size, transactionPayloadReadChunk))
	for size > 0 {
		chunkSize := min(size, len(buffer))
		if _, err := readFullContext(ctx, reader, buffer[:chunkSize]); err != nil {
			return fmt.Errorf("discard transaction payload event body: %w", err)
		}
		size -= chunkSize
	}
	return nil
}

func readFullContext(ctx context.Context, reader io.Reader, data []byte) (int, error) {
	read := 0
	for read < len(data) {
		if err := ctx.Err(); err != nil {
			return read, err
		}
		chunkSize := min(len(data)-read, transactionPayloadReadChunk)
		n, err := reader.Read(data[read : read+chunkSize])
		read += n
		if err != nil {
			if err == io.EOF && read == len(data) {
				return read, nil
			}
			if err == io.EOF && read > 0 {
				return read, io.ErrUnexpectedEOF
			}
			return read, err
		}
		if n == 0 {
			return read, io.ErrNoProgress
		}
	}
	return read, nil
}

func fixedLengthInt(data []byte) uint64 {
	var value uint64
	for i := len(data) - 1; i >= 0; i-- {
		value = value<<8 | uint64(data[i])
	}
	return value
}

func cloneGTID(gtid gomysql.GTIDSet) gomysql.GTIDSet {
	if gtid == nil {
		return nil
	}
	return gtid.Clone()
}

func isRowsEvent(eventType replication.EventType) bool {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT,
		replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1,
		replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1,
		replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		return true
	default:
		return false
	}
}

func isInsertRowsEvent(eventType replication.EventType) bool {
	return eventType == replication.WRITE_ROWS_EVENTv0 ||
		eventType == replication.WRITE_ROWS_EVENTv1 ||
		eventType == replication.WRITE_ROWS_EVENTv2 ||
		eventType == replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1
}
