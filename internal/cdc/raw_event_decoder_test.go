package cdc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"

	"github.com/zyno-io/tailjet/internal/outbox"
)

func TestRawEventDecoderStreamsUnrelatedTransactionPayloadRows(t *testing.T) {
	decoder, err := newRawEventDecoder(
		gomysql.MySQLFlavor,
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7",
		testOutboxDecoder(),
		10000,
		64<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.Decode(context.Background(), testFormatDescriptionEvent()); err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.Decode(context.Background(), testGTIDEvent(8)); err != nil {
		t.Fatal(err)
	}

	// This body is intentionally not a valid rows event. It represents the large
	// unrelated row image that must be streamed past rather than parsed or held.
	unrelatedRows := append(testTableID(11), bytes.Repeat([]byte{0}, 512*1024)...)
	payload := bytes.Join([][]byte{
		testTableMapEvent(11, "app", "sales_financial_transactions_items"),
		testEmbeddedEvent(replication.WRITE_ROWS_EVENTv2, unrelatedRows),
		testTableMapEvent(12, "app", "tailjet_outbox"),
		testOutboxRowsEvent(12),
	}, nil)

	encoded, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	compressed := encoded.EncodeAll(payload, nil)
	decoded, err := decoder.Decode(context.Background(), testTransactionPayloadEvent(compressed, len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	payloadEvent, ok := decoded.Event.(*decodedTransactionPayloadEvent)
	if !ok {
		t.Fatalf("event = %T", decoded.Event)
	}
	if len(payloadEvent.messages) != 1 {
		t.Fatalf("retained payload messages = %d, want 1", len(payloadEvent.messages))
	}
	if payloadEvent.gtid == nil || payloadEvent.gtid.String() != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-8" {
		t.Fatalf("checkpoint GTID = %#v", payloadEvent.gtid)
	}
}

func TestRawEventDecoderDoesNotAttachGTIDsInFilePositionMode(t *testing.T) {
	decoder, err := newRawEventDecoder(gomysql.MySQLFlavor, "", testOutboxDecoder(), 10000, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := decoder.Decode(ctx, testFormatDescriptionEvent()); err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.Decode(ctx, testGTIDEvent(8)); err != nil {
		t.Fatal(err)
	}

	encoded, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	event, err := decoder.Decode(ctx, testTransactionPayloadEvent(encoded.EncodeAll(nil, nil), 0))
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := event.Event.(*decodedTransactionPayloadEvent)
	if !ok || len(payload.messages) != 0 {
		t.Fatalf("event = %#v", event.Event)
	}
	if payload.gtid != nil {
		t.Fatalf("file-position GTID = %#v", payload.gtid)
	}
}

func TestRawEventDecoderAppliesPayloadTransactionLimitsBeforeRetainingRows(t *testing.T) {
	payload := bytes.Join([][]byte{
		testTableMapEvent(12, "app", "tailjet_outbox"),
		testOutboxRowsEvent(12),
	}, nil)
	encoded, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	compressed := encoded.EncodeAll(payload, nil)

	for _, test := range []struct {
		name     string
		maxRows  int
		maxBytes int64
		want     string
	}{
		{name: "exact message size", maxRows: 1, maxBytes: 12},
		{name: "bytes", maxRows: 10000, maxBytes: 1, want: "TAILJET_MAX_TRANSACTION_BYTES"},
		{name: "rows", maxRows: 0, maxBytes: 64 << 20, want: "TAILJET_MAX_TRANSACTION_ROWS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			decoder, err := newRawEventDecoder(gomysql.MySQLFlavor, "", testOutboxDecoder(), test.maxRows, test.maxBytes)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if _, err := decoder.Decode(ctx, testFormatDescriptionEvent()); err != nil {
				t.Fatal(err)
			}
			_, err = decoder.Decode(ctx, testTransactionPayloadEvent(compressed, len(payload)))
			if test.want == "" {
				if err != nil {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err == nil || !bytes.Contains([]byte(err.Error()), []byte(test.want)) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRawEventDecoderAcceptsMySQLMaximumZstdWindow(t *testing.T) {
	payload := testTableMapEvent(12, "app", "tailjet_outbox")
	// 0x88 encodes a 128 MiB Zstd window. A raw block keeps the fixture small
	// while exercising the frame-header window check.
	compressed := testZstdRawFrame(0x88, payload)
	belowMaximum, err := zstd.NewReader(bytes.NewReader(compressed), zstd.WithDecoderMaxWindow(64<<20))
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(belowMaximum)
	belowMaximum.Close()
	if err == nil {
		t.Fatal("test payload did not require its declared 128 MiB Zstd window")
	}

	decoder, err := newRawEventDecoder(gomysql.MySQLFlavor, "", testOutboxDecoder(), 10000, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := decoder.Decode(ctx, testFormatDescriptionEvent()); err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.Decode(ctx, testTransactionPayloadEvent(compressed, len(payload))); err != nil {
		t.Fatalf("decode payload at maximum supported Zstd window: %v", err)
	}
}

func TestRawEventDecoderChecksCancelledContext(t *testing.T) {
	decoder, err := newRawEventDecoder(gomysql.MySQLFlavor, "", testOutboxDecoder(), 10000, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := decoder.Decode(ctx, testFormatDescriptionEvent()); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestRawEventDecoderRejectsTruncatedEmbeddedHeader(t *testing.T) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	compressed := encoder.EncodeAll([]byte{0, 1, 2}, nil)

	decoder, err := newRawEventDecoder(gomysql.MySQLFlavor, "", testOutboxDecoder(), 10000, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := decoder.Decode(ctx, testFormatDescriptionEvent()); err != nil {
		t.Fatal(err)
	}
	event, err := decoder.Decode(ctx, testTransactionPayloadEvent(compressed, 3))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v, want unexpected EOF", err)
	}
	if event != nil {
		t.Fatalf("emitted compact payload event %#v for truncated input", event)
	}
}

func TestDiscardContextStopsMidBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelAfterFirstRead{reader: bytes.NewReader(bytes.Repeat([]byte{1}, transactionPayloadReadChunk*2)), cancel: cancel}
	err := discardContext(ctx, reader, transactionPayloadReadChunk*2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if reader.reads != 1 {
		t.Fatalf("reads = %d, want 1", reader.reads)
	}
}

type cancelAfterFirstRead struct {
	reader io.Reader
	cancel func()
	reads  int
}

func (r *cancelAfterFirstRead) Read(data []byte) (int, error) {
	r.reads++
	n, err := r.reader.Read(data)
	if r.reads == 1 {
		r.cancel()
	}
	return n, err
}

func testFormatDescriptionEvent() *replication.BinlogEvent {
	// The parser only needs the table-map header length to distinguish 4- and
	// 6-byte table IDs. The rest of this minimal format description is unused.
	body := make([]byte, 2+50+4+1+40+5)
	binary.LittleEndian.PutUint16(body, 4)
	copy(body[2:], "8.0.36")
	body[56] = replication.EventHeaderSize
	body[57+int(replication.TABLE_MAP_EVENT)-1] = 8
	body[len(body)-5] = byte(replication.BINLOG_CHECKSUM_ALG_OFF)
	return testOuterEvent(replication.FORMAT_DESCRIPTION_EVENT, body)
}

func testTransactionPayloadEvent(compressed []byte, uncompressedSize int) *replication.BinlogEvent {
	body := make([]byte, 0, len(compressed)+16)
	body = append(body, 1, 4)
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(compressed)))
	body = append(body, size[:]...)
	body = append(body, 2, 1, 0) // ZSTD
	body = append(body, 3, 4)
	binary.LittleEndian.PutUint32(size[:], uint32(uncompressedSize))
	body = append(body, size[:]...)
	body = append(body, 0)
	body = append(body, compressed...)
	return testOuterEvent(replication.TRANSACTION_PAYLOAD_EVENT, body)
}

func testGTIDEvent(gno uint64) *replication.BinlogEvent {
	body := []byte{
		1,
		0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1,
		0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62,
	}
	var number [8]byte
	binary.LittleEndian.PutUint64(number[:], gno)
	body = append(body, number[:]...)
	return testOuterEvent(replication.GTID_EVENT, body)
}

func testTableMapEvent(tableID uint64, schema, table string) []byte {
	body := append(testTableID(tableID), 0, 0)
	body = append(body, byte(len(schema)))
	body = append(body, schema...)
	body = append(body, 0)
	body = append(body, byte(len(table)))
	body = append(body, table...)
	body = append(body, 0)
	body = append(body, 1, gomysql.MYSQL_TYPE_VAR_STRING, 2, 0xff, 0, 0)
	return testEmbeddedEvent(replication.TABLE_MAP_EVENT, body)
}

func testOutboxRowsEvent(tableID uint64) []byte {
	body := append(testTableID(tableID), 0, 0, 2, 0, 1, 1, 0, 2, '4', '2')
	return testEmbeddedEvent(replication.WRITE_ROWS_EVENTv2, body)
}

func testOutboxDecoder() *Decoder {
	return NewDecoder("app", "tailjet_outbox", outbox.ColumnIndexes{})
}

func testTableID(id uint64) []byte {
	data := make([]byte, 6)
	for i := range data {
		data[i] = byte(id >> (8 * i))
	}
	return data
}

func testOuterEvent(eventType replication.EventType, body []byte) *replication.BinlogEvent {
	raw := testEmbeddedEvent(eventType, body)
	header := &replication.EventHeader{EventType: eventType, EventSize: uint32(len(raw)), LogPos: uint32(len(raw) + 4)}
	return &replication.BinlogEvent{RawData: raw, Header: header}
}

func testEmbeddedEvent(eventType replication.EventType, body []byte) []byte {
	event := make([]byte, replication.EventHeaderSize+len(body))
	event[4] = byte(eventType)
	binary.LittleEndian.PutUint32(event[9:], uint32(len(event)))
	copy(event[replication.EventHeaderSize:], body)
	return event
}

func testZstdRawFrame(windowDescriptor byte, payload []byte) []byte {
	if len(payload) > 0x1fffff {
		panic("test payload exceeds a Zstd raw block")
	}
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0, windowDescriptor}
	blockHeader := uint32(len(payload))<<3 | 1 // last raw block
	frame = append(frame, byte(blockHeader), byte(blockHeader>>8), byte(blockHeader>>16))
	return append(frame, payload...)
}
