package cdc

import (
	"fmt"
	"strconv"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/zyno-io/tailjet/internal/outbox"
)

type Decoder struct {
	database string
	table    string
	columns  outbox.ColumnIndexes
}

func NewDecoder(database, table string, columns outbox.ColumnIndexes) *Decoder {
	return &Decoder{database: database, table: table, columns: columns}
}

// Decode decodes a WRITE_ROWS event. The caller is responsible for filtering
// the binlog event type before passing it here.
func (d *Decoder) Decode(event *replication.RowsEvent) ([]outbox.Message, error) {
	if event.Table == nil || string(event.Table.Schema) != d.database || string(event.Table.Table) != d.table {
		return nil, nil
	}

	messages := make([]outbox.Message, 0, len(event.Rows))
	for _, row := range event.Rows {
		message, err := d.decodeRow(row)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func (d *Decoder) decodeRow(row []any) (outbox.Message, error) {
	maxIndex := max(d.columns.ID, d.columns.Subject, d.columns.Payload, d.columns.Headers, d.columns.MessageID, d.columns.TTLSeconds)
	if len(row) <= maxIndex {
		return outbox.Message{}, fmt.Errorf("outbox binlog row has %d columns; expected index %d", len(row), maxIndex)
	}

	id, err := uint64Value(row[d.columns.ID])
	if err != nil {
		return outbox.Message{}, fmt.Errorf("decode outbox id: %w", err)
	}
	if id == 0 {
		return outbox.Message{}, fmt.Errorf("decode outbox id: value is zero")
	}
	subject, err := stringValue(row[d.columns.Subject], false)
	if err != nil {
		return outbox.Message{}, fmt.Errorf("decode outbox row %d subject: %w", id, err)
	}
	payload, err := bytesValue(row[d.columns.Payload], false)
	if err != nil {
		return outbox.Message{}, fmt.Errorf("decode outbox row %d payload: %w", id, err)
	}
	headers, err := bytesValue(row[d.columns.Headers], true)
	if err != nil {
		return outbox.Message{}, fmt.Errorf("decode outbox row %d headers: %w", id, err)
	}
	messageID, err := stringValue(row[d.columns.MessageID], true)
	if err != nil {
		return outbox.Message{}, fmt.Errorf("decode outbox row %d message_id: %w", id, err)
	}
	ttlSeconds, err := optionalUint32Value(row[d.columns.TTLSeconds])
	if err != nil {
		return outbox.Message{}, fmt.Errorf("decode outbox row %d ttl_seconds: %w", id, err)
	}
	return outbox.Message{
		ID:         id,
		Subject:    subject,
		Payload:    payload,
		Headers:    headers,
		MessageID:  messageID,
		TTLSeconds: ttlSeconds,
	}, nil
}

func optionalUint32Value(value any) (*uint32, error) {
	if value == nil {
		return nil, nil
	}
	parsed, err := uint64Value(value)
	if err != nil {
		return nil, err
	}
	if parsed > uint64(^uint32(0)) {
		return nil, fmt.Errorf("value %d exceeds INT UNSIGNED", parsed)
	}
	result := uint32(parsed)
	return &result, nil
}

func uint64Value(value any) (uint64, error) {
	switch value := value.(type) {
	case uint64:
		return value, nil
	case uint32:
		return uint64(value), nil
	case uint:
		return uint64(value), nil
	case int64:
		if value < 0 {
			return 0, fmt.Errorf("negative value %d", value)
		}
		return uint64(value), nil
	case int32:
		if value < 0 {
			return 0, fmt.Errorf("negative value %d", value)
		}
		return uint64(value), nil
	case int:
		if value < 0 {
			return 0, fmt.Errorf("negative value %d", value)
		}
		return uint64(value), nil
	case []byte:
		return strconv.ParseUint(string(value), 10, 64)
	case string:
		return strconv.ParseUint(value, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected %T", value)
	}
}

func stringValue(value any, nullable bool) (string, error) {
	bytes, err := bytesValue(value, nullable)
	return string(bytes), err
}

func bytesValue(value any, nullable bool) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		if nullable {
			return nil, nil
		}
		return nil, fmt.Errorf("unexpected NULL")
	case []byte:
		return append([]byte(nil), value...), nil
	case string:
		return []byte(value), nil
	default:
		return nil, fmt.Errorf("unexpected %T", value)
	}
}
