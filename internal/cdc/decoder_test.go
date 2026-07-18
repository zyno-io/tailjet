package cdc

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/zyno-io/tailjet/internal/outbox"
)

func TestDecoderDecodesOwnedInsert(t *testing.T) {
	decoder := NewDecoder("app", "tailjet_outbox", outbox.ColumnIndexes{
		ID: 0, Subject: 1, Payload: 2, Headers: 3, MessageID: 4, TTLSeconds: 5,
	})
	event := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("app"),
			Table:  []byte("tailjet_outbox"),
		},
		Rows: [][]any{{int64(7), []byte("records.changed"), []byte(`{"id":7}`), []byte(`{"X-Origin":"example"}`), nil, uint64(30)}},
	}

	messages, err := decoder.Decode(event)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %d", len(messages))
	}
	if messages[0].ID != 7 || messages[0].Subject != "records.changed" || string(messages[0].Payload) != `{"id":7}` {
		t.Fatalf("message = %#v", messages[0])
	}
	if messages[0].TTLSeconds == nil || *messages[0].TTLSeconds != 30 {
		t.Fatalf("ttl_seconds = %#v", messages[0].TTLSeconds)
	}
}

func TestDecoderDecodesNullTTL(t *testing.T) {
	decoder := NewDecoder("app", "tailjet_outbox", outbox.ColumnIndexes{
		ID: 0, Subject: 1, Payload: 2, Headers: 3, MessageID: 4, TTLSeconds: 5,
	})
	event := &replication.RowsEvent{
		Table: &replication.TableMapEvent{Schema: []byte("app"), Table: []byte("tailjet_outbox")},
		Rows:  [][]any{{uint64(8), "records.changed", "{}", nil, nil, nil}},
	}

	messages, err := decoder.Decode(event)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].TTLSeconds != nil {
		t.Fatalf("messages = %#v", messages)
	}
}

func TestDecoderRejectsTTLOutsideMySQLUnsignedInt(t *testing.T) {
	decoder := NewDecoder("app", "tailjet_outbox", outbox.ColumnIndexes{
		ID: 0, Subject: 1, Payload: 2, Headers: 3, MessageID: 4, TTLSeconds: 5,
	})
	event := &replication.RowsEvent{
		Table: &replication.TableMapEvent{Schema: []byte("app"), Table: []byte("tailjet_outbox")},
		Rows:  [][]any{{uint64(8), "records.changed", "{}", nil, nil, uint64(^uint32(0)) + 1}},
	}

	if _, err := decoder.Decode(event); err == nil {
		t.Fatal("expected ttl_seconds range error")
	}
}

func TestDecoderIgnoresOtherTables(t *testing.T) {
	decoder := NewDecoder("app", "tailjet_outbox", outbox.ColumnIndexes{})
	event := &replication.RowsEvent{
		Table: &replication.TableMapEvent{Schema: []byte("app"), Table: []byte("other_table")},
		Rows:  [][]any{{int64(7)}},
	}
	messages, err := decoder.Decode(event)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 {
		t.Fatalf("messages = %#v", messages)
	}
}
