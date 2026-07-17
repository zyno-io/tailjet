package cdc

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/zyno-io/tailjet/internal/outbox"
)

func TestDecoderDecodesOwnedInsert(t *testing.T) {
	decoder := NewDecoder("app", "tailjet_outbox", outbox.ColumnIndexes{
		ID: 0, Subject: 1, Payload: 2, Headers: 3, MessageID: 4,
	})
	event := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("app"),
			Table:  []byte("tailjet_outbox"),
		},
		Rows: [][]any{{int64(7), []byte("records.changed"), []byte(`{"id":7}`), []byte(`{"X-Origin":"example"}`), nil}},
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
