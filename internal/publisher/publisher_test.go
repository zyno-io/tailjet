package publisher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zyno-io/tailjet/internal/outbox"
)

type disconnectedConnection struct{}

func (disconnectedConnection) IsConnected() bool { return false }

func TestPublishFailsImmediatelyWhenNATSIsDisconnected(t *testing.T) {
	publisher := New(nil, disconnectedConnection{}, "app", "tailjet_outbox", "EVENTS", time.Hour)
	started := time.Now()
	err := publisher.Publish(context.Background(), outbox.Message{ID: 7, Subject: "events.example"})
	if err == nil || !strings.Contains(err.Error(), "NATS is disconnected") {
		t.Fatalf("error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("disconnected publish took %s", elapsed)
	}
}

func TestParseHeaders(t *testing.T) {
	headers, err := parseHeaders([]byte(`{"Content-Type":"application/json","X-Tag":["one","two"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := headers.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := headers.Values("X-Tag"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("X-Tag = %#v", got)
	}
}

func TestParseHeadersRejectsReservedNATSHeaders(t *testing.T) {
	if _, err := parseHeaders([]byte(`{"nats-msg-id":"other"}`)); err == nil {
		t.Fatal("expected reserved header error")
	}
}

func TestValidateSubject(t *testing.T) {
	for _, subject := range []string{"events.record-changed", "records.changed"} {
		if err := validateSubject(subject); err != nil {
			t.Fatalf("%q: %v", subject, err)
		}
	}
	for _, subject := range []string{"", "records.*", "records..changed", "records changed"} {
		if err := validateSubject(subject); err == nil {
			t.Fatalf("%q unexpectedly valid", subject)
		}
	}
}
