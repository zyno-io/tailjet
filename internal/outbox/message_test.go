package outbox

import "testing"

func TestStableMessageID(t *testing.T) {
	message := Message{ID: 42}
	if got := message.StableMessageID("app", "tailjet_outbox"); got != "tailjet:app:tailjet_outbox:42" {
		t.Fatalf("generated ID = %q", got)
	}
	message.MessageID = "provided-id"
	if got := message.StableMessageID("app", "tailjet_outbox"); got != "provided-id" {
		t.Fatalf("provided ID = %q", got)
	}
}

func TestMessageSizeIncludesTTL(t *testing.T) {
	ttlSeconds := uint32(30)
	message := Message{
		Subject:    "events.example",
		Payload:    []byte("payload"),
		Headers:    []byte("headers"),
		MessageID:  "message-id",
		TTLSeconds: &ttlSeconds,
	}

	want := int64(len(message.Subject) + len(message.Payload) + len(message.Headers) + len(message.MessageID) + 4)
	if got := message.Size(); got != want {
		t.Fatalf("Size() = %d, want %d", got, want)
	}
}
