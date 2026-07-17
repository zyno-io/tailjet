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
