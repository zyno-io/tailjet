package outbox

import "fmt"

type Message struct {
	ID           uint64
	Subject      string
	Payload      []byte
	Headers      []byte
	MessageID    string
	AttemptCount uint32
}

type PublishFailure struct {
	ID    uint64
	Error string
}

func (m Message) StableMessageID(database, table string) string {
	if m.MessageID != "" {
		return m.MessageID
	}
	return fmt.Sprintf("tailjet:%s:%s:%d", database, table, m.ID)
}

func (m Message) Size() int64 {
	return int64(len(m.Subject) + len(m.Payload) + len(m.Headers) + len(m.MessageID))
}

type Position struct {
	Name string
	Pos  uint32
	GTID string
}

type ColumnIndexes struct {
	ID        int
	Subject   int
	Payload   int
	Headers   int
	MessageID int
}
