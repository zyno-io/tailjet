package cdc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/zyno-io/tailjet/internal/config"
	"github.com/zyno-io/tailjet/internal/health"
	"github.com/zyno-io/tailjet/internal/outbox"
)

type recordingPublisher struct {
	failures map[uint64]error
	calls    []uint64
}

func (p *recordingPublisher) Publish(_ context.Context, message outbox.Message) error {
	p.calls = append(p.calls, message.ID)
	return p.failures[message.ID]
}

type recordingCheckpointStore struct {
	position  outbox.Position
	succeeded []uint64
	failed    []outbox.PublishFailure
	saves     []outbox.Position
}

func (s *recordingCheckpointStore) Complete(
	_ context.Context,
	position outbox.Position,
	succeeded []uint64,
	failed []outbox.PublishFailure,
) error {
	s.position = position
	s.succeeded = append([]uint64(nil), succeeded...)
	s.failed = append([]outbox.PublishFailure(nil), failed...)
	return nil
}

func (s *recordingCheckpointStore) SavePosition(_ context.Context, position outbox.Position) error {
	s.saves = append(s.saves, position)
	return nil
}

func TestAttemptMessagesSkipsPublishFailures(t *testing.T) {
	publisher := &recordingPublisher{failures: map[uint64]error{1: errors.New("missing stream")}}
	tracker := health.NewTracker()
	runner := &Runner{
		config:    config.Config{MySQLDatabase: "app", OutboxTable: "tailjet_outbox"},
		publisher: publisher,
		health:    tracker,
		logger:    testLogger(),
	}

	result, err := runner.attemptMessages(context.Background(), []outbox.Message{
		{ID: 1, Subject: "unconfigured.example"},
		{ID: 2, Subject: "events.example"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(result.succeeded, []uint64{2}) || len(result.failed) != 1 || result.failed[0].ID != 1 {
		t.Fatalf("result = %#v", result)
	}
	if !slices.Equal(publisher.calls, []uint64{1, 2}) {
		t.Fatalf("publish calls = %#v", publisher.calls)
	}
	status := tracker.Snapshot()
	if status.FailedRows != 1 || status.PublishErrorsTotal != 1 {
		t.Fatalf("status = %#v", status)
	}
}

func TestCommitCheckpointsPastFailedRows(t *testing.T) {
	publisher := &recordingPublisher{failures: map[uint64]error{1: errors.New("missing stream")}}
	store := &recordingCheckpointStore{}
	tracker := health.NewTracker()
	processor := &streamProcessor{
		currentFile:        "mysql-bin.000001",
		pending:            []outbox.Message{{ID: 1, Subject: "unconfigured.example"}, {ID: 2, Subject: "events.example"}},
		pendingBytes:       32,
		lastCheckpoint:     time.Now(),
		checkpointInterval: time.Second,
		store:              store,
		publisher:          publisher,
		health:             tracker,
		logger:             testLogger(),
		database:           "app",
		outboxTable:        "tailjet_outbox",
	}

	if err := processor.commit(context.Background(), 123); err != nil {
		t.Fatal(err)
	}
	if store.position != (outbox.Position{Name: "mysql-bin.000001", Pos: 123}) {
		t.Fatalf("position = %#v", store.position)
	}
	if !slices.Equal(store.succeeded, []uint64{2}) || len(store.failed) != 1 || store.failed[0].ID != 1 {
		t.Fatalf("stored result = succeeded %#v, failed %#v", store.succeeded, store.failed)
	}
	if len(processor.pending) != 0 || processor.pendingBytes != 0 {
		t.Fatalf("pending rows were not cleared: %#v", processor.pending)
	}
	status := tracker.Snapshot()
	if status.PublishedMessages != 1 || status.FailedRows != 1 || status.PublishErrorsTotal != 1 || status.LastCheckpointAt == nil {
		t.Fatalf("status = %#v", status)
	}
}

func TestCancelledPublishDoesNotAdvanceCheckpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	publisher := &recordingPublisher{failures: map[uint64]error{1: context.Canceled}}
	store := &recordingCheckpointStore{}
	processor := &streamProcessor{
		currentFile: "mysql-bin.000001",
		pending:     []outbox.Message{{ID: 1, Subject: "events.example"}},
		store:       store,
		publisher:   publisher,
		health:      health.NewTracker(),
		logger:      testLogger(),
	}

	if err := processor.commit(ctx, 123); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if store.position != (outbox.Position{}) {
		t.Fatalf("checkpoint advanced to %#v", store.position)
	}
	if len(processor.pending) != 1 {
		t.Fatal("pending row was cleared")
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
