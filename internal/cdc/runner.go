package cdc

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/zyno-io/tailjet/internal/config"
	"github.com/zyno-io/tailjet/internal/health"
	"github.com/zyno-io/tailjet/internal/outbox"
)

type MessagePublisher interface {
	Publish(context.Context, outbox.Message) error
}

type Runner struct {
	config    config.Config
	mysqlTLS  *tls.Config
	store     *outbox.Store
	publisher MessagePublisher
	health    *health.Tracker
	logger    *slog.Logger
	retry     chan struct{}
}

func NewRunner(
	cfg config.Config,
	mysqlTLS *tls.Config,
	store *outbox.Store,
	publisher MessagePublisher,
	tracker *health.Tracker,
	logger *slog.Logger,
) *Runner {
	return &Runner{
		config:    cfg,
		mysqlTLS:  mysqlTLS,
		store:     store,
		publisher: publisher,
		health:    tracker,
		logger:    logger,
		retry:     make(chan struct{}, 1),
	}
}

func (r *Runner) TriggerRetry() bool {
	select {
	case r.retry <- struct{}{}:
		r.health.RetryRequested()
		return true
	default:
		return false
	}
}

func (r *Runner) Run(ctx context.Context) error {
	prepared := false
	for ctx.Err() == nil {
		if !prepared {
			if err := r.prepare(ctx); err != nil {
				r.health.SetPhase("waiting-for-mysql", false, false, err)
				r.logger.Error("MySQL preparation failed", "error", err)
				if !wait(ctx, r.config.LeaderRetryInterval) {
					break
				}
				continue
			}
			prepared = true
		}

		leaderConn, acquired, err := r.store.AcquireLeader(ctx, r.config.LeaderLockName)
		if err != nil {
			prepared = false
			r.health.SetPhase("waiting-for-leader-lock", false, false, err)
			r.logger.Error("leader lock failed", "error", err)
			if !wait(ctx, r.config.LeaderRetryInterval) {
				break
			}
			continue
		}
		if !acquired {
			failedRows, countErr := r.store.CountFailed(ctx)
			if countErr != nil {
				prepared = false
				r.health.SetPhase("waiting-for-mysql", false, false, countErr)
				r.logger.Error("failed to count retained outbox rows", "error", countErr)
				if !wait(ctx, r.config.LeaderRetryInterval) {
					break
				}
				continue
			}
			r.health.SetFailedRows(failedRows)
			r.health.SetPhase("standby", true, false, nil)
			if !wait(ctx, r.config.LeaderRetryInterval) {
				break
			}
			continue
		}

		r.logger.Info("acquired MySQL leader lock", "lock", r.config.LeaderLockName)
		leaderCtx, cancelLeader := context.WithCancelCause(ctx)
		go monitorLeaderConnection(leaderCtx, leaderConn, cancelLeader)
		err = r.runLeader(leaderCtx)
		cancelLeader(nil)
		prepared = false

		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
		outbox.ReleaseLeader(releaseCtx, leaderConn, r.config.LeaderLockName)
		releaseCancel()
		if ctx.Err() != nil {
			break
		}
		if cause := context.Cause(leaderCtx); cause != nil && !errors.Is(cause, context.Canceled) {
			err = errors.Join(err, cause)
		}
		if err == nil {
			err = errors.New("leader loop stopped unexpectedly")
		}
		r.health.SetPhase("recovering", false, false, err)
		r.logger.Error("CDC leader stopped; retrying from the durable checkpoint", "error", err)
		if !wait(ctx, r.config.LeaderRetryInterval) {
			break
		}
	}
	return ctx.Err()
}

func (r *Runner) prepare(ctx context.Context) error {
	if err := r.store.EnsureSchema(ctx); err != nil {
		return err
	}
	if err := r.store.CheckPrerequisites(ctx); err != nil {
		return err
	}
	return nil
}

func (r *Runner) runLeader(ctx context.Context) error {
	columns, err := r.store.ColumnIndexes(ctx)
	if err != nil {
		return err
	}
	failedRows, err := r.store.CountFailed(ctx)
	if err != nil {
		return err
	}
	r.health.SetFailedRows(failedRows)
	position, exists, err := r.store.LoadPosition(ctx)
	if err != nil {
		return err
	}
	gtidEnabled, err := r.store.GTIDEnabled(ctx)
	if err != nil {
		return err
	}
	if exists && position.GTID == "" && gtidEnabled {
		r.logger.Info("migrating file-position checkpoint to GTID auto-positioning")
		exists = false
	}
	if exists && position.GTID != "" && !gtidEnabled {
		return errors.New("durable checkpoint uses GTIDs but MySQL gtid_mode is not ON")
	}
	if exists {
		available, err := r.store.PositionAvailable(ctx, position)
		if err != nil {
			return err
		}
		if !available {
			r.logger.Warn("checkpoint binlog was purged; recovering from remaining outbox rows", "binlog", position.Name, "position", position.Pos)
			exists = false
		}
	}
	if !exists {
		position, err = r.snapshot(ctx)
		if err != nil {
			return err
		}
		if gtidEnabled && position.GTID == "" {
			return errors.New("MySQL gtid_mode is ON but binary log status returned no executed GTID set")
		}
	}
	return r.stream(ctx, position, columns)
}

func (r *Runner) snapshot(ctx context.Context) (outbox.Position, error) {
	r.health.SetPhase("snapshotting", false, true, nil)
	position, err := r.store.MasterPosition(ctx)
	if err != nil {
		return outbox.Position{}, err
	}
	r.logger.Info("starting outbox snapshot", "binlog", position.Name, "position", position.Pos, "gtid", position.GTID != "")

	var afterID uint64
	for {
		messages, err := r.store.SnapshotBatch(ctx, afterID, r.config.SnapshotBatchSize)
		if err != nil {
			return outbox.Position{}, err
		}
		if len(messages) == 0 {
			break
		}
		result, err := r.attemptMessages(ctx, messages)
		if err != nil {
			return outbox.Position{}, err
		}
		afterID = messages[len(messages)-1].ID
		if err := r.store.CompleteSnapshotBatch(ctx, result.succeeded, result.failed); err != nil {
			return outbox.Position{}, err
		}
		r.recordApplied(result)
	}
	if err := r.store.SavePosition(ctx, position); err != nil {
		return outbox.Position{}, err
	}
	r.health.Checkpointed()
	r.logger.Info("outbox snapshot complete", "binlog", position.Name, "position", position.Pos, "gtid", position.GTID != "")
	return position, nil
}

func (r *Runner) stream(ctx context.Context, position outbox.Position, columns outbox.ColumnIndexes) error {
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:             r.config.MySQLServerID,
		Flavor:               r.config.MySQLFlavor,
		Host:                 r.config.MySQLHost,
		Port:                 r.config.MySQLPort,
		User:                 r.config.MySQLUser,
		Password:             r.config.MySQLPassword,
		Charset:              "utf8mb4",
		TLSConfig:            r.mysqlTLS,
		HeartbeatPeriod:      time.Second,
		ReadTimeout:          30 * time.Second,
		MaxReconnectAttempts: 0,
		VerifyChecksum:       true,
		Logger:               r.logger,
	})
	defer syncer.Close()

	var streamer *replication.BinlogStreamer
	var err error
	if position.GTID != "" {
		gtidSet, err := gomysql.ParseGTIDSet(r.config.MySQLFlavor, position.GTID)
		if err != nil {
			return fmt.Errorf("parse durable GTID checkpoint: %w", err)
		}
		streamer, err = syncer.StartSyncGTID(gtidSet)
		if err != nil {
			return fmt.Errorf("start MySQL replication from GTID checkpoint: %w", err)
		}
	} else {
		streamer, err = syncer.StartSync(gomysql.Position{Name: position.Name, Pos: position.Pos})
		if err != nil {
			return fmt.Errorf("start MySQL replication at %s:%d: %w", position.Name, position.Pos, err)
		}
	}
	r.health.SetPhase("streaming", true, true, nil)
	r.logger.Info("MySQL CDC stream started", "binlog", position.Name, "position", position.Pos, "checkpoint_mode", checkpointMode(position))

	processor := &streamProcessor{
		currentFile:        position.Name,
		currentGTID:        position.GTID,
		lastCheckpoint:     time.Now(),
		checkpointInterval: r.config.CheckpointInterval,
		maxRows:            r.config.MaxTransactionRows,
		maxBytes:           r.config.MaxTransactionBytes,
		decoder:            NewDecoder(r.config.MySQLDatabase, r.config.OutboxTable, columns),
		store:              r.store,
		publisher:          r.publisher,
		health:             r.health,
		logger:             r.logger,
		database:           r.config.MySQLDatabase,
		outboxTable:        r.config.OutboxTable,
	}
	streamContext, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	events := make(chan binlogResult)
	go func() {
		for {
			event, err := streamer.GetEvent(streamContext)
			select {
			case events <- binlogResult{event: event, err: err}:
			case <-streamContext.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-r.retry:
			if err := r.retryFailed(ctx); err != nil {
				return err
			}
		case result := <-events:
			if result.err != nil {
				if ctx.Err() != nil {
					return context.Cause(ctx)
				}
				return fmt.Errorf("read MySQL binlog event: %w", result.err)
			}
			if err := processor.process(ctx, result.event); err != nil {
				return err
			}
		}
	}
}

type binlogResult struct {
	event *replication.BinlogEvent
	err   error
}

type publishResult struct {
	succeeded []uint64
	failed    []outbox.PublishFailure
	published int
	retried   int
}

func (r *Runner) attemptMessages(ctx context.Context, messages []outbox.Message) (publishResult, error) {
	result := publishResult{
		succeeded: make([]uint64, 0, len(messages)),
		failed:    make([]outbox.PublishFailure, 0),
	}
	for _, message := range messages {
		err := r.publisher.Publish(ctx, message)
		if err == nil {
			result.succeeded = append(result.succeeded, message.ID)
			result.published++
			if message.AttemptCount > 0 {
				result.retried++
			}
			continue
		}
		if ctx.Err() != nil {
			return publishResult{}, context.Cause(ctx)
		}
		r.logger.Error("JetStream publish failed; retaining outbox row for retry",
			"row_id", message.ID,
			"subject", message.Subject,
			"message_id", message.StableMessageID(r.config.MySQLDatabase, r.config.OutboxTable),
			"attempt", message.AttemptCount+1,
			"error", err,
		)
		r.health.PublishFailed(message.AttemptCount == 0, err)
		result.failed = append(result.failed, outbox.PublishFailure{ID: message.ID, Error: err.Error()})
	}
	return result, nil
}

func (r *Runner) recordApplied(result publishResult) {
	if result.published > 0 {
		r.health.Published(result.published)
	}
	if result.retried > 0 {
		r.health.RetrySucceeded(result.retried)
	}
}

func (r *Runner) retryFailed(ctx context.Context) error {
	r.logger.Info("retrying failed outbox rows")
	var afterID uint64
	var attempted, succeeded int
	for {
		messages, err := r.store.FailedBatch(ctx, afterID, r.config.SnapshotBatchSize)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			break
		}
		attempted += len(messages)
		afterID = messages[len(messages)-1].ID
		result, err := r.attemptMessages(ctx, messages)
		if err != nil {
			return err
		}
		if err := r.store.CompleteRetryBatch(ctx, result.succeeded, result.failed); err != nil {
			return err
		}
		r.recordApplied(result)
		succeeded += result.published
	}
	r.logger.Info("failed outbox retry complete", "attempted", attempted, "succeeded", succeeded, "failed", attempted-succeeded)
	return nil
}

type streamProcessor struct {
	currentFile        string
	currentGTID        string
	pending            []outbox.Message
	pendingBytes       int64
	lastCheckpoint     time.Time
	checkpointInterval time.Duration
	maxRows            int
	maxBytes           int64
	decoder            *Decoder
	store              checkpointStore
	publisher          MessagePublisher
	health             *health.Tracker
	logger             *slog.Logger
	database           string
	outboxTable        string
}

type checkpointStore interface {
	Complete(context.Context, outbox.Position, []uint64, []outbox.PublishFailure) error
	SavePosition(context.Context, outbox.Position) error
}

func (p *streamProcessor) process(ctx context.Context, event *replication.BinlogEvent) error {
	switch value := event.Event.(type) {
	case *replication.RotateEvent:
		if len(p.pending) != 0 {
			return errors.New("binlog rotated with an open outbox transaction")
		}
		p.currentFile = string(value.NextLogName)
		return p.maybeCheckpoint(ctx, p.position(uint32(value.Position), nil))

	case *replication.RowsEvent:
		if value.Type() != replication.EnumRowsEventTypeInsert {
			return nil
		}
		return p.addRows(value)

	case *replication.XIDEvent:
		return p.commit(ctx, event.Header.LogPos, value.GSet)

	case *replication.QueryEvent:
		switch strings.ToUpper(strings.TrimSpace(string(value.Query))) {
		case "COMMIT":
			return p.commit(ctx, event.Header.LogPos, value.GSet)
		case "ROLLBACK":
			p.clearPending()
			return p.maybeCheckpoint(ctx, p.position(event.Header.LogPos, value.GSet))
		}

	case *replication.TransactionPayloadEvent:
		if len(p.pending) != 0 {
			return errors.New("compressed transaction began with pending outbox rows")
		}
		for _, nested := range value.Events {
			rows, ok := nested.Event.(*replication.RowsEvent)
			if !ok || rows.Type() != replication.EnumRowsEventTypeInsert {
				continue
			}
			if err := p.addRows(rows); err != nil {
				return err
			}
		}
		return p.commit(ctx, event.Header.LogPos, transactionPayloadGTID(value))
	}
	return nil
}

func (p *streamProcessor) addRows(event *replication.RowsEvent) error {
	messages, err := p.decoder.Decode(event)
	if err != nil {
		return err
	}
	for _, message := range messages {
		p.pending = append(p.pending, message)
		p.pendingBytes += message.Size()
		if len(p.pending) > p.maxRows {
			return fmt.Errorf("outbox transaction exceeds TAILJET_MAX_TRANSACTION_ROWS (%d)", p.maxRows)
		}
		if p.pendingBytes > p.maxBytes {
			return fmt.Errorf("outbox transaction exceeds TAILJET_MAX_TRANSACTION_BYTES (%d)", p.maxBytes)
		}
	}
	return nil
}

func (p *streamProcessor) commit(ctx context.Context, logPosition uint32, gtidSet gomysql.GTIDSet) error {
	if logPosition == 0 {
		return errors.New("transaction commit has a zero binlog position")
	}
	position := p.position(logPosition, gtidSet)
	if len(p.pending) == 0 {
		return p.maybeCheckpoint(ctx, position)
	}

	succeeded := make([]uint64, 0, len(p.pending))
	failed := make([]outbox.PublishFailure, 0)
	for _, message := range p.pending {
		err := p.publisher.Publish(ctx, message)
		if err == nil {
			succeeded = append(succeeded, message.ID)
			continue
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		p.logger.Error("JetStream publish failed; retaining outbox row for retry",
			"row_id", message.ID,
			"subject", message.Subject,
			"message_id", message.StableMessageID(p.database, p.outboxTable),
			"attempt", 1,
			"error", err,
		)
		p.health.PublishFailed(true, err)
		failed = append(failed, outbox.PublishFailure{ID: message.ID, Error: err.Error()})
	}
	if err := p.store.Complete(ctx, position, succeeded, failed); err != nil {
		return err
	}
	if len(succeeded) > 0 {
		p.health.Published(len(succeeded))
	}
	p.health.Checkpointed()
	p.lastCheckpoint = time.Now()
	p.clearPending()
	return nil
}

func (p *streamProcessor) position(logPosition uint32, gtidSet gomysql.GTIDSet) outbox.Position {
	if gtidSet != nil {
		p.currentGTID = gtidSet.String()
	}
	return outbox.Position{Name: p.currentFile, Pos: logPosition, GTID: p.currentGTID}
}

func transactionPayloadGTID(event *replication.TransactionPayloadEvent) gomysql.GTIDSet {
	for i := len(event.Events) - 1; i >= 0; i-- {
		switch value := event.Events[i].Event.(type) {
		case *replication.XIDEvent:
			if value.GSet != nil {
				return value.GSet
			}
		case *replication.QueryEvent:
			if value.GSet != nil {
				return value.GSet
			}
		}
	}
	return nil
}

func checkpointMode(position outbox.Position) string {
	if position.GTID != "" {
		return "gtid"
	}
	return "file-position"
}

func (p *streamProcessor) maybeCheckpoint(ctx context.Context, position outbox.Position) error {
	if time.Since(p.lastCheckpoint) < p.checkpointInterval {
		return nil
	}
	if err := p.store.SavePosition(ctx, position); err != nil {
		return err
	}
	p.health.Checkpointed()
	p.lastCheckpoint = time.Now()
	return nil
}

func (p *streamProcessor) clearPending() {
	p.pending = nil
	p.pendingBytes = 0
}

func monitorLeaderConnection(ctx context.Context, conn *sql.Conn, cancel context.CancelCauseFunc) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			err := conn.PingContext(pingCtx)
			pingCancel()
			if err != nil {
				cancel(fmt.Errorf("MySQL leader lock connection was lost: %w", err))
				return
			}
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
