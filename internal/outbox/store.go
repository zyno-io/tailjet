package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type Store struct {
	db           *sql.DB
	database     string
	outboxTable  string
	stateTable   string
	consumerName string
}

func NewStore(db *sql.DB, database, outboxTable, stateTable, consumerName string) *Store {
	return &Store{
		db:           db,
		database:     database,
		outboxTable:  outboxTable,
		stateTable:   stateTable,
		consumerName: consumerName,
	}
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	outboxDDL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    subject VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload LONGBLOB NOT NULL,
    headers JSON NULL,
    message_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ttl_seconds INT UNSIGNED NULL,
    attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP(6) NULL,
    last_error TEXT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_tailjet_message_id (message_id),
    CHECK (
        CHAR_LENGTH(subject) > 0
        AND subject NOT REGEXP '[[:space:]*>]'
        AND subject NOT LIKE '.%%'
        AND subject NOT LIKE '%%.'
        AND subject NOT LIKE '%%..%%'
    ),
    CHECK (
        headers IS NULL OR JSON_TYPE(headers) = 'OBJECT'
    ),
    CHECK (
        ttl_seconds IS NULL OR ttl_seconds > 0
    )
) ENGINE=InnoDB`, s.qualified(s.outboxTable))
	if _, err := s.db.ExecContext(ctx, outboxDDL); err != nil {
		return fmt.Errorf("create outbox table: %w", err)
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{"ttl_seconds", "INT UNSIGNED NULL AFTER message_id"},
		{"attempt_count", "INT UNSIGNED NOT NULL DEFAULT 0 AFTER ttl_seconds"},
		{"last_attempt_at", "TIMESTAMP(6) NULL AFTER attempt_count"},
		{"last_error", "TEXT NULL AFTER last_attempt_at"},
	} {
		if err := s.ensureColumn(ctx, s.outboxTable, column.name, column.definition); err != nil {
			return err
		}
	}

	stateDDL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    consumer_name VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    binlog_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    binlog_position BIGINT UNSIGNED NOT NULL,
    gtid_set LONGTEXT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (consumer_name)
) ENGINE=InnoDB`, s.qualified(s.stateTable))
	if _, err := s.db.ExecContext(ctx, stateDDL); err != nil {
		return fmt.Errorf("create checkpoint table: %w", err)
	}
	if err := s.ensureColumn(ctx, s.stateTable, "gtid_set", "LONGTEXT NULL AFTER binlog_position"); err != nil {
		return err
	}
	if err := s.validateSchema(ctx); err != nil {
		return err
	}
	return nil
}

type columnDefinition struct {
	dataType   string
	columnType string
	nullable   bool
	extra      string
}

func (s *Store) validateSchema(ctx context.Context) error {
	for _, table := range []string{s.outboxTable, s.stateTable} {
		var engine sql.NullString
		err := s.db.QueryRowContext(ctx, `
SELECT ENGINE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, s.database, table).Scan(&engine)
		if err != nil {
			return fmt.Errorf("read storage engine for %s: %w", s.qualified(table), err)
		}
		if !engine.Valid || !strings.EqualFold(engine.String, "InnoDB") {
			return fmt.Errorf("table %s must use InnoDB, found %q", s.qualified(table), engine.String)
		}
	}

	outboxColumns, err := s.readColumnDefinitions(ctx, s.outboxTable)
	if err != nil {
		return err
	}
	checks := []struct {
		name  string
		valid func(columnDefinition) bool
	}{
		{"id", func(c columnDefinition) bool {
			return c.dataType == "bigint" && strings.Contains(c.columnType, "unsigned") && !c.nullable && strings.Contains(c.extra, "auto_increment")
		}},
		{"subject", func(c columnDefinition) bool { return c.dataType == "varchar" && !c.nullable }},
		{"payload", func(c columnDefinition) bool { return c.dataType == "longblob" && !c.nullable }},
		{"headers", func(c columnDefinition) bool { return (c.dataType == "json" || c.dataType == "longtext") && c.nullable }},
		{"message_id", func(c columnDefinition) bool { return c.dataType == "varchar" && c.nullable }},
		{"ttl_seconds", func(c columnDefinition) bool {
			return c.dataType == "int" && strings.Contains(c.columnType, "unsigned") && c.nullable
		}},
		{"attempt_count", func(c columnDefinition) bool {
			return c.dataType == "int" && strings.Contains(c.columnType, "unsigned") && !c.nullable
		}},
		{"last_attempt_at", func(c columnDefinition) bool {
			return (c.dataType == "timestamp" || c.dataType == "datetime") && c.nullable
		}},
		{"last_error", func(c columnDefinition) bool { return c.dataType == "text" && c.nullable }},
		{"created_at", func(c columnDefinition) bool {
			return (c.dataType == "timestamp" || c.dataType == "datetime") && !c.nullable
		}},
	}
	if err := validateColumns(s.qualified(s.outboxTable), outboxColumns, checks); err != nil {
		return err
	}
	if err := s.validateIndexes(ctx, s.outboxTable, "id", "message_id"); err != nil {
		return err
	}

	stateColumns, err := s.readColumnDefinitions(ctx, s.stateTable)
	if err != nil {
		return err
	}
	stateChecks := []struct {
		name  string
		valid func(columnDefinition) bool
	}{
		{"consumer_name", func(c columnDefinition) bool { return c.dataType == "varchar" && !c.nullable }},
		{"binlog_name", func(c columnDefinition) bool { return c.dataType == "varchar" && !c.nullable }},
		{"binlog_position", func(c columnDefinition) bool {
			return c.dataType == "bigint" && strings.Contains(c.columnType, "unsigned") && !c.nullable
		}},
		{"gtid_set", func(c columnDefinition) bool { return c.dataType == "longtext" && c.nullable }},
		{"updated_at", func(c columnDefinition) bool {
			return (c.dataType == "timestamp" || c.dataType == "datetime") && !c.nullable
		}},
	}
	if err := validateColumns(s.qualified(s.stateTable), stateColumns, stateChecks); err != nil {
		return err
	}
	return s.validateIndexes(ctx, s.stateTable, "consumer_name", "")
}

func (s *Store) readColumnDefinitions(ctx context.Context, table string) (map[string]columnDefinition, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, EXTRA
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, s.database, table)
	if err != nil {
		return nil, fmt.Errorf("read schema for %s: %w", s.qualified(table), err)
	}
	defer rows.Close()

	columns := make(map[string]columnDefinition)
	for rows.Next() {
		var name, dataType, columnType, nullable, extra string
		if err := rows.Scan(&name, &dataType, &columnType, &nullable, &extra); err != nil {
			return nil, fmt.Errorf("scan schema for %s: %w", s.qualified(table), err)
		}
		columns[strings.ToLower(name)] = columnDefinition{
			dataType:   strings.ToLower(dataType),
			columnType: strings.ToLower(columnType),
			nullable:   strings.EqualFold(nullable, "YES"),
			extra:      strings.ToLower(extra),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema for %s: %w", s.qualified(table), err)
	}
	return columns, nil
}

func validateColumns(
	table string,
	columns map[string]columnDefinition,
	checks []struct {
		name  string
		valid func(columnDefinition) bool
	},
) error {
	for _, check := range checks {
		column, ok := columns[check.name]
		if !ok {
			return fmt.Errorf("table %s is missing required column %q", table, check.name)
		}
		if !check.valid(column) {
			return fmt.Errorf("column %s.%s is incompatible with Tailjet's schema contract", table, check.name)
		}
	}
	return nil
}

func (s *Store) validateIndexes(ctx context.Context, table, primaryColumn, uniqueColumn string) error {
	rows, err := s.db.QueryContext(ctx, `
SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY INDEX_NAME, SEQ_IN_INDEX`, s.database, table)
	if err != nil {
		return fmt.Errorf("read indexes for %s: %w", s.qualified(table), err)
	}
	defer rows.Close()

	type indexDefinition struct {
		unique  bool
		columns []string
	}
	indexes := make(map[string]indexDefinition)
	for rows.Next() {
		var name string
		var column sql.NullString
		var nonUnique, sequence int
		if err := rows.Scan(&name, &nonUnique, &sequence, &column); err != nil {
			return fmt.Errorf("scan indexes for %s: %w", s.qualified(table), err)
		}
		index := indexes[name]
		index.unique = nonUnique == 0
		if column.Valid {
			index.columns = append(index.columns, strings.ToLower(column.String))
		}
		indexes[name] = index
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate indexes for %s: %w", s.qualified(table), err)
	}
	primary, ok := indexes["PRIMARY"]
	if !ok || len(primary.columns) != 1 || primary.columns[0] != primaryColumn {
		return fmt.Errorf("table %s must have PRIMARY KEY (%s)", s.qualified(table), primaryColumn)
	}
	if uniqueColumn == "" {
		return nil
	}
	for _, index := range indexes {
		if index.unique && len(index.columns) == 1 && index.columns[0] == uniqueColumn {
			return nil
		}
	}
	return fmt.Errorf("table %s must have a unique index on %s", s.qualified(table), uniqueColumn)
}

func (s *Store) ensureColumn(ctx context.Context, table, column, definition string) error {
	var count int
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`, s.database, table, column).Scan(&count)
	if err != nil {
		return fmt.Errorf("check column %s.%s: %w", table, column, err)
	}
	if count > 0 {
		return nil
	}
	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN `%s` %s", s.qualified(table), column, definition)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1060 {
			return nil
		}
		return fmt.Errorf("add column %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) CheckPrerequisites(ctx context.Context) error {
	var logBin, format, rowImage string
	err := s.db.QueryRowContext(ctx, `
SELECT CAST(@@GLOBAL.log_bin AS CHAR), @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image`).Scan(&logBin, &format, &rowImage)
	if err != nil {
		return fmt.Errorf("read MySQL binlog settings: %w", err)
	}
	if logBin != "1" && !strings.EqualFold(logBin, "ON") {
		return errors.New("MySQL binary logging is disabled; set log_bin=ON")
	}
	if !strings.EqualFold(format, "ROW") {
		return fmt.Errorf("MySQL binlog_format is %q; Tailjet requires ROW", format)
	}
	if !strings.EqualFold(rowImage, "FULL") {
		return fmt.Errorf("MySQL binlog_row_image is %q; Tailjet requires FULL", rowImage)
	}
	return nil
}

func (s *Store) GTIDEnabled(ctx context.Context) (bool, error) {
	var mode string
	if err := s.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil {
		return false, fmt.Errorf("read MySQL GTID mode: %w", err)
	}
	return strings.EqualFold(mode, "ON"), nil
}

func (s *Store) ColumnIndexes(ctx context.Context) (ColumnIndexes, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT COLUMN_NAME, ORDINAL_POSITION
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION`, s.database, s.outboxTable)
	if err != nil {
		return ColumnIndexes{}, fmt.Errorf("read outbox columns: %w", err)
	}
	defer rows.Close()

	columns := make(map[string]int)
	for rows.Next() {
		var name string
		var ordinal int
		if err := rows.Scan(&name, &ordinal); err != nil {
			return ColumnIndexes{}, fmt.Errorf("scan outbox column: %w", err)
		}
		columns[strings.ToLower(name)] = ordinal - 1
	}
	if err := rows.Err(); err != nil {
		return ColumnIndexes{}, fmt.Errorf("iterate outbox columns: %w", err)
	}

	index := ColumnIndexes{}
	required := []struct {
		name string
		to   *int
	}{
		{"id", &index.ID},
		{"subject", &index.Subject},
		{"payload", &index.Payload},
		{"headers", &index.Headers},
		{"message_id", &index.MessageID},
		{"ttl_seconds", &index.TTLSeconds},
	}
	for _, column := range required {
		value, ok := columns[column.name]
		if !ok {
			return ColumnIndexes{}, fmt.Errorf("outbox table %s is missing required column %q", s.qualified(s.outboxTable), column.name)
		}
		*column.to = value
	}
	return index, nil
}

func (s *Store) LoadPosition(ctx context.Context) (Position, bool, error) {
	var position Position
	var value uint64
	var gtid sql.NullString
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT binlog_name, binlog_position, gtid_set
FROM %s
WHERE consumer_name = ?`, s.qualified(s.stateTable)), s.consumerName).Scan(&position.Name, &value, &gtid)
	if errors.Is(err, sql.ErrNoRows) {
		return Position{}, false, nil
	}
	if err != nil {
		return Position{}, false, fmt.Errorf("load checkpoint: %w", err)
	}
	if value > uint64(^uint32(0)) {
		return Position{}, false, fmt.Errorf("checkpoint position %d exceeds the replication protocol limit", value)
	}
	position.Pos = uint32(value)
	if gtid.Valid {
		position.GTID = gtid.String
	}
	return position, true, nil
}

func (s *Store) SavePosition(ctx context.Context, position Position) error {
	return s.savePositionAndApply(ctx, position, nil, nil)
}

func (s *Store) Complete(ctx context.Context, position Position, succeeded []uint64, failed []PublishFailure) error {
	return s.savePositionAndApply(ctx, position, succeeded, failed)
}

func (s *Store) savePositionAndApply(
	ctx context.Context,
	position Position,
	succeeded []uint64,
	failed []PublishFailure,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.applyResults(ctx, tx, succeeded, failed); err != nil {
		return err
	}
	query := fmt.Sprintf(`
INSERT INTO %s (consumer_name, binlog_name, binlog_position, gtid_set)
VALUES (?, ?, ?, NULLIF(?, ''))
ON DUPLICATE KEY UPDATE
    binlog_name = VALUES(binlog_name),
    binlog_position = VALUES(binlog_position),
    gtid_set = VALUES(gtid_set)`, s.qualified(s.stateTable))
	if _, err := tx.ExecContext(ctx, query, s.consumerName, position.Name, position.Pos, position.GTID); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit checkpoint transaction: %w", err)
	}
	return nil
}

func (s *Store) CompleteSnapshotBatch(ctx context.Context, succeeded []uint64, failed []PublishFailure) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin snapshot result transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.applyResults(ctx, tx, succeeded, failed); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit snapshot result transaction: %w", err)
	}
	return nil
}

func (s *Store) applyResults(ctx context.Context, tx *sql.Tx, succeeded []uint64, failed []PublishFailure) error {
	if len(succeeded) > 0 {
		query, args := s.deleteQuery(succeeded)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("delete published outbox rows: %w", err)
		}
	}
	query := fmt.Sprintf(`
UPDATE %s
SET attempt_count = attempt_count + 1,
    last_attempt_at = UTC_TIMESTAMP(6),
    last_error = ?
WHERE id = ?`, s.qualified(s.outboxTable))
	for _, failure := range failed {
		message := failure.Error
		if len(message) > 4096 {
			message = message[:4096]
		}
		if _, err := tx.ExecContext(ctx, query, message, failure.ID); err != nil {
			return fmt.Errorf("record failure for outbox row %d: %w", failure.ID, err)
		}
	}
	return nil
}

func (s *Store) SnapshotBatch(ctx context.Context, afterID uint64, limit int) ([]Message, error) {
	query := fmt.Sprintf(`
SELECT id, subject, payload, headers, message_id, ttl_seconds, attempt_count
FROM %s
WHERE id > ?
ORDER BY id
LIMIT ?`, s.qualified(s.outboxTable))
	rows, err := s.db.QueryContext(ctx, query, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("read outbox snapshot: %w", err)
	}
	defer rows.Close()

	return scanMessages(rows, "outbox snapshot")
}

func (s *Store) FailedBatch(ctx context.Context, afterID uint64, limit int) ([]Message, error) {
	query := fmt.Sprintf(`
SELECT id, subject, payload, headers, message_id, ttl_seconds, attempt_count
FROM %s
WHERE id > ? AND attempt_count > 0
ORDER BY id
LIMIT ?`, s.qualified(s.outboxTable))
	rows, err := s.db.QueryContext(ctx, query, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("read failed outbox rows: %w", err)
	}
	defer rows.Close()
	return scanMessages(rows, "failed outbox row")
}

func (s *Store) CompleteRetryBatch(ctx context.Context, succeeded []uint64, failed []PublishFailure) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin retry result transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.applyResults(ctx, tx, succeeded, failed); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit retry result transaction: %w", err)
	}
	return nil
}

func (s *Store) CountFailed(ctx context.Context) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE attempt_count > 0", s.qualified(s.outboxTable))
	if err := s.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("count failed outbox rows: %w", err)
	}
	return count, nil
}

func (s *Store) MasterPosition(ctx context.Context) (Position, error) {
	position, err := s.masterPositionWithQuery(ctx, "SHOW BINARY LOG STATUS")
	if err == nil {
		return position, nil
	}
	position, fallbackErr := s.masterPositionWithQuery(ctx, "SHOW MASTER STATUS")
	if fallbackErr != nil {
		return Position{}, fmt.Errorf("read binary log position: %w (fallback: %v)", err, fallbackErr)
	}
	return position, nil
}

func (s *Store) masterPositionWithQuery(ctx context.Context, query string) (Position, error) {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return Position{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return Position{}, err
	}
	values, err := scanFirstRow(rows)
	if err != nil {
		return Position{}, err
	}
	if len(values) < 2 {
		return Position{}, errors.New("binary log status returned fewer than two columns")
	}
	name := stringValue(values[0])
	posValue, err := strconv.ParseUint(stringValue(values[1]), 10, 32)
	if err != nil {
		return Position{}, fmt.Errorf("parse binary log position: %w", err)
	}
	if name == "" {
		return Position{}, errors.New("binary log status returned an empty log name")
	}
	position := Position{Name: name, Pos: uint32(posValue)}
	for i, column := range columns {
		if !strings.EqualFold(column, "Executed_Gtid_Set") || i >= len(values) {
			continue
		}
		raw := strings.TrimSpace(stringValue(values[i]))
		if raw == "" {
			break
		}
		set, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, raw)
		if err != nil {
			return Position{}, fmt.Errorf("parse executed GTID set: %w", err)
		}
		position.GTID = set.String()
		break
	}
	return position, nil
}

func (s *Store) PositionAvailable(ctx context.Context, position Position) (bool, error) {
	if position.GTID != "" {
		return s.gtidPositionAvailable(ctx, position.GTID)
	}
	if position.Pos < 4 {
		return false, nil
	}
	rows, err := s.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return false, fmt.Errorf("list binary logs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		values, err := scanCurrentRow(rows)
		if err != nil {
			return false, err
		}
		if len(values) > 1 && stringValue(values[0]) == position.Name {
			size, err := strconv.ParseUint(stringValue(values[1]), 10, 64)
			if err != nil {
				return false, fmt.Errorf("parse binary log size: %w", err)
			}
			return uint64(position.Pos) <= size, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate binary logs: %w", err)
	}
	return false, nil
}

func (s *Store) gtidPositionAvailable(ctx context.Context, checkpoint string) (bool, error) {
	checkpointSet, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, checkpoint)
	if err != nil {
		return false, fmt.Errorf("parse checkpoint GTID set: %w", err)
	}
	var executed, purged string
	if err := s.db.QueryRowContext(ctx, `
SELECT @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged`).Scan(&executed, &purged); err != nil {
		return false, fmt.Errorf("read MySQL GTID state: %w", err)
	}
	executedSet, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, executed)
	if err != nil {
		return false, fmt.Errorf("parse executed GTID set: %w", err)
	}
	if !executedSet.Contain(checkpointSet) {
		return false, errors.New("MySQL source has not executed the durable GTID checkpoint")
	}
	purgedSet, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, purged)
	if err != nil {
		return false, fmt.Errorf("parse purged GTID set: %w", err)
	}
	return checkpointSet.Contain(purgedSet), nil
}

func (s *Store) qualified(table string) string {
	return "`" + s.database + "`.`" + table + "`"
}

func (s *Store) deleteQuery(ids []uint64) (string, []any) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	return fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", s.qualified(s.outboxTable), strings.Join(placeholders, ",")), args
}

func scanMessages(rows *sql.Rows, description string) ([]Message, error) {
	var messages []Message
	for rows.Next() {
		var message Message
		var headers, messageID sql.NullString
		var ttlSeconds sql.NullInt64
		if err := rows.Scan(
			&message.ID,
			&message.Subject,
			&message.Payload,
			&headers,
			&messageID,
			&ttlSeconds,
			&message.AttemptCount,
		); err != nil {
			return nil, fmt.Errorf("scan %s: %w", description, err)
		}
		if headers.Valid {
			message.Headers = []byte(headers.String)
		}
		if messageID.Valid {
			message.MessageID = messageID.String
		}
		if ttlSeconds.Valid {
			if ttlSeconds.Int64 < 0 || uint64(ttlSeconds.Int64) > uint64(^uint32(0)) {
				return nil, fmt.Errorf("scan %s: ttl_seconds %d exceeds INT UNSIGNED", description, ttlSeconds.Int64)
			}
			value := uint32(ttlSeconds.Int64)
			message.TTLSeconds = &value
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s: %w", description, err)
	}
	return messages, nil
}

func scanFirstRow(rows *sql.Rows) ([]any, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	return scanCurrentRow(rows)
}

func scanCurrentRow(rows *sql.Rows) ([]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	if err := rows.Scan(pointers...); err != nil {
		return nil, err
	}
	return values, nil
}

func stringValue(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(value)
	case string:
		return value
	default:
		return fmt.Sprint(value)
	}
}
