# Tailjet

Tailjet turns a MySQL transactional outbox into acknowledged NATS JetStream
publishes. An application writes its data changes and an outbox row in the
same MySQL transaction; Tailjet follows the row-based binary log and publishes
the outbox row only after MySQL commits that transaction.

This removes the unsafe gap between “database commit succeeded” and “the
process reached NATS” without adding NATS concerns to application code.

## Delivery model

1. On startup, Tailjet creates `tailjet_outbox` and its internal
   `tailjet_state` checkpoint table if they do not exist.
2. One replica acquires a MySQL advisory lock. Additional Kubernetes replicas
   remain hot standbys.
3. The leader snapshots any existing outbox rows, then resumes the MySQL
   replication stream from its durable GTID checkpoint. If GTIDs are disabled,
   Tailjet falls back to a file/position checkpoint.
4. Insert row events are buffered until the transaction's `XID`/`COMMIT` event.
5. Tailjet synchronously publishes each message and waits for a JetStream
   persistence acknowledgement.
6. It deletes acknowledged rows. Failed rows are retained with their attempt
   count, last attempt time, and last error.
7. Row cleanup/failure metadata and the transaction checkpoint are committed
   together in MySQL, so one bad row never blocks later transactions.

If NATS is unavailable, Tailjet retains each row and continues following the
binlog. A NATS reconnect automatically queues a retry of retained rows.
Operators can also trigger a pass with `POST /retry` or `SIGHUP`. A retry pass
attempts every retained row once and leaves any row that still fails in place.

Delivery is **at least once**. A crash after JetStream stores a message but
before MySQL stores the checkpoint can cause a retry. Every publish includes a
stable `Nats-Msg-Id`, so JetStream suppresses the retry while the ID remains in
the stream's duplicate window. Set that window longer than the longest
expected outage/recovery interval, and keep consumers idempotent because no
finite deduplication window can provide permanent global exactly-once
delivery.

Initial publish attempts happen sequentially in MySQL commit order. Rows
created in one transaction are attempted in row-event order. A failed row is
necessarily delivered later than rows that succeed after it.

## Outbox contract

Tailjet creates this application-facing table:

```sql
CREATE TABLE tailjet_outbox (
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
        AND subject NOT LIKE '.%'
        AND subject NOT LIKE '%.'
        AND subject NOT LIKE '%..%'
    ),
    CHECK (
        headers IS NULL OR JSON_TYPE(headers) = 'OBJECT'
    ),
    CHECK (
        ttl_seconds IS NULL OR ttl_seconds > 0
    )
) ENGINE=InnoDB;
```

- `subject` is the concrete NATS subject. Wildcards are rejected.
- `payload` is published byte-for-byte.
- `headers` is optional JSON whose values are strings or arrays of strings.
- `message_id` is optional but recommended. It must be globally unique within
  the target stream. If omitted, Tailjet derives a stable ID from the database,
  table, and row ID.
- `ttl_seconds` is an optional positive per-message JetStream TTL. The TTL
  starts when JetStream stores the message. The target stream must enable
  per-message TTLs (`allow_msg_ttl`).
- The `Nats-` header namespace is reserved. Put `Nats-Msg-Id` in
  `message_id`, put `Nats-TTL` in `ttl_seconds`, and configure the expected
  stream through Tailjet.
- `attempt_count`, `last_attempt_at`, and `last_error` are managed by Tailjet.
  Producers must leave them at their defaults.

The application only needs `INSERT` permission on this table. Tailjet owns row
deletion; do not update, manually delete, or alter this table while the relay
is running.

### Transaction example

```sql
START TRANSACTION;

UPDATE example_records
SET value = 'updated',
    revision = revision + 1
WHERE id = 'example-1';

INSERT INTO tailjet_outbox (subject, payload, headers, message_id, ttl_seconds)
VALUES (
    'events.record-changed',
    JSON_OBJECT(
        'recordId', 'example-1',
        'value', 'updated',
        'revision', 42
    ),
    JSON_OBJECT('Content-Type', 'application/json'),
    'record:example-1:42',
    300
);

COMMIT;
```

If the transaction rolls back, Tailjet publishes nothing.

## MySQL requirements

- MySQL 8.0.16 or newer; MySQL 8.4 LTS with binary-log transaction
  compression is used by the integration gate
- MySQL with `log_bin=ON`
- `binlog_format=ROW`
- `binlog_row_image=FULL`
- `gtid_mode=ON` and `enforce_gtid_consistency=ON` for node-independent
  checkpoints and writer failover
- InnoDB for the outbox and state tables
- NATS Server 2.11 or newer and a stream with `allow_msg_ttl` enabled when
  producers use `ttl_seconds`
- A unique, non-zero `TAILJET_MYSQL_SERVER_ID` for this replication client
- Enough binlog retention to cover normal downtime. Tailjet verifies that a
  GTID checkpoint includes every purged transaction before resuming. If the
  required history has been purged, it safely snapshots the remaining
  unacknowledged outbox rows and starts at the current GTID set.

With GTIDs enabled, Tailjet stores `Executed_Gtid_Set` with each durable
checkpoint and reconnects with MySQL auto-positioning. Binlog filenames are
kept for diagnostics but are not used to choose the resume point. Every
Tailjet replica must use the same single-writer endpoint (for example, the PXC
HAProxy writer service), and that endpoint may move between synchronized
nodes. An existing file/position checkpoint is migrated by snapshotting only
the rows still present in the outbox and recording a fresh GTID boundary; rows
already acknowledged and deleted are not republished by that migration.

Example relay grants:

```sql
CREATE USER 'tailjet'@'%' IDENTIFIED BY 'replace-me';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER ON app.* TO 'tailjet'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'tailjet'@'%';
```

The database-scoped grants allow Tailjet to create and maintain its two
tables. Use a separate application account with only `INSERT` on
`app.tailjet_outbox`.

## NATS requirements

NATS Server with JetStream must be enabled; the integration gate uses NATS
2.14.1. A stream must already cover every subject used by outbox rows. Tailjet
deliberately does not create or mutate streams because
retention, replication, storage, and subject ownership are infrastructure
decisions.

For a development stream:

```sh
nats stream add EVENTS \
  --subjects 'events.>' \
  --storage file \
  --replicas 1 \
  --dupe-window 24h
```

In production, use a replicated stream and set
`TAILJET_NATS_EXPECTED_STREAM` so a publish is rejected if it would land in a
different stream.

## Run locally

Docker Compose starts MySQL with row binlogs, NATS with JetStream, creates an
`events.>` stream, and runs Tailjet:

```sh
docker compose up --build
curl -i http://localhost:18080/readyz
```

Insert a message:

```sh
docker compose exec mysql mysql -utailjet -ptailjet tailjet -e \
  "INSERT INTO tailjet_outbox(subject,payload,message_id)
   VALUES('events.demo','{\"hello\":\"world\"}','demo-1')"
```

Inspect the stream:

```sh
docker compose run --rm nats-init \
  nats --server=nats://nats:4222 stream view EVENTS
```

## Kubernetes

Starter manifests live in `deploy/kubernetes`. Create the Secret separately,
replace the image and endpoints in the ConfigMap/Deployment, and apply with
Kustomize:

```sh
kubectl create secret generic tailjet \
  --from-literal=TAILJET_MYSQL_PASSWORD='replace-me'
kubectl apply -k deploy/kubernetes
```

The example uses two pods. Only the pod holding the MySQL advisory lock opens a
replication stream; the other reports ready in the `standby` phase. Losing the
lock connection immediately cancels the leader loop before another pod can
take over.

The container runs non-root with a read-only root filesystem. Configure TLS
and credential volumes in your overlay; do not put production credentials in
the ConfigMap. The starter also disables service-account token mounting,
spreads replicas across nodes when capacity permits, and protects one replica
from voluntary disruption with a PodDisruptionBudget. Its `GOMEMLIMIT` leaves
headroom below the container memory limit; keep those settings aligned in
production overlays.

## Configuration

All configuration is via environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `TAILJET_MYSQL_HOST` | `127.0.0.1` | MySQL source host |
| `TAILJET_MYSQL_PORT` | `3306` | MySQL source port |
| `TAILJET_MYSQL_USER` | `tailjet` | DDL/DML and replication user |
| `TAILJET_MYSQL_PASSWORD` | empty | MySQL password |
| `TAILJET_MYSQL_DATABASE` | required | Database containing the outbox |
| `TAILJET_MYSQL_FLAVOR` | `mysql` | Replication protocol flavor; currently `mysql` |
| `TAILJET_MYSQL_SERVER_ID` | `240024` | Unique replication client ID |
| `TAILJET_MYSQL_TLS_MODE` | `disable` | `disable`, `verify`, or `skip-verify` |
| `TAILJET_MYSQL_TLS_CA` | empty | Optional CA PEM path |
| `TAILJET_MYSQL_TLS_CERT` / `_KEY` | empty | Optional client certificate paths |
| `TAILJET_OUTBOX_TABLE` | `tailjet_outbox` | Application-facing table name |
| `TAILJET_STATE_TABLE` | `tailjet_state` | Internal checkpoint table name |
| `TAILJET_CONSUMER_NAME` | `default` | Checkpoint identity |
| `TAILJET_LEADER_LOCK_NAME` | derived | MySQL advisory lock, at most 64 bytes |
| `TAILJET_NATS_URL` | `nats://127.0.0.1:4222` | One or more NATS URLs |
| `TAILJET_NATS_USER` / `_PASSWORD` | empty | User/password authentication |
| `TAILJET_NATS_TOKEN` | empty | Token authentication |
| `TAILJET_NATS_CREDS` | empty | NATS credentials file path |
| `TAILJET_NATS_TLS_CA` | empty | NATS CA PEM path |
| `TAILJET_NATS_TLS_CERT` / `_KEY` | empty | NATS client certificate paths |
| `TAILJET_NATS_EXPECTED_STREAM` | empty | Assert the destination stream |
| `TAILJET_PUBLISH_TIMEOUT` | `10s` | Per-message acknowledgement timeout |
| `TAILJET_CHECKPOINT_INTERVAL` | `5s` | Checkpoint interval without messages |
| `TAILJET_LEADER_RETRY_INTERVAL` | `5s` | Standby/recovery retry interval |
| `TAILJET_SNAPSHOT_BATCH_SIZE` | `500` | Rows per startup snapshot batch |
| `TAILJET_MAX_TRANSACTION_ROWS` | `10000` | Memory guard for one transaction |
| `TAILJET_MAX_TRANSACTION_BYTES` | `67108864` | Byte guard for one transaction |
| `TAILJET_HTTP_ADDRESS` | `:8080` | Health/status listen address |

Configure only one NATS authentication method.

Credentials embedded in `TAILJET_NATS_URL` are rejected so they cannot leak
through connection errors or diagnostics. Use the dedicated authentication
variables instead.

## Operations

- `GET /livez` says the process is alive.
- `GET /readyz` returns 200 for an active streaming leader or a healthy standby.
- `GET /status` returns the current phase, last error, checkpoint time, and
  publish count.
- `GET /metrics` exposes Prometheus readiness/leadership gauges, lifecycle
  phase, failed-row gauge, publish/error/retry counters, and last
  publish/checkpoint timestamps.
- `POST /retry` queues one retry pass on the active leader and returns 202. A
  standby returns 409; target the leader pod shown by `GET /status`.

Send `SIGHUP` to the active process for the same retry behavior. Restrict the
health port with Kubernetes network policy or an authenticated proxy if the
retry endpoint should not be directly reachable in your cluster.

```sh
curl -X POST http://localhost:18080/retry
kill -HUP <tailjet-pid>
```

A malformed message or a subject not covered by a JetStream stream is retained
without blocking the queue. Every failed attempt emits an error log containing
the row ID, subject, message ID, attempt number, and error; it also increments
`tailjet_errors_total` and `tailjet_publish_errors_total`. Inspect
`attempt_count`, `last_attempt_at`, and `last_error` on the row, correct the
external condition, then trigger a retry.

### Production checklist

- Keep MySQL and NATS traffic encrypted and mount private CA/client credential
  files from Kubernetes Secrets.
- Ensure source-side binlog filters include the outbox database and application
  sessions do not disable binary logging.
- Enable GTIDs and point every replica at the same single-writer endpoint; the
  advisory lock, replication stream, outbox cleanup, and checkpoint writes
  must reach one current primary at a time. Configure the proxy to terminate
  existing sessions whenever it changes the active writer so the old
  node-local advisory lock cannot overlap a new leader.
- Size MySQL binlog retention and the JetStream duplicate window for the longest
  credible outage, and keep downstream consumers idempotent.
- Grant the NATS account publish access only to owned subjects and subscribe
  access to its generated inboxes so it can receive publish acknowledgements.
- Alert on `tailjet_ready == 0`, absence of a leader, increasing
  `tailjet_publish_errors_total`, and `tailjet_failed_rows > 0`.
- Run only one Tailjet deployment per outbox table. Standby replicas provide
  availability, not additional publish throughput; load-test the single
  leader against the expected peak outbox rate and message sizes.
- Pin the published container by digest in the production overlay.

## Development

```sh
go test ./...
go vet ./...
go build ./cmd/tailjet
make vuln
make integration
```

## Releasing

Tags matching `vMAJOR.MINOR.PATCH` run the complete race, vulnerability, and
integration gates before publishing multi-architecture images to
`ghcr.io/zyno-io/tailjet`. Published images include OCI metadata, an SBOM, and
build provenance. Pin the resulting digest in production rather than relying
on a mutable version tag.

## License

Tailjet is available under the [MIT License](LICENSE).
