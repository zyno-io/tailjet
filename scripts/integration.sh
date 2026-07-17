#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

for command in curl docker jq; do
  command -v "$command" >/dev/null || {
    echo "required command not found: $command" >&2
    exit 1
  }
done

cleanup() {
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

mysql_query() {
  docker compose exec -T mysql \
    mysql -utailjet -ptailjet --batch --skip-column-names tailjet -e "$1" 2>/dev/null
}

wait_for_mysql() {
  for _ in {1..120}; do
    if [[ "$(mysql_query "SELECT 1" || true)" == "1" ]]; then
      return 0
    fi
    sleep 0.5
  done
  docker compose logs mysql >&2
  return 1
}

wait_for_ready() {
  for _ in {1..120}; do
    if curl -fsS http://localhost:18080/readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  docker compose logs tailjet >&2
  return 1
}

wait_for_sql() {
  local expected=$1
  local query=$2
  local actual=""
  for _ in {1..120}; do
    actual=$(mysql_query "$query" || true)
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "timed out waiting for SQL result '$expected'; got '$actual'" >&2
  return 1
}

stream_messages() {
  docker compose run --rm nats-init \
    nats --server=nats://nats:4222 stream info EVENTS --json 2>/dev/null |
    jq -r '.state.messages'
}

docker compose up -d mysql nats
wait_for_mysql
mysql_query "
CREATE TABLE tailjet_outbox (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  subject VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  payload LONGBLOB NOT NULL,
  headers JSON NULL,
  message_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uq_tailjet_message_id (message_id)
) ENGINE=InnoDB"
docker compose up -d --build tailjet
wait_for_ready

columns=$(mysql_query "
SELECT COUNT(*)
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='tailjet'
  AND TABLE_NAME='tailjet_outbox'
  AND COLUMN_NAME IN ('attempt_count','last_attempt_at','last_error')")
[[ "$columns" == "3" ]] || {
  echo "managed failure columns were not created" >&2
  exit 1
}

before_rollback=$(stream_messages)
mysql_query "
START TRANSACTION;
INSERT INTO tailjet_outbox(subject,payload,message_id)
VALUES ('events.rollback','{}','integration-rollback');
ROLLBACK;"
sleep 1
[[ "$(stream_messages)" == "$before_rollback" ]]
[[ "$(mysql_query "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-rollback'")" == "0" ]]

before_skip=$(stream_messages)
mysql_query "
START TRANSACTION;
INSERT INTO tailjet_outbox(subject,payload,message_id) VALUES
  ('integration-unconfigured.example','{}','integration-retained'),
  ('events.after-failure','{}','integration-delivered');
COMMIT;"
wait_for_sql "1" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-retained' AND attempt_count=1"
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-delivered'"
[[ "$(stream_messages)" == "$((before_skip + 1))" ]]

metrics=$(curl -fsS http://localhost:18080/metrics)
grep -q '^tailjet_failed_rows 1$' <<<"$metrics"
grep -q '^tailjet_publish_errors_total 1$' <<<"$metrics"

curl -fsS -X POST http://localhost:18080/retry >/dev/null
wait_for_sql "2" "SELECT attempt_count FROM tailjet_outbox WHERE message_id='integration-retained'"
failure_logs=$(docker compose logs tailjet 2>&1 | grep -c 'integration-retained' || true)
[[ "$failure_logs" -ge 2 ]] || {
  echo "expected an error log for every failed attempt" >&2
  exit 1
}

docker compose run --rm nats-init \
  nats --server=nats://nats:4222 stream edit EVENTS \
  --subjects 'events.>' --subjects 'integration-unconfigured.>' --force >/dev/null
curl -fsS -X POST http://localhost:18080/retry >/dev/null
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-retained'"

docker compose stop nats >/dev/null
mysql_query "
INSERT INTO tailjet_outbox(subject,payload,message_id)
VALUES ('events.reconnect','{}','integration-reconnect')"
wait_for_sql "1" "SELECT attempt_count FROM tailjet_outbox WHERE message_id='integration-reconnect'"
docker compose start nats >/dev/null
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-reconnect'"

mysql_query "
INSERT INTO tailjet_outbox(subject,payload,message_id)
VALUES ('integration-signal.example','{}','integration-signal')"
wait_for_sql "1" "SELECT attempt_count FROM tailjet_outbox WHERE message_id='integration-signal'"
docker compose run --rm nats-init \
  nats --server=nats://nats:4222 stream edit EVENTS \
  --subjects 'events.>' \
  --subjects 'integration-unconfigured.>' \
  --subjects 'integration-signal.>' \
  --force >/dev/null
docker kill --signal=HUP "$(docker compose ps -q tailjet)" >/dev/null
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-signal'"

status=$(curl -fsS http://localhost:18080/status)
[[ "$(jq -r '.ready' <<<"$status")" == "true" ]]
[[ "$(jq -r '.leader' <<<"$status")" == "true" ]]
[[ "$(jq -r '.failedRows' <<<"$status")" == "0" ]]

standby_container=$(docker compose run -d --no-deps --name tailjet-standby -p 18081:8080 tailjet)
standby_status=""
for _ in {1..60}; do
  standby_status=$(curl -fsS http://localhost:18081/status 2>/dev/null || true)
  if [[ -n "$standby_status" ]] && [[ "$(jq -r '.ready // false' <<<"$standby_status")" == "true" ]]; then
    break
  fi
  sleep 0.25
done
[[ -n "$standby_status" ]]
if [[ "$(jq -r '.phase' <<<"$standby_status")" != "standby" ]] ||
  [[ "$(jq -r '.leader' <<<"$standby_status")" != "false" ]]; then
  echo "second replica did not become a ready standby: $standby_status" >&2
  exit 1
fi
wait_for_sql "1" "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'"

docker compose stop tailjet >/dev/null
for _ in {1..80}; do
  promoted_status=$(curl -fsS http://localhost:18081/status 2>/dev/null || true)
  if [[ -n "$promoted_status" ]] && [[ "$(jq -r '.leader' <<<"$promoted_status")" == "true" ]]; then
    break
  fi
  sleep 0.25
done
[[ -n "$promoted_status" ]] && [[ "$(jq -r '.leader' <<<"$promoted_status")" == "true" ]] || {
  echo "standby was not promoted after leader termination" >&2
  exit 1
}
before_failover=$(stream_messages)
mysql_query "
INSERT INTO tailjet_outbox(subject,payload,message_id)
VALUES ('events.failover','{}','integration-failover')"
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-failover'"
[[ "$(stream_messages)" == "$((before_failover + 1))" ]]

docker stop "$standby_container" >/dev/null

echo "integration checks passed"
