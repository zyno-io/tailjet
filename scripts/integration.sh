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

last_message_header() {
  local subject=$1
  local header=$2
  docker compose run --rm nats-init \
    nats --server=nats://nats:4222 stream get EVENTS --last-for="$subject" --json 2>/dev/null |
    jq -r '.hdrs' |
    base64 -d |
    tr -d '\r' |
    awk -F': ' -v expected="$header" 'tolower($1) == tolower(expected) { print $2 }'
}

last_message_exists() {
  local subject=$1
  docker compose run --rm nats-init \
    nats --server=nats://nats:4222 stream get EVENTS --last-for="$subject" --json >/dev/null 2>&1
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
read -r legacy_binlog legacy_position _ <<<"$(mysql_query "SHOW BINARY LOG STATUS")"
mysql_query "
CREATE TABLE tailjet_state (
  consumer_name VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  binlog_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  binlog_position BIGINT UNSIGNED NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (consumer_name)
) ENGINE=InnoDB;
INSERT INTO tailjet_state (consumer_name, binlog_name, binlog_position)
VALUES ('default', '${legacy_binlog}', ${legacy_position})"
docker compose up -d --build tailjet
wait_for_ready

columns=$(mysql_query "
SELECT COUNT(*)
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='tailjet'
  AND TABLE_NAME='tailjet_outbox'
  AND COLUMN_NAME IN ('ttl_seconds','attempt_count','last_attempt_at','last_error')")
[[ "$columns" == "4" ]] || {
  echo "managed TTL/failure columns were not created" >&2
  exit 1
}

wait_for_sql "1" "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='tailjet' AND TABLE_NAME='tailjet_state' AND COLUMN_NAME='gtid_set' AND DATA_TYPE='longtext'"
wait_for_sql "1" "SELECT COUNT(*) FROM tailjet_state WHERE consumer_name='default' AND gtid_set IS NOT NULL AND CHAR_LENGTH(gtid_set) > 0"

mysql_query "
INSERT INTO tailjet_outbox(subject,payload,message_id,ttl_seconds)
VALUES ('events.ttl','{}','integration-ttl',10)"
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-ttl'"
ttl_header=$(last_message_header events.ttl Nats-TTL)
[[ "$ttl_header" == "10s" ]] || {
  echo "expected Nats-TTL: 10s; got '$ttl_header'" >&2
  exit 1
}
for _ in {1..120}; do
  if ! last_message_exists events.ttl; then
    break
  fi
  sleep 0.25
done
if last_message_exists events.ttl; then
  echo "TTL message did not expire from JetStream" >&2
  exit 1
fi

mysql_query "
START TRANSACTION;
INSERT INTO tailjet_outbox(subject,payload,message_id,ttl_seconds) VALUES
  ('events.invalid-ttl','{}','integration-invalid-ttl',0),
  ('events.after-invalid-ttl','{}','integration-after-invalid-ttl',NULL);
COMMIT;"
wait_for_sql "1" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-invalid-ttl' AND attempt_count=1"
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-after-invalid-ttl'"
invalid_ttl_logs=$(docker compose logs tailjet 2>&1 | grep -c 'integration-invalid-ttl' || true)
[[ "$invalid_ttl_logs" -ge 1 ]] || {
  echo "expected an error log for invalid ttl_seconds" >&2
  exit 1
}
mysql_query "UPDATE tailjet_outbox SET ttl_seconds=5 WHERE message_id='integration-invalid-ttl'"
curl -fsS -X POST http://localhost:18080/retry >/dev/null
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-invalid-ttl'"
retried_ttl_header=$(last_message_header events.invalid-ttl Nats-TTL)
[[ "$retried_ttl_header" == "5s" ]] || {
  echo "expected retried Nats-TTL: 5s; got '$retried_ttl_header'" >&2
  exit 1
}

mysql_query "
START TRANSACTION;
INSERT INTO tailjet_outbox(subject,payload,message_id)
VALUES ('events.rollback','{}','integration-rollback');
ROLLBACK;"
sleep 1
if last_message_exists events.rollback; then
  echo "rolled-back message was published" >&2
  exit 1
fi
[[ "$(mysql_query "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-rollback'")" == "0" ]]

mysql_query "
START TRANSACTION;
INSERT INTO tailjet_outbox(subject,payload,message_id) VALUES
  ('integration-unconfigured.example','{}','integration-retained'),
  ('events.after-failure','{}','integration-delivered');
COMMIT;"
wait_for_sql "1" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-retained' AND attempt_count=1"
wait_for_sql "0" "SELECT COUNT(*) FROM tailjet_outbox WHERE message_id='integration-delivered'"
last_message_exists events.after-failure || {
  echo "row after a failed publish was not delivered" >&2
  exit 1
}

metrics=$(curl -fsS http://localhost:18080/metrics)
grep -q '^tailjet_failed_rows 1$' <<<"$metrics"
grep -q '^tailjet_publish_errors_total 2$' <<<"$metrics"

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

greenfield_container=$(docker compose run -d --no-deps \
  --name tailjet-greenfield \
  -e TAILJET_OUTBOX_TABLE=greenfield_outbox \
  -e TAILJET_STATE_TABLE=greenfield_state \
  -e TAILJET_CONSUMER_NAME=greenfield \
  -e TAILJET_MYSQL_SERVER_ID=240025 \
  tailjet)
wait_for_sql "1" "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='tailjet' AND TABLE_NAME='greenfield_outbox' AND COLUMN_NAME='ttl_seconds' AND COLUMN_TYPE='int unsigned' AND IS_NULLABLE='YES'"
wait_for_sql "1" "
SELECT COUNT(*)
FROM information_schema.TABLE_CONSTRAINTS tc
JOIN information_schema.CHECK_CONSTRAINTS cc
  ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
 AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
WHERE tc.CONSTRAINT_SCHEMA='tailjet'
  AND tc.TABLE_NAME='greenfield_outbox'
  AND cc.CHECK_CLAUSE LIKE '%ttl_seconds%'"
if mysql_query "INSERT INTO greenfield_outbox(subject,payload,message_id,ttl_seconds) VALUES ('events.greenfield-invalid','{}','greenfield-invalid',0)"; then
  echo "clean-install outbox accepted zero ttl_seconds" >&2
  exit 1
fi
mysql_query "
INSERT INTO greenfield_outbox(subject,payload,message_id,ttl_seconds) VALUES
  ('events.greenfield-ttl','{}','greenfield-ttl',3),
  ('events.greenfield-no-ttl','{}','greenfield-no-ttl',NULL)"
wait_for_sql "0" "SELECT COUNT(*) FROM greenfield_outbox"
[[ "$(last_message_header events.greenfield-ttl Nats-TTL)" == "3s" ]]
docker stop "$greenfield_container" >/dev/null

echo "integration checks passed"
