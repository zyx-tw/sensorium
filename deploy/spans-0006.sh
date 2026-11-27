#!/bin/bash
# Moves a spans table too large to copy in place through migration 0006
# (deploy/README.md, "Large spans tables"). Run from the compose directory.
#
#   deploy/spans-0006.sh dump FILE   writers must be stopped; dumps spans to FILE
#                                    (zstd text COPY), checks the row count read
#                                    back from FILE, and only then truncates spans
#   deploy/spans-0006.sh load FILE   after 0006 ran: loads FILE back one day at a
#                                    time, skipping rows past retention
set -euo pipefail
cmd=${1:?usage: spans-0006.sh dump|load FILE}
file=${2:?usage: spans-0006.sh dump|load FILE}
psql() { docker compose exec -T postgres psql -U sensorium -d sensorium -v ON_ERROR_STOP=1 -qAt "$@"; }

case "$cmd" in
dump)
  [ ! -e "$file" ] || { echo "$file exists; refusing to overwrite" >&2; exit 1; }
  running=$(docker compose ps --status running --services | grep -E '^(ingest|mcp|retention)$' || true)
  [ -z "$running" ] || { echo "stop the writers first: docker compose stop ingest mcp retention" >&2; exit 1; }
  [ "$(psql -c "select count(*) from schema_migrations where filename = '0006_partition_spans_logs.sql'")" = 0 ] ||
    { echo "0006 already applied; nothing to do" >&2; exit 1; }
  rows=$(psql -c "select count(*) from spans")
  echo "spans: $rows rows, $(psql -c "select pg_size_pretty(pg_total_relation_size('spans'))")"
  # Text COPY escapes newlines inside values, so every row is exactly one line.
  psql -c "\copy spans to stdout" | zstd -q -3 -T0 -o "$file"
  zstd -q -t "$file"
  lines=$(zstd -dc "$file" | wc -l)
  echo "$file: $(du -h "$file" | cut -f1), $lines rows"
  [ "$lines" = "$rows" ] || { echo "row count mismatch ($lines in file, $rows in table); spans left as is" >&2; exit 1; }
  echo "$rows" > "$file.count"
  psql -c "truncate spans"
  echo "spans truncated; deploy the new image now (docker compose up -d), then run: $0 load $file"
  ;;
load)
  [ -s "$file.count" ] || { echo "$file.count missing: was $file written by '$0 dump'?" >&2; exit 1; }
  [ "$(psql -c "select relkind from pg_class where relname = 'spans'")" = p ] ||
    { echo "spans is not partitioned yet: deploy the image with migration 0006 first" >&2; exit 1; }
  days=${SENSORIUM_RETENTION_SPAN_DAYS:-$(grep -E '^SENSORIUM_RETENTION_SPAN_DAYS=.' .env | cut -d= -f2 || true)}
  days=${days:-30}
  psql -c "create unlogged table if not exists spans_restore (like spans)" -c "truncate spans_restore"
  # One pass per day, oldest first, then one for anything dated after tomorrow.
  # The staging table plus `on conflict do nothing` keeps a span that arrived
  # again since the restart from failing the load.
  # The day windows come from one base date, so a load that runs past UTC
  # midnight cannot skip a day.
  base=$(date -u +%F)
  for d in $(seq "$days" -1 0) future; do
    if [ "$d" = future ]; then
      where="start_ts >= '$(date -u -d "$base 1 days" +%F)'"
    else
      where="start_ts >= '$(date -u -d "$base $((-d)) days" +%F)' and start_ts < '$(date -u -d "$base $((1 - d)) days" +%F)'"
    fi
    zstd -dc "$file" | psql -c "\copy spans_restore from stdin where $where"
    n=$(psql -c "with i as (insert into spans select * from spans_restore on conflict do nothing returning 1) select count(*) from i")
    psql -c "truncate spans_restore"
    echo "$d: $n"
  done
  psql -c "drop table spans_restore"
  echo "spans now $(psql -c "select count(*) from spans") rows; the file had $(cat "$file.count") (rows older than $days days are not loaded)"
  echo "check the numbers, then delete $file and $file.count"
  ;;
*) echo "usage: spans-0006.sh dump|load FILE" >&2; exit 1 ;;
esac
