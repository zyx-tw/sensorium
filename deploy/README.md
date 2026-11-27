# deploy

systemd units for running sensorium without Docker. Docker Compose is the
standard way to deploy it (see the README); these are for a host where Docker
does not fit, such as an LXC container. Copy them to `/etc/systemd/system/`,
then `systemctl daemon-reload`.

An instance is a checkout at `/opt/sensorium` with `bun install` run in it, an
env file at `/etc/sensorium/app.env` (see `.env.example`), and a local
Postgres.

| Unit                          | What it does                        |
| ----------------------------- | ----------------------------------- |
| `sensorium-ingest.service`    | OTLP/HTTP receiver on `PORT` (8787) |
| `sensorium-mcp.service`       | MCP endpoint on `MCP_PORT` (8788)   |
| `sensorium-retention.timer`   | fires the sweep daily at 03:20 UTC  |
| `sensorium-retention.service` | the sweep itself (`Type=oneshot`)   |

```sh
systemctl enable --now sensorium-ingest sensorium-mcp
systemctl enable --now sensorium-retention.timer
```

## Retention

`metric_points`, `spans` and `logs` are partitioned by UTC day, so expiring
them is a `DROP TABLE` per day rather than a `DELETE`: the space returns to the
filesystem straight away. Each has a `<table>_default` partition as a backstop,
which the sweep drains into real partitions.

Windows are set per instance in `app.env`; the defaults are 14 days of metrics
and 30 days of spans and logs. Size the metric window against real throughput:
one busy PVE project alone writes roughly 2.4GB/day, so 14 days is about 33GB.

The timer is not optional. Without it, partitions stop being created (rows fall
into the `_default` partitions, which never expire) and nothing is ever dropped.
That is how a 20G disk filled in seven days on 2026-07-25 and put Postgres into
a checkpoint-PANIC crash loop, with `systemctl` still reporting both services
active the whole time.

Run it by hand any time — every step is idempotent:

```sh
cd /opt/sensorium && bun run db:retention
```

## Health check

Green units are not enough; the failure mode above kept both services active
while ingest silently dropped everything. Check the two things that actually
move:

```sh
df -h /
sudo -u postgres psql -d sensorium -Atc "select max(ts) from metric_points"
```

## Large spans tables (migration 0006)

Migration 0006 copies `spans` and `logs` into partitioned tables inside one
transaction, so it needs free disk about the size of both. When `spans` is too
large for that, empty it into a compressed file first, let the migration run on
the empty table, then load the file back one day at a time. Ingest is down only
for the dump. Run from the compose directory; `psql` below is
`docker compose exec -T postgres psql -U sensorium -d sensorium`.

1. Stop the writers: `docker compose stop ingest mcp retention`.
2. Dump and count (binary COPY keeps the column order 0006 uses):

   ```sh
   psql -Atc "select count(*) from spans"
   psql -c "\copy spans to stdout (format binary)" | zstd -3 -T0 > spans-pre-0006.bin.zst
   ```

3. Empty the table, which returns its disk at once: `psql -c "truncate spans"`.
4. Deploy the new image (`SENSORIUM_VERSION` in `.env`, then
   `docker compose up -d`). 0006 now runs in seconds and creates a partition
   for every day from 31 days back.
5. Load the file back, one day per pass, through a staging table, so a span
   that arrived again after the restart cannot fail the load:

   ```sh
   psql -c "create unlogged table spans_restore (like spans)"
   for d in $(seq 31 -1 0); do
     from=$(date -u -d "$d days ago" +%F); to=$(date -u -d "$((d - 1)) days ago" +%F)
     zstd -dc spans-pre-0006.bin.zst | psql -c "\copy spans_restore from stdin (format binary) where start_ts >= '$from' and start_ts < '$to'"
     psql -c "insert into spans select * from spans_restore on conflict do nothing" -c "truncate spans_restore"
   done
   psql -c "drop table spans_restore" -Atc "select count(*) from spans"
   ```

6. Compare the counts, then delete the file. The loop loads the last 31 days;
   if `SENSORIUM_RETENTION_SPAN_DAYS` is larger, start `seq` at that number.
