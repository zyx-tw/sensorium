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
| `sensorium-retention.timer`   | fires the sweep hourly at :20 UTC   |
| `sensorium-retention.service` | the sweep itself (`Type=oneshot`)   |

```sh
systemctl enable --now sensorium-ingest sensorium-mcp
systemctl enable --now sensorium-retention.timer
```

## Retention

`metric_samples`, `metric_rollups`, `spans` and `logs` are partitioned by UTC day, so expiring
them is a `DROP TABLE` per day rather than a `DELETE`: the space returns to the
filesystem straight away. Each has a `<table>_default` partition as a backstop,
which the sweep drains into real partitions. Each sweep first folds finished
5-minute buckets into `metric_rollups`, and a samples day is dropped only once
it is rolled up. Rows from before migration 0008 stay in
`metric_points_legacy` until their days expire.

Windows are set per instance in `app.env`; the defaults are 14 days of raw
metrics, 90 days of rollups, and 30 days of spans and logs. Size the metric
window against real throughput: each point takes about 90 bytes on disk.

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
sudo -u postgres psql -d sensorium -Atc "select max(last_metric_at) from projects"
```

## Large spans tables (migration 0006)

Migration 0006 copies `spans` and `logs` into partitioned tables inside one
transaction, so it needs free disk about the size of both. When `spans` is too
large for that, `deploy/spans-0006.sh` empties it into a compressed file first,
the migration then runs on the empty table, and the script loads the file back
one day at a time. Ingest is down only for the dump. From the compose
directory:

```sh
docker compose stop ingest mcp retention
deploy/spans-0006.sh dump spans-pre-0006.zst   # checks the count read back from the file before it truncates
# set SENSORIUM_VERSION in .env to the image with 0006
docker compose up -d                           # 0006 runs in seconds on the empty table
deploy/spans-0006.sh load spans-pre-0006.zst   # per day, skipping rows past retention
```

The file is about a tenth of the table. The script stops at the first error,
refuses to truncate when the row count in the file differs from the table, and
keeps the file until you delete it.

0006 makes day partitions for the last 60 days. With `SENSORIUM_RETENTION_SPAN_DAYS`
or `SENSORIUM_RETENTION_LOG_DAYS` above 60, older rows land in the default
partition, and the first retention run moves them into their own days while it
holds a lock on the table. Run retention by hand once, at a quiet time, right
after 0006. Before migrating, check `\dp spans logs`: 0006 restores table
grants but not column grants or `WITH GRANT OPTION`.

## PostgreSQL version

Compose runs `postgres:16-alpine` by default, so an existing volume keeps working. CI tests both 16 and 18. A new instance can start on 18 with `SENSORIUM_POSTGRES_IMAGE=postgres:18-alpine` in `.env`.

An existing volume cannot change major version in place. To move a 16 volume to 18:

1. Stop ingest and mcp: `docker compose stop ingest mcp`.
2. Dump: `docker compose exec postgres pg_dumpall -U sensorium > sensorium.sql`.
3. Set `SENSORIUM_POSTGRES_IMAGE=postgres:18-alpine` and give the postgres service a new volume. The 18 image keeps its data in `/var/lib/postgresql/18/docker`, so mount the volume at `/var/lib/postgresql`.
4. Start postgres alone, then restore: `docker compose exec -T postgres psql -U sensorium -d postgres < sensorium.sql`.
5. Start everything with `docker compose up -d` and call `storage_stats` to check the row counts.

A large metrics table makes the dump slow. Drop old partitions with retention first.
