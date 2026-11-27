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
