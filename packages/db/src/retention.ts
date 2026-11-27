import type { Pool } from "pg";

/**
 * Age-based expiry for the three signal tables.
 *
 * All three are partitioned by UTC day (metric_points in migration 0004, spans
 * and logs in 0006), so expiry is a DROP TABLE per day: constant time, and the
 * space comes back immediately. A DELETE would leave the bytes behind.
 *
 * Meant to run daily from a timer. Every step is idempotent, so a missed run
 * costs disk, not correctness, and the next run catches up.
 */

/** A table partitioned by UTC day into <table>_YYYYMMDD, with <table>_default as a backstop. */
export type PartitionedTable = "metric_points" | "spans" | "logs";

export const PARTITIONED_TABLES: readonly PartitionedTable[] = ["metric_points", "spans", "logs"];

/** <table>_YYYYMMDD; the default partition deliberately does not match. */
function partitionRe(table: PartitionedTable): RegExp {
  return new RegExp(`^${table}_(\\d{4})(\\d{2})(\\d{2})$`);
}

export interface RetentionConfig {
  /** Days of raw metric points to keep. ~2.4GB/day for one busy PVE project. */
  metricDays: number;
  spanDays: number;
  logDays: number;
  /** How many days of partitions to keep pre-created ahead of now. */
  aheadDays: number;
}

export const DEFAULT_RETENTION: RetentionConfig = {
  metricDays: 14,
  spanDays: 30,
  logDays: 30,
  aheadDays: 7,
};

/** Reads the config from env, falling back to DEFAULT_RETENTION per field. */
export function retentionConfigFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): RetentionConfig {
  const num = (key: string, fallback: number): number => {
    const raw = env[key];
    if (raw === undefined || raw === "") return fallback;
    const parsed = Number(raw);
    if (!Number.isInteger(parsed) || parsed < 1) {
      throw new Error(
        `${key} must be a positive integer, got ${JSON.stringify(raw)}`,
      );
    }
    return parsed;
  };
  return {
    metricDays: num(
      "SENSORIUM_RETENTION_METRIC_DAYS",
      DEFAULT_RETENTION.metricDays,
    ),
    spanDays: num("SENSORIUM_RETENTION_SPAN_DAYS", DEFAULT_RETENTION.spanDays),
    logDays: num("SENSORIUM_RETENTION_LOG_DAYS", DEFAULT_RETENTION.logDays),
    aheadDays: num(
      "SENSORIUM_RETENTION_AHEAD_DAYS",
      DEFAULT_RETENTION.aheadDays,
    ),
  };
}

/** UTC midnight of the day `offsetDays` from `from`. */
export function utcDay(from: Date, offsetDays = 0): Date {
  return new Date(
    Date.UTC(
      from.getUTCFullYear(),
      from.getUTCMonth(),
      from.getUTCDate() + offsetDays,
      0,
      0,
      0,
      0,
    ),
  );
}

export function partitionName(day: Date, table: PartitionedTable = "metric_points"): string {
  const y = day.getUTCFullYear().toString().padStart(4, "0");
  const m = (day.getUTCMonth() + 1).toString().padStart(2, "0");
  const d = day.getUTCDate().toString().padStart(2, "0");
  return `${table}_${y}${m}${d}`;
}

/** Inverse of partitionName; null for anything that is not a daily partition. */
export function partitionDay(name: string, table: PartitionedTable = "metric_points"): Date | null {
  const m = partitionRe(table).exec(name);
  if (!m) return null;
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
}

/** [start, end) bounds of a day partition, as the SQL literals the DDL wants. */
export function partitionBounds(day: Date): { from: string; to: string } {
  return { from: day.toISOString(), to: utcDay(day, 1).toISOString() };
}

const ISO_UTC_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

/**
 * Quotes an ISO timestamp for inlining into DDL.
 *
 * `CREATE TABLE ... PARTITION OF ... FOR VALUES` is DDL, and Postgres does not
 * accept bind parameters there — passing $1/$2 fails with "bind message
 * supplies 2 parameters, but prepared statement requires 0". These values come
 * from Date.toISOString(), so the assertion is a guard against a future caller
 * rather than against today's input.
 */
function tsLiteral(iso: string): string {
  if (!ISO_UTC_RE.test(iso)) {
    throw new Error(
      `refusing to inline a non-ISO timestamp into DDL: ${JSON.stringify(iso)}`,
    );
  }
  return `'${iso}'`;
}

async function listDayPartitions(pool: Pool, table: PartitionedTable): Promise<string[]> {
  const { rows } = await pool.query<{ relname: string }>(
    `select c.relname
       from pg_inherits i
       join pg_class c on c.oid = i.inhrelid
       join pg_class p on p.oid = i.inhparent
      where p.relname = $1
      order by c.relname`,
    [table],
  );
  const re = partitionRe(table);
  return rows.map((r) => r.relname).filter((n) => re.test(n));
}

/** Creates today's partition and the next `aheadDays`. Idempotent. */
export async function ensurePartitions(
  pool: Pool,
  aheadDays: number,
  now = new Date(),
  table: PartitionedTable = "metric_points",
): Promise<string[]> {
  // Checked against the catalog rather than leaning on `if not exists`, whose
  // result still reports CREATE for a table that was already there — the
  // report would then claim work it did not do on every single run.
  const existing = new Set(await listDayPartitions(pool, table));
  const created: string[] = [];
  for (let offset = 0; offset <= aheadDays; offset++) {
    const day = utcDay(now, offset);
    const name = partitionName(day, table);
    if (existing.has(name)) continue;
    const { from, to } = partitionBounds(day);
    await pool.query(
      `create table if not exists ${name}
         partition of ${table} for values from (${tsLiteral(from)}) to (${tsLiteral(to)})`,
    );
    created.push(name);
  }
  return created;
}

/** Drops partitions whose whole day is older than the cutoff. */
export async function dropExpiredPartitions(
  pool: Pool,
  keepDays: number,
  now = new Date(),
  table: PartitionedTable = "metric_points",
): Promise<string[]> {
  const cutoff = utcDay(now, -keepDays);
  const dropped: string[] = [];
  for (const name of await listDayPartitions(pool, table)) {
    const day = partitionDay(name, table);
    // A partition covers [day, day+1), so it is only fully expired once its
    // end is at or before the cutoff. Never drop a day still partly in window.
    if (!day || utcDay(day, 1) > cutoff) continue;
    await pool.query(`drop table ${name}`);
    dropped.push(name);
  }
  return dropped;
}

/**
 * Moves anything that landed in the default partition into a real one.
 *
 * Only reachable if a day arrived with no partition pre-created, which means
 * the timer has been down for longer than aheadDays. The detach is what makes
 * the re-insert legal: while attached, a row cannot move out of default into
 * an overlapping partition.
 */
export async function drainDefaultPartition(
  pool: Pool,
  table: PartitionedTable = "metric_points",
): Promise<number> {
  const defaultPartition = `${table}_default`;
  const column = table === "spans" ? "start_ts" : "ts";
  const { rows } = await pool.query<{ n: string }>(
    `select count(*) as n from ${defaultPartition}`,
  );
  const pending = Number(rows[0]?.n ?? 0);
  if (pending === 0) return 0;

  const { rows: dayRows } = await pool.query<{ day: Date }>(
    `select distinct (${column} at time zone 'UTC')::date as day from ${defaultPartition} order by day`,
  );

  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query(
      `alter table ${table} detach partition ${defaultPartition}`,
    );
    for (const { day } of dayRows) {
      const utc = utcDay(new Date(day));
      const { from, to } = partitionBounds(utc);
      await client.query(
        `create table if not exists ${partitionName(utc, table)}
           partition of ${table} for values from (${tsLiteral(from)}) to (${tsLiteral(to)})`,
      );
    }
    // The detached default keeps the parent's column order, so select * lines up.
    await client.query(`insert into ${table} select * from ${defaultPartition}`);
    await client.query(`truncate ${defaultPartition}`);
    await client.query(
      `alter table ${table} attach partition ${defaultPartition} default`,
    );
    await client.query("commit");
  } catch (err) {
    await client.query("rollback");
    throw err;
  } finally {
    client.release();
  }
  return pending;
}

export interface TableRetention {
  partitionsCreated: string[];
  partitionsDropped: string[];
  defaultRowsRescued: number;
}

export type RetentionReport = Record<PartitionedTable, TableRetention>;

export function keepDaysFor(table: PartitionedTable, config: RetentionConfig): number {
  if (table === "spans") return config.spanDays;
  if (table === "logs") return config.logDays;
  return config.metricDays;
}

export async function runRetention(
  pool: Pool,
  config: RetentionConfig = DEFAULT_RETENTION,
  now = new Date(),
): Promise<RetentionReport> {
  const report = {} as RetentionReport;
  for (const table of PARTITIONED_TABLES) {
    // Order matters: rescue before dropping, so a rescued row from an expired
    // day is dropped in the same run instead of lingering a day longer.
    const defaultRowsRescued = await drainDefaultPartition(pool, table);
    const partitionsCreated = await ensurePartitions(pool, config.aheadDays, now, table);
    const partitionsDropped = await dropExpiredPartitions(pool, keepDaysFor(table, config), now, table);
    report[table] = { partitionsCreated, partitionsDropped, defaultRowsRescued };
  }
  return report;
}
