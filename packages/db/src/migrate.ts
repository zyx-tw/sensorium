import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Pool, PoolClient } from "pg";

const MIGRATIONS_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "migrations");

/** pg_advisory_lock key held while migrations run ("sensoriu" as ASCII). */
const MIGRATION_LOCK = 0x73656e736f726975n.toString();

const BOOTSTRAP_SQL = `
create table if not exists schema_migrations (
  filename text primary key,
  applied_at timestamptz not null default now()
);
`;

export interface MigrateOptions {
  /** Stop after this file (inclusive), so a test can build an older schema. */
  until?: string;
  /**
   * How long one migration waits for a table lock before it gives up and tries
   * again. A migration that waits holds the locks it already has, so without a
   * limit one long read can stall ingest behind it.
   */
  lockTimeoutMs?: number;
  /** Tries per migration when it times out on a lock. */
  lockAttempts?: number;
}

/** SQLSTATE lock_not_available, raised when lock_timeout expires. */
const LOCK_NOT_AVAILABLE = "55P03";

/** Applies every *.sql file in migrations/ that isn't already recorded in schema_migrations. Safe to re-run. */
export async function runMigrations(pool: Pool, options: MigrateOptions = {}): Promise<{ applied: string[]; skipped: string[] }> {
  // One migrator at a time per database: two processes (or two test files)
  // starting together would otherwise both apply the same file.
  const lock = await pool.connect();
  try {
    await lock.query("select pg_advisory_lock($1)", [MIGRATION_LOCK]);
    return await migrateLocked(lock, options);
  } finally {
    await lock.query("select pg_advisory_unlock($1)", [MIGRATION_LOCK]).catch(() => {});
    lock.release();
  }
}

async function migrateLocked(
  client: PoolClient,
  { until, lockTimeoutMs = 5_000, lockAttempts = 10 }: MigrateOptions,
): Promise<{ applied: string[]; skipped: string[] }> {
  await client.query(BOOTSTRAP_SQL);

  // `until` (inclusive) stops early, so a test can build an older schema and then migrate it.
  const files = (await readdir(MIGRATIONS_DIR))
    .filter((f) => f.endsWith(".sql") && (until === undefined || f <= until))
    .sort();
  const { rows } = await client.query<{ filename: string }>("select filename from schema_migrations");
  const applied = new Set(rows.map((r) => r.filename));

  const newlyApplied: string[] = [];
  const skipped: string[] = [];

  for (const file of files) {
    if (applied.has(file)) {
      skipped.push(file);
      continue;
    }
    const sql = await readFile(path.join(MIGRATIONS_DIR, file), "utf8");
    for (let attempt = 1; ; attempt++) {
      try {
        await client.query("begin");
        await client.query(`set local lock_timeout = ${Math.max(1, Math.floor(lockTimeoutMs))}`);
        await client.query(sql);
        await client.query("insert into schema_migrations (filename) values ($1)", [file]);
        await client.query("commit");
        break;
      } catch (err) {
        await client.query("rollback");
        const code = (err as { code?: string }).code;
        if (code === LOCK_NOT_AVAILABLE && attempt < lockAttempts) {
          // Back off so the reads holding the lock can finish.
          await new Promise((resolve) => setTimeout(resolve, Math.min(10_000, 500 * attempt)));
          continue;
        }
        throw new Error(`migration ${file} failed: ${(err as Error).message}`, { cause: err });
      }
    }
    newlyApplied.push(file);
  }

  return { applied: newlyApplied, skipped };
}
