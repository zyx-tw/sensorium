import { Pool, type PoolConfig } from "pg";

let pool: Pool | undefined;

/** Lazily-created singleton pool, sourced from DATABASE_URL. Call `closePool()` to shut down (tests, CLIs). */
export function getPool(config?: PoolConfig): Pool {
  if (!pool) {
    const connectionString = config?.connectionString ?? process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error("DATABASE_URL is not set");
    }
    pool = new Pool({ connectionString, ...config });
  }
  return pool;
}

/** A pool of its own, outside the singleton (tests that need two sessions with different settings). */
export function createPool(config: PoolConfig): Pool {
  return new Pool(config);
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = undefined;
  }
}

export type { Pool } from "pg";
