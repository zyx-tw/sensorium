import { getProjectByToken, type Pool } from "@sensorium/db";

/** How long a resolved token stays cached. A rotated token keeps working for at most this long. */
export const TOKEN_CACHE_MS = 60_000;

export interface IngestAuth {
  /** The project bound to the request's bearer token, or null when the token is missing or unknown. */
  authenticate(req: Request): Promise<string | null>;
}

/**
 * Resolves the ingest bearer token to its bound project. This project name
 * is authoritative: callers may claim any `service.namespace` in the OTLP
 * payload, but only the token's binding decides where rows land.
 *
 * Known tokens are cached for TOKEN_CACHE_MS so a busy producer costs no lookup per
 * request; unknown tokens are never cached, so a newly registered project works at once.
 * A database error propagates, so the caller can answer with a retryable status.
 */
export function createIngestAuth(pool: Pool, now: () => number = Date.now): IngestAuth {
  const cache = new Map<string, { project: string; expires: number }>();
  return {
    async authenticate(req) {
      const header = req.headers.get("authorization");
      if (!header?.startsWith("Bearer ")) return null;
      const token = header.slice("Bearer ".length).trim();
      if (!token) return null;
      const hit = cache.get(token);
      if (hit && hit.expires > now()) return hit.project;
      const project = await getProjectByToken(pool, token);
      if (project) {
        if (cache.size > 1000) cache.clear();
        cache.set(token, { project, expires: now() + TOKEN_CACHE_MS });
      } else {
        cache.delete(token);
      }
      return project;
    },
  };
}
