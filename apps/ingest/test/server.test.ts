import { describe, expect, test } from "bun:test";
import { gzipSync } from "node:zlib";
import type { Pool } from "@sensorium/db";
import { createIngestApp, isPermanentDbError, LAST_SEEN_INTERVAL_MS } from "../src/server.js";
import { encodeRpcStatus } from "../src/otlp-response.js";

const nowNanos = () => String(BigInt(Date.now()) * 1_000_000n);
const logBody = () =>
  JSON.stringify({
    resourceLogs: [
      { scopeLogs: [{ logRecords: [{ timeUnixNano: nowNanos(), severityText: "INFO", body: { stringValue: "hi" } }] }] },
    ],
  });

/** A pg.Pool stand-in: the first query of each statement kind is handled by `on`. */
function fakePool(on: (sql: string) => unknown) {
  const calls: string[] = [];
  const query = async (sql: string) => {
    calls.push(sql);
    const out = on(sql);
    if (out instanceof Error) throw out;
    return (out ?? { rows: [] }) as { rows: unknown[] };
  };
  const pool = { query, connect: async () => ({ query, release() {} }) } as unknown as Pool;
  return { pool, calls };
}

const tokenOk = (sql: string) => (sql.startsWith("select name from projects") ? { rows: [{ name: "p" }] } : undefined);
const dbError = (code: string) => Object.assign(new Error("db"), { code });

function post(path: string, body: BodyInit, headers: Record<string, string> = {}) {
  return new Request(`http://x${path}`, {
    method: "POST",
    body,
    headers: { authorization: "Bearer t", "content-type": "application/json", ...headers },
  });
}

describe("ingest status codes", () => {
  test("a stored export is 200", async () => {
    const { pool } = fakePool(tokenOk);
    const res = await createIngestApp(pool).fetch(post("/v1/logs", logBody()));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true, inserted: 1 });
  });

  test("disk full and lost connections are 503 with Retry-After, so exporters retry", async () => {
    for (const err of [dbError("53100"), dbError("57P03"), dbError("ECONNREFUSED"), new Error("Connection terminated")]) {
      const { pool } = fakePool((sql) => tokenOk(sql) ?? (sql.startsWith("insert") ? err : undefined));
      const res = await createIngestApp(pool).fetch(post("/v1/logs", logBody()));
      expect(res.status).toBe(503);
      expect(res.headers.get("retry-after")).toBe("5");
    }
  });

  test("a token lookup that fails is 503, not 500 or 401", async () => {
    const { pool } = fakePool(() => dbError("ECONNREFUSED"));
    const res = await createIngestApp(pool).fetch(post("/v1/logs", logBody()));
    expect(res.status).toBe(503);
  });

  test("data the database rejects is 400 (never retried)", async () => {
    const { pool } = fakePool((sql) => tokenOk(sql) ?? (sql.startsWith("insert") ? dbError("22P02") : undefined));
    expect((await createIngestApp(pool).fetch(post("/v1/logs", logBody()))).status).toBe(400);
    expect(isPermanentDbError(dbError("23505"))).toBe(true);
    expect(isPermanentDbError(dbError("53100"))).toBe(false);
    expect(isPermanentDbError(new Error("x"))).toBe(false);
  });

  test("a body that is not JSON is 400 and nothing is inserted", async () => {
    const { pool, calls } = fakePool(tokenOk);
    expect((await createIngestApp(pool).fetch(post("/v1/logs", "{nope"))).status).toBe(400);
    expect(calls.some((c) => c.startsWith("insert"))).toBe(false);
  });

  test("an unknown token is 401", async () => {
    const { pool } = fakePool(() => undefined);
    expect((await createIngestApp(pool).fetch(post("/v1/logs", logBody()))).status).toBe(401);
  });

  test("more exports in flight than allowed is 429", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const { pool } = fakePool(tokenOk);
    const slowPool = {
      ...pool,
      query: async (sql: string) => {
        if (sql.startsWith("insert")) await gate;
        return pool.query(sql);
      },
    } as unknown as Pool;
    const app = createIngestApp(slowPool, { maxInFlight: 1 });
    const first = app.fetch(post("/v1/logs", logBody()));
    await Bun.sleep(5);
    const second = await app.fetch(post("/v1/logs", logBody()));
    expect(second.status).toBe(429);
    expect(second.headers.get("retry-after")).toBe("5");
    release();
    expect((await first).status).toBe(200);
  });
});

describe("ingest gzip and size caps", () => {
  test("a gzip body is accepted", async () => {
    const { pool } = fakePool(tokenOk);
    const res = await createIngestApp(pool).fetch(post("/v1/logs", gzipSync(logBody()), { "content-encoding": "gzip" }));
    expect(res.status).toBe(200);
  });

  test("a body that decompresses past the cap is 413", async () => {
    const { pool } = fakePool(tokenOk);
    const bomb = gzipSync(Buffer.alloc(1024 * 1024, 0x20));
    const res = await createIngestApp(pool, { maxBodyBytes: 64 * 1024 }).fetch(
      post("/v1/logs", bomb, { "content-encoding": "gzip" }),
    );
    expect(res.status).toBe(413);
  });

  test("broken gzip is 400 and an unknown encoding is 415", async () => {
    const { pool } = fakePool(tokenOk);
    const app = createIngestApp(pool);
    expect((await app.fetch(post("/v1/logs", "not gzip", { "content-encoding": "gzip" }))).status).toBe(400);
    expect((await app.fetch(post("/v1/logs", logBody(), { "content-encoding": "br" }))).status).toBe(415);
  });
});

describe("ingest protobuf responses", () => {
  test("success is an empty protobuf body", async () => {
    const { pool } = fakePool(tokenOk);
    const res = await createIngestApp(pool).fetch(
      post("/v1/logs", new Uint8Array(), { "content-type": "application/x-protobuf" }),
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("application/x-protobuf");
    expect((await res.arrayBuffer()).byteLength).toBe(0);
  });

  test("failure is a google.rpc.Status in protobuf", async () => {
    const { pool } = fakePool(() => undefined);
    const res = await createIngestApp(pool).fetch(
      post("/v1/logs", new Uint8Array(), { "content-type": "application/x-protobuf" }),
    );
    expect(res.status).toBe(401);
    expect(res.headers.get("content-type")).toBe("application/x-protobuf");
    expect(new Uint8Array(await res.arrayBuffer())).toEqual(encodeRpcStatus(16, "unauthorized"));
  });

  test("encodeRpcStatus writes code then message", () => {
    expect([...encodeRpcStatus(14, "x")]).toEqual([0x08, 14, 0x12, 1, 0x78]);
  });
});

describe("ingest per-request database work", () => {
  test("a known token is looked up once, and last_seen is written once per interval", async () => {
    let clock = 1_000_000;
    const { pool, calls } = fakePool(tokenOk);
    const app = createIngestApp(pool, { now: () => clock });
    for (let i = 0; i < 5; i++) expect((await app.fetch(post("/v1/logs", logBody()))).status).toBe(200);
    expect(calls.filter((c) => c.startsWith("select name from projects")).length).toBe(1);
    expect(calls.filter((c) => c.startsWith("update projects")).length).toBe(1);
    clock += LAST_SEEN_INTERVAL_MS;
    await app.fetch(post("/v1/logs", logBody()));
    expect(calls.filter((c) => c.startsWith("update projects")).length).toBe(2);
  });

  test("an unknown token is not cached, so a project registered later works at once", async () => {
    let registered = false;
    const { pool } = fakePool((sql) => (registered ? tokenOk(sql) : undefined));
    const app = createIngestApp(pool);
    expect((await app.fetch(post("/v1/logs", logBody()))).status).toBe(401);
    registered = true;
    expect((await app.fetch(post("/v1/logs", logBody()))).status).toBe(200);
  });
});
