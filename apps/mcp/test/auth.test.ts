import { describe, expect, test } from "bun:test";
import { createLocalJWKSet, exportJWK, generateKeyPair, SignJWT, type JWTPayload } from "jose";
import type { Pool } from "@sensorium/db";
import { createMcpAuth, mcpAuthConfigFromEnv, type McpAuthConfig } from "../src/auth.js";
import { createMcpApp } from "../src/server.js";

const ISSUER = "https://id.example.com/realms/lab";
const RESOURCE = "https://sensorium.example.com/mcp";
const { publicKey, privateKey } = await generateKeyPair("RS256");
const jwk = { ...(await exportJWK(publicKey)), kid: "k1", alg: "RS256" };
const keys = createLocalJWKSet({ keys: [jwk] });
const other = await generateKeyPair("RS256");

async function token(claims: JWTPayload = {}, key = privateKey, header: Record<string, string> = {}) {
  const now = Math.floor(Date.now() / 1000);
  return new SignJWT({
    typ: "Bearer",
    azp: "sensorium-mcp",
    sub: "u1",
    scope: "openid",
    iss: ISSUER,
    aud: RESOURCE,
    iat: now,
    exp: now + 300,
    ...claims,
  })
    .setProtectedHeader({ alg: "RS256", kid: "k1", ...header })
    .sign(key);
}

const oidcConfig: McpAuthConfig = {
  staticToken: "static-secret",
  oidc: { issuer: ISSUER, resource: RESOURCE, clientIds: ["sensorium-mcp"], role: "sensorium-reader" },
};
const fakePool = { query: async () => ({ rows: [] }) } as unknown as Pool;
const app = createMcpApp(fakePool, createMcpAuth(oidcConfig, keys));

function mcp(authorization?: string) {
  return new Request("http://x/mcp", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      ...(authorization ? { authorization } : {}),
    },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
  });
}

async function status(authorization?: string) {
  return (await app.fetch(mcp(authorization))).status;
}

const reader = { realm_access: { roles: ["sensorium-reader"] } };

describe("mcp auth", () => {
  test("the static token and a valid access token both list tools", async () => {
    for (const auth of ["Bearer static-secret", `Bearer ${await token(reader)}`]) {
      const res = await app.fetch(mcp(auth));
      expect(res.status).toBe(200);
      const text = await res.text();
      expect(text).toContain("list_projects");
    }
  });

  test("a missing or wrong token is 401 with a challenge naming the metadata", async () => {
    for (const auth of [undefined, "Bearer nope", "Basic abc"]) {
      const res = await app.fetch(mcp(auth));
      expect(res.status).toBe(401);
      expect(res.headers.get("www-authenticate")).toBe(
        'Bearer resource_metadata="https://sensorium.example.com/.well-known/oauth-protected-resource/mcp", error="invalid_token"',
      );
    }
  });

  test("tokens for another audience, issuer, client, or signed by another key are refused", async () => {
    expect(await status(`Bearer ${await token({ ...reader, aud: "https://other.example.com/mcp" })}`)).toBe(401);
    expect(await status(`Bearer ${await token({ ...reader, iss: "https://evil.example.com" })}`)).toBe(401);
    expect(await status(`Bearer ${await token({ ...reader, azp: "some-other-app" })}`)).toBe(401);
    expect(await status(`Bearer ${await token(reader, other.privateKey)}`)).toBe(401);
  });

  test("an ID token with the same audience is refused", async () => {
    expect(await status(`Bearer ${await token({ ...reader, typ: "ID" })}`)).toBe(401);
  });

  test("an RFC 9068 token (typ at+jwt in the header, none in the payload) is accepted", async () => {
    expect(await status(`Bearer ${await token({ ...reader, typ: undefined }, privateKey, { typ: "at+jwt" })}`)).toBe(200);
  });

  test("a token with neither marker is refused", async () => {
    expect(await status(`Bearer ${await token({ ...reader, typ: undefined })}`)).toBe(401);
  });

  test("a signed token without exp is refused", async () => {
    expect(await status(`Bearer ${await token({ ...reader, exp: undefined })}`)).toBe(401);
  });

  test("an expired token is refused", async () => {
    expect(await status(`Bearer ${await token({ ...reader, exp: Math.floor(Date.now() / 1000) - 60 })}`)).toBe(401);
  });

  test("a valid token without the required role is 403", async () => {
    expect(await status(`Bearer ${await token({ realm_access: { roles: ["member"] } })}`)).toBe(403);
  });

  test("an issuer whose keys cannot be fetched is 503, not 401", async () => {
    const down = createMcpApp(fakePool, createMcpAuth({ ...oidcConfig, oidc: { ...oidcConfig.oidc!, issuer: "http://127.0.0.1:9" } }));
    const res = await down.fetch(mcp(`Bearer ${await token(reader)}`));
    expect(res.status).toBe(503);
    expect(res.headers.get("retry-after")).toBe("5");
  });

  test("a failed discovery is not retried on every request", async () => {
    let hits = 0;
    const issuer = Bun.serve({ port: 0, fetch: () => (hits++, new Response("down", { status: 500 })) });
    try {
      const auth = createMcpAuth({ ...oidcConfig, oidc: { ...oidcConfig.oidc!, issuer: `http://127.0.0.1:${issuer.port}` } });
      const app = createMcpApp(fakePool, auth);
      for (let i = 0; i < 3; i++) expect((await app.fetch(mcp(`Bearer ${await token(reader)}`))).status).toBe(503);
      expect(hits).toBe(1);
    } finally {
      issuer.stop(true);
    }
  });

  test("a 403 tells the client the scope is insufficient", async () => {
    const res = await app.fetch(mcp(`Bearer ${await token({ realm_access: { roles: ["member"] } })}`));
    expect(res.status).toBe(403);
    expect(res.headers.get("www-authenticate")).toBe(`Bearer error="insufficient_scope"`);
  });

  test("a 2025 client gets one JSON body, not an SSE stream", async () => {
    const res = await app.fetch(mcp("Bearer static-secret"));
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("application/json");
    const body = (await res.json()) as { result: { tools: unknown[] } };
    expect(body.result.tools.length).toBeGreaterThan(0);
  });

  test("a body that is not JSON is a parse error", async () => {
    const res = await app.fetch(new Request("http://x/mcp", { method: "POST", headers: { authorization: "Bearer static-secret", "content-type": "application/json" }, body: "{" }));
    expect(res.status).toBe(400);
  });

  test("the protected resource metadata names the issuer", async () => {
    const res = await app.fetch(new Request("http://x/.well-known/oauth-protected-resource/mcp"));
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ resource: RESOURCE, authorization_servers: [ISSUER] });
  });

  test("with only the static token there is no metadata and the challenge has none", async () => {
    const plain = createMcpApp(fakePool, { staticToken: "s" });
    expect((await plain.fetch(new Request("http://x/.well-known/oauth-protected-resource/mcp"))).status).toBe(404);
    const res = await plain.fetch(mcp("Bearer wrong"));
    expect(res.status).toBe(401);
    expect(res.headers.get("www-authenticate")).toBe('Bearer error="invalid_token"');
    expect((await plain.fetch(mcp("Bearer s"))).status).toBe(200);
  });
});

describe("mcpAuthConfigFromEnv", () => {
  test("needs at least one way in", () => {
    expect(() => mcpAuthConfigFromEnv({})).toThrow();
    expect(mcpAuthConfigFromEnv({ SENSORIUM_MCP_TOKEN: "t" })).toEqual({ staticToken: "t" });
  });

  test("an issuer needs the resource URL and client ids", () => {
    expect(() => mcpAuthConfigFromEnv({ SENSORIUM_MCP_OIDC_ISSUER: ISSUER })).toThrow(/RESOURCE/);
    expect(() =>
      mcpAuthConfigFromEnv({ SENSORIUM_MCP_OIDC_ISSUER: ISSUER, SENSORIUM_MCP_RESOURCE: RESOURCE }),
    ).toThrow(/CLIENT_IDS/);
    expect(
      mcpAuthConfigFromEnv({
        SENSORIUM_MCP_OIDC_ISSUER: `${ISSUER}/`,
        SENSORIUM_MCP_RESOURCE: RESOURCE,
        SENSORIUM_MCP_OIDC_CLIENT_IDS: "a, b",
      }),
    ).toEqual({ staticToken: undefined, oidc: { issuer: ISSUER, resource: RESOURCE, clientIds: ["a", "b"], role: undefined } });
  });
});
