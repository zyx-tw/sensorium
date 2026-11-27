import { timingSafeEqual } from "node:crypto";
import { createRemoteJWKSet, errors, jwtVerify, type JWTPayload, type JWTVerifyGetKey } from "jose";

/**
 * Who may call /mcp. Two ways in, either or both configured:
 *
 * - a static bearer token (SENSORIUM_MCP_TOKEN), one shared secret for trusted readers;
 * - OAuth access tokens from an OpenID Connect issuer (SENSORIUM_MCP_OIDC_ISSUER), checked
 *   as a resource server: signature against the issuer's keys, issuer, audience equal to
 *   this server's /mcp URL, an access token (not an ID token), issued to an allowed client,
 *   and carrying a realm role unless SENSORIUM_MCP_OIDC_ROLE is "*".
 *
 * The protected resource metadata (RFC 9728) and the 401 challenge that points at it are
 * served only when the issuer is configured.
 */
export interface McpAuthConfig {
  staticToken?: string;
  oidc?: {
    issuer: string;
    /** This server's /mcp URL: the token audience, and the `resource` in the metadata. */
    resource: string;
    /** `azp` values accepted: the MCP clients registered at the issuer for this server. */
    clientIds: string[];
    /** A role the caller must hold in `realm_access.roles` (Keycloak), when set. */
    role?: string;
  };
}

export type McpCaller = { kind: "static" } | { kind: "oidc"; subject: string; clientId: string; scopes: string[] };

export class McpAuthError extends Error {
  constructor(
    readonly status: 401 | 403 | 503,
    message: string,
  ) {
    super(message);
  }
}

function list(value: string | undefined): string[] {
  return (value ?? "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

export function mcpAuthConfigFromEnv(env: Record<string, string | undefined> = process.env): McpAuthConfig {
  const staticToken = env.SENSORIUM_MCP_TOKEN?.trim() || undefined;
  const issuer = env.SENSORIUM_MCP_OIDC_ISSUER?.trim().replace(/\/+$/, "") || undefined;
  if (!staticToken && !issuer) {
    throw new Error("set SENSORIUM_MCP_TOKEN, SENSORIUM_MCP_OIDC_ISSUER, or both");
  }
  if (!issuer) return { staticToken };
  if (!URL.canParse(issuer) || !/^https?:$/.test(new URL(issuer).protocol)) {
    throw new Error("SENSORIUM_MCP_OIDC_ISSUER must be an http(s) URL, such as https://auth.example.org/realms/main");
  }
  const resource = env.SENSORIUM_MCP_RESOURCE?.trim();
  const clientIds = list(env.SENSORIUM_MCP_OIDC_CLIENT_IDS);
  if (!resource || !URL.canParse(resource)) {
    throw new Error("SENSORIUM_MCP_OIDC_ISSUER needs SENSORIUM_MCP_RESOURCE, this server's public /mcp URL");
  }
  if (clientIds.length === 0) {
    throw new Error("SENSORIUM_MCP_OIDC_ISSUER needs SENSORIUM_MCP_OIDC_CLIENT_IDS, the MCP client ids allowed");
  }
  // Without a role, every user of the realm who signs in through an allowed client
  // reads every project. Make that a choice: "*" accepts any realm user.
  const role = env.SENSORIUM_MCP_OIDC_ROLE?.trim();
  if (!role) {
    throw new Error('SENSORIUM_MCP_OIDC_ISSUER needs SENSORIUM_MCP_OIDC_ROLE, the realm role a caller must hold ("*" for any realm user)');
  }
  return {
    staticToken,
    oidc: { issuer, resource, clientIds, role: role === "*" ? undefined : role },
  };
}

function safeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  return bufA.length === bufB.length && timingSafeEqual(bufA, bufB);
}

type KeySet = JWTVerifyGetKey;

/** How long a failed discovery is reused before the issuer is asked again. */
export const DISCOVERY_RETRY_MS = 5_000;

export function createMcpAuth(config: McpAuthConfig, keySet?: KeySet) {
  const { oidc } = config;
  let keys: KeySet | undefined = keySet;
  let keysPending: Promise<KeySet> | undefined;
  // A failed discovery is remembered for a few seconds, so an issuer that is
  // down is not asked again on every request.
  let failed: { at: number; error: unknown } | undefined;

  /** The issuer's key set, from its discovery document (fetched once). */
  async function issuerKeys(): Promise<KeySet> {
    if (keys) return keys;
    if (failed && Date.now() - failed.at < DISCOVERY_RETRY_MS) throw failed.error;
    keysPending ??= (async () => {
      const res = await fetch(`${oidc!.issuer}/.well-known/openid-configuration`, {
        signal: AbortSignal.timeout(5_000),
      });
      if (!res.ok) throw new Error(`discovery answered ${res.status}`);
      const { jwks_uri } = (await res.json()) as { jwks_uri?: string };
      if (!jwks_uri) throw new Error("discovery has no jwks_uri");
      return (keys = createRemoteJWKSet(new URL(jwks_uri)));
    })().catch((error: unknown) => {
      failed = { at: Date.now(), error };
      throw error;
    }).finally(() => {
      keysPending = undefined;
    });
    return keysPending;
  }

  async function verifyOidc(token: string): Promise<McpCaller> {
    let claims: JWTPayload;
    let headerType: string | undefined;
    try {
      const verified = await jwtVerify(token, await issuerKeys(), {
        issuer: oidc!.issuer,
        audience: oidc!.resource,
        // jose checks exp only when present; a token without one would never expire.
        requiredClaims: ["exp", "sub"],
      });
      claims = verified.payload;
      headerType = verified.protectedHeader.typ?.toLowerCase();
    } catch (err) {
      // The issuer's keys out of reach is an outage, not a bad token: a 401 would send
      // every client back to sign in. jose throws a bare JOSEError for a non-200 key set.
      if (
        !(err instanceof errors.JOSEError) ||
        err.constructor === errors.JOSEError ||
        err instanceof errors.JWKSTimeout ||
        err instanceof errors.JWKSInvalid
      ) {
        throw new McpAuthError(503, "cannot reach the issuer to check the token");
      }
      throw new McpAuthError(401, "invalid token");
    }
    // An ID token can carry the same issuer and audience; only access tokens are accepted:
    // RFC 9068 marks them in the signed header (typ at+jwt), Keycloak in the payload (typ Bearer).
    const accessToken = headerType === "at+jwt" || headerType === "application/at+jwt" || claims.typ === "Bearer";
    if (!accessToken) throw new McpAuthError(401, "not an access token");
    if (typeof claims.sub !== "string") throw new McpAuthError(401, "token has no subject");
    if (typeof claims.azp !== "string" || !oidc!.clientIds.includes(claims.azp)) {
      throw new McpAuthError(401, "token not issued for this MCP server");
    }
    if (oidc!.role) {
      const roles = (claims.realm_access as { roles?: unknown } | undefined)?.roles;
      if (!Array.isArray(roles) || !roles.includes(oidc!.role)) {
        throw new McpAuthError(403, "the account lacks the role this server requires");
      }
    }
    return {
      kind: "oidc",
      subject: claims.sub,
      clientId: claims.azp,
      scopes: String(claims.scope ?? "")
        .split(" ")
        .filter(Boolean),
    };
  }

  /** The caller of a request, or McpAuthError. */
  async function authenticate(req: Request): Promise<McpCaller> {
    const token = /^Bearer\s+(\S+)\s*$/i.exec(req.headers.get("authorization") ?? "")?.[1];
    if (!token) throw new McpAuthError(401, "missing bearer token");
    if (config.staticToken && safeEqual(token, config.staticToken)) return { kind: "static" };
    if (!oidc) throw new McpAuthError(401, "invalid token");
    return verifyOidc(token);
  }

  /** RFC 9728 metadata path for the resource (path-aware, as MCP clients look it up). */
  const metadataPath = oidc ? `/.well-known/oauth-protected-resource${new URL(oidc.resource).pathname}` : undefined;

  function metadataUrl(): string | undefined {
    return oidc ? new URL(metadataPath!, oidc.resource).toString() : undefined;
  }

  /** Serves the protected resource metadata, or undefined when the request is for something else. */
  function metadataResponse(req: Request, url: URL): Response | undefined {
    if (!oidc || req.method !== "GET") return undefined;
    if (url.pathname !== metadataPath && url.pathname !== "/.well-known/oauth-protected-resource") return undefined;
    return Response.json(
      {
        resource: oidc.resource,
        authorization_servers: [oidc.issuer],
        bearer_methods_supported: ["header"],
        scopes_supported: ["openid", "offline_access"],
        resource_name: "sensorium",
      },
      { headers: { "cache-control": "public, max-age=300", "access-control-allow-origin": "*" } },
    );
  }

  /** The JSON-RPC error a refused request gets; a 401 carries the challenge pointing at the metadata. */
  function refusal(err: McpAuthError): Response {
    const headers: Record<string, string> = {};
    if (err.status === 401) {
      const meta = metadataUrl();
      headers["www-authenticate"] = meta
        ? `Bearer resource_metadata="${meta}", error="invalid_token"`
        : `Bearer error="invalid_token"`;
    }
    if (err.status === 403) headers["www-authenticate"] = `Bearer error="insufficient_scope"`;
    if (err.status === 503) headers["retry-after"] = "5";
    return Response.json({ jsonrpc: "2.0", error: { code: -32000, message: err.message }, id: null }, { status: err.status, headers });
  }

  return { authenticate, metadataResponse, refusal };
}
