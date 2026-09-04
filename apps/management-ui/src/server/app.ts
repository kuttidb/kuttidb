import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import cookie from "@fastify/cookie";
import fastifyStatic from "@fastify/static";
import Fastify, { type FastifyInstance, type FastifyRequest } from "fastify";
import { z } from "zod";
import { ConnectionStore, newRequestId } from "./connection-store.js";
import type { GatewayConfig } from "./config.js";
import { redact } from "./redaction.js";
import { TargetPolicyError, validateTarget } from "./target-policy.js";

const sessionCookie = "kuttidb_console";
const connectionInput = z.object({
  profileId: z.string().uuid(),
  endpoint: z.string().min(1).max(2_048),
  token: z.string().min(1).max(1_024)
});

const safeProxyHeaders = new Set(["accept", "content-type", "idempotency-key", "if-match", "x-kuttidb-confirm", "x-kuttidb-request-id"]);
const safeResponseHeaders = new Set(["content-type", "etag", "retry-after", "cache-control", "x-kuttidb-request-id"]);

export async function buildApp(config: GatewayConfig, staticRoot?: string): Promise<FastifyInstance> {
  const app = Fastify({ logger: { level: config.logLevel, redact: { paths: ["req.headers.authorization", "req.headers.cookie", "req.body.token"], censor: "[REDACTED]" } }, bodyLimit: 262_144 });
  const store = new ConnectionStore({ maxSessions: config.maxSessions, maxConnections: config.maxConnectionsPerSession, idleMs: config.sessionIdleMs, maxMs: config.sessionMaxMs });
  await app.register(cookie);

  app.addHook("onRequest", async (request, reply) => {
    if (request.url.startsWith("/ui-api/") && !["GET", "HEAD", "OPTIONS"].includes(request.method)) {
      const origin = request.headers.origin;
      if (config.publicOrigin && origin !== config.publicOrigin) return reply.code(403).send({ error: { code: "forbidden_origin", message: "This console only accepts requests from its configured origin." } });
      if (config.production && !origin) return reply.code(403).send({ error: { code: "forbidden_origin", message: "An Origin header is required." } });
    }
  });

  app.get("/health/live", async () => ({ status: "ok" }));
  app.get("/health/ready", async () => ({ status: "ready" }));
  app.get("/ui-api/session", async (request) => {
    const session = store.get(request.cookies[sessionCookie]);
    return { authenticated: Boolean(session), connections: session ? store.list(session) : [] };
  });

  app.post("/ui-api/connections", async (request, reply) => {
    const parsed = connectionInput.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: { code: "validation_failed", message: "Enter a valid profile, endpoint, and token." } });
    try {
      const endpoint = validateTarget(parsed.data.endpoint, config);
      const capabilities = await fetchCapabilities(endpoint, parsed.data.token);
      const session = store.get(request.cookies[sessionCookie]) ?? store.create();
      const connection = store.connect(session, { ...parsed.data, endpoint: endpoint.origin, capabilities });
      reply.setCookie(sessionCookie, session.id, { httpOnly: true, sameSite: "strict", secure: config.production, path: "/" });
      return reply.code(201).send({ connection });
    } catch (error) {
      return sendSafeError(reply, error);
    }
  });

  app.get("/ui-api/connections/:profileId", async (request, reply) => {
    const session = store.get(request.cookies[sessionCookie]);
    const connection = session && store.connection(session, (request.params as { profileId: string }).profileId);
    if (!connection) return reply.code(401).send({ error: { code: "unauthorized", message: "This connection is locked. Enter its token to reconnect." } });
    return { connection: store.safe(connection) };
  });

  app.delete("/ui-api/connections/:profileId", async (request, reply) => {
    const session = store.get(request.cookies[sessionCookie]);
    if (!session) return reply.code(204).send();
    store.disconnect(session, (request.params as { profileId: string }).profileId);
    return reply.code(204).send();
  });

  app.post("/ui-api/lock", async (request, reply) => {
    const session = store.get(request.cookies[sessionCookie]);
    if (session) store.lock(session);
    reply.clearCookie(sessionCookie, { path: "/" });
    return reply.code(204).send();
  });

  app.all("/ui-api/connections/:profileId/admin/*", async (request, reply) => {
    if (!new Set(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"]).has(request.method)) return reply.code(405).send({ error: { code: "method_not_allowed", message: "This method is not supported by the console gateway." } });
    const session = store.get(request.cookies[sessionCookie]);
    const params = request.params as { profileId: string; "*": string };
    const connection = session && store.connection(session, params.profileId);
    if (!connection) return reply.code(401).send({ error: { code: "unauthorized", message: "This connection is locked. Enter its token to reconnect." } });
    const relativePath = params["*"];
    if (!isSafeAdminPath(relativePath)) return reply.code(400).send({ error: { code: "validation_failed", message: "Invalid Management API path." } });
    const requestId = typeof request.headers["x-kuttidb-request-id"] === "string" ? request.headers["x-kuttidb-request-id"] : newRequestId();
    const headers = new Headers({ authorization: `Bearer ${connection.token.toString("utf8")}`, "x-kuttidb-request-id": requestId });
    for (const [key, value] of Object.entries(request.headers)) {
      if (safeProxyHeaders.has(key.toLowerCase()) && typeof value === "string") headers.set(key, value);
    }
    const query = request.url.includes("?") ? request.url.slice(request.url.indexOf("?")) : "";
    const upstream = new URL(`/api/admin/v1/${relativePath}${query}`, connection.endpoint);
    try {
      const requestBody = request.method === "GET" || request.method === "HEAD" || !request.body ? undefined : JSON.stringify(request.body);
      const response = await fetch(upstream, { method: request.method, headers, ...(requestBody ? { body: requestBody } : {}), redirect: "error", signal: AbortSignal.timeout(30_000) });
      for (const [key, value] of response.headers) if (safeResponseHeaders.has(key.toLowerCase())) reply.header(key, value);
      const body = Buffer.from(await response.arrayBuffer());
      if (body.byteLength > 1_048_576) return reply.code(502).send({ error: { code: "response_too_large", message: "The Management API response exceeded the console safety limit." } });
      return reply.code(response.status).type(response.headers.get("content-type") ?? "application/json").send(body);
    } catch (error) {
      request.log.warn({ err: redact(error) }, "admin proxy request failed");
      return reply.code(502).send({ error: { code: "upstream_unavailable", message: "The KuttiDB Management API could not be reached." } });
    }
  });

  if (staticRoot) {
    await app.register(fastifyStatic, { root: staticRoot, wildcard: false });
    app.get("/*", async (_request, reply) => reply.sendFile("index.html"));
  }
  return app;
}

async function fetchCapabilities(endpoint: URL, token: string): Promise<unknown> {
  const response = await fetch(new URL("/api/admin/v1/capabilities", endpoint), { headers: { authorization: `Bearer ${token}`, accept: "application/json" }, redirect: "error", signal: AbortSignal.timeout(10_000) });
  const requestId = response.headers.get("x-kuttidb-request-id");
  if (!response.ok) throw new GatewayUpstreamError(response.status === 401 ? "The token was not accepted by KuttiDB." : "KuttiDB did not accept this connection.", requestId ?? undefined);
  const body: unknown = await response.json();
  const parsed = z.object({ product: z.literal("KuttiDB"), management_api_contract: z.literal("1.0") }).passthrough().safeParse(body);
  if (!parsed.success) throw new GatewayUpstreamError("This target does not expose a supported KuttiDB Management API.", requestId ?? undefined);
  return parsed.data;
}

class GatewayUpstreamError extends Error {
  constructor(message: string, readonly requestId?: string) { super(message); }
}

function sendSafeError(reply: { code: (status: number) => { send: (value: unknown) => unknown } }, error: unknown): unknown {
  if (error instanceof TargetPolicyError) return reply.code(400).send({ error: { code: "forbidden_origin", message: error.message } });
  if (error instanceof GatewayUpstreamError) return reply.code(502).send({ error: { code: "upstream_unavailable", message: error.message, ...(error.requestId ? { requestId: error.requestId } : {}) } });
  if (error instanceof Error && error.message === "connection_limit") return reply.code(429).send({ error: { code: "resource_exhausted", message: "This browser session has reached its connection limit." } });
  return reply.code(502).send({ error: { code: "upstream_unavailable", message: "The KuttiDB Management API could not be reached." } });
}

function isSafeAdminPath(path: string): boolean {
  return Boolean(path) && !path.includes("//") && !path.split("/").some((segment) => segment === ".." || segment === ".") && !path.includes("://");
}

export function productionStaticRoot(): string {
  return resolve(dirname(fileURLToPath(import.meta.url)), "../client");
}
