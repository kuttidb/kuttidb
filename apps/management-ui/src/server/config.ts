import { z } from "zod";

const booleanFromEnvironment = z.enum(["true", "false"]).optional().transform((value) => value === "true");

const configSchema = z.object({
  HOST: z.string().default("0.0.0.0"),
  PORT: z.coerce.number().int().min(1).max(65535).default(8080),
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PUBLIC_ORIGIN: z.string().url().optional(),
  SESSION_IDLE_MINUTES: z.coerce.number().int().min(1).max(480).default(30),
  SESSION_MAX_MINUTES: z.coerce.number().int().min(1).max(1440).default(480),
  MAX_SESSIONS: z.coerce.number().int().min(1).max(10_000).default(100),
  MAX_CONNECTIONS_PER_SESSION: z.coerce.number().int().min(1).max(50).default(10),
  MAX_SSE_PER_SESSION: z.coerce.number().int().min(1).max(20).default(2),
  TARGET_ALLOWLIST: z.string().optional(),
  ALLOW_PRIVATE_TARGETS: booleanFromEnvironment,
  ALLOW_LOOPBACK_HTTP: booleanFromEnvironment,
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug"]).default("info")
});

export type GatewayConfig = {
  host: string;
  port: number;
  production: boolean;
  publicOrigin?: string;
  sessionIdleMs: number;
  sessionMaxMs: number;
  maxSessions: number;
  maxConnectionsPerSession: number;
  maxSsePerSession: number;
  targetAllowlist: string[];
  allowPrivateTargets: boolean;
  allowLoopbackHttp: boolean;
  logLevel: "fatal" | "error" | "warn" | "info" | "debug";
};

export function readConfig(environment: NodeJS.ProcessEnv = process.env): GatewayConfig {
  const value = configSchema.parse(environment);
  const production = value.NODE_ENV === "production";
  if (production && !value.PUBLIC_ORIGIN) {
    throw new Error("PUBLIC_ORIGIN is required in production.");
  }
  if (production && !value.TARGET_ALLOWLIST) {
    throw new Error("TARGET_ALLOWLIST is required in production.");
  }
  if (production && value.ALLOW_LOOPBACK_HTTP) {
    throw new Error("ALLOW_LOOPBACK_HTTP cannot be enabled in production.");
  }
  return {
    host: value.HOST,
    port: value.PORT,
    production,
    ...(value.PUBLIC_ORIGIN ? { publicOrigin: new URL(value.PUBLIC_ORIGIN).origin } : {}),
    sessionIdleMs: value.SESSION_IDLE_MINUTES * 60_000,
    sessionMaxMs: value.SESSION_MAX_MINUTES * 60_000,
    maxSessions: value.MAX_SESSIONS,
    maxConnectionsPerSession: value.MAX_CONNECTIONS_PER_SESSION,
    maxSsePerSession: value.MAX_SSE_PER_SESSION,
    targetAllowlist: value.TARGET_ALLOWLIST?.split(",").map((entry) => entry.trim().toLowerCase()).filter(Boolean) ?? [],
    allowPrivateTargets: value.ALLOW_PRIVATE_TARGETS,
    allowLoopbackHttp: value.ALLOW_LOOPBACK_HTTP,
    logLevel: value.LOG_LEVEL
  };
}
