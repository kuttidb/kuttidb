const sensitiveKeys = new Set(["authorization", "cookie", "set-cookie", "token", "adminToken"]);

export function redact(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(redact);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, sensitiveKeys.has(key.toLowerCase()) ? "[REDACTED]" : redact(item)]));
}
