/**
 * Gateway-side redaction for anything that can end up in logs or error
 * reports. Delivery proofs and receipt request secrets are always redacted;
 * harmless operation ids are deliberately NOT redacted — operators need the
 * Completion ID to look up a receipt after a timeout or restart.
 */
const sensitiveKeys = new Set([
  "authorization",
  "cookie",
  "set-cookie",
  "token",
  "admintoken",
  // Opaque one-use delivery proof (POST /job-completions input, log echoes).
  "delivery_proof",
  "deliveryproof",
  "proof"
]);

export function redact(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(redact);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, sensitiveKeys.has(key.toLowerCase()) ? "[REDACTED]" : redact(item)]));
}
