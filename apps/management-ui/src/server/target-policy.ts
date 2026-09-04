import { isIP } from "node:net";
import type { GatewayConfig } from "./config.js";

const blockedIpv4 = /^(0|127|169\.254|224|2(2[4-9]|3[0-9])|255)(\.|$)/;
const loopbackHosts = new Set(["localhost", "127.0.0.1", "::1"]);

export class TargetPolicyError extends Error {}

export function validateTarget(input: string, config: GatewayConfig): URL {
  let endpoint: URL;
  try {
    endpoint = new URL(input);
  } catch {
    throw new TargetPolicyError("Enter a valid Management API origin.");
  }
  if (endpoint.username || endpoint.password || endpoint.search || endpoint.hash || (endpoint.pathname !== "/" && endpoint.pathname !== "")) {
    throw new TargetPolicyError("Use an origin only; credentials, paths, queries, and fragments are not allowed.");
  }
  if (endpoint.protocol !== "https:" && endpoint.protocol !== "http:") {
    throw new TargetPolicyError("Only HTTPS endpoints are accepted.");
  }
  const hostname = endpoint.hostname.toLowerCase();
  const isLoopback = loopbackHosts.has(hostname);
  if (endpoint.protocol === "http:" && !(config.allowLoopbackHttp && isLoopback)) {
    throw new TargetPolicyError("HTTP is allowed only for explicitly enabled loopback development targets.");
  }
  if (isBlockedAddress(hostname) && !(config.allowPrivateTargets && !isUnconditionallyBlocked(hostname)) && !isLoopback) {
    throw new TargetPolicyError("This address is not allowed by the target policy.");
  }
  if (config.targetAllowlist.length > 0 && !config.targetAllowlist.includes(hostname)) {
    throw new TargetPolicyError("This endpoint is not in the configured target allowlist.");
  }
  return endpoint;
}

function isUnconditionallyBlocked(hostname: string): boolean {
  return hostname === "0.0.0.0" || hostname.startsWith("169.254.") || hostname.startsWith("224.") || hostname === "::";
}

function isBlockedAddress(hostname: string): boolean {
  if (isIP(hostname) === 4) return blockedIpv4.test(hostname) || hostname.startsWith("10.") || hostname.startsWith("192.168.") || hostname.startsWith("172.16.") || hostname.startsWith("172.17.") || hostname.startsWith("172.18.") || hostname.startsWith("172.19.") || hostname.startsWith("172.2") || hostname.startsWith("172.30.") || hostname.startsWith("172.31.");
  if (isIP(hostname) === 6) return hostname === "::" || hostname.startsWith("fe80:") || hostname.startsWith("ff");
  return false;
}
