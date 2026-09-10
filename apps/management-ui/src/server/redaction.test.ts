import { describe, expect, it } from "vitest";
import { redact } from "./redaction.js";

describe("gateway redaction", () => {
  it("redacts delivery proofs from request payloads and log echoes", () => {
    const input = {
      operation_id: "f797bef0-8716-4e96-ade7-a0d75fca08c9",
      input: { queue: "jobs", delivery_proof: "cHJvb2Y=", message_id: "17" },
      nested: { proof: "cHJvb2Y=" }
    };
    const output = redact(input) as Record<string, unknown>;
    expect((output.input as Record<string, unknown>).delivery_proof).toBe("[REDACTED]");
    expect(((output.nested) as Record<string, unknown>).proof).toBe("[REDACTED]");
  });

  it("keeps harmless operation ids visible for receipt lookup", () => {
    const value = {
      operation_id: "f797bef0-8716-4e96-ade7-a0d75fca08c9",
      idempotencyKey: "f797bef0-8716-4e96-ade7-a0d75fca08c9",
      path: "/api/admin/v1/job-completions/f797bef0-8716-4e96-ade7-a0d75fca08c9"
    };
    expect(redact(value)).toEqual(value);
  });

  it("still redacts tokens and auth material", () => {
    const output = redact({ authorization: "Bearer x", token: "t", adminToken: "y", nested: { cookie: "z" } }) as Record<string, unknown>;
    expect(output.authorization).toBe("[REDACTED]");
    expect(output.token).toBe("[REDACTED]");
    expect(output.adminToken).toBe("[REDACTED]");
    expect((output.nested as Record<string, unknown>).cookie).toBe("[REDACTED]");
  });
});
