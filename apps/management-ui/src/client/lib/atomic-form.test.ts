import { describe, expect, it } from "vitest";
import { ApiError } from "./api";
import { mutationOutcome, validateAtomicInput } from "./atomic-form";

const SPEC = { op: "put-and-route", needsRoutingKey: true, needsValue: true, needsBody: false };
// Mirrors the real codec pair: plain text converts to a b64u ID.
const toId = (text: string) => `b64u:${btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "")}`;
const isValidId = (id: string) => id.startsWith("b64u:");

describe("validateAtomicInput", () => {
  const base = { operation: "put-and-route", keyText: "user:42", targetName: "jobs", routingKey: "resize", valueText: "v", bodyText: "" };

  it("passes a complete form with no field errors", () => {
    expect(validateAtomicInput(base, SPEC, toId, isValidId)).toEqual({});
  });

  it("reports empty and malformed keys", () => {
    const errors = validateAtomicInput({ ...base, keyText: "" }, SPEC, toId, isValidId);
    expect(errors.key).toBe("A cache key is required.");
  });

  it("reports target-queue problems", () => {
    const empty = validateAtomicInput({ ...base, targetName: "" }, SPEC, toId, isValidId);
    expect(empty.target).toBe("A target queue is required.");
  });

  it("enforces the operation's own field requirements", () => {
    expect(validateAtomicInput({ ...base, routingKey: "" }, SPEC, toId, isValidId).routingKey).toBeTruthy();
    expect(validateAtomicInput({ ...base, valueText: "" }, SPEC, toId, isValidId).value).toBeTruthy();
    const putAndEnqueue = { op: "put-and-enqueue", needsRoutingKey: false, needsValue: true, needsBody: false };
    expect(validateAtomicInput({ ...base, routingKey: "" }, putAndEnqueue, toId, isValidId).routingKey).toBeUndefined();
  });
});

describe("mutationOutcome", () => {
  it("classifies a missing error as clean", () => {
    expect(mutationOutcome(null)).toBe("clean");
    expect(mutationOutcome(undefined)).toBe("clean");
  });

  it("classifies operation_in_doubt as unresolved", () => {
    expect(mutationOutcome(new ApiError("operation_in_doubt", "Outcome unknown.", 500))).toBe("in_doubt");
  });

  it("classifies any other failure as a normal failure", () => {
    expect(mutationOutcome(new ApiError("validation_failed", "Bad input.", 400))).toBe("failed");
    expect(mutationOutcome(new Error("network down"))).toBe("failed");
  });
});
