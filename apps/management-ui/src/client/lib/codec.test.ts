import { describe, expect, it } from "vitest";
import {
  base64FromUtf8, b64uFromBytes, bytesFromBase64, bytesFromB64u, idFromName, isValidB64uId,
  jsonPreviewFromBase64, nameFromId, utf8FromBase64
} from "./codec";

describe("codec", () => {
  it("round-trips b64u identifiers without padding", () => {
    expect(idFromName("jobs")).toBe("b64u:am9icw");
    expect(nameFromId("b64u:am9icw")).toBe("jobs");
    expect(nameFromId(idFromName("orders/order-42"))).toBe("orders/order-42");
  });

  it("rejects malformed identifiers safely", () => {
    expect(isValidB64uId("b64u:am9icw")).toBe(true);
    expect(isValidB64uId("b64u:am9icw=")).toBe(false);
    expect(isValidB64uId("am9icw")).toBe(false);
    expect(nameFromId("d:0123")).toBeNull();
  });

  it("keeps bodies as canonical padded base64", () => {
    const encoded = base64FromUtf8("hello world");
    expect(encoded).toBe("aGVsbG8gd29ybGQ=");
    expect(utf8FromBase64(encoded)).toBe("hello world");
    expect(bytesFromBase64(encoded).byteLength).toBe(11);
  });

  it("falls back rather than throwing on invalid UTF-8", () => {
    const bytes = new Uint8Array([0xff, 0xfe, 0x01]);
    const encoded = b64uFromBytes(bytes);
    expect(bytesFromB64u(encoded)).toEqual(bytes);
    expect(utf8FromBase64(b64uFromBytes(bytes))).toBeNull();
  });

  it("previews JSON only when the body is valid UTF-8 JSON", () => {
    expect(jsonPreviewFromBase64(base64FromUtf8('{"a":1}'))).toEqual({ a: 1 });
    expect(jsonPreviewFromBase64(base64FromUtf8("plain text"))).toBeNull();
    expect(jsonPreviewFromBase64("/////w==")).toBeNull();
  });
});
