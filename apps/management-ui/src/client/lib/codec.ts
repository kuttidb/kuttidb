/**
 * Binary codecs for the KuttiDB Management API.
 *
 * Two distinct encodings exist:
 *  - opaque identifiers use URL-safe *unpadded* Base64 with the `b64u:` prefix
 *  - bodies/values use canonical *padded* standard Base64
 */

const textEncoder = new TextEncoder();
const textDecoderStrict = new TextDecoder("utf-8", { fatal: true });
const textDecoderLoose = new TextDecoder("utf-8");

export function bytesFromBase64(value: string): Uint8Array {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

export function base64FromBytes(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

export function b64uFromBytes(bytes: Uint8Array): string {
  return base64FromBytes(bytes).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function bytesFromB64u(value: string): Uint8Array {
  return bytesFromBase64(value);
}

/** "jobs" -> "b64u:am9icw" */
export function idFromName(name: string): string {
  return `b64u:${b64uFromBytes(textEncoder.encode(name))}`;
}

/** "b64u:am9icw" -> "jobs" when the identifier is valid UTF-8, otherwise null. */
export function nameFromId(id: string): string | null {
  const match = /^b64u:([A-Za-z0-9_-]+)$/.exec(id);
  if (!match) return null;
  try {
    return textDecoderStrict.decode(bytesFromB64u(match[1] as string));
  } catch {
    return null;
  }
}

export function utf8FromBase64(value: string): string | null {
  try {
    return textDecoderStrict.decode(bytesFromBase64(value));
  } catch {
    return null;
  }
}

export function looseUtf8FromBytes(bytes: Uint8Array): string {
  return textDecoderLoose.decode(bytes);
}

export function base64FromUtf8(text: string): string {
  return base64FromBytes(textEncoder.encode(text));
}

export function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(" ");
}

/** Strict-JSON preview of a Base64 body, or null when it is not valid UTF-8 JSON. */
export function jsonPreviewFromBase64(value: string): unknown | null {
  const text = utf8FromBase64(value);
  if (text === null) return null;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return null;
  }
}

export function isValidB64uId(value: string): boolean {
  return /^b64u:[A-Za-z0-9_-]+$/.test(value);
}
