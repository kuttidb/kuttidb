import { describe, expect, it } from "vitest";
import { readConfig } from "./config.js";
import { validateTarget } from "./target-policy.js";

const developmentConfig = readConfig({ NODE_ENV: "test", ALLOW_LOOPBACK_HTTP: "true" });

describe("target policy", () => {
  it("permits the explicit loopback development target", () => {
    expect(validateTarget("http://127.0.0.1:7380", developmentConfig).origin).toBe("http://127.0.0.1:7380");
  });

  it("rejects endpoint paths and credential-bearing URLs", () => {
    expect(() => validateTarget("https://token@example.test/path", developmentConfig)).toThrow(/origin only/i);
  });

  it("requires HTTPS when loopback HTTP is not enabled", () => {
    const config = readConfig({ NODE_ENV: "test" });
    expect(() => validateTarget("http://127.0.0.1:7380", config)).toThrow(/HTTP is allowed/i);
  });
});
