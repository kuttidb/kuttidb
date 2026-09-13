import { describe, expect, it } from "vitest";
import { parseArgs } from "./cli.js";

describe("cli argument parsing", () => {
  it("defaults to opening the browser with loopback-only targets", () => {
    const args = parseArgs([]);
    expect(args.open).toBe(true);
    expect(args.allowLoopbackHttp).toBe(true);
    expect(args.allowPrivateTargets).toBe(false);
    expect(args.host).toBeUndefined();
    expect(args.port).toBeUndefined();
  });

  it("parses space- and equals-separated flags", () => {
    expect(parseArgs(["--port", "9000", "--host=0.0.0.0"])).toMatchObject({ port: "9000", host: "0.0.0.0" });
    expect(parseArgs(["-p", "9001", "-H", "example.test"])).toMatchObject({ port: "9001", host: "example.test" });
  });

  it("parses the target allowlist and boolean switches", () => {
    const args = parseArgs(["--no-open", "--allow-private-targets", "--strict-https", "--target-allowlist=a.test,b.test"]);
    expect(args.open).toBe(false);
    expect(args.allowPrivateTargets).toBe(true);
    expect(args.allowLoopbackHttp).toBe(false);
    expect(args.targetAllowlist).toBe("a.test,b.test");
  });

  it("recognizes help and version flags", () => {
    expect(parseArgs(["--help"]).help).toBe(true);
    expect(parseArgs(["-h"]).help).toBe(true);
    expect(parseArgs(["--version"]).version).toBe(true);
    expect(parseArgs(["-v"]).version).toBe(true);
  });

  it("ignores a dangling flag with no value rather than crashing", () => {
    expect(() => parseArgs(["--port"])).not.toThrow();
    expect(parseArgs(["--port"]).port).toBeUndefined();
  });
});
