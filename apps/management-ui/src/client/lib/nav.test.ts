import { describe, expect, it } from "vitest";
import { isNavActive, routeSegments } from "./nav";

const NAV_BASES = {
  overview: ["overview"],
  keyspace: ["keyspace"],
  queues: ["queues"],
  streams: ["streams"],
  groups: ["groups"],
  routing: ["routing"],
  atomic: ["operations", "atomic"],
  maintenance: ["operations", "maintenance"]
} as const;

describe("routeSegments", () => {
  it("composes a single connection prefix for view-relative paths", () => {
    expect(routeSegments("p1", ["keyspace"])).toEqual(["c", "p1", "keyspace"]);
    expect(routeSegments("p1", ["queues", "q-abc"])).toEqual(["c", "p1", "queues", "q-abc"]);
    expect(routeSegments("p1", ["operations", "maintenance"])).toEqual(["c", "p1", "operations", "maintenance"]);
  });

  it("does not double the connection prefix when views pass link paths", () => {
    // The old bug: views passed ["c", profileId, ...] and the shell prepended
    // the prefix again, producing "#/c/p1/c/p1/keyspace". The contract is
    // relative segments only, so composition stays single-prefixed.
    const relative = ["queues"];
    const composed = routeSegments("p1", relative);
    expect(composed.filter((segment) => segment === "c")).toHaveLength(1);
    expect(composed.filter((segment) => segment === "p1")).toHaveLength(1);
    expect(composed.join("/")).not.toContain("c/p1/c");
  });

  it("drops empty profile segments defensively", () => {
    expect(routeSegments("", ["overview"])).toEqual(["c", "overview"]);
  });
});

describe("isNavActive", () => {
  const segments = (view: string, sub?: string) =>
    sub ? ["c", "p1", view, sub] : ["c", "p1", view];

  it("matches list items on their detail routes", () => {
    expect(isNavActive(NAV_BASES.queues, segments("queues", "q-abc"))).toBe(true);
    expect(isNavActive(NAV_BASES.streams, segments("streams", "gs-xyz"))).toBe(true);
  });

  it("matches the full item base so only one Operations entry is active", () => {
    expect(isNavActive(NAV_BASES.atomic, segments("operations", "atomic"))).toBe(true);
    expect(isNavActive(NAV_BASES.maintenance, segments("operations", "atomic"))).toBe(false);
    expect(isNavActive(NAV_BASES.maintenance, segments("operations", "maintenance"))).toBe(true);
    expect(isNavActive(NAV_BASES.atomic, segments("operations", "maintenance"))).toBe(false);
  });

  it("does not match a different view", () => {
    expect(isNavActive(NAV_BASES.overview, segments("queues"))).toBe(false);
    expect(isNavActive(NAV_BASES.keyspace, segments("overview"))).toBe(false);
    // A two-segment base never activates from a bare group prefix.
    expect(isNavActive(NAV_BASES.atomic, segments("operations"))).toBe(false);
    expect(isNavActive(NAV_BASES.maintenance, segments("operations"))).toBe(false);
  });

  it("is exclusive: at most one item is active for any route", () => {
    const route = segments("operations", "maintenance");
    const active = Object.values(NAV_BASES).filter((base) => isNavActive(base, route));
    expect(active).toEqual([NAV_BASES.maintenance]);
  });
});
