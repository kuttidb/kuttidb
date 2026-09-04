import { describe, expect, it } from "vitest";
import { ConnectionStore } from "./connection-store.js";

describe("ConnectionStore", () => {
  it("never returns a token in connection data", () => {
    const store = new ConnectionStore({ maxSessions: 1, maxConnections: 1, idleMs: 1_000, maxMs: 1_000 });
    const session = store.create();
    const connection = store.connect(session, { profileId: "profile", endpoint: "https://db.example.test", token: "secret-token", capabilities: { product: "KuttiDB" } });
    expect(JSON.stringify(connection)).not.toContain("secret-token");
    expect(store.list(session)).toEqual([connection]);
  });

  it("clears a live connection when it is disconnected", () => {
    const store = new ConnectionStore({ maxSessions: 1, maxConnections: 1, idleMs: 1_000, maxMs: 1_000 });
    const session = store.create();
    store.connect(session, { profileId: "profile", endpoint: "https://db.example.test", token: "secret-token", capabilities: {} });
    expect(store.disconnect(session, "profile")).toBe(true);
    expect(store.connection(session, "profile")).toBeUndefined();
  });
});
