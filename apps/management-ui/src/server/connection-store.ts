import { randomBytes, randomUUID } from "node:crypto";

export type SafeConnection = {
  profileId: string;
  endpoint: string;
  capabilities: unknown;
  connectedAt: string;
  lastUsedAt: string;
};

type LiveConnection = SafeConnection & { token: Buffer };
type Session = { id: string; createdAt: number; lastUsedAt: number; connections: Map<string, LiveConnection> };

export class ConnectionStore {
  readonly #sessions = new Map<string, Session>();

  constructor(
    private readonly limits: { maxSessions: number; maxConnections: number; idleMs: number; maxMs: number },
    private readonly now: () => number = Date.now
  ) {}

  create(): Session {
    this.sweep();
    while (this.#sessions.size >= this.limits.maxSessions) {
      const oldest = this.#sessions.values().next().value as Session | undefined;
      if (!oldest) break;
      this.destroy(oldest.id);
    }
    const timestamp = this.now();
    const session = { id: randomBytes(32).toString("base64url"), createdAt: timestamp, lastUsedAt: timestamp, connections: new Map<string, LiveConnection>() };
    this.#sessions.set(session.id, session);
    return session;
  }

  get(id: string | undefined): Session | undefined {
    if (!id) return undefined;
    const session = this.#sessions.get(id);
    if (!session) return undefined;
    const timestamp = this.now();
    if (timestamp - session.lastUsedAt > this.limits.idleMs || timestamp - session.createdAt > this.limits.maxMs) {
      this.destroy(id);
      return undefined;
    }
    session.lastUsedAt = timestamp;
    return session;
  }

  connect(session: Session, input: { profileId: string; endpoint: string; token: string; capabilities: unknown }): SafeConnection {
    const current = session.connections.get(input.profileId);
    if (!current && session.connections.size >= this.limits.maxConnections) {
      throw new Error("connection_limit");
    }
    if (current) current.token.fill(0);
    const timestamp = new Date(this.now()).toISOString();
    const connection: LiveConnection = {
      profileId: input.profileId,
      endpoint: input.endpoint,
      token: Buffer.from(input.token, "utf8"),
      capabilities: input.capabilities,
      connectedAt: timestamp,
      lastUsedAt: timestamp
    };
    session.connections.set(input.profileId, connection);
    return this.safe(connection);
  }

  connection(session: Session, profileId: string): LiveConnection | undefined {
    const connection = session.connections.get(profileId);
    if (connection) connection.lastUsedAt = new Date(this.now()).toISOString();
    return connection;
  }

  disconnect(session: Session, profileId: string): boolean {
    const connection = session.connections.get(profileId);
    if (!connection) return false;
    connection.token.fill(0);
    session.connections.delete(profileId);
    return true;
  }

  lock(session: Session): void {
    for (const profileId of [...session.connections.keys()]) this.disconnect(session, profileId);
  }

  destroy(id: string): void {
    const session = this.#sessions.get(id);
    if (!session) return;
    this.lock(session);
    this.#sessions.delete(id);
  }

  sweep(): void {
    for (const session of this.#sessions.values()) this.get(session.id);
  }

  safe(connection: LiveConnection): SafeConnection {
    const { token: _token, ...safe } = connection;
    return safe;
  }

  list(session: Session): SafeConnection[] {
    return [...session.connections.values()].map((connection) => this.safe(connection));
  }
}

export function newRequestId(): string {
  return randomUUID();
}
