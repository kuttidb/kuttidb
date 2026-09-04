import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { admin, ApiError, capabilitiesSchema, type Capabilities } from "@/lib/api";

export type ConnectionProfile = {
  id: string;
  label: string;
  endpoint: string;
  rememberProfile: boolean;
  lastConnectedAt?: string | undefined;
};

type LiveConnection = {
  profileId: string;
  endpoint: string;
  capabilities: Capabilities;
  connectedAt: string;
  lastUsedAt: string;
};

type ProfileStorage = {
  id: string;
  label: string;
  endpoint: string;
  rememberProfile: boolean;
  lastConnectedAt?: string;
};

const profileKey = "kuttidb.console.connection-profiles.v2";

function loadProfiles(): ConnectionProfile[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(profileKey) ?? "[]");
    if (!Array.isArray(value)) return [];
    return value.flatMap((entry) => {
      if (!entry || typeof entry !== "object") return [];
      const candidate = entry as Partial<ProfileStorage>;
      if (typeof candidate.id !== "string" || typeof candidate.label !== "string" || typeof candidate.endpoint !== "string" || typeof candidate.rememberProfile !== "boolean") return [];
      return [{
        id: candidate.id,
        label: candidate.label,
        endpoint: candidate.endpoint,
        rememberProfile: candidate.rememberProfile,
        ...(typeof candidate.lastConnectedAt === "string" ? { lastConnectedAt: candidate.lastConnectedAt } : {})
      }];
    });
  } catch {
    return [];
  }
}

function saveProfiles(profiles: ConnectionProfile[]): void {
  const safe = profiles.filter((profile) => profile.rememberProfile).map((profile) => ({
    id: profile.id, label: profile.label, endpoint: profile.endpoint, rememberProfile: profile.rememberProfile,
    ...(profile.lastConnectedAt ? { lastConnectedAt: profile.lastConnectedAt } : {})
  }));
  localStorage.setItem(profileKey, JSON.stringify(safe));
}

type ConnectionState = {
  profiles: ConnectionProfile[];
  live: Map<string, LiveConnection>;
  capabilities: Map<string, Capabilities>;
  connect: (input: { profileId: string; label: string; endpoint: string; token: string; rememberProfile: boolean }) => Promise<void>;
  reconnect: (profileId: string, token: string) => Promise<void>;
  disconnect: (profileId: string) => Promise<void>;
  removeProfile: (profileId: string) => Promise<void>;
  lockAll: () => Promise<void>;
  refreshCapabilities: (profileId: string) => Promise<void>;
  /** Guards mutations: false while the audit trail is unhealthy. */
  mutationsBlocked: (profileId: string) => boolean;
};

const ConnectionContext = createContext<ConnectionState | null>(null);

export function ConnectionsProvider({ children }: { children: ReactNode }) {
  const [profiles, setProfiles] = useState<ConnectionProfile[]>(loadProfiles);
  const [live, setLive] = useState<Map<string, LiveConnection>>(new Map());
  const [capabilities, setCapabilities] = useState<Map<string, Capabilities>>(new Map());

  const persist = useCallback((next: ConnectionProfile[]) => {
    setProfiles(next);
    saveProfiles(next);
  }, []);

  const upsertProfile = useCallback((input: { profileId: string; label: string; endpoint: string; rememberProfile: boolean }) => {
    setProfiles((current) => {
      const existing = current.find((profile) => profile.id === input.profileId);
      const next = [
        ...(existing ? current.filter((profile) => profile.id !== input.profileId) : current),
        { id: input.profileId, label: input.label, endpoint: input.endpoint, rememberProfile: input.rememberProfile, lastConnectedAt: new Date().toISOString() }
      ];
      saveProfiles(next);
      return next;
    });
  }, []);

  const refreshSession = useCallback(async () => {
    const response = await fetch("/ui-api/session", { credentials: "same-origin" });
    if (!response.ok) return;
    const session = (await response.json()) as { connections?: { profileId: string; endpoint: string; capabilities: unknown }[] };
    const restored = new Map<string, LiveConnection>();
    for (const connection of session.connections ?? []) {
      const parsed = capabilitiesSchema.safeParse(connection.capabilities);
      if (parsed.success) restored.set(connection.profileId, { ...connection, capabilities: parsed.data } as LiveConnection);
    }
    setLive(restored);
    setCapabilities((current) => {
      const next = new Map(current);
      for (const [id, connection] of restored) next.set(id, connection.capabilities);
      return next;
    });
  }, []);

  useEffect(() => { void refreshSession(); }, [refreshSession]);

  const establishConnection = useCallback(async (profileId: string, endpoint: string, token: string) => {
    const response = await fetch("/ui-api/connections", {
      method: "POST",
      credentials: "same-origin",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ profileId, endpoint, token })
    });
    const body = (await response.json().catch(() => undefined)) as { connection?: { profileId: string; endpoint: string; capabilities: unknown }; error?: { code?: string; message?: string } } | undefined;
    if (!response.ok || !body?.connection) {
      const code = body?.error?.code ?? "connection_failed";
      throw new ApiError(code, body?.error?.message ?? "Connection could not be established.", response.status);
    }
    const parsed = capabilitiesSchema.safeParse(body.connection.capabilities);
    if (!parsed.success) throw new ApiError("unsupported_target", "This target does not expose a supported KuttiDB Management API.", response.status);
    const connection: LiveConnection = { profileId: body.connection.profileId, endpoint: body.connection.endpoint, capabilities: parsed.data, connectedAt: new Date().toISOString(), lastUsedAt: new Date().toISOString() };
    setLive((current) => new Map(current).set(profileId, connection));
    setCapabilities((current) => new Map(current).set(profileId, parsed.data));
  }, []);

  const connect = useCallback<ConnectionState["connect"]>(async (input) => {
    await establishConnection(input.profileId, input.endpoint, input.token);
    upsertProfile({ profileId: input.profileId, label: input.label, endpoint: input.endpoint, rememberProfile: input.rememberProfile });
  }, [establishConnection, upsertProfile]);

  const reconnect = useCallback<ConnectionState["reconnect"]>(async (profileId, token) => {
    const profile = profiles.find((entry) => entry.id === profileId);
    if (!profile) throw new ApiError("no_profile", "This profile no longer exists on this device.", 404);
    await establishConnection(profileId, profile.endpoint, token);
  }, [profiles, establishConnection]);

  const disconnect = useCallback<ConnectionState["disconnect"]>(async (profileId) => {
    await fetch(`/ui-api/connections/${encodeURIComponent(profileId)}`, { method: "DELETE", credentials: "same-origin" });
    setLive((current) => {
      const next = new Map(current);
      next.delete(profileId);
      return next;
    });
  }, []);

  const removeProfile = useCallback<ConnectionState["removeProfile"]>(async (profileId) => {
    await disconnect(profileId);
    setProfiles((current) => {
      const next = current.filter((profile) => profile.id !== profileId);
      saveProfiles(next);
      return next;
    });
  }, [disconnect]);

  const lockAll = useCallback<ConnectionState["lockAll"]>(async () => {
    await fetch("/ui-api/lock", { method: "POST", credentials: "same-origin" });
    setLive(new Map());
  }, []);

  const refreshCapabilities = useCallback<ConnectionState["refreshCapabilities"]>(async (profileId) => {
    const response = await admin<unknown>(profileId, "capabilities", {});
    const parsed = capabilitiesSchema.safeParse(response.json);
    if (!parsed.success) throw new ApiError("unsupported_target", "This target does not expose a supported KuttiDB Management API.", 502);
    setCapabilities((current) => new Map(current).set(profileId, parsed.data));
    // Also re-sync the gateway session so connection state stays authoritative.
    await refreshSession();
  }, [refreshSession]);

  const mutationsBlocked = useCallback((profileId: string) => {
    const caps = capabilities.get(profileId);
    return Boolean(caps?.audit?.required && caps.audit.healthy === false);
  }, [capabilities]);

  const value = useMemo<ConnectionState>(() => ({
    profiles, live, capabilities, connect, reconnect, disconnect, removeProfile, lockAll, refreshCapabilities, mutationsBlocked
  }), [profiles, live, capabilities, connect, reconnect, disconnect, removeProfile, lockAll, refreshCapabilities, mutationsBlocked]);

  return <ConnectionContext.Provider value={value}>{children}</ConnectionContext.Provider>;
}

export function useConnections(): ConnectionState {
  const context = useContext(ConnectionContext);
  if (!context) throw new Error("useConnections must be used within ConnectionsProvider");
  return context;
}
