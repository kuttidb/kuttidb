export type ConnectionProfile = {
  id: string;
  label: string;
  endpoint: string;
  color: "amber" | "cocoa" | "sage" | "sky";
  rememberProfile: boolean;
  lastConnectedAt?: string;
};

const profileKey = "kuttidb.console.connection-profiles.v1";

export function loadProfiles(): ConnectionProfile[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(profileKey) ?? "[]");
    if (!Array.isArray(value)) return [];
    return value.flatMap((profile) => {
      if (!profile || typeof profile !== "object") return [];
      const candidate = profile as Partial<ConnectionProfile>;
      if (typeof candidate.id !== "string" || typeof candidate.label !== "string" || typeof candidate.endpoint !== "string" || typeof candidate.rememberProfile !== "boolean") return [];
      return [{ id: candidate.id, label: candidate.label, endpoint: candidate.endpoint, color: candidate.color === "cocoa" || candidate.color === "sage" || candidate.color === "sky" ? candidate.color : "amber", rememberProfile: candidate.rememberProfile, ...(typeof candidate.lastConnectedAt === "string" ? { lastConnectedAt: candidate.lastConnectedAt } : {}) }];
    });
  } catch {
    return [];
  }
}

export function saveProfiles(profiles: ConnectionProfile[]): void {
  const safeProfiles = profiles.filter((profile) => profile.rememberProfile).map(({ id, label, endpoint, color, rememberProfile, lastConnectedAt }) => ({ id, label, endpoint, color, rememberProfile, ...(lastConnectedAt ? { lastConnectedAt } : {}) }));
  localStorage.setItem(profileKey, JSON.stringify(safeProfiles));
}
