/**
 * Theme resolution for the console: an explicitly stored preference wins over
 * the OS preference; the resolved theme applies one `.dark` class and a matching
 * `color-scheme` to <html> so portals and dialogs follow. Only this harmless
 * display preference is persisted — never credentials or connection data.
 */
export type ThemePreference = "light" | "dark" | "system";

const STORAGE_KEY = "kuttidb.console.theme.v1";

export function loadThemePreference(): ThemePreference {
  try {
    const value = localStorage.getItem(STORAGE_KEY);
    if (value === "light" || value === "dark" || value === "system") return value;
  } catch {
    // Storage can be unavailable (private mode); fall through to system.
  }
  return "system";
}

export function saveThemePreference(preference: ThemePreference): void {
  try {
    localStorage.setItem(STORAGE_KEY, preference);
  } catch {
    // Ignore persistence failures; the in-memory choice still applies.
  }
}

export function resolvedTheme(preference: ThemePreference): "light" | "dark" {
  if (preference !== "system") return preference;
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export function applyTheme(preference: ThemePreference): "light" | "dark" {
  const resolved = resolvedTheme(preference);
  document.documentElement.classList.toggle("dark", resolved === "dark");
  document.documentElement.style.colorScheme = resolved;
  return resolved;
}

import { useEffect, useState } from "react";

/** Owns the preference lifecycle; every route and portal inherits the class. */
export function useTheme(): { preference: ThemePreference; setPreference: (value: ThemePreference) => void; resolved: "light" | "dark" } {
  const [preference, setPreference] = useState<ThemePreference>(() => loadThemePreference());
  const [resolved, setResolved] = useState<"light" | "dark">(() => resolvedTheme(preference));

  useEffect(() => {
    const applied = applyTheme(preference);
    setResolved(applied);
    saveThemePreference(preference);
  }, [preference]);

  useEffect(() => {
    if (preference !== "system") return;
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const onChange = () => setResolved(applyTheme("system"));
    media.addEventListener("change", onChange);
    return () => media.removeEventListener("change", onChange);
  }, [preference]);

  return { preference, setPreference, resolved };
}
