// @vitest-environment happy-dom
import { afterEach, describe, expect, it } from "vitest";
import { applyTheme, loadThemePreference, resolvedTheme, saveThemePreference } from "./theme";

describe("theme preference", () => {
  afterEach(() => {
    localStorage.removeItem("kuttidb.console.theme.v1");
    document.documentElement.classList.remove("dark");
    document.documentElement.style.colorScheme = "";
  });

  it("defaults to system when nothing is stored", () => {
    expect(loadThemePreference()).toBe("system");
  });

  it("round-trips an explicit preference and applies it to <html>", () => {
    saveThemePreference("dark");
    expect(loadThemePreference()).toBe("dark");
    const resolved = applyTheme("dark");
    expect(resolved).toBe("dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.style.colorScheme).toBe("dark");

    applyTheme("light");
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    expect(document.documentElement.style.colorScheme).toBe("light");
  });

  it("resolves system against the OS preference", () => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    expect(resolvedTheme("system")).toBe(media.matches ? "dark" : "light");
  });
});
