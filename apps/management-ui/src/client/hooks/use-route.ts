import { useCallback, useEffect, useState } from "react";

/** Hash router: "#/c/p1/overview" -> ["c","p1","overview"]. */
function currentSegments(): string[] {
  const hash = window.location.hash.replace(/^#\/?/, "");
  return hash.length === 0 ? [] : hash.split("/").filter((segment) => segment.length > 0);
}

export function useRoute(): { segments: string[]; navigate: (to: string[]) => void } {
  const [segments, setSegments] = useState<string[]>(currentSegments);
  useEffect(() => {
    const onChange = () => setSegments(currentSegments());
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  const navigate = useCallback((to: string[]) => {
    window.location.hash = `/${to.map(encodeURIComponent).join("/")}`;
  }, []);
  return { segments, navigate };
}
