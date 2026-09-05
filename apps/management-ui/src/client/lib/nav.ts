/**
 * Route composition and navigation-active matching, shared by the shell and
 * the behavior tests. Hash routes look like `#/c/{profileId}/queues/{queueId}`
 * -> ["c", "{profileId}", "queues", "{queueId}"].
 */

/** Segments of a connection-relative view path (no profile prefix). */
export function routeSegments(profileId: string, relativePath: string[]): string[] {
  return ["c", profileId, ...relativePath].filter((segment) => segment.length > 0);
}

/**
 * True when a navigation item matches the current segments. Matching uses the
 * item's full base, so `operations/atomic` and `operations/maintenance` are
 * exclusive, while `queues` stays active on a queue detail route.
 */
export function isNavActive(base: readonly string[], segments: readonly string[]): boolean {
  return base.every((segment, index) => segments[2 + index] === segment);
}
