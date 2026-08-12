/**
 * Resolve and publish a mutable snapshot before scheduling any React update.
 *
 * @template T
 * @param {{ current: T }} ref
 * @param {T | ((current: T) => T)} next
 * @returns {T}
 */
export function commitSynchronousSnapshot(ref, next) {
  const resolved = typeof next === "function" ? next(ref.current) : next;
  ref.current = resolved;
  return resolved;
}
