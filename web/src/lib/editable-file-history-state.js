export function mergeDeletedEditableFileIds(current, ids) {
  return new Set([...current, ...ids.filter(Boolean)]);
}
