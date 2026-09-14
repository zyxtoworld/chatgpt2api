export function mergeDeletedEditableFileIds(current, ids) {
  return new Set([...current, ...ids.filter(Boolean)]);
}

export async function resolveDeletedEditableFileIds(current, loadStoredIds) {
  try {
    const stored = await loadStoredIds();
    return {
      ids: mergeDeletedEditableFileIds(current, [...stored]),
      storageFailed: false,
    };
  } catch {
    return {
      ids: new Set(current),
      storageFailed: true,
    };
  }
}
