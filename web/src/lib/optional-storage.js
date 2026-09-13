export function readOptionalStorageItem(storage, key) {
  try {
    return storage.getItem(key);
  } catch {
    return null;
  }
}

export function writeOptionalStorageItem(storage, key, value) {
  try {
    storage.setItem(key, value);
    return true;
  } catch {
    return false;
  }
}

export function removeOptionalStorageItem(storage, key) {
  try {
    storage.removeItem(key);
    return true;
  } catch {
    return false;
  }
}
