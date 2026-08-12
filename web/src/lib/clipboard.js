/**
 * @param {string} value
 * @param {{writeText(value: string): Promise<void>} | undefined} [clipboard]
 */
export async function writeClipboardText(value, clipboard = globalThis.navigator?.clipboard) {
  if (!clipboard || typeof clipboard.writeText !== "function") {
    return false;
  }
  try {
    await clipboard.writeText(value);
    return true;
  } catch {
    return false;
  }
}
