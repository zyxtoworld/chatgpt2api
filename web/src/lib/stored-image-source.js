export function normalizeStoredImageUrl(value) {
  if (typeof value !== "string") return "";
  const text = value.trim();
  if (!text || text.startsWith("//") || text.includes("\\") || [...text].some((character) => {
    const codePoint = character.codePointAt(0);
    return /\s/u.test(character) || codePoint < 32 || codePoint === 127;
  })) {
    return "";
  }
  if (text.startsWith("/")) return text;
  if (!/^https?:\/\//iu.test(text)) return "";
  try {
    const parsed = new URL(text);
    if (!parsed.hostname || parsed.username || parsed.password) return "";
    return parsed.href;
  } catch {
    return "";
  }
}

export function getStoredImageSrc(image) {
  if (image?.b64_json) {
    return `data:image/png;base64,${image.b64_json}`;
  }
  return normalizeStoredImageUrl(image?.url);
}
