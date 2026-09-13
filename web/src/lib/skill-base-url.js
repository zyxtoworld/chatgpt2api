export function normalizeSkillBaseUrl(value) {
  if (typeof value !== "string") return "";
  const text = value.trim();
  if (!text) return "";
  try {
    const parsed = new URL(text);
    if (!/^https?:$/.test(parsed.protocol) || parsed.username || parsed.password || parsed.search || parsed.hash) {
      return "";
    }
    return `${parsed.origin}${parsed.pathname.replace(/\/$/, "")}`;
  } catch {
    return "";
  }
}
