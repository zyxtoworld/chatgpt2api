/**
 * @typedef {{title?: unknown, url?: unknown, snippet?: unknown}} RawSearchSource
 * @typedef {{title: string, url: string, snippet: string}} SearchSource
 */

const DNS_LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

/** @param {URL} parsed */
function normalizeSearchHostname(parsed) {
  if (parsed.hostname.startsWith("[") && parsed.hostname.endsWith("]")) {
    return true;
  }
  const hostname = parsed.hostname.endsWith(".") ? parsed.hostname.slice(0, -1) : parsed.hostname;
  if (
    !hostname ||
    hostname.length > 253 ||
    hostname.split(".").some((label) => !DNS_LABEL.test(label))
  ) {
    return false;
  }
  parsed.hostname = hostname;
  return true;
}

/** @param {unknown} value */
function normalizeSearchSourceUrl(value) {
  if (typeof value !== "string") return "";
  const text = value.trim();
  if (!text || text.includes("\\") || [...text].some((character) => /\s/u.test(character) || character.codePointAt(0) < 32 || character.codePointAt(0) === 127)) {
    return "";
  }
  try {
    const authority = text.match(/^[a-z][a-z0-9+.-]*:\/\/([^/?#]*)/iu)?.[1] || "";
    if (!authority || authority.includes("%")) return "";
    const parsed = new URL(text);
    if (
      !["http:", "https:"].includes(parsed.protocol) ||
      parsed.username ||
      parsed.password ||
      !parsed.hostname ||
      !normalizeSearchHostname(parsed)
    ) {
      return "";
    }
    parsed.hash = "";
    return parsed.href;
  } catch {
    return "";
  }
}

/**
 * @param {unknown} values
 * @returns {SearchSource[]}
 */
export function normalizeSearchSources(values) {
  if (!Array.isArray(values)) return [];
  const seen = new Set();
  /** @type {SearchSource[]} */
  const output = [];
  for (const item of values) {
    if (!item || typeof item !== "object") continue;
    const source = /** @type {RawSearchSource} */ (item);
    const url = normalizeSearchSourceUrl(source.url);
    if (!url || seen.has(url)) continue;
    seen.add(url);
    output.push({
      title: String(source.title || "").trim(),
      url,
      snippet: String(source.snippet || "").trim(),
    });
  }
  return output;
}
