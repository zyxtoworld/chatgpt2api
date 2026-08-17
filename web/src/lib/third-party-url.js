export function buildThirdPartyHref(appUrl, baseUrl) {
  if (typeof appUrl !== "string" || typeof baseUrl !== "string") {
    return "";
  }
  const url = appUrl.trim();
  if (!url || !baseUrl.trim()) {
    return "";
  }
  try {
    const target = new URL(url);
    target.searchParams.set("baseUrl", baseUrl);
    return target.toString();
  } catch {
    return `${url}${url.includes("?") ? "&" : "?"}baseUrl=${encodeURIComponent(baseUrl)}`;
  }
}

export function formatThirdPartyDisplayHref(href) {
  try {
    return decodeURIComponent(href);
  } catch {
    return href;
  }
}
