export function buildThirdPartyHref(appUrl, baseUrl) {
  const url = appUrl.trim();
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
