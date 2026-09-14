export function filenameFromUrl(url) {
  const decodeSegment = (value) => {
    try {
      return decodeURIComponent(value);
    } catch {
      return value;
    }
  };
  try {
    return decodeSegment(new URL(url).pathname.split("/").pop() || "");
  } catch {
    return decodeSegment(String(url || "").split("/").pop() || "");
  }
}
