/** @param {string} value */
export function encodeApiPath(value) {
  return value.split("/").map((segment) => encodeURIComponent(segment)).join("/");
}
