function isModelEntry(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      typeof value.id === "string" &&
      value.id.trim(),
  );
}

export function parseModelList(value) {
  if (
    !value ||
    typeof value !== "object" ||
    value.object !== "list" ||
    !Array.isArray(value.data) ||
    !value.data.every(isModelEntry)
  ) {
    return null;
  }
  return value.data;
}
