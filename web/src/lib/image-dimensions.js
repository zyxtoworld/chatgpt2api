export function setImageDimension(current, id, width, height) {
  const dimensions = `${width} x ${height}`;
  if (!id || current[id] === dimensions) {
    return current;
  }
  return { ...current, [id]: dimensions };
}
