export function getElapsedSeconds(baseSeconds, updatedAt, currentTime) {
  const base = Number.isFinite(baseSeconds) ? Math.max(0, baseSeconds) : 0;
  if (!Number.isFinite(updatedAt) || !Number.isFinite(currentTime) || currentTime < updatedAt) {
    return base;
  }
  return base + (currentTime - updatedAt) / 1000;
}

export function formatElapsedSeconds(seconds) {
  const wholeSeconds = Number.isFinite(seconds) ? Math.max(0, Math.floor(seconds)) : 0;
  const minutes = Math.floor(wholeSeconds / 60);
  const remainingSeconds = wholeSeconds % 60;
  return `${minutes}m ${String(remainingSeconds).padStart(2, "0")}s`;
}
