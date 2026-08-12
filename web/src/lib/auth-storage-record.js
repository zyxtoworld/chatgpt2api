export function normalizeStoredAuthSession(storedKey, storedSession) {
  if (typeof storedKey !== "string" || !storedKey || !storedSession || typeof storedSession !== "object") {
    return null;
  }

  const candidate = storedSession;
  if (
    typeof candidate.key !== "string" ||
    !candidate.key ||
    candidate.key !== storedKey ||
    (candidate.role !== "admin" && candidate.role !== "user")
  ) {
    return null;
  }

  return {
    key: candidate.key,
    role: candidate.role,
    subjectId: typeof candidate.subjectId === "string" ? candidate.subjectId : "",
    name: typeof candidate.name === "string" ? candidate.name : "",
  };
}
