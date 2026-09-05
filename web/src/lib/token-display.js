const TOKEN_EDGE_LENGTH = 4;

export function maskToken(token) {
  if (typeof token !== "string" || token.length === 0) return "—";
  if (token.length <= 2) return "…";

  const edgeLength =
    token.length <= TOKEN_EDGE_LENGTH * 2
      ? Math.max(1, Math.floor((token.length - 1) / 2))
      : TOKEN_EDGE_LENGTH;
  return `${token.slice(0, edgeLength)}...${token.slice(-edgeLength)}`;
}
