export function createScrollCleanupSnapshot(viewport, positions) {
  return {
    persist(conversationId) {
      if (!viewport || !conversationId) {
        return false;
      }
      positions.set(conversationId, viewport.scrollTop);
      return true;
    },
  };
}
