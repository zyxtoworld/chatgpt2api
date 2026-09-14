export function applyImageConversationUpdate(conversations, conversationId, updater) {
  const current = conversations.find((conversation) => conversation.id === conversationId) ?? null;
  const next = updater(current);
  if (!next) {
    return { conversations, changed: false };
  }

  return {
    conversations: [next, ...conversations.filter((conversation) => conversation.id !== conversationId)],
    changed: true,
  };
}

export function findImageTaskConversation(conversations, taskId) {
  if (!taskId) return null;
  return conversations.find((conversation) =>
    conversation.turns?.some((turn) => turn.images?.some((image) => image.taskId === taskId)),
  ) ?? null;
}
