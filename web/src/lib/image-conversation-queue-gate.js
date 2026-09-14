export function createConversationQueueGate() {
  let active = false;
  let epoch = 0;
  let nextToken = 0;
  const leases = new Map();

  const accepts = (lease) => Boolean(
    active &&
    lease &&
    lease.epoch === epoch &&
    leases.get(lease.conversationId) === lease,
  );

  return {
    activate() {
      active = true;
      epoch += 1;
      leases.clear();
    },

    begin(conversationId) {
      if (!active || !conversationId || leases.has(conversationId)) {
        return null;
      }
      const lease = {
        conversationId,
        epoch,
        token: ++nextToken,
      };
      leases.set(conversationId, lease);
      return lease;
    },

    accepts,

    invalidate(conversationId) {
      if (conversationId) {
        leases.delete(conversationId);
      }
    },

    invalidateAll() {
      epoch += 1;
      leases.clear();
    },

    finish(lease) {
      if (!accepts(lease)) {
        return false;
      }
      leases.delete(lease.conversationId);
      return true;
    },

    isRunning(conversationId) {
      return Boolean(active && leases.has(conversationId));
    },

    cancel() {
      active = false;
      epoch += 1;
      leases.clear();
    },
  };
}
