import { createLatestActionOwner } from "./latest-action-owner.js";

export function createProxyRuntimeRequestGate() {
  const proxy = createLatestActionOwner();
  const clearance = createLatestActionOwner();

  return {
    activate() {
      proxy.activate();
      clearance.activate();
    },

    beginProxy() {
      return proxy.begin();
    },

    acceptsProxy(action) {
      return proxy.accepts(action);
    },

    beginClearance(identity) {
      return clearance.begin(identity);
    },

    acceptsClearance(action, identity) {
      return clearance.accepts(action, identity);
    },

    invalidateProxy() {
      proxy.invalidate();
    },

    invalidateClearance() {
      clearance.invalidate();
    },

    cancel() {
      proxy.cancel();
      clearance.cancel();
    },
  };
}
