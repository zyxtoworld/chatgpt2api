import { createLatestActionOwner } from "./latest-action-owner.js";

export function createLoginRequestGate(beginAuthMutation) {
  const presentation = createLatestActionOwner();
  let currentOwner = null;

  return {
    begin(identity) {
      if (currentOwner && presentation.accepts(currentOwner.action, currentOwner.identity)) {
        return null;
      }
      currentOwner = {
        action: presentation.begin(identity),
        authLease: beginAuthMutation(),
        identity,
      };
      return currentOwner;
    },

    accepts(owner) {
      return currentOwner === owner && presentation.accepts(owner?.action, owner?.identity);
    },

    finish(owner) {
      if (!this.accepts(owner)) {
        return false;
      }
      currentOwner = null;
      presentation.invalidate();
      return true;
    },

    activate() {
      presentation.activate();
    },

    invalidate() {
      currentOwner = null;
      presentation.invalidate();
      beginAuthMutation();
    },

    cancel() {
      currentOwner = null;
      presentation.cancel();
      beginAuthMutation();
    },
  };
}
