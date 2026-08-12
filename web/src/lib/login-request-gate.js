import { createLatestActionOwner } from "./latest-action-owner.js";

export function createLoginRequestGate(beginAuthMutation) {
  const presentation = createLatestActionOwner();

  return {
    begin(identity) {
      return {
        action: presentation.begin(identity),
        authLease: beginAuthMutation(),
        identity,
      };
    },

    accepts(owner) {
      return presentation.accepts(owner?.action, owner?.identity);
    },

    finish(owner) {
      if (!this.accepts(owner)) {
        return false;
      }
      presentation.invalidate();
      return true;
    },

    activate() {
      presentation.activate();
    },

    cancel() {
      presentation.cancel();
      beginAuthMutation();
    },
  };
}
