import { createLatestActionOwner } from "./latest-action-owner.js";

export function createChatPanelRequestGate() {
  const chat = createLatestActionOwner();
  let imageEpoch = 0;
  let imageActive = true;
  let nextImageToken = 0;

  return {
    activate() {
      chat.activate();
      imageActive = true;
      imageEpoch += 1;
    },

    beginChat() {
      return chat.begin();
    },

    acceptsChat(action) {
      return chat.accepts(action);
    },

    beginImageRead() {
      return { epoch: imageEpoch, token: ++nextImageToken };
    },

    acceptsImageRead(action) {
      return Boolean(imageActive && action && action.epoch === imageEpoch);
    },

    clear() {
      chat.invalidate();
      imageEpoch += 1;
    },

    cancel() {
      chat.cancel();
      imageActive = false;
      imageEpoch += 1;
    },
  };
}
