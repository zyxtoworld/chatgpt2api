import { createMutationRequestGate } from "./mutation-request-gate.js";

export function createProxySettingsWriteGate() {
  const gate = createMutationRequestGate();
  return {
    begin() {
      return gate.beginMutation();
    },
    accepts(owner) {
      return gate.acceptsMutation(owner);
    },
    finish(owner) {
      return gate.finishMutation(owner);
    },
  };
}

export const proxySettingsWriteGate = createProxySettingsWriteGate();
