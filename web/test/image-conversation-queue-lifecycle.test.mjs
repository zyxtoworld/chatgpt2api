import assert from "node:assert/strict";
import test from "node:test";

import { createConversationQueueGate } from "../src/lib/image-conversation-queue-gate.js";

test("invalidating a conversation rejects a late queue callback", () => {
  const gate = createConversationQueueGate();
  gate.activate();
  const lease = gate.begin("conversation-a");
  const conversations = [{ id: "conversation-a" }];

  gate.invalidate("conversation-a");
  if (gate.accepts(lease)) {
    conversations.push({ id: "conversation-a" });
  }

  assert.deepEqual(conversations, [{ id: "conversation-a" }]);
  assert.equal(gate.finish(lease), false);
});

test("one conversation is mutually exclusive and can restart after its lease finishes", () => {
  const gate = createConversationQueueGate();
  gate.activate();

  const first = gate.begin("conversation-a");
  const duplicate = gate.begin("conversation-a");

  assert.ok(first);
  assert.equal(duplicate, null);
  assert.equal(gate.finish(first), true);

  const restarted = gate.begin("conversation-a");
  assert.ok(restarted);
  assert.equal(gate.accepts(restarted), true);
});

test("an old lease cannot release a newer lease for the same conversation", () => {
  const gate = createConversationQueueGate();
  gate.activate();

  const first = gate.begin("conversation-a");
  gate.invalidate("conversation-a");
  const second = gate.begin("conversation-a");

  assert.ok(second);
  assert.equal(gate.finish(first), false);
  assert.equal(gate.isRunning("conversation-a"), true);
  assert.equal(gate.finish(second), true);
});

test("cancelAll invalidates every lease and prevents new queues", () => {
  const gate = createConversationQueueGate();
  gate.activate();
  const first = gate.begin("conversation-a");
  const second = gate.begin("conversation-b");

  gate.cancel();

  assert.equal(gate.accepts(first), false);
  assert.equal(gate.accepts(second), false);
  assert.equal(gate.finish(first), false);
  assert.equal(gate.begin("conversation-c"), null);
});
