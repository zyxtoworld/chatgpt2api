import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(new URL("../src/app/image/page.tsx", import.meta.url), "utf8");

function functionBody(signature, nextSignature) {
  const start = source.indexOf(signature);
  assert.ok(start >= 0, `missing production anchor: ${signature}`);
  const end = source.indexOf(nextSignature, start + signature.length);
  return source.slice(start, end >= 0 ? end : source.length);
}

test("image page uses a conversation lease for queue lifecycle", () => {
  assert.match(source, /createConversationQueueGate/);
  assert.match(source, /const conversationQueueGateRef = useRef\(createConversationQueueGate\(\)\)/);
  assert.match(source, /conversationQueueGate\.activate\(\)/);
  assert.match(source, /return \(\) => conversationQueueGate\.cancel\(\)/);

  const queueBody = functionBody(
    "const runConversationQueue = useCallback(",
    "const handleRegenerateTurn = useCallback(",
  );
  assert.match(queueBody, /queueGate\.begin\(conversationId\)/);
  assert.match(queueBody, /queueGate\.accepts\(queueLease\)/);
  assert.match(queueBody, /queueGate\.finish\(queueLease\)/);
  assert.doesNotMatch(queueBody, /current \?\? snapshot/);
  assert.doesNotMatch(queueBody, /activeConversationQueueIds/);
});

test("destructive image actions invalidate before their API mutation", () => {
  const deleteBody = functionBody(
    "const handleDeleteConversation = async",
    "const handleDeleteTurnPart = async",
  );
  assert.ok(deleteBody.indexOf("conversationQueueGateRef.current.invalidate(id)") >= 0);
  assert.ok(deleteBody.indexOf("conversationQueueGateRef.current.invalidate(id)") < deleteBody.indexOf("deleteImageConversation(id)"));

  const clearBody = functionBody("const handleClearHistory = async", "const handleRenameConversation = async");
  assert.ok(clearBody.indexOf("conversationQueueGateRef.current.invalidateAll()") >= 0);
  assert.ok(clearBody.indexOf("conversationQueueGateRef.current.invalidateAll()") < clearBody.indexOf("clearImageConversations()"));
  assert.match(clearBody, /listImageConversations\(\)/);
});

test("queue no-op updates return null instead of fabricating a conversation", () => {
  const queueBody = functionBody(
    "const runConversationQueue = useCallback(",
    "const handleRegenerateTurn = useCallback(",
  );
  assert.match(queueBody, /if \(!acceptsQueue\(\) \|\| !current\) \{\s*return null;/);
  assert.doesNotMatch(queueBody, /current \?\? snapshot/);
});
