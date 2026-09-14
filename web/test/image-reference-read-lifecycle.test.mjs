import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLifecycleActionOwner } from "../src/lib/lifecycle-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/image/page.tsx", import.meta.url)),
  "utf8",
).replace(/\r\n/g, "\n");

test("image reference reads are wired to a dedicated lifecycle owner", () => {
  assert.match(source, /referenceImageReadOwnerRef/);
  assert.match(source, /const readOwner = referenceImageReadOwnerRef\.current;/);
  assert.match(source, /const readAction = readOwner\.begin\(\)/);
  assert.match(source, /readOwner\.accepts\(readAction\)/);
  assert.match(source, /referenceImageReadOwnerRef\.current\.invalidate\(\)/);
  assert.match(source, /referenceImageReadOwner\.cancel\(\)/);
  const selectionEffect = source.slice(
    source.indexOf("useEffect(() => {\n    continueEditOwnerRef.current.invalidate();"),
    source.indexOf("}, [selectedConversationId]);") + "}, [selectedConversationId]);".length,
  );
  assert.match(selectionEffect, /referenceImageReadOwnerRef\.current\.invalidate\(\)/);
});

test("concurrent reference reads share an active epoch and clear invalidates all", () => {
  const owner = createLifecycleActionOwner();
  owner.activate();
  const first = owner.begin();
  const second = owner.begin();

  assert.equal(owner.accepts(first), true);
  assert.equal(owner.accepts(second), true);

  owner.invalidate();
  assert.equal(owner.accepts(first), false);
  assert.equal(owner.accepts(second), false);

  const next = owner.begin();
  assert.equal(owner.accepts(next), true);
});

test("changing conversation identity invalidates old reads while allowing a new read", () => {
  const owner = createLifecycleActionOwner();
  owner.activate();
  const conversationARead = owner.begin();
  const concurrentARead = owner.begin();

  assert.equal(owner.accepts(conversationARead), true);
  assert.equal(owner.accepts(concurrentARead), true);

  // The selected-conversation effect represents switching from A to B.
  owner.invalidate();
  assert.equal(owner.accepts(conversationARead), false);
  assert.equal(owner.accepts(concurrentARead), false);

  const conversationBRead = owner.begin();
  assert.equal(owner.accepts(conversationBRead), true);
});

test("unmount rejects late reference read callbacks", () => {
  const owner = createLifecycleActionOwner();
  owner.activate();
  const read = owner.begin();

  owner.cancel();
  assert.equal(owner.accepts(read), false);
});
