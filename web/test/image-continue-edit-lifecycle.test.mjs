import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/image/page.tsx", import.meta.url)),
  "utf8",
);

test("continue-edit results are owned by the current conversation and component lifetime", () => {
  assert.match(source, /continueEditOwnerRef/);
  assert.match(source, /continueEditAbortRef\.current\?\.abort\(\)/);
  assert.match(source, /buildReferenceImageFromStoredImage\([\s\S]*abortController\.signal/);
  assert.match(source, /continueEditOwner\.cancel\(\)/);
  assert.match(source, /continueEditOwner\.accepts\(requestOwner, conversationId\)/);
});

test("a late continue-edit response from conversation A cannot select A after switching to B", () => {
  const owner = createLatestActionOwner();
  const requestOwner = owner.begin("conversation-a");
  let selectedConversation = "conversation-b";
  let referenceImages = ["current-b"];

  owner.invalidate();
  if (owner.accepts(requestOwner, "conversation-a")) {
    selectedConversation = "conversation-a";
    referenceImages = ["stale-a"];
  }

  assert.equal(selectedConversation, "conversation-b");
  assert.deepEqual(referenceImages, ["current-b"]);
});

test("an unmounted continue-edit response publishes no selection or reference image", () => {
  const owner = createLatestActionOwner();
  const requestOwner = owner.begin("conversation-a");
  const events = [];

  owner.cancel();
  if (owner.accepts(requestOwner, "conversation-a")) events.push("selection");
  if (owner.accepts(requestOwner, "conversation-a")) events.push("reference");
  if (owner.accepts(requestOwner, "conversation-a")) events.push("toast");

  assert.deepEqual(events, []);
});
