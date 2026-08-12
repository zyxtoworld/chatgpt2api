import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createChatPanelRequestGate } from "../src/lib/chat-panel-request-gate.js";
import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/debug/components/chat-panel.tsx", import.meta.url)),
  "utf8",
);

test("ChatPanel owns the chat request and cancels it on unmount", () => {
  assert.match(source, /createChatPanelRequestGate/);
  assert.match(source, /chatRequestGateRef/);
  assert.match(source, /requestGate\.cancel\(\)/);
  assert.match(source, /requestGate\.acceptsChat\(requestOwner\)/);
});

test("clearing ChatPanel invalidates the current request and immediately clears loading", () => {
  assert.match(source, /chatRequestGateRef\.current\.clear\(\)/);
  assert.match(source, /chatRequestGateRef\.current\.clear\(\)[\s\S]*?setLoading\(false\)/);

  const owner = createLatestActionOwner();
  const requestOwner = owner.begin();
  const events = ["loading:true"];
  let loading = true;

  owner.invalidate();
  loading = false;
  events.push("clear", `loading:${loading}`);
  if (owner.accepts(requestOwner)) events.push("result");
  if (owner.accepts(requestOwner)) events.push("error");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.equal(loading, false);
  assert.deepEqual(events, ["loading:true", "clear", "loading:false"]);

  const nextRequestOwner = owner.begin();
  assert.equal(owner.accepts(nextRequestOwner), true, "clear must keep the mounted owner reusable");

  if (owner.accepts(requestOwner)) events.push("late-success");
  if (owner.accepts(requestOwner)) events.push("late-error");
  if (owner.accepts(requestOwner)) events.push("late-finally");
  assert.deepEqual(events, ["loading:true", "clear", "loading:false"]);
});

test("the production ChatPanel request gate drops all pre-clear callbacks and allows a new send", () => {
  const gate = createChatPanelRequestGate();
  gate.activate();
  const chatRequest = gate.beginChat();
  const imageRead = gate.beginImageRead();
  let loading = true;
  const events = [];

  gate.clear();
  loading = false;
  if (gate.acceptsChat(chatRequest)) events.push("success");
  if (gate.acceptsChat(chatRequest)) events.push("error");
  if (gate.acceptsChat(chatRequest)) events.push("finally");
  if (gate.acceptsImageRead(imageRead)) events.push("image");

  assert.equal(loading, false);
  assert.deepEqual(events, []);

  const nextChatRequest = gate.beginChat();
  assert.equal(gate.acceptsChat(nextChatRequest), true);
});

test("concurrent image reads share the active epoch, while clear and cancel invalidate all reads", () => {
  const gate = createChatPanelRequestGate();
  gate.activate();
  const first = gate.beginImageRead();
  const second = gate.beginImageRead();

  assert.equal(gate.acceptsImageRead(first), true);
  assert.equal(gate.acceptsImageRead(second), true);

  gate.clear();
  assert.equal(gate.acceptsImageRead(first), false);
  assert.equal(gate.acceptsImageRead(second), false);

  const next = gate.beginImageRead();
  assert.equal(gate.acceptsImageRead(next), true);
  gate.cancel();
  assert.equal(gate.acceptsImageRead(next), false);
});

test("async image reads are owned by ChatPanel lifetime and clear action", () => {
  assert.match(source, /chatRequestGateRef/);
  assert.match(source, /requestGate\.cancel\(\)/);
  assert.match(source, /requestGate\.acceptsImageRead\(readOwner\)/);

  const owner = createLatestActionOwner();
  const readOwner = owner.begin();
  const images = [];
  owner.invalidate();
  if (owner.accepts(readOwner)) images.push("late-image");
  assert.deepEqual(images, []);

  const unmountedOwner = createLatestActionOwner();
  const unmountedRead = unmountedOwner.begin();
  unmountedOwner.cancel();
  assert.equal(unmountedOwner.accepts(unmountedRead), false);
});

test("a late chat response after leaving the tab publishes no result, error, or loading state", () => {
  const owner = createLatestActionOwner();
  const requestOwner = owner.begin();
  const events = [];

  owner.cancel();
  if (owner.accepts(requestOwner)) events.push("result");
  if (owner.accepts(requestOwner)) events.push("error");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});

test("a replaced chat request cannot overwrite the latest request", () => {
  const owner = createLatestActionOwner();
  const first = owner.begin();
  const second = owner.begin();
  let result = "current";

  if (owner.accepts(first)) result = "stale";
  assert.equal(result, "current");
  if (owner.accepts(second)) result = "latest";
  assert.equal(result, "latest");
});
