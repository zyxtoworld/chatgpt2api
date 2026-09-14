import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { applyImageConversationUpdate } from "../src/lib/image-conversation-update.js";
import { createLifecycleActionOwner } from "../src/lib/lifecycle-action-owner.js";
import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(new URL("../src/app/image/page.tsx", import.meta.url), "utf8");

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return { promise, resolve, reject };
}

function functionBody(signature, nextSignature) {
  const start = source.indexOf(signature);
  assert.ok(start >= 0, `missing production anchor: ${signature}`);
  const end = source.indexOf(nextSignature, start + signature.length);
  return source.slice(start, end >= 0 ? end : source.length);
}

test("a history mutation fences an in-flight history load and permits a fresh load", () => {
  const loadOwner = createLatestActionOwner();
  const staleLoad = loadOwner.begin();

  loadOwner.invalidate();
  assert.equal(loadOwner.accepts(staleLoad), false);

  const freshLoad = loadOwner.begin();
  assert.equal(loadOwner.accepts(freshLoad), true);
});

test("image history production wiring fences reads before recovery or mutation commits", () => {
  assert.match(source, /const historyLoadOwnerRef = useRef\(createLatestActionOwner\(\)\)/);
  const loadBody = functionBody("const loadHistory = useCallback(async () => {", "// Handle bfcache");
  assert.match(loadBody, /const requestOwner = historyLoadOwner\.begin\(\)/);
  assert.match(loadBody, /recoverConversationHistory\(items, isCurrentRequest\)/);

  const invalidateBody = functionBody("const invalidateHistoryLoad = useCallback", "const beginHistoryMutation = useCallback");
  assert.match(invalidateBody, /historyLoadOwnerRef\.current\.invalidate\(\)/);
  assert.match(invalidateBody, /setIsLoadingHistory\(false\)/);
  const mutationBody = functionBody("const beginHistoryMutation = useCallback", "const persistConversation = useCallback");
  assert.match(mutationBody, /invalidateHistoryLoad\(\)/);
  assert.match(mutationBody, /historyMutationOwnerRef\.current\.begin\(\)/);
  assert.doesNotMatch(source, /const mutationOwner = historyMutationOwner\.begin\(\)/);

  const recoveryBody = functionBody("async function recoverConversationHistory", "function ImagePageContent");
  assert.match(recoveryBody, /if \(changed && isCurrent\(\)\)/);
  assert.match(recoveryBody, /syncConversationImageTasks\(normalized, isCurrent\)/);
});

test("image destructive actions have a component-lifetime owner", () => {
  assert.match(source, /const historyMutationOwnerRef = useRef\(createLifecycleActionOwner\(\)\)/);
  assert.match(source, /historyMutationOwner\.cancel\(\)/);

  for (const [signature, nextSignature] of [
    ["const handleDeleteConversation = async", "const handleDeleteTurnPart = async"],
    ["const handleClearHistory = async", "const handleRenameConversation = async"],
    ["const handleRenameConversation = async", "const openDeleteConversationConfirm ="],
  ]) {
    const body = functionBody(signature, nextSignature);
    assert.match(body, /const historyMutationOwner = historyMutationOwnerRef\.current;/);
    assert.match(body, /const mutationOwner = beginHistoryMutation\(\)/);
    assert.match(body, /historyMutationOwner\.accepts\(mutationOwner\)/);
  }
});

test("unmount drops late delete failure, reload, success, and finally callbacks", async () => {
  const owner = createLifecycleActionOwner();
  const action = owner.begin();
  const request = deferred();
  const events = [];

  const run = request.promise.then(
    () => {
      if (owner.accepts(action)) events.push("success");
    },
    () => {
      if (owner.accepts(action)) {
        events.push("error");
        events.push("reload");
      }
    },
  ).finally(() => {
    if (owner.accepts(action)) events.push("finally");
  });

  owner.cancel();
  request.reject(new Error("late delete failure"));
  await run;

  assert.deepEqual(events, []);
});

test("clear and delete actions may coexist, but both stop publishing after unmount", async () => {
  const owner = createLifecycleActionOwner();
  const first = owner.begin();
  const second = owner.begin();
  const events = [];

  assert.equal(owner.accepts(first), true);
  assert.equal(owner.accepts(second), true);
  owner.cancel();

  for (const action of [first, second]) {
    if (owner.accepts(action)) events.push("publish");
  }
  assert.deepEqual(events, []);
});

test("post-save image actions guard queue starts and success toasts", () => {
  for (const [signature, nextSignature] of [
    ["const handleRegenerateTurn = useCallback(", "const handleRetryImage = useCallback("],
    ["const handleRetryImage = useCallback(", "const handleTimeoutRetryContinue = useCallback("],
    ["const handleSubmit = async () => {", "\n  return ("],
  ]) {
    const body = functionBody(signature, nextSignature);
    const persistIndex = body.indexOf("await persistConversation(");
    assert.ok(persistIndex >= 0, `${signature} must persist before publishing follow-up effects`);
    const guardIndex = body.indexOf("historyMutationOwner.accepts(mutationOwner)", persistIndex);
    assert.ok(guardIndex > persistIndex, `${signature} must guard effects after persistence`);
  }
});

test("an unmounted post-save action publishes neither queue work nor success", async () => {
  const owner = createLifecycleActionOwner();
  const action = owner.begin();
  const save = deferred();
  const events = [];

  const run = save.promise.then(() => {
    if (!owner.accepts(action)) return;
    events.push("queue", "success");
  });

  owner.cancel();
  save.resolve();
  await run;
  assert.deepEqual(events, []);
});

test("timeout retry continuation guards resume completion by the same lifetime owner", () => {
  const body = functionBody(
    "const handleTimeoutRetryContinue = useCallback(async (taskId: string) => {",
    "const handleDismissErrors = useCallback",
  );
  const resumeIndex = body.indexOf("await resumeImagePoll(");
  assert.ok(resumeIndex >= 0, "timeout continuation must await the resume API");
  assert.ok(
    body.indexOf("historyMutationOwner.accepts(mutationOwner)", resumeIndex) > resumeIndex,
    "timeout continuation must reject late state/toast effects",
  );
});

test("sequential renames compose from the current snapshot instead of a stale render", () => {
  let conversations = [
    { id: "a", title: "A", updatedAt: "1" },
    { id: "b", title: "B", updatedAt: "1" },
  ];

  conversations = applyImageConversationUpdate(conversations, "a", (current) => (
    current ? { ...current, title: "A2", updatedAt: "2" } : null
  )).conversations;
  conversations = applyImageConversationUpdate(conversations, "b", (current) => (
    current ? { ...current, title: "B2", updatedAt: "3" } : null
  )).conversations;

  assert.deepEqual(
    Object.fromEntries(conversations.map((conversation) => [conversation.id, conversation.title])),
    { a: "A2", b: "B2" },
  );
});

test("the production rename handler updates the authoritative conversation snapshot", () => {
  const body = functionBody(
    "const handleRenameConversation = async",
    "const openDeleteConversationConfirm =",
  );
  assert.match(body, /await updateConversation\(\s*id,/);
  assert.match(body, /\{ persist: false \}/);
  assert.doesNotMatch(body, /conversations\.map\(/);
});

test("timeout continuation resolves the exact clicked task across conversations", async () => {
  const helpers = await import("../src/lib/image-conversation-update.js");
  assert.equal(typeof helpers.findImageTaskConversation, "function");

  const conversations = [
    { id: "conversation-a", turns: [{ images: [{ taskId: "task-a", status: "error", error: "A 超时" }] }] },
    { id: "conversation-b", turns: [{ images: [{ taskId: "task-b", status: "error", error: "B 超时" }] }] },
  ];

  assert.equal(helpers.findImageTaskConversation(conversations, "task-a")?.id, "conversation-a");
  assert.equal(helpers.findImageTaskConversation(conversations, "task-b")?.id, "conversation-b");
  assert.equal(helpers.findImageTaskConversation(conversations, "missing"), null);
});

test("the production timeout handler consumes the clicked task id without a global timeout slot", () => {
  const body = functionBody(
    "const handleTimeoutRetryContinue = useCallback(",
    "const handleDismissErrors = useCallback",
  );
  assert.match(body, /async \(taskId: string\)/);
  assert.match(body, /findImageTaskConversation\(conversationsRef\.current, taskId\)/);
  assert.doesNotMatch(body, /timeoutRetry/);
  assert.doesNotMatch(source, /const \[timeoutRetry, setTimeoutRetry\]/);
});
