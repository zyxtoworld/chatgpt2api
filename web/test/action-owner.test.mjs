import assert from "node:assert/strict";
import test from "node:test";

import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

test("latest action owner rejects a late result after the candidate changes", () => {
  const owner = createLatestActionOwner();
  const first = owner.begin("http://proxy-a.test");

  owner.invalidate();

  assert.equal(owner.accepts(first, "http://proxy-a.test"), false);
  assert.equal(owner.accepts(first, "http://proxy-b.test"), false);
});

test("latest action owner rejects results after setup cleanup and accepts a new setup", () => {
  const owner = createLatestActionOwner();
  const first = owner.begin("http://proxy-a.test");

  owner.cancel();
  assert.equal(owner.accepts(first, "http://proxy-a.test"), false);

  owner.activate();
  const second = owner.begin("http://proxy-b.test");
  assert.equal(owner.accepts(first, "http://proxy-a.test"), false);
  assert.equal(owner.accepts(second, "http://proxy-b.test"), true);
});

test("an editor-scoped group commit ignores a late response after switch, close, or cleanup", () => {
  const owner = createLatestActionOwner();
  let groups = ["current"];
  let loading = false;
  let toasts = 0;

  const submit = (action, serverId, nextGroups) => {
    if (!owner.accepts(action, serverId)) return;
    groups = nextGroups;
    loading = false;
    toasts += 1;
  };

  const first = owner.begin("server-a");
  loading = true;
  owner.invalidate();
  submit(first, "server-a", ["stale-a"]);
  assert.deepEqual(groups, ["current"]);
  assert.equal(loading, true);
  assert.equal(toasts, 0);

  const second = owner.begin("server-b");
  owner.invalidate();
  submit(second, "server-b", ["closed-b"]);
  assert.deepEqual(groups, ["current"]);
  assert.equal(loading, true);
  assert.equal(toasts, 0);

  const third = owner.begin("server-c");
  owner.cancel();
  submit(third, "server-c", ["stale-c"]);
  assert.deepEqual(groups, ["current"]);
  assert.equal(loading, true);
  assert.equal(toasts, 0);
});
