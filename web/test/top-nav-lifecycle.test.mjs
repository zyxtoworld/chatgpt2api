import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";
import { runLogoutAfterClear } from "../src/lib/logout-action.js";

const source = readFileSync(new URL("../src/components/top-nav.tsx", import.meta.url), "utf8");

test("TopNav owns session validation and invalidates it before logout", () => {
  assert.match(source, /createLatestActionOwner/);
  assert.match(source, /authSessionOwnerRef/);
  assert.match(source, /authSessionOwner\.accepts/);
  assert.match(source, /authSessionOwnerRef\.current\.invalidate\(\)/);
  assert.match(source, /runLogoutAfterClear/);
});

test("TopNav third-party reloads accept only the latest response for the session", async () => {
  assert.match(source, /thirdPartyOwnerRef/);
  assert.match(source, /thirdPartyOwner\.begin\(owner\)/);
  assert.match(source, /thirdPartyOwner\.accepts\(requestOwner, owner\)/);
  assert.match(source, /thirdPartyOwner\.cancel\(\)/);

  const owner = createLatestActionOwner();
  const session = { id: "session" };
  const events = [];
  const first = owner.begin(session);
  const second = owner.begin(session);

  if (owner.accepts(first, session)) events.push("old");
  if (owner.accepts(second, session)) events.push("latest");

  assert.deepEqual(events, ["latest"]);
});

test("TopNav aborts a third-party request when its session owner is cleaned up", () => {
  assert.match(source, /thirdPartyAbortControllerRef/);
  assert.match(source, /fetchThirdPartyApps\(abortController\.signal\)/);
  assert.match(source, /thirdPartyAbortControllerRef\.current\?\.abort\(\);/);
  assert.match(source, /return \(\) => \{[\s\S]*?thirdPartyAbortControllerRef\.current\?\.abort\(\);/);
});

test("logout drops all callbacks from the pending session validation", () => {
  const owner = createLatestActionOwner();
  const pending = owner.begin("/accounts");
  const events = [];

  owner.invalidate();
  if (owner.accepts(pending)) events.push("session", "third-party", "redirect", "finally");

  assert.deepEqual(events, []);

  const next = owner.begin("/image");
  assert.equal(owner.accepts(next), true, "a later route may establish a fresh validation");
});

test("logout does not navigate or claim success when storage clear fails", async () => {
  for (const failure of [new Error("session removal failed"), new Error("key removal failed")]) {
    const events = [];
    const result = await runLogoutAfterClear({
      clearSession: async () => {
        events.push("clear");
        throw failure;
      },
      onSuccess: () => events.push("navigate"),
      onFailure: () => events.push("toast"),
    });
    assert.equal(result, false);
    assert.deepEqual(events, ["clear", "toast"]);
  }
});
