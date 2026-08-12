import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createScrollCleanupSnapshot } from "../src/lib/image-scroll-cleanup.js";

const pageSource = readFileSync(
  fileURLToPath(new URL("../src/app/image/page.tsx", import.meta.url)),
  "utf8",
);

test("scroll cleanup keeps the mounted viewport even after the React ref changes", () => {
  const mountedViewport = { scrollTop: 321 };
  const viewportRef = { current: mountedViewport };
  const positions = new Map();
  const snapshot = createScrollCleanupSnapshot(viewportRef.current, positions);

  viewportRef.current = null;

  snapshot.persist("conversation-a");
  assert.equal(positions.get("conversation-a"), 321);
});

test("scroll cleanup safely ignores a missing mounted viewport or conversation", () => {
  const positions = new Map();
  createScrollCleanupSnapshot(null, positions).persist("conversation-a");
  createScrollCleanupSnapshot({ scrollTop: 12 }, positions).persist(null);
  assert.equal(positions.size, 0);
});

test("image page cleanup persists through the setup-time viewport snapshot", () => {
  const effectIndex = pageSource.indexOf("loadCancelledRef.current = false;");
  const positionsIndex = pageSource.indexOf("const scrollPositions = scrollPositionsRef.current", effectIndex);
  const snapshotIndex = pageSource.indexOf(
    "createScrollCleanupSnapshot(resultsViewportRef.current, scrollPositions)",
    positionsIndex,
  );
  const cleanupIndex = pageSource.indexOf("return () => {", snapshotIndex);
  const persistIndex = pageSource.indexOf("scrollCleanup.persist(lastConversationIdRef.current)", cleanupIndex);

  assert.ok(effectIndex >= 0);
  assert.ok(positionsIndex > effectIndex);
  assert.ok(snapshotIndex > positionsIndex);
  assert.ok(cleanupIndex > snapshotIndex);
  assert.ok(persistIndex > cleanupIndex);
  assert.doesNotMatch(pageSource.slice(cleanupIndex, persistIndex), /resultsViewportRef\.current/);
});
