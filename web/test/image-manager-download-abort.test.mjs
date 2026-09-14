import assert from "node:assert/strict";
import test from "node:test";

import { createDownloadAbortRegistry } from "../src/lib/download-lifecycle.js";

test("unmount cancellation aborts every active image download", () => {
  const registry = createDownloadAbortRegistry();
  registry.activate();
  const first = registry.begin();
  const second = registry.begin();

  registry.cancel();

  assert.equal(first.signal.aborted, true);
  assert.equal(second.signal.aborted, true);
  assert.equal(registry.activeCount(), 0);
});

test("one download settling does not cancel a concurrent download", () => {
  const registry = createDownloadAbortRegistry();
  registry.activate();
  const first = registry.begin();
  const second = registry.begin();

  registry.finish(first);

  assert.equal(first.signal.aborted, false);
  assert.equal(second.signal.aborted, false);
  assert.equal(registry.activeCount(), 1);
});
