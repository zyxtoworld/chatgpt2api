import assert from "node:assert/strict";
import test from "node:test";

import { commitSynchronousSnapshot } from "../src/lib/synchronous-snapshot.js";

test("synchronous snapshot commit updates identity before a React update is scheduled", () => {
  const ref = { current: [{ id: "old" }] };

  const removed = commitSynchronousSnapshot(ref, []);
  assert.strictEqual(removed, ref.current);
  assert.deepEqual(ref.current, []);

  const replaced = commitSynchronousSnapshot(ref, (current) => [
    ...current,
    { id: "new" },
  ]);
  assert.strictEqual(replaced, ref.current);
  assert.deepEqual(ref.current, [{ id: "new" }]);
});
