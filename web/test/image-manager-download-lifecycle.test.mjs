import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLifecycleActionOwner } from "../src/lib/lifecycle-action-owner.js";

const pageSource = readFileSync(
  fileURLToPath(new URL("../src/app/image-manager/page.tsx", import.meta.url)),
  "utf8",
);
const apiSource = readFileSync(
  fileURLToPath(new URL("../src/lib/api.ts", import.meta.url)),
  "utf8",
);

test("image-manager downloads use a shared lifecycle owner and cancel it on unmount", () => {
  assert.match(pageSource, /createLifecycleActionOwner/);
  assert.match(pageSource, /downloadOwnerRef/);
  assert.match(pageSource, /downloadOwner\.cancel\(\)/);
  assert.match(pageSource, /downloadImages\(paths, isDownloadActive, controller\.signal\)/);
  assert.match(pageSource, /downloadSingleImage\(item\.rel, isDownloadActive, controller\.signal\)/);
  assert.match(apiSource, /downloadImages\(paths: string\[], isActive\?: \(\) => boolean, signal\?: AbortSignal\)/);
  assert.match(apiSource, /downloadSingleImage\(path: string, isActive\?: \(\) => boolean, signal\?: AbortSignal\)/);
  assert.match(apiSource, /signal\?: AbortSignal/);
  assert.match(pageSource, /downloadAbortRegistryRef/);
  assert.match(pageSource, /downloadAbortRegistry\.cancel\(\)/);
  assert.match(apiSource, /if \(isActive && !isActive\(\)\) return false;/);
});

test("multiple downloads in one active page share an epoch, while unmount rejects all", () => {
  const owner = createLifecycleActionOwner();
  owner.activate();
  const first = owner.begin();
  const second = owner.begin();
  assert.equal(owner.accepts(first), true);
  assert.equal(owner.accepts(second), true);

  owner.cancel();
  assert.equal(owner.accepts(first), false);
  assert.equal(owner.accepts(second), false);
});
