import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { loadManagedImagesWithTags } from "../src/lib/image-manager-load.js";

const imageManagerSource = readFileSync(
  new URL("../src/app/image-manager/page.tsx", import.meta.url),
  "utf8",
);

test("a failed auxiliary tag request does not hide a valid image list", async () => {
  const data = {
    items: [
      { rel: "a.png", tags: ["red", "fruit"] },
      { rel: "b.png", tags: ["fruit", "green"] },
    ],
  };

  const result = await loadManagedImagesWithTags(
    async () => data,
    async () => Promise.reject(new Error("tags unavailable")),
  );

  assert.equal(result.data, data);
  assert.deepEqual(result.tags, ["red", "fruit", "green"]);
});

test("the authoritative tag response is used when it succeeds", async () => {
  const result = await loadManagedImagesWithTags(
    async () => ({ items: [{ rel: "a.png", tags: ["derived"] }] }),
    async () => ({ tags: ["server", "ordered"] }),
  );

  assert.deepEqual(result.tags, ["server", "ordered"]);
});

test("a failed image list request still rejects the load", async () => {
  await assert.rejects(
    loadManagedImagesWithTags(
      async () => Promise.reject(new Error("images unavailable")),
      async () => ({ tags: ["server"] }),
    ),
    /images unavailable/,
  );
});

test("the image manager production entry uses the auxiliary-failure fallback", () => {
  assert.match(imageManagerSource, /loadManagedImagesWithTags/);
  assert.doesNotMatch(imageManagerSource, /const \[data, tagsData\] = await Promise\.all\(/);
});
