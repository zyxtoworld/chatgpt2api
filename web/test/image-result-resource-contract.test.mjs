import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(
  new URL("../src/app/image/components/image-results.tsx", import.meta.url),
  "utf8",
);

test("stored base64 images use a stable data URL without process-lifetime caches", async () => {
  const { getStoredImageSrc } = await import("../src/lib/stored-image-source.js");

  assert.equal(
    getStoredImageSrc({ b64_json: "aGVsbG8=", url: "https://ignored.example/image.png" }),
    "data:image/png;base64,aGVsbG8=",
  );
  assert.equal(
    getStoredImageSrc({ url: "/images/result.png" }),
    "/images/result.png",
  );
  assert.equal(getStoredImageSrc({}), "");
});

test("ImageResults does not retain base64 payloads or blob URLs in module caches", () => {
  assert.match(source, /import \{ getStoredImageSrc \} from "@\/lib\/stored-image-source"/);
  assert.doesNotMatch(source, /b64BlobUrlCache/);
  assert.doesNotMatch(source, /base64SizeCache/);
});
