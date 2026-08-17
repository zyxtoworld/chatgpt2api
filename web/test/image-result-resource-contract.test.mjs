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

test("stored image URLs reject executable and protocol-relative schemes", async () => {
  const { getStoredImageSrc } = await import("../src/lib/stored-image-source.js");

  assert.equal(getStoredImageSrc({ url: "javascript:alert('image-url-canary')" }), "");
  assert.equal(getStoredImageSrc({ url: "//attacker.example/image.png" }), "");
  assert.equal(getStoredImageSrc({ url: "data:text/html,image-url-canary" }), "");
  assert.equal(getStoredImageSrc({ url: "https://cdn.example/image.png?signature=ok" }), "https://cdn.example/image.png?signature=ok");
  assert.equal(getStoredImageSrc({ url: "/files/image.png" }), "/files/image.png");
});

test("ImageResults does not retain base64 payloads or blob URLs in module caches", () => {
  assert.match(source, /import \{[^}]*getStoredImageSrc[^}]*\} from "@\/lib\/stored-image-source"/);
  assert.doesNotMatch(source, /b64BlobUrlCache/);
  assert.doesNotMatch(source, /base64SizeCache/);
});

test("ImageResults downloads are owned by a cancellable component registry", () => {
  assert.match(source, /createDownloadAbortRegistry/);
  assert.match(source, /downloadOwnerRef/);
  assert.match(source, /downloadOwner\.cancel\(\)/);
  assert.match(source, /fetchImageAsFile/);
  assert.match(source, /downloadOwner\.begin\(\)/);
  assert.match(source, /downloadOwner\.finish\(/);
});
