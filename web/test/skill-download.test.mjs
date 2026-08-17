import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { downloadBlobFile, downloadTextFile } from "../src/lib/download-text.js";

test("all one-shot browser downloads use the shared delayed-release helper", () => {
  const callers = [
    ["../src/lib/api.ts", "downloadBlobFile"],
    ["../src/app/accounts/page.tsx", "downloadTextFile"],
    ["../src/app/settings/components/backup-settings-card.tsx", "downloadBlobFile"],
    ["../src/app/image/components/image-results.tsx", "downloadBlobFile"],
  ];
  for (const [relativePath, helper] of callers) {
    const source = readFileSync(new URL(relativePath, import.meta.url), "utf8");
    assert.match(source, new RegExp(`\\b${helper}\\b`), relativePath);
    assert.doesNotMatch(source, /revokeObjectURL/, relativePath);
  }
});

test("downloadBlobFile keeps the object URL alive until after click", () => {
  const events = [];
  const scheduled = [];
  const urlApi = {
    createObjectURL(blob) {
      events.push(["create", blob]);
      return "blob:file";
    },
    revokeObjectURL(url) {
      events.push(["revoke", url]);
    },
  };
  const documentRef = {
    createElement(tagName) {
      assert.equal(tagName, "a");
      return {
        click() {
          events.push(["click"]);
          assert.deepEqual(events.map(([kind]) => kind), ["create", "click"]);
        },
      };
    },
  };

  downloadBlobFile({ bytes: "payload" }, "export.bin", {
    documentRef,
    urlApi,
    schedule(callback) {
      scheduled.push(callback);
    },
  });

  assert.deepEqual(events.map(([kind]) => kind), ["create", "click"]);
  assert.equal(scheduled.length, 1);
  scheduled[0]();
  assert.deepEqual(events.map(([kind]) => kind), ["create", "click", "revoke"]);
  assert.equal(events[2][1], "blob:file");
});

test("downloadTextFile keeps the object URL alive until after click", () => {
  const events = [];
  const scheduled = [];
  const urlApi = {
    createObjectURL(blob) {
      events.push(["create", blob]);
      return "blob:skill";
    },
    revokeObjectURL(url) {
      events.push(["revoke", url]);
    },
  };
  const documentRef = {
    createElement(tagName) {
      assert.equal(tagName, "a");
      return {
        click() {
          events.push(["click", urlApi.revokeObjectURL]);
          assert.deepEqual(events.map(([kind]) => kind), ["create", "click"]);
        },
      };
    },
  };

  downloadTextFile("---\nname: demo\n---", "SKILL.md", {
    documentRef,
    urlApi,
    blobCtor: class FakeBlob {
      constructor(parts, options) {
        this.parts = parts;
        this.options = options;
      }
    },
    schedule(callback) {
      scheduled.push(callback);
    },
  });

  assert.deepEqual(events.map(([kind]) => kind), ["create", "click"]);
  assert.equal(scheduled.length, 1);
  scheduled[0]();
  assert.deepEqual(events.map(([kind]) => kind), ["create", "click", "revoke"]);
  assert.equal(events[2][1], "blob:skill");
  assert.equal(events[0][1].options.type, "text/markdown;charset=utf-8");
});

test("downloadTextFile preserves a caller-provided MIME type", () => {
  let blobOptions;
  downloadTextFile("token", "accounts.txt", {
    blobCtor: class FakeBlob {
      constructor(_parts, options) {
        blobOptions = options;
      }
    },
    urlApi: {
      createObjectURL() {
        return "blob:text";
      },
      revokeObjectURL() {},
    },
    documentRef: {
      createElement() {
        return { click() {} };
      },
    },
    schedule(callback) {
      callback();
    },
    mimeType: "text/plain;charset=utf-8",
  });
  assert.deepEqual(blobOptions, { type: "text/plain;charset=utf-8" });
});

test("downloadBlobFile revokes the URL when link creation fails", () => {
  const revoked = [];
  assert.throws(
    () => downloadBlobFile({}, "broken.bin", {
      urlApi: {
        createObjectURL() {
          return "blob:broken";
        },
        revokeObjectURL(url) {
          revoked.push(url);
        },
      },
      documentRef: {
        createElement() {
          throw new Error("document unavailable");
        },
      },
      schedule(callback) {
        callback();
      },
    }),
    /document unavailable/,
  );
  assert.deepEqual(revoked, ["blob:broken"]);
});
