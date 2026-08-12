import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { normalizeSearchSources } from "../src/lib/search-source-url.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/debug/components/search-panel.tsx", import.meta.url)),
  "utf8",
);

test("SearchPanel renders and counts only normalized source links", () => {
  assert.match(source, /normalizeSearchSources\(result\?\.sources\)/);
  assert.match(source, /safeSources\.length/);
  assert.match(source, /safeSources\.map/);
  assert.doesNotMatch(source, /result\.sources\.map/);
});

test("search source links only expose canonical HTTP URLs", () => {
  const sources = normalizeSearchSources([
    { title: "valid", url: " HTTPS://Example.Test/path?q=1#section ", snippet: "ok" },
    { title: "duplicate", url: "https://example.test/path?q=1#other" },
    { title: "data", url: "data:text/html,<script>alert(1)</script>" },
    { title: "javascript", url: "javascript:alert(1)" },
    { title: "relative", url: "/settings" },
    { title: "protocol relative", url: "//example.test/path" },
    { title: "userinfo", url: "https://user:secret@example.test/path" },
    { title: "backslash", url: "https://example.test\\@attacker.test/path" },
    { title: "bad port", url: "https://example.test:bad/path" },
    { title: "underscore host", url: "https://bad_host.test/path" },
    { title: "empty label", url: "https://foo..bar/path" },
    { title: "leading hyphen", url: "https://-bad.test/path" },
    { title: "trailing hyphen", url: "https://bad-.test/path" },
    { title: "encoded host", url: "https://%65xample.test/path" },
    { title: "control", url: "https://example.test/\nsecret" },
  ]);

  assert.deepEqual(sources, [
    {
      title: "valid",
      url: "https://example.test/path?q=1",
      snippet: "ok",
    },
  ]);
});

test("search source links normalize one DNS root dot", () => {
  assert.deepEqual(
    normalizeSearchSources([
      { title: "root dot", url: "https://Example.Test./path#fragment" },
    ]),
    [
      {
        title: "root dot",
        url: "https://example.test/path",
        snippet: "",
      },
    ],
  );
});

test("search source links preserve valid IPv6 and non-default ports", () => {
  assert.deepEqual(
    normalizeSearchSources([
      { title: "ipv6", url: "http://[2001:db8::1]:8080/a?b=c#fragment" },
    ]),
    [
      {
        title: "ipv6",
        url: "http://[2001:db8::1]:8080/a?b=c",
        snippet: "",
      },
    ],
  );
});
