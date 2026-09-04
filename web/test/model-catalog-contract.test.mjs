import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { parseModelList } from "../src/lib/model-catalog.js";

const apiSource = readFileSync(new URL("../src/lib/api.ts", import.meta.url), "utf8");
const requestSource = readFileSync(new URL("../src/lib/request.ts", import.meta.url), "utf8");
const accountsSource = readFileSync(new URL("../src/app/accounts/page.tsx", import.meta.url), "utf8");

test("model parsing preserves the authenticated upstream list and every model field", () => {
  const models = [{
    id: "fixture-model",
    object: "model",
    created: 1,
    owned_by: "fixture-owner",
    permission: [{ id: "fixture-permission" }],
    root: "fixture-root",
    parent: null,
    allow_anonymous: false,
    supported_account_types: ["free"],
    supported_reasoning_efforts: ["standard"],
  }];

  assert.deepEqual(parseModelList({ object: "list", data: models }), models);
});

test("invalid model responses fail closed so the caller can retain its last good list", () => {
  const previous = [{ id: "last-good-model" }];

  assert.equal(parseModelList(undefined), null);
  assert.equal(parseModelList({ object: "list", data: {} }), null);
  assert.equal(parseModelList({ object: "list", data: [{ id: 42 }] }), null);
  assert.deepEqual(previous, [{ id: "last-good-model" }]);
});

test("the account directory uses the authenticated non-paginated upstream model endpoint", () => {
  assert.match(apiSource, /httpRequest<ModelListResponse>\("\/v1\/models", \{ signal \}\)/);
  assert.match(requestSource, /headers\.Authorization = `Bearer \$\{authKey\}`/);
  assert.match(apiSource, /supported_reasoning_efforts\?: string\[\]/);
  assert.match(accountsSource, /parseModelList\(data\)/);
  assert.match(accountsSource, /if \(models === null\)/);
  assert.doesNotMatch(accountsSource, /setAvailableModels\(\[\]\)/);
});
