import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const root = new URL("../../", import.meta.url);
const read = (relativePath) => readFileSync(new URL(relativePath, root), "utf8");

const dockerfile = read("Dockerfile");
const dockerWorkflow = read(".github/workflows/docker-publish.yml");
const compose = read("docker-compose.yml");
const localCompose = read("docker-compose.local.yml");

test("the published app target is the Rust runtime without private config or Python runtime", () => {
  assert.match(dockerfile, /AS app/);
  assert.match(dockerfile, /chatgpt2api-rust/);
  assert.doesNotMatch(dockerfile, /python:3\.13/);
  assert.doesNotMatch(dockerfile, /COPY\s+config\.json/);
  assert.match(dockerWorkflow, /target:\s*app/);
});

test("main Compose entrypoints keep config outside the image", () => {
  for (const source of [compose, localCompose]) {
    assert.match(source, /\.\/config\.json:\/app\/config\.json/);
    assert.doesNotMatch(source, /config\.example\.json:\/app\/config\.json/);
  }
});

test("the checked-in config example is an explicit non-secret placeholder", () => {
  const example = JSON.parse(read("config.example.json"));
  assert.equal(example["auth-key"], "CHANGE_ME_BEFORE_USE");
  assert.doesNotMatch(JSON.stringify(example), /(?:sk-|Bearer\s+|ghp_|xox[baprs]-)/i);
});
