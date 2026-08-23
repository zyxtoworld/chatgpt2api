import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/debug/components/skill-panel.tsx", import.meta.url)),
  "utf8",
);

test("SkillPanel aborts its settings request on unmount", () => {
  assert.match(source, /const controller = new AbortController\(\);/);
  assert.match(source, /fetchSettingsConfig\(controller\.signal\)/);
  assert.match(source, /return \(\) => \{[\s\S]*?controller\.abort\(\);/);
});
