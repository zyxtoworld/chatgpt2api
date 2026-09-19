import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { createMutationRequestGate } from "../src/lib/mutation-request-gate.js";

const componentSource = readFileSync(
  fileURLToPath(new URL("../src/app/settings/components/ccload-connections.tsx", import.meta.url)),
  "utf8",
);
const readmeSource = readFileSync(fileURLToPath(new URL("../../README.md", import.meta.url)), "utf8");
const apiSource = readFileSync(fileURLToPath(new URL("../src/lib/api.ts", import.meta.url)), "utf8");

test("ccLoad uses stable connection and channel wording without a pinned preview version", () => {
  assert.match(componentSource, />ccLoad 连接管理</);
  assert.match(componentSource, /配置 ccLoad 服务连接，选择渠道后导入本地号池。/);
  assert.match(componentSource, /读取渠道/);
  assert.doesNotMatch(componentSource, /预览版|preview|指定版本|固定版本|v\d+(?:\.\d+)+|频道/i);
  assert.doesNotMatch(readmeSource, /ccLoad[^\n]*(?:预览版|v4\.6\.12)/);
  assert.doesNotMatch(apiSource, /ccLoad[^\n]*preview/i);
});

test("ccLoad only reads channel metadata before import", () => {
  assert.match(componentSource, /normalizeCCLoadChannels\(data\.channels\)/);
  assert.match(componentSource, /await fetchCCLoadChannels\(server\.id\)/);
  assert.match(componentSource, /startCCLoadImport\(browserServer\.id, importIds\)/);
  assert.doesNotMatch(componentSource, /fetchCCLoadChannelModels/);
  assert.doesNotMatch(componentSource, /channel-models/);
  assert.doesNotMatch(componentSource, /读取渠道模型/);
});

test("closing ccLoad browser invalidates an in-flight channel query", () => {
  const closeStart = componentSource.indexOf("const closeBrowser = () => {");
  const closeEnd = componentSource.indexOf("const selectableFilteredChannelIds", closeStart);
  assert.ok(closeStart >= 0 && closeEnd > closeStart);
  const closeBody = componentSource.slice(closeStart, closeEnd);
  assert.match(closeBody, /invalidateQueries\("channels"\)/);
  assert.match(closeBody, /setLoadingChannelsId\(null\)/);
  assert.doesNotMatch(closeBody, /channel-models/);

  const gate = createMutationRequestGate();
  const channelQuery = gate.beginQuery("channels");
  gate.invalidateQueries("channels");
  assert.equal(gate.acceptsQuery(channelQuery), false);
});

test("ccLoad card and dialogs keep the CPA visual structure", () => {
  assert.match(componentSource, /<Card className="rounded-2xl border-white\/80 bg-white\/90 shadow-sm">/);
  assert.match(componentSource, /<CardContent className="space-y-6 p-6">/);
  assert.match(componentSource, /<div className="flex items-start justify-between">/);
  assert.match(componentSource, /<div className="flex size-10 items-center justify-center rounded-xl bg-stone-100">/);
  assert.match(componentSource, /className="h-9 rounded-xl bg-stone-950 px-4 text-white hover:bg-stone-800"/);
  assert.match(
    componentSource,
    /<DialogContent showCloseButton=\{false\} className="max-h-\[90vh\] max-w-5xl rounded-2xl p-6">/,
  );
  assert.match(componentSource, /<Search className="pointer-events-none absolute top-1\/2 left-3 size-4 -translate-y-1\/2 text-stone-400"/);
  assert.match(componentSource, /<div className="rounded-xl border border-stone-200">/);
  assert.match(componentSource, /<div className="divide-y divide-stone-100">/);
  assert.match(componentSource, /<DialogFooter className="pt-2">/);
});
