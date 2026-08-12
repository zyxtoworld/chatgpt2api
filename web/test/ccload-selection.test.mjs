import assert from "node:assert/strict";
import test from "node:test";

import {
  areAllCCLoadChannelsSelected,
  filterCCLoadChannels,
  getCCLoadPage,
  getValidCCLoadSelectedIds,
  getSelectableCCLoadChannelIds,
  replaceCCLoadChannelModels,
  toggleAllCCLoadChannels,
} from "../src/lib/ccload-selection.js";

const channels = [
  { id: "alpha", enabled: true },
  { id: "disabled", enabled: false },
  { id: "beta", enabled: true },
  { id: "alpha", enabled: true },
];

test("ccLoad selection exposes only unique enabled channels", () => {
  assert.deepEqual(getSelectableCCLoadChannelIds(channels), ["alpha", "beta"]);
  assert.equal(areAllCCLoadChannelsSelected([], channels), false);
  assert.equal(areAllCCLoadChannelsSelected(["alpha", "beta"], channels), true);
  assert.equal(areAllCCLoadChannelsSelected(["alpha", "disabled"], channels), false);
});

test("ccLoad select-all preserves unrelated selections and removes only selectable ids", () => {
  assert.deepEqual(
    toggleAllCCLoadChannels(["external"], channels, true),
    ["external", "alpha", "beta"],
  );
  assert.deepEqual(
    toggleAllCCLoadChannels(["external", "alpha", "disabled", "beta"], channels, false),
    ["external", "disabled"],
  );
});

test("ccLoad search matches channel fields", () => {
  assert.deepEqual(
    filterCCLoadChannels([
      { id: "alpha", name: "Team Alpha", plan_type: "pro", models: ["gpt-5"] },
      { id: "beta", name: "Team Beta", plan_type: "plus", models: ["gpt-4o"] },
    ], "GPT-5"),
    [{ id: "alpha", name: "Team Alpha", plan_type: "pro", models: ["gpt-5"] }],
  );
  assert.equal(filterCCLoadChannels(channels, "  ").length, channels.length);
});

test("ccLoad channels use the chatgpt2api model list instead of remote channel models", () => {
  assert.deepEqual(
    replaceCCLoadChannelModels(
      [
        { id: "alpha", models: ["remote-only-model"] },
        { id: "beta", models: [] },
      ],
      [
        { id: " gpt-5 " },
        { id: "gpt-image-2" },
        { id: "gpt-5" },
        { id: "" },
        null,
      ],
    ),
    [
      { id: "alpha", models: ["gpt-5", "gpt-image-2"] },
      { id: "beta", models: ["gpt-5", "gpt-image-2"] },
    ],
  );
});

test("ccLoad pagination clamps page boundaries and reports visible range", () => {
  const items = Array.from({ length: 5 }, (_, index) => ({ id: String(index + 1) }));
  assert.deepEqual(getCCLoadPage(items, 2, 2), {
    items: [{ id: "3" }, { id: "4" }],
    page: 2,
    pageCount: 3,
    total: 5,
    start: 3,
    end: 4,
  });
  assert.deepEqual(getCCLoadPage(items, 99, 2), {
    items: [{ id: "5" }],
    page: 3,
    pageCount: 3,
    total: 5,
    start: 5,
    end: 5,
  });
  assert.deepEqual(getCCLoadPage([], 2, 0), {
    items: [],
    page: 1,
    pageCount: 1,
    total: 0,
    start: 0,
    end: 0,
  });
});

test("ccLoad select-all only changes enabled channels in the current filter", () => {
  const filtered = [
    { id: "alpha", enabled: true },
    { id: "disabled", enabled: false },
    { id: "beta", enabled: true },
  ];
  assert.deepEqual(
    toggleAllCCLoadChannels(["outside"], filtered, true),
    ["outside", "alpha", "beta"],
  );
  assert.deepEqual(
    toggleAllCCLoadChannels(["outside", "alpha", "disabled", "beta"], filtered, false),
    ["outside", "disabled"],
  );
  assert.equal(areAllCCLoadChannelsSelected(["alpha", "beta"], filtered), true);
  assert.equal(areAllCCLoadChannelsSelected(["alpha"], filtered), false);
});

test("ccLoad import selection keeps only unique enabled current channel ids", () => {
  assert.deepEqual(
    getValidCCLoadSelectedIds(
      ["beta", "disabled", "missing", "alpha", "beta"],
      channels,
    ),
    ["beta", "alpha"],
  );
});
