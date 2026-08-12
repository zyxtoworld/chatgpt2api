import assert from "node:assert/strict";
import test from "node:test";

import {
  areAllCCLoadChannelsSelected,
  filterCCLoadChannels,
  getCCLoadPage,
  getValidCCLoadSelectedIds,
  getSelectableCCLoadChannelIds,
  getUnloadedCCLoadChannelIds,
  mergeCCLoadChannelModels,
  normalizeCCLoadChannels,
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

test("ccLoad channels preserve each authenticated channel catalog instead of one shared union", () => {
  assert.deepEqual(
    normalizeCCLoadChannels([
      { id: "free", name: " Free ", enabled: true, plan_type: "free", models: [" common ", "free-only", "common", ""] },
      { id: "pro", name: "Pro", enabled: true, plan_type: "Pro", models: ["common", "pro-only", "pro-extra"] },
      { id: "pro", name: "duplicate", enabled: true, plan_type: "Pro", models: ["wrong"] },
    ]),
    [
      { id: "free", name: "Free", enabled: true, plan_type: "free", subscription_active_until: "", models: ["common", "free-only"], models_loaded: false },
      { id: "pro", name: "Pro", enabled: true, plan_type: "Pro", subscription_active_until: "", models: ["common", "pro-only", "pro-extra"], models_loaded: false },
    ],
  );
});

test("ccLoad requests one unloaded representative for each account type in the full list", () => {
  assert.deepEqual(getUnloadedCCLoadChannelIds([
    { id: "free-a", enabled: true, plan_type: " free ", models_loaded: false },
    { id: "free-b", enabled: true, plan_type: "FREE", models_loaded: false },
    { id: "pro-a", enabled: true, plan_type: "pro", models_loaded: false },
    { id: "pro-b", enabled: true, plan_type: "Pro", models_loaded: false },
    { id: "team", enabled: true, plan_type: "team", models_loaded: false },
    { id: "loaded", enabled: true, plan_type: "plus", models_loaded: true },
    { id: "disabled", enabled: false, plan_type: "business", models_loaded: false },
    { id: "unknown-a", enabled: true, plan_type: "", models_loaded: false },
    { id: "unknown-b", enabled: true, models_loaded: false },
  ]), ["free-a", "pro-a", "team", "unknown-a", "unknown-b"]);
});

test("ccLoad reuses one account-type catalog across pagination", () => {
  const allChannels = Array.from({ length: 100 }, (_, index) => ({
    id: `free-${index + 1}`,
    name: `Free ${index + 1}`,
    enabled: true,
    plan_type: "free",
    models: [],
    models_loaded: false,
  }));

  assert.deepEqual(getUnloadedCCLoadChannelIds(allChannels), ["free-1"]);
  const merged = mergeCCLoadChannelModels(allChannels, [{
    id: "free-1",
    plan_type: "free",
    models: ["gpt-free"],
    models_loaded: true,
  }]);
  const secondPage = getCCLoadPage(merged, 2, 50).items;

  assert.equal(secondPage.length, 50);
  assert.equal(secondPage.every((channel) => channel.models_loaded === true), true);
  assert.deepEqual(getUnloadedCCLoadChannelIds(secondPage), []);
});

test("ccLoad shares one model catalog with every channel of the same account type", () => {
  assert.deepEqual(
    mergeCCLoadChannelModels(
      [
        { id: "free-a", name: "Free A", enabled: true, plan_type: "free", models: [], models_loaded: false },
        { id: "free-b", name: "Free B", enabled: true, plan_type: "FREE", models: [], models_loaded: false },
        { id: "pro", name: "Pro", enabled: true, plan_type: "pro", models: [], models_loaded: false },
        { id: "unknown", name: "Unknown", enabled: true, plan_type: "", models: [], models_loaded: false },
      ],
      [
        { id: "free-a", plan_type: " Free ", models: [" gpt-free ", "gpt-free", ""], models_loaded: true },
        { id: "unknown", plan_type: "", models: ["gpt-unknown"], models_loaded: true },
      ],
    ),
    [
      { id: "free-a", name: "Free A", enabled: true, plan_type: "free", models: ["gpt-free"], models_loaded: true },
      { id: "free-b", name: "Free B", enabled: true, plan_type: "FREE", models: ["gpt-free"], models_loaded: true },
      { id: "pro", name: "Pro", enabled: true, plan_type: "pro", models: [], models_loaded: false },
      { id: "unknown", name: "Unknown", enabled: true, plan_type: "", models: ["gpt-unknown"], models_loaded: true },
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
