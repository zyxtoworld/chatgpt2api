import assert from "node:assert/strict";
import test from "node:test";

import {
  areAllCCLoadChannelsSelected,
  filterCCLoadChannels,
  getCCLoadModelErrorIds,
  getCCLoadPage,
  getValidCCLoadSelectedIds,
  getSelectableCCLoadChannelIds,
  getUnloadedCCLoadChannelIds,
  mergeCCLoadChannelModels,
  normalizeCCLoadChannels,
  resetCCLoadModelState,
  toggleAllCCLoadChannels,
} from "../src/lib/ccload-selection.js";

const channels = [
  { id: "1", enabled: true },
  { id: "2", enabled: false },
  { id: "3", enabled: true },
  { id: "1", enabled: true },
];

test("ccLoad starts each channel browser with a clean model loading state", () => {
  assert.deepEqual(resetCCLoadModelState(), {
    loadingModelIds: [],
    modelLoadErrorIds: [],
  });
});

test("ccLoad selection exposes only unique enabled channels", () => {
  assert.deepEqual(getSelectableCCLoadChannelIds(channels), ["1", "3"]);
  assert.equal(areAllCCLoadChannelsSelected([], channels), false);
  assert.equal(areAllCCLoadChannelsSelected(["1", "3"], channels), true);
  assert.equal(areAllCCLoadChannelsSelected(["1", "2"], channels), false);
});

test("ccLoad select-all preserves unrelated selections and removes only selectable ids", () => {
  assert.deepEqual(
    toggleAllCCLoadChannels(["external"], channels, true),
    ["external", "1", "3"],
  );
  assert.deepEqual(
    toggleAllCCLoadChannels(["external", "1", "2", "3"], channels, false),
    ["external", "2"],
  );
});

test("ccLoad search matches channel fields", () => {
  assert.deepEqual(
    filterCCLoadChannels([
      { id: "1", name: "Team Alpha", plan_type: "pro", models: ["gpt-5"] },
      { id: "2", name: "Team Beta", plan_type: "plus", models: ["gpt-4o"] },
    ], "GPT-5"),
    [{ id: "1", name: "Team Alpha", plan_type: "pro", models: ["gpt-5"] }],
  );
  assert.equal(filterCCLoadChannels(channels, "  ").length, channels.length);
});

test("ccLoad channels preserve each authenticated channel catalog instead of one shared union", () => {
  assert.deepEqual(
    normalizeCCLoadChannels([
      { id: "10", name: " Free ", enabled: true, plan_type: "free", models: [" common ", "free-only", "common", ""] },
      { id: "11", name: "Pro", enabled: true, plan_type: "Pro", models: ["common", "pro-only", "pro-extra"] },
      { id: "11", name: "duplicate", enabled: true, plan_type: "Pro", models: ["wrong"] },
    ]),
    [
      { id: "10", name: "Free", enabled: true, plan_type: "free", subscription_active_until: "", models: ["common", "free-only"], models_loaded: false },
      { id: "11", name: "Pro", enabled: true, plan_type: "Pro", subscription_active_until: "", models: ["common", "pro-only", "pro-extra"], models_loaded: false },
    ],
  );
});

test("ccLoad normalization does not stringify container metadata", () => {
  const canary = "frontend-channel-container-canary";
  const normalized = normalizeCCLoadChannels([{
    id: { secret: canary },
    name: { secret: canary },
    plan_type: [canary],
    subscription_active_until: { secret: canary },
    enabled: true,
    models: [{ secret: canary }, "safe-model"],
  }]);

  assert.deepEqual(normalized, []);
  assert.equal(JSON.stringify(normalized).includes(canary), false);
});

test("ccLoad channel ids accept only ASCII nonzero decimal values", () => {
  const valid = normalizeCCLoadChannels([
    { id: "0007", enabled: true, models: [] },
    { id: "9".repeat(64), enabled: true, models: [] },
  ]);
  assert.deepEqual(valid.map((channel) => channel.id), ["0007", "9".repeat(64)]);

  const invalid = normalizeCCLoadChannels([
    { id: "٠", enabled: true, models: [] },
    { id: "١٢", enabled: true, models: [] },
    { id: "１２", enabled: true, models: [] },
    { id: "000", enabled: true, models: [] },
    { id: "9".repeat(65), enabled: true, models: [] },
    { id: -1, enabled: true, models: [] },
    { id: 1.5, enabled: true, models: [] },
  ]);
  assert.deepEqual(invalid, []);
});

test("ccLoad requests every unloaded channel independently", () => {
  assert.deepEqual(getUnloadedCCLoadChannelIds([
    { id: "1", enabled: true, plan_type: " free ", models_loaded: false },
    { id: "2", enabled: true, plan_type: "FREE", models_loaded: false },
    { id: "3", enabled: true, plan_type: "pro", models_loaded: false },
    { id: "4", enabled: true, plan_type: "Pro", models_loaded: false },
    { id: "5", enabled: true, plan_type: "team", models_loaded: false },
    { id: "6", enabled: true, plan_type: "plus", models_loaded: true },
    { id: "7", enabled: false, plan_type: "business", models_loaded: false },
    { id: "8", enabled: true, plan_type: "", models_loaded: false },
    { id: "9", enabled: true, models_loaded: false },
  ]), ["1", "2", "3", "4", "5", "8", "9"]);
});

test("ccLoad keeps catalogs isolated across pagination", () => {
  const allChannels = Array.from({ length: 100 }, (_, index) => ({
    id: String(1000 + index + 1),
    name: `Free ${index + 1}`,
    enabled: true,
    plan_type: "free",
    models: [],
    models_loaded: false,
  }));

  assert.equal(getUnloadedCCLoadChannelIds(allChannels).length, 100);
  const merged = mergeCCLoadChannelModels(allChannels, [{
    id: "1001",
    plan_type: "free",
    models: ["gpt-free"],
    models_loaded: true,
  }]);
  const secondPage = getCCLoadPage(merged, 2, 50).items;

  assert.equal(secondPage.length, 50);
  assert.equal(secondPage.every((channel) => channel.models_loaded === false), true);
  assert.equal(getUnloadedCCLoadChannelIds(secondPage).length, 50);
});

test("ccLoad never copies one channel catalog to another same-plan channel", () => {
  assert.deepEqual(
    mergeCCLoadChannelModels(
      [
        { id: "20", name: "Free A", enabled: true, plan_type: "free", models: [], models_loaded: false },
        { id: "21", name: "Free B", enabled: true, plan_type: "FREE", models: [], models_loaded: false },
        { id: "22", name: "Pro", enabled: true, plan_type: "pro", models: [], models_loaded: false },
        { id: "23", name: "Unknown", enabled: true, plan_type: "", models: [], models_loaded: false },
      ],
      [
        { id: "20", plan_type: " Free ", models: [" gpt-free ", "gpt-free", ""], models_loaded: true },
        { id: "23", plan_type: "", models: ["gpt-unknown"], models_loaded: true },
      ],
    ),
    [
      { id: "20", name: "Free A", enabled: true, plan_type: "free", models: ["gpt-free"], models_loaded: true },
      { id: "21", name: "Free B", enabled: true, plan_type: "FREE", models: [], models_loaded: false },
      { id: "22", name: "Pro", enabled: true, plan_type: "pro", models: [], models_loaded: false },
      { id: "23", name: "Unknown", enabled: true, plan_type: "", models: ["gpt-unknown"], models_loaded: true },
    ],
  );
});

test("ccLoad uses only the catalog for the exact channel id", () => {
  const channels = [
    { id: "30", enabled: true, plan_type: "business", models: [], models_loaded: false },
    { id: "31", enabled: true, plan_type: "Team", models: [], models_loaded: false },
    { id: "32", enabled: true, plan_type: "pro_lite", models: [], models_loaded: false },
    { id: "33", enabled: true, plan_type: "ProLite", models: [], models_loaded: false },
  ];

  const merged = mergeCCLoadChannelModels(channels, [
    { id: "30", plan_type: "business", models: ["gpt-team"], models_loaded: true },
    { id: "32", plan_type: "pro_lite", models: ["gpt-pro-lite"], models_loaded: true },
  ]);

  assert.deepEqual(merged.map((channel) => channel.models), [
    ["gpt-team"],
    [],
    ["gpt-pro-lite"],
    [],
  ]);
  assert.deepEqual(getUnloadedCCLoadChannelIds(merged), ["31", "33"]);
});

test("ccLoad keeps empty plan types isolated by channel", () => {
  assert.deepEqual(
    getUnloadedCCLoadChannelIds([
      { id: "40", enabled: true, plan_type: "", models_loaded: false },
      { id: "41", enabled: true, plan_type: "", models_loaded: false },
    ]),
    ["40", "41"],
  );
});

test("ccLoad keeps a failed channel unloaded when another same-plan channel succeeds", () => {
  const merged = mergeCCLoadChannelModels(
    [
      { id: "50", enabled: true, plan_type: "pro", models: [], models_loaded: false },
      { id: "51", enabled: true, plan_type: "pro", models: [], models_loaded: false },
    ],
    [
      { id: "50", plan_type: "pro", models: [], models_loaded: false },
      { id: "51", plan_type: "pro", models: ["gpt-pro"], models_loaded: true },
    ],
  );

  assert.deepEqual(merged.map((channel) => channel.models), [[], ["gpt-pro"]]);
  assert.deepEqual(merged.map((channel) => channel.models_loaded), [false, true]);
});

test("ccLoad keeps failed and other unloaded channels independently retryable", () => {
  const channels = [
    { id: "50", enabled: true, plan_type: "pro", models: [], models_loaded: false },
    { id: "51", enabled: true, plan_type: "pro", models: [], models_loaded: false },
  ];
  const afterFailure = mergeCCLoadChannelModels(channels, [
    { id: "50", plan_type: "pro", models: [], models_loaded: false },
  ]);

    assert.deepEqual(getUnloadedCCLoadChannelIds(afterFailure), ["50", "51"]);
});

test("ccLoad keeps a failed visible channel retryable", () => {
  assert.deepEqual(
    getUnloadedCCLoadChannelIds([
      { id: "70", enabled: true, models: [], models_loaded: false, models_attempted: true },
    ]),
    ["70"],
  );
});

test("ccLoad exposes partial model catalog failures for explicit retry", () => {
  assert.deepEqual(
    getCCLoadModelErrorIds([
      { id: "50", models: ["gpt-pro"], models_loaded: true },
      { id: "51", models: [], models_loaded: false },
      { id: "52", models: [], models_loaded: false },
    ]),
    ["51", "52"],
  );
});

test("ccLoad treats an omitted requested catalog as a retryable failure", () => {
  assert.deepEqual(
    getCCLoadModelErrorIds(
      [{ id: "50", models: ["gpt-pro"], models_loaded: true }],
      ["50", "51"],
    ),
    ["51"],
  );
});

test("ccLoad does not persist the failed-attempt marker into normalized channel state", () => {
  assert.deepEqual(
    normalizeCCLoadChannels([
      { id: "71", enabled: true, models: [], models_loaded: false, models_attempted: true },
    ]),
    [{
      id: "71",
      name: "",
      enabled: true,
      plan_type: "",
      subscription_active_until: "",
      models: [],
      models_loaded: false,
    }],
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
    { id: "60", enabled: true },
    { id: "61", enabled: false },
    { id: "62", enabled: true },
  ];
  assert.deepEqual(
    toggleAllCCLoadChannels(["outside"], filtered, true),
    ["outside", "60", "62"],
  );
  assert.deepEqual(
    toggleAllCCLoadChannels(["outside", "60", "61", "62"], filtered, false),
    ["outside", "61"],
  );
  assert.equal(areAllCCLoadChannelsSelected(["60", "62"], filtered), true);
  assert.equal(areAllCCLoadChannelsSelected(["60"], filtered), false);
});

test("ccLoad import selection keeps only unique enabled current channel ids", () => {
  assert.deepEqual(
    getValidCCLoadSelectedIds(
      ["3", "2", "999", "1", "3"],
      channels,
    ),
    ["3", "1"],
  );
});
