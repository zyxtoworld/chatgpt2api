import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  canSubmitImage,
  resolveImageModelLoadError,
  resolveImageModelLoadSuccess,
  selectImageModel,
} from "../src/lib/image-model-state.js";

const composerSource = readFileSync(
  new URL("../src/app/image/components/image-composer.tsx", import.meta.url),
  "utf8",
);
const imagePageSource = readFileSync(
  new URL("../src/app/image/page.tsx", import.meta.url),
  "utf8",
);

test("an empty model response leaves no fake model and blocks submit", () => {
  const state = resolveImageModelLoadSuccess({ data: [{ id: "gpt-5" }] });

  assert.deepEqual(state, { status: "empty", models: [] });
  assert.equal(
    canSubmitImage({
      prompt: "a red apple",
      model: "gpt-image-2",
      models: state.models,
      status: state.status,
    }),
    false,
  );
});

test("a failed model request leaves no fake model and blocks submit", () => {
  const state = resolveImageModelLoadError();

  assert.deepEqual(state, { status: "error", models: [] });
  assert.equal(
    canSubmitImage({
      prompt: "a red apple",
      model: "gpt-image-2",
      models: state.models,
      status: state.status,
    }),
    false,
  );
});

test("a ready model state only accepts a model returned by the directory", () => {
  const state = resolveImageModelLoadSuccess({
    data: [{ id: "gpt-image-2" }, { id: "codex-gpt-image-2" }, { id: "gpt-5" }],
  });

  assert.deepEqual(state, {
    status: "ready",
    models: ["gpt-image-2", "codex-gpt-image-2"],
  });
  assert.equal(selectImageModel("missing-image-model", state.models), "gpt-image-2");
  assert.equal(
    canSubmitImage({
      prompt: "a red apple",
      model: "gpt-image-2",
      models: state.models,
      status: state.status,
    }),
    true,
  );
  assert.equal(
    canSubmitImage({
      prompt: "a red apple",
      model: "missing-image-model",
      models: state.models,
      status: state.status,
    }),
    false,
  );
});

test("image model filtering does not stringify container ids", () => {
  const canary = "image-model-container-canary";
  const state = resolveImageModelLoadSuccess({
    data: [
      { id: [canary] },
      { id: { secret: canary } },
      { id: "gpt-image-2" },
    ],
  });

  assert.deepEqual(state, { status: "ready", models: ["gpt-image-2"] });
  assert.equal(JSON.stringify(state).includes(canary), false);
});

test("the composer applies the same gate to Enter and the submit button", () => {
  assert.match(composerSource, /canSubmit: boolean/);
  assert.match(composerSource, /disabled=\{!canSubmit\}/);
  assert.match(composerSource, /if \(event\.key === "Enter" && !event\.shiftKey\)/);
  assert.match(composerSource, /if \(canSubmit\) \{\s*void onSubmit\(\);/s);
});

test("the image page has explicit model loading state without a hard-coded fallback", () => {
  assert.match(imagePageSource, /useState<ImageModel>\(""\)/);
  assert.match(imagePageSource, /useState<ImageModel\[\]>\(\[\]\)/);
  assert.match(imagePageSource, /setImageModelStatus\(/);
  assert.match(imagePageSource, /canSubmitImage\(/);
  assert.doesNotMatch(imagePageSource, /useState<ImageModel>\("gpt-image-2"\)/);
  assert.doesNotMatch(imagePageSource, /setImageModels\(\["gpt-image-2"\]\)/);
});
