import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  PUBLIC_NETWORK_ERROR_MESSAGE,
  PUBLIC_SERVER_ERROR_MESSAGE,
  requestErrorMessage,
} from "../src/lib/request-error-message.js";

const requestSource = readFileSync(new URL("../src/lib/request.ts", import.meta.url), "utf8");

test("proxy-generated 5xx text never reaches the page", () => {
  const proxyText = "The origin web server did not return a complete response within the 120-second Proxy Read Timeout window.";

  assert.equal(requestErrorMessage({ status: 524, payload: { message: proxyText } }), PUBLIC_SERVER_ERROR_MESSAGE);
  assert.equal(requestErrorMessage({ status: 502, payload: { detail: { error: proxyText } } }), PUBLIC_SERVER_ERROR_MESSAGE);
  assert.doesNotMatch(requestErrorMessage({ status: 503, payload: { error: proxyText } }), /origin web server|Proxy Read Timeout/i);
});

test("network failures use a fixed message instead of Axios internals", () => {
  assert.equal(requestErrorMessage({ fallback: "Network Error" }), PUBLIC_NETWORK_ERROR_MESSAGE);
  assert.equal(requestErrorMessage({ fallback: "socket hang up" }), PUBLIC_NETWORK_ERROR_MESSAGE);
});

test("structured 4xx domain messages remain actionable", () => {
  assert.equal(
    requestErrorMessage({ status: 400, payload: { detail: { error: "请输入有效的搜索内容" } } }),
    "请输入有效的搜索内容",
  );
  assert.equal(requestErrorMessage({ status: 404, payload: { message: "任务不存在" } }), "任务不存在");
});

test("the Axios interceptor delegates public projection to the shared helper", () => {
  const helperImport = requestSource.match(/import\s*\{([\s\S]*?)\}\s*from "@\/lib\/request-error-message"/);
  assert.ok(helperImport, "missing request error helper import");
  assert.match(helperImport[1], /\brequestErrorMessage\b/);
  assert.match(requestSource, /requestErrorMessage\(\{status, payload, fallback: error\.message\}\)/);
});
