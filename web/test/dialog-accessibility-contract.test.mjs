import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const logsSource = readFileSync(
  new URL("../src/app/logs/page.tsx", import.meta.url),
  "utf8",
);
const topNavSource = readFileSync(
  new URL("../src/components/top-nav.tsx", import.meta.url),
  "utf8",
);
const imageManagerSource = readFileSync(
  new URL("../src/app/image-manager/page.tsx", import.meta.url),
  "utf8",
);

test("日志详情弹窗提供可访问描述", () => {
  const detailStart = logsSource.indexOf("<Dialog open={detailOpen}");
  const detailEnd = logsSource.indexOf("</Dialog>", detailStart);
  assert.ok(detailStart >= 0 && detailEnd > detailStart);
  const detailDialog = logsSource.slice(detailStart, detailEnd);

  assert.match(detailDialog, /<DialogTitle>日志详情<\/DialogTitle>/);
  assert.match(detailDialog, /<DialogDescription[^>]*>/);
});

test("移动导航 Sheet 提供可访问描述", () => {
  const sheetStart = topNavSource.indexOf("<Sheet>");
  const sheetEnd = topNavSource.indexOf("</Sheet>", sheetStart);
  assert.ok(sheetStart >= 0 && sheetEnd > sheetStart);
  const mobileSheet = topNavSource.slice(sheetStart, sheetEnd);

  assert.match(topNavSource, /\bSheetDescription\b/);
  assert.match(mobileSheet, /<SheetDescription[^>]*>/);
});

test("图片管理的所有确认弹窗提供可访问描述", () => {
  for (const marker of [
    '<Dialog open={deleteMode === "byDate"}',
    "<Dialog open={dialogVisible}",
    '<Dialog open={deleteMode === "selected" || deleteMode === "filtered"}',
    "<Dialog open={Boolean(tagDeleteTarget)}",
  ]) {
    const dialogStart = imageManagerSource.indexOf(marker);
    const dialogEnd = imageManagerSource.indexOf("</Dialog>", dialogStart);
    assert.ok(dialogStart >= 0 && dialogEnd > dialogStart, `missing dialog for ${marker}`);
    assert.match(
      imageManagerSource.slice(dialogStart, dialogEnd),
      /<DialogDescription[^>]*>/,
      `missing accessible description for ${marker}`,
    );
  }
});
