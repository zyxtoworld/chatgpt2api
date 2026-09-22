"use client";

import { AlertCircle, CheckCircle2, LoaderCircle } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import type { CPAImportJob } from "@/lib/api";

const phaseLabels: Record<string, string> = {
  pending: "等待开始",
  connecting: "连接远程服务",
  fetching_credentials: "获取账号凭据",
  downloading_credentials: "下载账号凭据",
  merging_accounts: "写入本地账号",
  refreshing_accounts: "刷新账号信息",
  processing: "处理中",
  completed: "导入完成",
  failed: "导入失败",
};

function clamp(value: number, maximum: number) {
  return Math.max(0, Math.min(maximum, value));
}

function statusLabel(status: CPAImportJob["status"]) {
  if (status === "completed") return "完成";
  if (status === "failed") return "失败";
  if (status === "pending") return "等待中";
  return "进行中";
}

export function ImportProgressCard({ job }: { job: CPAImportJob }) {
  const phase = job.phase || (job.status === "completed" ? "completed" : job.status);
  const phaseTotal = Math.max(0, job.phase_total ?? job.total);
  const phaseCompleted = clamp(job.phase_completed ?? job.completed, phaseTotal);
  const progress = phaseTotal > 0
    ? Math.round((phaseCompleted / phaseTotal) * 100)
    : job.status === "completed"
      ? 100
      : 0;
  const running = job.status === "pending" || job.status === "running";
  const phaseText = phaseLabels[phase] || phase;
  const barClass = job.status === "completed"
    ? "bg-emerald-500"
    : job.status === "failed"
      ? "bg-rose-500"
      : "bg-sky-500";

  return (
    <div className="space-y-2 rounded-xl bg-stone-50 px-3 py-3">
      <div className="text-xs font-medium tracking-[0.16em] text-stone-400 uppercase">导入任务</div>
      <div className="rounded-lg border border-stone-200 bg-white px-3 py-3">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="flex items-center gap-2 text-sm font-semibold text-stone-800">
              {running ? (
                <LoaderCircle className="size-4 animate-spin text-sky-500" />
              ) : job.status === "completed" ? (
                <CheckCircle2 className="size-4 text-emerald-500" />
              ) : (
                <AlertCircle className="size-4 text-rose-500" />
              )}
              <span>{phaseText}</span>
              <span className="font-normal text-stone-400">· {statusLabel(job.status)}</span>
            </div>
            <div className="mt-1 text-xs text-stone-500">
              阶段进度 <span className="font-semibold text-stone-700">{phaseCompleted}/{phaseTotal}</span>
              <span className="mx-1 text-stone-300">·</span>
              总任务 <span className="font-semibold text-stone-700">{job.completed}/{job.total}</span>
            </div>
            <div className="mt-1 truncate text-xs text-stone-400">
              任务 {job.job_id.slice(0, 8)} · 更新于 {job.updated_at || job.created_at}
            </div>
          </div>
          <Badge
            variant={job.status === "completed" ? "success" : job.status === "failed" ? "danger" : "info"}
            className="shrink-0 rounded-md px-2.5 py-1 text-sm"
          >
            {progress}%
          </Badge>
        </div>

        <div
          className="mt-3 h-3 overflow-hidden rounded-full bg-stone-200"
          role="progressbar"
          aria-label={`${phaseText}进度`}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={progress}
        >
          <div className={`h-full rounded-full transition-[width] duration-500 ${barClass}`} style={{ width: `${progress}%` }} />
        </div>

        <div className="mt-3 grid grid-cols-4 gap-2 text-center text-xs">
          <div className="rounded-md bg-stone-50 px-1.5 py-1.5">
            <div className="font-semibold text-stone-800">{job.added}</div>
            <div className="mt-0.5 text-stone-400">新增</div>
          </div>
          <div className="rounded-md bg-stone-50 px-1.5 py-1.5">
            <div className="font-semibold text-stone-800">{job.skipped}</div>
            <div className="mt-0.5 text-stone-400">跳过</div>
          </div>
          <div className="rounded-md bg-stone-50 px-1.5 py-1.5">
            <div className="font-semibold text-stone-800">{job.refreshed}</div>
            <div className="mt-0.5 text-stone-400">已刷新</div>
          </div>
          <div className="rounded-md bg-stone-50 px-1.5 py-1.5">
            <div className={job.failed > 0 ? "font-semibold text-rose-600" : "font-semibold text-stone-800"}>{job.failed}</div>
            <div className="mt-0.5 text-stone-400">失败</div>
          </div>
        </div>

        {job.errors.length > 0 ? (
          <div className="mt-2 truncate text-xs text-rose-500">
            最近错误：{job.errors[job.errors.length - 1]?.error || "未知错误"}
          </div>
        ) : null}
      </div>
    </div>
  );
}
