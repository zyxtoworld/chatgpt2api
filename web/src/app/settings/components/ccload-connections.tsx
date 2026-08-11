"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Eye, EyeOff, Import, LoaderCircle, Pencil, Plus, RefreshCcw, ServerCog, Trash2 } from "lucide-react";
import { toast } from "sonner";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import {
  createCCLoadServer,
  deleteCCLoadServer,
  fetchCCLoadChannels,
  fetchCCLoadServers,
  startCCLoadImport,
  updateCCLoadServer,
  type CCLoadChannel,
  type CCLoadServer,
} from "@/lib/api";

function normalizeChannels(items: CCLoadChannel[]) {
  const seen = new Set<string>();
  return items.flatMap((item) => {
    const id = String(item.id || "").trim();
    if (!id || seen.has(id)) return [];
    seen.add(id);
    return [{
      id,
      name: String(item.name || "").trim(),
      enabled: Boolean(item.enabled),
      plan_type: String(item.plan_type || "").trim(),
      subscription_active_until: String(item.subscription_active_until || "").trim(),
      models: Array.isArray(item.models) ? item.models.map(String) : [],
    }];
  });
}

export function CCLoadConnections() {
  const didLoadRef = useRef(false);
  const [servers, setServers] = useState<CCLoadServer[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingServer, setEditingServer] = useState<CCLoadServer | null>(null);
  const [formName, setFormName] = useState("");
  const [formBaseUrl, setFormBaseUrl] = useState("");
  const [formPassword, setFormPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [loadingChannelsId, setLoadingChannelsId] = useState<string | null>(null);
  const [browserOpen, setBrowserOpen] = useState(false);
  const [browserServer, setBrowserServer] = useState<CCLoadServer | null>(null);
  const [channels, setChannels] = useState<CCLoadChannel[]>([]);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [isStartingImport, setIsStartingImport] = useState(false);

  const loadServers = useCallback(async (silent = false) => {
    if (!silent) setIsLoading(true);
    try {
      const data = await fetchCCLoadServers();
      setServers(data.servers);
    } catch (error) {
      if (!silent) toast.error(error instanceof Error ? error.message : "加载 ccLoad 连接失败");
    } finally {
      if (!silent) setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    if (didLoadRef.current) return;
    didLoadRef.current = true;
    void loadServers();
  }, [loadServers]);

  useEffect(() => {
    const running = servers.some(({ import_job: job }) => job?.status === "pending" || job?.status === "running");
    if (!running) return;
    const timer = window.setInterval(() => void loadServers(true), 1500);
    return () => window.clearInterval(timer);
  }, [loadServers, servers]);

  const openEditor = (server: CCLoadServer | null) => {
    setEditingServer(server);
    setFormName(server?.name || "");
    setFormBaseUrl(server?.base_url || "");
    setFormPassword("");
    setShowPassword(false);
    setDialogOpen(true);
  };

  const saveServer = async () => {
    const baseUrl = formBaseUrl.trim();
    const password = formPassword.trim();
    if (!baseUrl || (!editingServer && !password)) {
      toast.error("请填写 ccLoad 地址和管理员密码");
      return;
    }
    setIsSaving(true);
    try {
      const data = editingServer
        ? await updateCCLoadServer(editingServer.id, {
          name: formName.trim(),
          base_url: baseUrl,
          ...(password ? { password } : {}),
        })
        : await createCCLoadServer({ name: formName.trim(), base_url: baseUrl, password });
      setServers(data.servers);
      setDialogOpen(false);
      toast.success(editingServer ? "ccLoad 连接已更新" : "ccLoad 连接已添加");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存 ccLoad 连接失败");
    } finally {
      setIsSaving(false);
    }
  };

  const removeServer = async (server: CCLoadServer) => {
    if (!window.confirm(`删除 ccLoad 连接「${server.name || server.base_url}」？`)) return;
    setDeletingId(server.id);
    try {
      const data = await deleteCCLoadServer(server.id);
      setServers(data.servers);
      toast.success("ccLoad 连接已删除");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "删除 ccLoad 连接失败");
    } finally {
      setDeletingId(null);
    }
  };

  const browseChannels = async (server: CCLoadServer) => {
    setLoadingChannelsId(server.id);
    try {
      const data = await fetchCCLoadChannels(server.id);
      const nextChannels = normalizeChannels(data.channels);
      setBrowserServer(server);
      setChannels(nextChannels);
      setSelectedIds([]);
      setBrowserOpen(true);
      toast.success(`读取到 ${nextChannels.length} 个 Codex OAuth 频道`);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "读取 ccLoad 频道失败");
    } finally {
      setLoadingChannelsId(null);
    }
  };

  const startImport = async () => {
    if (!browserServer || selectedIds.length === 0) {
      toast.error("请选择要导入的频道");
      return;
    }
    setIsStartingImport(true);
    try {
      const result = await startCCLoadImport(browserServer.id, selectedIds);
      setServers((current) => current.map((server) => (
        server.id === browserServer.id ? { ...server, import_job: result.import_job } : server
      )));
      setBrowserOpen(false);
      toast.success("ccLoad 导入任务已启动");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "启动 ccLoad 导入失败");
    } finally {
      setIsStartingImport(false);
    }
  };

  return (
    <>
      <Card className="border-stone-200 bg-white/95 shadow-sm">
        <CardContent className="space-y-5 p-5 sm:p-6">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-center gap-3">
              <div className="rounded-xl bg-sky-50 p-2 text-sky-700"><ServerCog className="size-5" /></div>
              <div>
                <h2 className="text-lg font-semibold tracking-tight">ccLoad 预览版连接</h2>
                <p className="text-sm text-stone-500">读取 Codex OAuth 频道并导入完整可刷新凭据。</p>
              </div>
            </div>
            <Button onClick={() => openEditor(null)}><Plus className="mr-2 size-4" />添加连接</Button>
          </div>

          {isLoading ? (
            <div className="flex min-h-28 items-center justify-center"><LoaderCircle className="size-5 animate-spin" /></div>
          ) : servers.length === 0 ? (
            <div className="rounded-xl border border-dashed border-stone-200 p-8 text-center text-sm text-stone-500">
              暂无 ccLoad 连接。
            </div>
          ) : (
            <div className="space-y-3">
              {servers.map((server) => {
                const job = server.import_job;
                const running = job?.status === "pending" || job?.status === "running";
                return (
                  <div key={server.id} className="rounded-xl border border-stone-200 p-4">
                    <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                      <div className="min-w-0 space-y-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <p className="font-medium text-stone-800">{server.name || server.base_url}</p>
                          <Badge variant="secondary">{server.has_password ? "已配置密码" : "缺少密码"}</Badge>
                          {job && <Badge variant={job.status === "completed" ? "default" : "secondary"}>{job.status}</Badge>}
                        </div>
                        <p className="truncate text-sm text-stone-500">{server.base_url}</p>
                        {job && (
                          <p className="text-xs text-stone-500">
                            {job.completed}/{job.total} · 新增 {job.added} · 跳过 {job.skipped} · 刷新 {job.refreshed} · 失败 {job.failed}
                          </p>
                        )}
                      </div>
                      <div className="flex flex-wrap gap-2">
                        <Button variant="outline" size="sm" disabled={running || loadingChannelsId === server.id} onClick={() => void browseChannels(server)}>
                          {loadingChannelsId === server.id ? <LoaderCircle className="mr-2 size-4 animate-spin" /> : <RefreshCcw className="mr-2 size-4" />}
                          读取频道
                        </Button>
                        <Button variant="outline" size="sm" onClick={() => openEditor(server)}><Pencil className="mr-2 size-4" />编辑</Button>
                        <Button variant="outline" size="sm" disabled={deletingId === server.id} onClick={() => void removeServer(server)}>
                          <Trash2 className="mr-2 size-4" />删除
                        </Button>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          <div className="rounded-xl bg-stone-50 p-4 text-xs leading-6 text-stone-600">
            ccLoad 管理员密码只保存在服务端；浏览器响应不包含密码、临时会话令牌或 OAuth 凭据。导入时会保留 access/refresh/id token，便于后续刷新。
          </div>
        </CardContent>
      </Card>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editingServer ? "编辑 ccLoad 连接" : "添加 ccLoad 连接"}</DialogTitle>
            <DialogDescription>适配 ccLoad v4.6.12-beta.1 的管理员登录与 Codex OAuth 频道接口。</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <Input value={formName} onChange={(event) => setFormName(event.target.value)} placeholder="连接名称" />
            <Input value={formBaseUrl} onChange={(event) => setFormBaseUrl(event.target.value)} placeholder="https://ccload.example.com" />
            <div className="relative">
              <Input
                type={showPassword ? "text" : "password"}
                value={formPassword}
                onChange={(event) => setFormPassword(event.target.value)}
                placeholder={editingServer ? "留空则保留现有管理员密码" : "管理员密码"}
                className="pr-10"
              />
              <button type="button" aria-label={showPassword ? "隐藏密码" : "显示密码"} className="absolute right-3 top-1/2 -translate-y-1/2 text-stone-400" onClick={() => setShowPassword((value) => !value)}>
                {showPassword ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
              </button>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>取消</Button>
            <Button disabled={isSaving} onClick={() => void saveServer()}>
              {isSaving && <LoaderCircle className="mr-2 size-4 animate-spin" />}保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={browserOpen} onOpenChange={setBrowserOpen}>
        <DialogContent className="max-h-[85vh] max-w-3xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>选择 Codex OAuth 频道</DialogTitle>
            <DialogDescription>{browserServer?.name || browserServer?.base_url}</DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            {channels.length === 0 ? <p className="py-8 text-center text-sm text-stone-500">没有可导入频道</p> : channels.map((channel) => (
              <label key={channel.id} className="flex cursor-pointer items-start gap-3 rounded-xl border border-stone-200 p-3">
                <Checkbox
                  checked={selectedIds.includes(channel.id)}
                  disabled={!channel.enabled}
                  onCheckedChange={(checked) => setSelectedIds((current) => (
                    checked ? [...new Set([...current, channel.id])] : current.filter((id) => id !== channel.id)
                  ))}
                />
                <span className="min-w-0 flex-1">
                  <span className="flex flex-wrap items-center gap-2 font-medium text-stone-800">
                    {channel.name || `频道 ${channel.id}`}
                    <Badge variant="secondary">{channel.plan_type || "unknown"}</Badge>
                    {!channel.enabled && <Badge variant="secondary">已禁用</Badge>}
                  </span>
                  <span className="mt-1 block text-xs text-stone-500">
                    {channel.models.join(", ") || "未声明模型"}{channel.subscription_active_until ? ` · 到期 ${channel.subscription_active_until}` : ""}
                  </span>
                </span>
              </label>
            ))}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setBrowserOpen(false)}>取消</Button>
            <Button disabled={selectedIds.length === 0 || isStartingImport} onClick={() => void startImport()}>
              {isStartingImport ? <LoaderCircle className="mr-2 size-4 animate-spin" /> : <Import className="mr-2 size-4" />}
              导入 {selectedIds.length > 0 ? selectedIds.length : ""} 个频道
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
