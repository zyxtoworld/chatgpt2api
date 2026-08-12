"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
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
import { createMutationRequestGate } from "@/lib/mutation-request-gate";
import { createOwnedQueryLoader, scheduleOwnedMicrotask } from "@/lib/query-lifecycle";
import { createSerialPoller } from "@/lib/serial-poll";
import { commitSynchronousSnapshot } from "@/lib/synchronous-snapshot";
import { PAGE_SIZE_OPTIONS, type PageSizeOption } from "../store";
import {
  areAllCCLoadChannelsSelected,
  filterCCLoadChannels,
  getCCLoadPage,
  getValidCCLoadSelectedIds,
  getSelectableCCLoadChannelIds,
  toggleAllCCLoadChannels,
} from "@/lib/ccload-selection";

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
  const requestGateRef = useRef(createMutationRequestGate());
  const serversRef = useRef<CCLoadServer[]>([]);
  const savingOwnerRef = useRef<{ epoch: number } | null>(null);
  const deletingOwnerRef = useRef<{ epoch: number } | null>(null);
  const importingOwnerRef = useRef<{ epoch: number } | null>(null);
  const browsingOwnerRef = useRef<{ generation: number; mutationEpoch: number; allowed: boolean } | null>(null);
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
  const [channelQuery, setChannelQuery] = useState("");
  const [channelPage, setChannelPage] = useState(1);
  const [channelPageSize, setChannelPageSize] = useState<PageSizeOption>("50");
  const [isStartingImport, setIsStartingImport] = useState(false);

  const commitServers = (next: CCLoadServer[] | ((current: CCLoadServer[]) => CCLoadServer[])) => {
    const resolved = commitSynchronousSnapshot(serversRef, next);
    setServers(resolved);
  };

  const listQueryRef = useRef<ReturnType<typeof createOwnedQueryLoader> | null>(null);

  const requestServers = async () => {
    const gate = requestGateRef.current;
    const queryOwner = gate.beginQuery("list");
    if (!queryOwner.allowed) return null;
    try {
      const data = await fetchCCLoadServers();
      return { queryOwner, data };
    } catch (error) {
      if (!gate.acceptsQuery(queryOwner)) return null;
      throw error;
    }
  };

  const loadServers = useCallback(() => listQueryRef.current?.run(), []);

  useEffect(() => {
    const gate = requestGateRef.current;
    const loader = createOwnedQueryLoader({
      gate,
      domain: "list",
      request: fetchCCLoadServers,
      onStart: () => setIsLoading(true),
      onCommit: (data: Awaited<ReturnType<typeof fetchCCLoadServers>>) => commitServers(data.servers),
      onError: (error: unknown) => toast.error(error instanceof Error ? error.message : "加载 ccLoad 连接失败"),
      onFinish: () => setIsLoading(false),
    });
    listQueryRef.current = loader;
    const cancelInitialLoad = scheduleOwnedMicrotask(() => loadServers());
    return () => {
      cancelInitialLoad();
      loader.cancel();
      if (listQueryRef.current === loader) {
        listQueryRef.current = null;
      }
      savingOwnerRef.current = null;
      deletingOwnerRef.current = null;
      importingOwnerRef.current = null;
      browsingOwnerRef.current = null;
      gate.cancel();
    };
  }, [loadServers]);

  const beginMutation = () => {
    const owner = requestGateRef.current.beginMutation();
    if (!owner.accepted) {
      toast.error("已有 ccLoad 操作正在进行，请稍候");
      return null;
    }
    listQueryRef.current?.clearLoadingForMutation();
    return owner;
  };

  const running = servers.some(({ import_job: job }) => job?.status === "pending" || job?.status === "running");

  useEffect(() => {
    if (!running) return;
    let cancelled = false;
    const gate = requestGateRef.current;
    const poller = createSerialPoller({
      intervalMs: 1500,
      initialDelayMs: 1500,
      poll: requestServers,
      isDone: () => false,
      onProgress: (result: Awaited<ReturnType<typeof requestServers>>) => {
        if (result && !cancelled && gate.acceptsQuery(result.queryOwner)) {
          commitServers(result.data.servers);
        }
      },
    });
    void poller.start().catch(() => undefined);
    return () => {
      cancelled = true;
      gate.invalidateQueries("list");
      poller.stop();
    };
  }, [running]);

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
    const mutationOwner = beginMutation();
    if (!mutationOwner) return;
    savingOwnerRef.current = mutationOwner;
    setIsSaving(true);
    try {
      const data = editingServer
        ? await updateCCLoadServer(editingServer.id, {
          name: formName.trim(),
          base_url: baseUrl,
          ...(password ? { password } : {}),
        })
        : await createCCLoadServer({ name: formName.trim(), base_url: baseUrl, password });
      if (requestGateRef.current.acceptsMutation(mutationOwner)) {
        commitServers(data.servers);
        setDialogOpen(false);
        toast.success(editingServer ? "ccLoad 连接已更新" : "ccLoad 连接已添加");
      }
    } catch (error) {
      if (requestGateRef.current.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "保存 ccLoad 连接失败");
      }
    } finally {
      const isOwner = savingOwnerRef.current === mutationOwner;
      if (isOwner) {
        savingOwnerRef.current = null;
        setIsSaving(false);
      }
      requestGateRef.current.finishMutation(mutationOwner);
    }
  };

  const removeServer = async (server: CCLoadServer) => {
    if (!window.confirm(`删除 ccLoad 连接「${server.name || server.base_url}」？`)) return;
    const mutationOwner = beginMutation();
    if (!mutationOwner) return;
    deletingOwnerRef.current = mutationOwner;
    setDeletingId(server.id);
    try {
      const data = await deleteCCLoadServer(server.id);
      if (requestGateRef.current.acceptsMutation(mutationOwner)) {
        commitServers(data.servers);
        toast.success("ccLoad 连接已删除");
      }
    } catch (error) {
      if (requestGateRef.current.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "删除 ccLoad 连接失败");
      }
    } finally {
      const isOwner = deletingOwnerRef.current === mutationOwner;
      if (isOwner) {
        deletingOwnerRef.current = null;
        setDeletingId(null);
      }
      requestGateRef.current.finishMutation(mutationOwner);
    }
  };

  const browseChannels = async (server: CCLoadServer) => {
    const gate = requestGateRef.current;
    const queryOwner = gate.beginQuery("channels");
    if (!queryOwner.allowed) return;
    browsingOwnerRef.current = queryOwner;
    setLoadingChannelsId(server.id);
    try {
      const data = await fetchCCLoadChannels(server.id);
      if (!gate.acceptsQuery(queryOwner)) return;
      const currentServer = serversRef.current.find((item) => item.id === server.id);
      if (!currentServer) return;
      const nextChannels = normalizeChannels(data.channels);
      setBrowserServer(currentServer);
      setChannels(nextChannels);
      setSelectedIds([]);
      setChannelQuery("");
      setChannelPage(1);
      setChannelPageSize("50");
      setBrowserOpen(true);
      toast.success(`读取到 ${nextChannels.length} 个 Codex OAuth 频道`);
    } catch (error) {
      if (gate.acceptsQuery(queryOwner)) {
        toast.error(error instanceof Error ? error.message : "读取 ccLoad 频道失败");
      }
    } finally {
      if (browsingOwnerRef.current === queryOwner) {
        browsingOwnerRef.current = null;
        setLoadingChannelsId(null);
      }
    }
  };

  const startImport = async () => {
    const importIds = getValidCCLoadSelectedIds(selectedIds, channels);
    if (!browserServer || importIds.length === 0) {
      toast.error("请选择要导入的频道");
      return;
    }
    const mutationOwner = beginMutation();
    if (!mutationOwner) return;
    importingOwnerRef.current = mutationOwner;
    setIsStartingImport(true);
    try {
      const result = await startCCLoadImport(browserServer.id, importIds);
      if (requestGateRef.current.acceptsMutation(mutationOwner)) {
        commitServers((current) => current.map((server) => (
          server.id === browserServer.id ? { ...server, import_job: result.import_job } : server
        )));
        setBrowserOpen(false);
        toast.success("ccLoad 导入任务已启动");
      }
    } catch (error) {
      if (requestGateRef.current.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "启动 ccLoad 导入失败");
      }
    } finally {
      const isOwner = importingOwnerRef.current === mutationOwner;
      if (isOwner) {
        importingOwnerRef.current = null;
        setIsStartingImport(false);
      }
      requestGateRef.current.finishMutation(mutationOwner);
    }
  };

  const hasMutation = isSaving || deletingId !== null || isStartingImport;
  const filteredChannels = useMemo(
    () => filterCCLoadChannels(channels, channelQuery),
    [channelQuery, channels],
  );
  const channelPageResult = useMemo(
    () => getCCLoadPage(filteredChannels, channelPage, Number(channelPageSize)),
    [channelPage, channelPageSize, filteredChannels],
  );
  const selectableFilteredChannelIds = getSelectableCCLoadChannelIds(filteredChannels);
  const validSelectedIds = getValidCCLoadSelectedIds(selectedIds, channels);
  const allChannelsSelected = areAllCCLoadChannelsSelected(selectedIds, filteredChannels);
  const toggleAllChannels = (checked: boolean) => {
    setSelectedIds((current) => toggleAllCCLoadChannels(current, filteredChannels, checked));
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
            <Button onClick={() => openEditor(null)} disabled={hasMutation}><Plus className="mr-2 size-4" />添加连接</Button>
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
                        <Button variant="outline" size="sm" disabled={hasMutation || running || loadingChannelsId === server.id} onClick={() => void browseChannels(server)}>
                          {loadingChannelsId === server.id ? <LoaderCircle className="mr-2 size-4 animate-spin" /> : <RefreshCcw className="mr-2 size-4" />}
                          读取频道
                        </Button>
                        <Button variant="outline" size="sm" onClick={() => openEditor(server)} disabled={hasMutation}><Pencil className="mr-2 size-4" />编辑</Button>
                        <Button variant="outline" size="sm" disabled={hasMutation} onClick={() => void removeServer(server)}>
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
            <Button variant="outline" onClick={() => setDialogOpen(false)} disabled={hasMutation}>取消</Button>
            <Button disabled={hasMutation} onClick={() => void saveServer()}>
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
          <div className="space-y-3">
            <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <Input
                value={channelQuery}
                onChange={(event) => {
                  setChannelQuery(event.target.value);
                  setChannelPage(1);
                }}
                placeholder="搜索频道名称、ID或模型"
                className="h-10 min-w-[260px] rounded-xl border-stone-200 bg-white lg:max-w-sm"
                disabled={hasMutation}
              />
              <div className="flex items-center gap-2">
                <Select
                  value={channelPageSize}
                  onValueChange={(value) => {
                    setChannelPageSize(value as PageSizeOption);
                    setChannelPage(1);
                  }}
                  disabled={hasMutation}
                >
                  <SelectTrigger className="h-10 w-[120px] rounded-xl border-stone-200 bg-white">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {PAGE_SIZE_OPTIONS.map((item) => (
                      <SelectItem key={item} value={item}>{item} / 页</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button
                  variant="outline"
                  className="h-10 rounded-xl border-stone-200 bg-white px-4 text-stone-700"
                  onClick={() => toggleAllChannels(!allChannelsSelected)}
                  disabled={hasMutation || selectableFilteredChannelIds.length === 0}
                >
                  {allChannelsSelected ? "取消全选" : "全选筛选结果"}
                </Button>
              </div>
            </div>
            <div className="flex items-center justify-between rounded-xl border border-stone-200 bg-stone-50 px-3 py-2 text-sm text-stone-500">
              <label className="flex items-center gap-3">
                <Checkbox
                  checked={allChannelsSelected}
                  disabled={selectableFilteredChannelIds.length === 0 || hasMutation}
                  onCheckedChange={(checked) => toggleAllChannels(Boolean(checked))}
                />
                <span>筛选结果 {filteredChannels.length} 个，可用 {selectableFilteredChannelIds.length} 个</span>
              </label>
              <span>已选 {validSelectedIds.length} 个</span>
            </div>
            {filteredChannels.length === 0 ? (
              <p className="py-8 text-center text-sm text-stone-500">
                {channels.length === 0 ? "没有可导入频道" : "没有匹配的频道"}
              </p>
            ) : (
              channelPageResult.items.map((channel) => (
                <label key={channel.id} className="flex cursor-pointer items-start gap-3 rounded-xl border border-stone-200 p-3">
                  <Checkbox
                    checked={selectedIds.includes(channel.id)}
                    disabled={!channel.enabled || hasMutation}
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
              ))
            )}
            <div className="flex items-center justify-between text-sm text-stone-500">
              <span>
                第 {channelPageResult.start} - {channelPageResult.end} 条，共 {channelPageResult.total} 条
              </span>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  className="h-9 rounded-xl border-stone-200 bg-white px-3"
                  onClick={() => setChannelPage(Math.max(1, channelPageResult.page - 1))}
                  disabled={hasMutation || channelPageResult.page <= 1}
                >
                  上一页
                </Button>
                <span>{channelPageResult.page}/{channelPageResult.pageCount}</span>
                <Button
                  variant="outline"
                  className="h-9 rounded-xl border-stone-200 bg-white px-3"
                  onClick={() => setChannelPage(Math.min(channelPageResult.pageCount, channelPageResult.page + 1))}
                  disabled={hasMutation || channelPageResult.page >= channelPageResult.pageCount}
                >
                  下一页
                </Button>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setBrowserOpen(false)} disabled={hasMutation}>取消</Button>
            <Button disabled={validSelectedIds.length === 0 || hasMutation} onClick={() => void startImport()}>
              {isStartingImport ? <LoaderCircle className="mr-2 size-4 animate-spin" /> : <Import className="mr-2 size-4" />}
              导入 {validSelectedIds.length > 0 ? validSelectedIds.length : ""} 个频道
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
