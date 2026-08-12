"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Eye, EyeOff, Import, KeyRound, Link2, LoaderCircle, Pencil, Plus, Save, Search, ServerCog, Trash2 } from "lucide-react";
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
  fetchModels,
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
  replaceCCLoadChannelModels,
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
      models: [],
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
      const [data, modelData] = await Promise.all([
        fetchCCLoadChannels(server.id),
        fetchModels(),
      ]);
      if (!gate.acceptsQuery(queryOwner)) return;
      const currentServer = serversRef.current.find((item) => item.id === server.id);
      if (!currentServer) return;
      const nextChannels = replaceCCLoadChannelModels(normalizeChannels(data.channels), modelData.data);
      setBrowserServer(currentServer);
      setChannels(nextChannels);
      setSelectedIds([]);
      setChannelQuery("");
      setChannelPage(1);
      setChannelPageSize("50");
      setBrowserOpen(true);
      toast.success(`读取到 ${nextChannels.length} 个 Codex OAuth 渠道`);
    } catch (error) {
      if (gate.acceptsQuery(queryOwner)) {
        toast.error(error instanceof Error ? error.message : "读取 ccLoad 渠道失败");
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
      toast.error("请选择要导入的渠道");
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
      <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
        <CardContent className="space-y-6 p-6">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3">
              <div className="flex size-10 items-center justify-center rounded-xl bg-stone-100">
                <ServerCog className="size-5 text-stone-600" />
              </div>
              <div>
                <h2 className="text-lg font-semibold tracking-tight">ccLoad 连接管理</h2>
                <p className="text-sm text-stone-500">先配置连接，再读取 Codex OAuth 渠道并选择导入到本地号池。</p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              {servers.length > 0 ? <Badge className="rounded-md px-2.5 py-1">{servers.length} 个连接</Badge> : null}
              <Button
                className="h-9 rounded-xl bg-stone-950 px-4 text-white hover:bg-stone-800"
                onClick={() => openEditor(null)}
                disabled={hasMutation}
              >
                <Plus className="size-4" />
                添加连接
              </Button>
            </div>
          </div>

          {isLoading ? (
            <div className="flex items-center justify-center py-10">
              <LoaderCircle className="size-5 animate-spin text-stone-400" />
            </div>
          ) : servers.length === 0 ? (
            <div className="flex flex-col items-center justify-center gap-3 rounded-xl bg-stone-50 px-6 py-10 text-center">
              <ServerCog className="size-8 text-stone-300" />
              <div className="space-y-1">
                <p className="text-sm font-medium text-stone-600">暂无 ccLoad 连接</p>
                <p className="text-sm text-stone-400">点击「添加连接」保存你的 ccLoad 管理信息。</p>
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              {servers.map((server) => {
                const job = server.import_job;
                const running = job?.status === "pending" || job?.status === "running";
                const progress = job?.total ? Math.round((job.completed / job.total) * 100) : 0;
                const isBusy = hasMutation || loadingChannelsId === server.id;
                return (
                  <div key={server.id} className="flex flex-col gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3">
                    <div className="flex items-center justify-between gap-3">
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <div className="truncate text-sm font-medium text-stone-800">{server.name || server.base_url}</div>
                          <Badge className="rounded-md bg-stone-100 text-stone-600">
                            {server.has_password ? "已配置密码" : "缺少密码"}
                          </Badge>
                        </div>
                        <div className="truncate text-xs text-stone-400">{server.base_url}</div>
                      </div>
                      <div className="flex items-center gap-1">
                        <button
                          type="button"
                          className="rounded-lg p-2 text-stone-400 transition hover:bg-stone-100 hover:text-stone-700"
                          onClick={() => openEditor(server)}
                          disabled={hasMutation}
                          title="编辑"
                        >
                          <Pencil className="size-4" />
                        </button>
                        <button
                          type="button"
                          className="rounded-lg p-2 text-stone-400 transition hover:bg-rose-50 hover:text-rose-500"
                          disabled={hasMutation}
                          onClick={() => void removeServer(server)}
                          title="删除"
                        >
                          {deletingId === server.id ? <LoaderCircle className="size-4 animate-spin" /> : <Trash2 className="size-4" />}
                        </button>
                      </div>
                    </div>

                    <div className="flex items-center gap-2">
                      <Button
                        variant="outline"
                        className="h-8 rounded-lg border-stone-200 bg-white px-3 text-xs text-stone-600"
                        disabled={isBusy || running}
                        onClick={() => void browseChannels(server)}
                      >
                        {loadingChannelsId === server.id ? <LoaderCircle className="size-3.5 animate-spin" /> : <Import className="size-3.5" />}
                        读取渠道
                      </Button>
                    </div>

                    {job ? (
                      <div className="space-y-2 rounded-xl bg-stone-50 px-3 py-3">
                        <div className="text-xs font-medium tracking-[0.16em] text-stone-400 uppercase">导入任务</div>
                        <div className="rounded-lg border border-stone-200 bg-white px-3 py-3">
                          <div className="flex items-center justify-between gap-3">
                            <div className="min-w-0">
                              <div className="text-sm font-medium text-stone-700">
                                状态 {job.status}，已处理 {job.completed}/{job.total}
                              </div>
                              <div className="truncate text-xs text-stone-400">
                                任务 {job.job_id.slice(0, 8)} · {job.created_at}
                              </div>
                            </div>
                            <Badge
                              variant={job.status === "completed" ? "success" : job.status === "failed" ? "danger" : "info"}
                              className="rounded-md"
                            >
                              {progress}%
                            </Badge>
                          </div>
                          <div className="mt-3 h-2 overflow-hidden rounded-full bg-stone-200">
                            <div className="h-full rounded-full bg-stone-900 transition-all" style={{ width: `${progress}%` }} />
                          </div>
                          <div className="mt-2 flex flex-wrap gap-2 text-xs text-stone-500">
                            <span>新增 {job.added}</span>
                            <span>跳过 {job.skipped}</span>
                            <span>刷新 {job.refreshed}</span>
                            <span>失败 {job.failed}</span>
                          </div>
                        </div>
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          )}

          <div className="rounded-xl bg-stone-50 px-4 py-3 text-sm leading-6 text-stone-500">
            <p className="font-medium text-stone-600">使用说明</p>
            <ul className="mt-1 list-inside list-disc space-y-0.5">
              <li>页面进入后先读取系统里已配置的 ccLoad 连接。</li>
              <li>点击某个连接的「读取渠道」后，会读取 Codex OAuth 渠道和 chatgpt2api 模型列表。</li>
              <li>确认选择后，后端后台获取对应凭据并导入本地号池。</li>
              <li>管理员密码和 OAuth 凭据不会返回浏览器。</li>
            </ul>
          </div>
        </CardContent>
      </Card>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent showCloseButton={false} className="rounded-2xl p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>{editingServer ? "编辑 ccLoad 连接" : "添加 ccLoad 连接"}</DialogTitle>
            <DialogDescription className="text-sm leading-6">
              {editingServer ? "修改 ccLoad 管理员连接信息" : "添加一个新的 ccLoad 管理员连接"}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700">名称（可选）</label>
              <Input
                value={formName}
                onChange={(event) => setFormName(event.target.value)}
                placeholder="例如：主连接、备用连接"
                className="h-11 rounded-xl border-stone-200 bg-white"
              />
            </div>
            <div className="space-y-2">
              <label className="flex items-center gap-1.5 text-sm font-medium text-stone-700">
                <Link2 className="size-3.5" />
                ccLoad 地址
              </label>
              <Input
                value={formBaseUrl}
                onChange={(event) => setFormBaseUrl(event.target.value)}
                placeholder="https://ccload.example.com"
                className="h-11 rounded-xl border-stone-200 bg-white"
              />
            </div>
            <div className="space-y-2">
              <label className="flex items-center gap-1.5 text-sm font-medium text-stone-700">
                <KeyRound className="size-3.5" />
                管理员密码
              </label>
              <div className="relative">
                <Input
                  type={showPassword ? "text" : "password"}
                  value={formPassword}
                  onChange={(event) => setFormPassword(event.target.value)}
                  placeholder={editingServer ? "留空则保留现有管理员密码" : "ccLoad 管理员密码"}
                  className="h-11 rounded-xl border-stone-200 bg-white pr-10"
                />
                <button
                  type="button"
                  aria-label={showPassword ? "隐藏密码" : "显示密码"}
                  className="absolute top-1/2 right-3 -translate-y-1/2 text-stone-400 transition hover:text-stone-600"
                  onClick={() => setShowPassword((value) => !value)}
                >
                  {showPassword ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
                </button>
              </div>
            </div>
          </div>
          <DialogFooter className="pt-2">
            <Button
              variant="secondary"
              className="h-10 rounded-xl bg-stone-100 px-5 text-stone-700 hover:bg-stone-200"
              onClick={() => setDialogOpen(false)}
              disabled={hasMutation}
            >
              取消
            </Button>
            <Button
              className="h-10 rounded-xl bg-stone-950 px-5 text-white hover:bg-stone-800"
              disabled={hasMutation}
              onClick={() => void saveServer()}
            >
              {isSaving ? <LoaderCircle className="size-4 animate-spin" /> : <Save className="size-4" />}
              {editingServer ? "保存修改" : "添加"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={browserOpen} onOpenChange={setBrowserOpen}>
        <DialogContent showCloseButton={false} className="max-h-[90vh] max-w-5xl rounded-2xl p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>选择要导入的渠道</DialogTitle>
            <DialogDescription className="text-sm leading-6">
              {browserServer ? `来自 ${browserServer.name || browserServer.base_url}` : "读取到的 Codex OAuth 渠道"}
            </DialogDescription>
          </DialogHeader>

          <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div className="relative min-w-[260px]">
              <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-stone-400" />
              <Input
                value={channelQuery}
                onChange={(event) => {
                  setChannelQuery(event.target.value);
                  setChannelPage(1);
                }}
                placeholder="搜索渠道名称、ID或模型"
                className="h-10 rounded-xl border-stone-200 bg-white pl-10"
                disabled={hasMutation}
              />
            </div>
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

          <div className="rounded-xl border border-stone-200">
            <div className="flex items-center justify-between border-b border-stone-100 px-4 py-3 text-sm text-stone-500">
              <div className="flex items-center gap-3">
                <Checkbox
                  checked={allChannelsSelected}
                  disabled={selectableFilteredChannelIds.length === 0 || hasMutation}
                  onCheckedChange={(checked) => toggleAllChannels(Boolean(checked))}
                />
                <span>筛选结果 {filteredChannels.length} 个，可用 {selectableFilteredChannelIds.length} 个</span>
              </div>
              <span>已选 {validSelectedIds.length} 个</span>
            </div>
            <div className="max-h-[420px] overflow-auto">
              {channelPageResult.items.length === 0 ? (
                <div className="flex items-center justify-center py-12 text-sm text-stone-400">
                  {channels.length === 0 ? "没有可导入渠道" : "没有匹配的渠道"}
                </div>
              ) : (
                <div className="divide-y divide-stone-100">
                  {channelPageResult.items.map((channel) => (
                    <label key={channel.id} className="flex cursor-pointer items-start gap-3 px-4 py-3 hover:bg-stone-50">
                      <Checkbox
                        checked={selectedIds.includes(channel.id)}
                        disabled={!channel.enabled || hasMutation}
                        onCheckedChange={(checked) => setSelectedIds((current) => (
                          checked ? [...new Set([...current, channel.id])] : current.filter((id) => id !== channel.id)
                        ))}
                      />
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="truncate text-sm font-medium text-stone-700">
                            {channel.name || `渠道 ${channel.id}`}
                          </span>
                          <Badge className="rounded-md bg-stone-100 text-stone-600">{channel.plan_type || "unknown"}</Badge>
                          {!channel.enabled ? <Badge variant="info" className="rounded-md">已禁用</Badge> : null}
                        </div>
                        <div className="mt-1 text-xs text-stone-400">
                          模型：{channel.models.join(", ") || "暂无可用模型"}
                          {channel.subscription_active_until ? ` · 到期 ${channel.subscription_active_until}` : ""}
                        </div>
                      </div>
                    </label>
                  ))}
                </div>
              )}
            </div>
          </div>

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

          <DialogFooter className="pt-2">
            <Button
              variant="secondary"
              className="h-10 rounded-xl bg-stone-100 px-5 text-stone-700 hover:bg-stone-200"
              onClick={() => setBrowserOpen(false)}
              disabled={hasMutation}
            >
              取消
            </Button>
            <Button
              className="h-10 rounded-xl bg-stone-950 px-5 text-white hover:bg-stone-800"
              disabled={validSelectedIds.length === 0 || hasMutation}
              onClick={() => void startImport()}
            >
              {isStartingImport ? <LoaderCircle className="size-4 animate-spin" /> : <Import className="size-4" />}
              导入选中渠道
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
