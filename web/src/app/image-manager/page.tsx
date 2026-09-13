"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { CalendarDays, ChevronLeft, ChevronRight, Copy, Download, ImageIcon, LoaderCircle, Maximize2, Plus, RefreshCw, Search, Tag, Trash2, X } from "lucide-react";
import { toast } from "sonner";

import { DateRangeFilter } from "@/components/date-range-filter";
import { ImageLightbox } from "@/components/image-lightbox";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { compressAllImages, deleteImageTag, deleteManagedImages, deleteToTarget, downloadImages, downloadSingleImage, fetchImageStorage, fetchImageTags, fetchManagedImages, setImageTags, type ImageStorageStats, type ManagedImage } from "@/lib/api";
import { writeClipboardText } from "@/lib/clipboard";
import { createMutationRequestGate } from "@/lib/mutation-request-gate";
import { finishMutationAndRefresh } from "@/lib/mutation-refresh-controller";
import { loadManagedImagesWithTags } from "@/lib/image-manager-load";
import { createRequestGate } from "@/lib/query-request-gate";
import { createReplaceableTimeout } from "@/lib/replaceable-timeout";
import { createLifecycleActionOwner } from "@/lib/lifecycle-action-owner";
import { createDownloadAbortRegistry } from "@/lib/download-lifecycle";
import { useAuthGuard } from "@/lib/use-auth-guard";

const LONG_PRESS_MS = 800;
const IMAGE_MANAGER_CHECKBOX_CLASS = "border-stone-300 bg-white/80 dark:border-white/35 dark:bg-white/5 data-[state=checked]:border-stone-950 dark:data-[state=checked]:border-white";

function formatSize(size: number) {
  return size > 1024 * 1024 ? `${(size / 1024 / 1024).toFixed(2)} MB` : `${Math.ceil(size / 1024)} KB`;
}

function imageKey(item: ManagedImage) {
  return item.rel || item.url;
}

async function copyImageUrl(url: string) {
  if (await writeClipboardText(url)) {
    toast.success("图片地址已复制");
    return;
  }
  toast.error("复制失败，请检查浏览器剪贴板权限");
}

const imageQueryKey = (startDate: string, endDate: string) => JSON.stringify([startDate, endDate]);

function useLongPress(onLongPress: () => void, ms = LONG_PRESS_MS) {
  const longPressTimerRef = useRef(createReplaceableTimeout());
  const activeRef = useRef(false);

  useEffect(() => {
    const longPressTimer = longPressTimerRef.current;
    return () => {
      activeRef.current = false;
      longPressTimer.cancel();
    };
  }, []);

  const start = useCallback((e: React.MouseEvent | React.TouchEvent) => {
    activeRef.current = true;
    longPressTimerRef.current.schedule(() => {
      if (activeRef.current) {
        onLongPress();
      }
    }, ms);
  }, [onLongPress, ms]);

  const stop = useCallback(() => {
    activeRef.current = false;
    longPressTimerRef.current.cancel();
  }, []);

  return {
    onMouseDown: start,
    onMouseUp: stop,
    onMouseLeave: stop,
    onTouchStart: start,
    onTouchEnd: stop,
  };
}

function ImageManagerContent() {
  const [items, setItems] = useState<ManagedImage[]>([]);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [lightboxIndex, setLightboxIndex] = useState(0);
  const [lightboxOpen, setLightboxOpen] = useState(false);
  const [page, setPage] = useState(1);
  const [isLoading, setIsLoading] = useState(true);
  const [deleteStartDate, setDeleteStartDate] = useState("");
  const [deleteTarget, setDeleteTarget] = useState<ManagedImage | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const [allTags, setAllTags] = useState<string[]>([]);
  const [storage, setStorage] = useState<ImageStorageStats | null>(null);
  const [storageLoading, setStorageLoading] = useState(true);
  const [compressResult, setCompressResult] = useState<string>("");
  const [targetFreeMb, setTargetFreeMb] = useState(500);
  const imageQueryRef = useRef({ startDate: "", endDate: "" });
  const imageRequestGateRef = useRef(createRequestGate(imageQueryKey("", "")));
  const storageRequestRef = useRef(0);
  const imageQueryAbortControllerRef = useRef<AbortController | null>(null);
  const storageAbortControllerRef = useRef<AbortController | null>(null);

  const updateImageQuery = useCallback((nextStartDate: string, nextEndDate: string) => {
    imageQueryRef.current = { startDate: nextStartDate, endDate: nextEndDate };
    imageRequestGateRef.current.setQuery(imageQueryKey(nextStartDate, nextEndDate));
    setStartDate(nextStartDate);
    setEndDate(nextEndDate);
  }, []);

  const loadStorage = useCallback(async () => {
    const queryOwner = imageMutationGateRef.current.beginQuery("storage");
    if (!queryOwner.allowed) return;
    const requestId = ++storageRequestRef.current;
    storageAbortControllerRef.current?.abort();
    const abortController = new AbortController();
    storageAbortControllerRef.current = abortController;
    try {
      const data = await fetchImageStorage(abortController.signal);
      if (!imageMutationGateRef.current.acceptsQuery(queryOwner) || requestId !== storageRequestRef.current) return;
      setStorage(data);
    } catch { /* ignore */ }
    finally {
      if (imageMutationGateRef.current.acceptsQuery(queryOwner) && requestId === storageRequestRef.current) setStorageLoading(false);
      if (storageAbortControllerRef.current === abortController) {
        storageAbortControllerRef.current = null;
      }
    }
  }, []);

  const refreshStorage = useCallback(() => {
    if (imageMutationGateRef.current.isMutationActive()) return Promise.resolve();
    setStorageLoading(true);
    return loadStorage();
  }, [loadStorage]);

  useEffect(() => {
    let active = true;
    queueMicrotask(() => {
      if (active) void loadStorage();
    });
    return () => {
      active = false;
    };
  }, [loadStorage]);
  const [selectedTags, setSelectedTags] = useState<string[]>([]);
  const [tagEditTarget, setTagEditTarget] = useState<ManagedImage | null>(null);
  const [tagInput, setTagInput] = useState("");
  const [dialogVisible, setDialogVisible] = useState(false);
  const [selectedPaths, setSelectedPaths] = useState<string[]>([]);
  const [deleteMode, setDeleteMode] = useState<"selected" | "filtered" | "byDate" | null>(null);
  const [isDownloading, setIsDownloading] = useState(false);
  const [isMutating, setIsMutating] = useState(false);
  const imageMutationGateRef = useRef(createMutationRequestGate());
  const downloadOwnerRef = useRef(createLifecycleActionOwner());
  const downloadAbortRegistryRef = useRef(createDownloadAbortRegistry());
  const deleteDialogCleanupTimerRef = useRef(createReplaceableTimeout());
  const tagPressTimerRef = useRef(createReplaceableTimeout());

  useEffect(() => {
    const mutationGate = imageMutationGateRef.current;
    const downloadOwner = downloadOwnerRef.current;
    const downloadAbortRegistry = downloadAbortRegistryRef.current;
    const cleanupTimer = deleteDialogCleanupTimerRef.current;
    const tagPressTimer = tagPressTimerRef.current;
    downloadOwner.activate();
    downloadAbortRegistry.activate();
    return () => {
      mutationGate.cancel();
      imageQueryAbortControllerRef.current?.abort();
      imageQueryAbortControllerRef.current = null;
      storageAbortControllerRef.current?.abort();
      storageAbortControllerRef.current = null;
      downloadOwner.cancel();
      downloadAbortRegistry.cancel();
      cleanupTimer.cancel();
      tagPressTimer.cancel();
    };
  }, []);

  const filteredItems = selectedTags.length > 0
    ? items.filter((item) => selectedTags.every((t) => (item.tags ?? []).includes(t)))
    : items;

  const lightboxImages = filteredItems.map((item) => ({
    id: item.name,
    src: item.url,
    sizeLabel: formatSize(item.size),
    dimensions: item.width && item.height ? `${item.width} x ${item.height}` : undefined,
  }));
  const pageSize = 12;
  const pageCount = Math.max(1, Math.ceil(filteredItems.length / pageSize));
  const safePage = Math.min(page, pageCount);
  const currentRows = filteredItems.slice((safePage - 1) * pageSize, safePage * pageSize);
  const selectedSet = useMemo(() => new Set(selectedPaths), [selectedPaths]);
  const selectedCount = deleteMode === "filtered" ? items.length : deleteMode === "byDate" ? 0 : selectedPaths.length;
  const currentPageSelected = currentRows.length > 0 && currentRows.every((item) => selectedSet.has(imageKey(item)));
  const allSelected = filteredItems.length > 0 && filteredItems.every((item) => selectedSet.has(imageKey(item)));

  const loadImages = useCallback(async () => {
    const query = imageQueryRef.current;
    const queryOwner = imageMutationGateRef.current.beginQuery("list");
    if (!queryOwner.allowed) return;
    const request = imageRequestGateRef.current.begin(imageQueryKey(query.startDate, query.endDate));
    if (request.sequence === null) return;
    imageQueryAbortControllerRef.current?.abort();
    const abortController = new AbortController();
    imageQueryAbortControllerRef.current = abortController;
    setIsLoading(true);
    const accepts = () => imageMutationGateRef.current.acceptsQuery(queryOwner) && imageRequestGateRef.current.isCurrent(request);
    try {
      const { data, tags } = await loadManagedImagesWithTags(
        (signal: AbortSignal) => fetchManagedImages({ start_date: query.startDate, end_date: query.endDate }, signal),
        (signal: AbortSignal) => fetchImageTags(signal),
        abortController.signal,
      );
      if (!accepts()) return;
      setItems(data.items);
      setAllTags(tags);
      setSelectedPaths((current) => current.filter((path) => data.items.some((item: ManagedImage) => imageKey(item) === path)));
      setPage(1);
    } catch (error) {
      if (!accepts()) return;
      toast.error(error instanceof Error ? error.message : "加载图片失败");
    } finally {
      if (accepts()) setIsLoading(false);
      if (imageQueryAbortControllerRef.current === abortController) {
        imageQueryAbortControllerRef.current = null;
      }
    }
  }, []);

  const beginImageMutation = useCallback(() => {
    const owner = imageMutationGateRef.current.beginMutation();
    if (!owner.accepted) {
      toast.error("已有图片操作正在进行");
      return null;
    }
    imageQueryAbortControllerRef.current?.abort();
    imageQueryAbortControllerRef.current = null;
    storageAbortControllerRef.current?.abort();
    storageAbortControllerRef.current = null;
    setIsMutating(true);
    return owner;
  }, []);

  const finishImageMutation = useCallback((
    owner: { accepted?: boolean; epoch?: number },
  ) => {
    return finishMutationAndRefresh({
      gate: imageMutationGateRef.current,
      owner,
      onBusyChange: setIsMutating,
      reloadList: loadImages,
      refreshSecondary: refreshStorage,
    });
  }, [loadImages, refreshStorage]);

  const closeDialog = useCallback(() => {
    setDialogVisible(false);
    deleteDialogCleanupTimerRef.current.schedule(() => {
      setDeleteTarget(null);
    }, 200);
  }, []);

  const openDeleteDialog = useCallback((item: ManagedImage) => {
    deleteDialogCleanupTimerRef.current.cancel();
    setDeleteTarget(item);
    setDialogVisible(true);
  }, []);

  const handleDelete = async () => {
    const target = deleteTarget;
    if (!target) return;
    const mutationOwner = beginImageMutation();
    if (!mutationOwner) return;
    const queryAtStart = imageQueryKey(imageQueryRef.current.startDate, imageQueryRef.current.endDate);
    setIsDeleting(true);
    try {
      await deleteManagedImages({ paths: [target.rel] });
      if (!imageMutationGateRef.current.acceptsMutation(mutationOwner)) return;
      if (queryAtStart === imageQueryKey(imageQueryRef.current.startDate, imageQueryRef.current.endDate)) {
        setItems((prev) => prev.filter((item) => item.rel !== target.rel));
        setSelectedPaths((prev) => prev.filter((p) => p !== imageKey(target)));
      } else {
        // The authoritative reload below reads the current query after any filter change.
      }
      toast.success("图片已删除");
    } catch (error) {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "删除失败");
      }
    } finally {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner) && finishImageMutation(mutationOwner)) {
        setIsDeleting(false);
        closeDialog();
      }
    }
  };

  const handleSetTags = async (item: ManagedImage, tags: string[]) => {
    const mutationOwner = beginImageMutation();
    if (!mutationOwner) return;
    try {
      const result = await setImageTags(item.rel, tags);
      if (!imageMutationGateRef.current.acceptsMutation(mutationOwner)) return;
      setItems((prev) => prev.map((i) => i.rel === item.rel ? { ...i, tags: result.tags } : i));
      const tagsData = await fetchImageTags();
      if (!imageMutationGateRef.current.acceptsMutation(mutationOwner)) return;
      setAllTags(tagsData.tags);
    } catch (error) {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "设置标签失败");
      }
    } finally {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) finishImageMutation(mutationOwner);
    }
  };

  const handleAddTag = (item: ManagedImage) => {
    const tag = tagInput.trim();
    if (!tag) return;
    const current = item.tags ?? [];
    if (current.includes(tag)) {
      toast.error("标签已存在");
      return;
    }
    void handleSetTags(item, [...current, tag]);
    setTagInput("");
  };

  const handleRemoveTag = (item: ManagedImage, tag: string) => {
    void handleSetTags(item, (item.tags ?? []).filter((t) => t !== tag));
  };

  const toggleFilterTag = (tag: string) => {
    setSelectedTags((prev) => prev.includes(tag) ? prev.filter((t) => t !== tag) : [...prev, tag]);
    setPage(1);
  };

  const [pressingTag, setPressingTag] = useState<string | null>(null);
  const [tagDeleteTarget, setTagDeleteTarget] = useState<string | null>(null);

  const handleDeleteTag = async (tag: string) => {
    const mutationOwner = beginImageMutation();
    if (!mutationOwner) return;
    try {
      const result = await deleteImageTag(tag);
      if (!imageMutationGateRef.current.acceptsMutation(mutationOwner)) return;
      setAllTags((prev) => prev.filter((t) => t !== tag));
      setSelectedTags((prev) => prev.filter((t) => t !== tag));
      setItems((prev) => prev.map((item) => ({
        ...item,
        tags: (item.tags ?? []).filter((t) => t !== tag),
      })));
      toast.success(`标签"${tag}"已删除，影响 ${result.removed_from} 张图片`);
      setTagDeleteTarget(null);
    } catch (error) {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "删除标签失败");
      }
    } finally {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) finishImageMutation(mutationOwner);
    }
  };

  const startTagPress = useCallback((tag: string) => {
    setPressingTag(tag);
    tagPressTimerRef.current.schedule(() => {
      setPressingTag(null);
      setTagDeleteTarget(tag);
    }, LONG_PRESS_MS);
  }, [setTagDeleteTarget]);

  const stopTagPress = useCallback(() => {
    setPressingTag(null);
    tagPressTimerRef.current.cancel();
  }, []);

  const clearFilters = () => {
    updateImageQuery("", "");
    setSelectedTags([]);
  };

  const togglePaths = (paths: string[], checked: boolean) => {
    setSelectedPaths((current) => checked ? Array.from(new Set([...current, ...paths])) : current.filter((path) => !paths.includes(path)));
  };

  const confirmDelete = async () => {
    if (!deleteMode || selectedCount === 0) return;
    const mode = deleteMode;
    const paths = [...selectedPaths];
    const queryAtStart = imageQueryKey(imageQueryRef.current.startDate, imageQueryRef.current.endDate);
    const mutationOwner = beginImageMutation();
    if (!mutationOwner) return;
    setIsDeleting(true);
    try {
      const data = await deleteManagedImages(mode === "filtered" ? { start_date: startDate, end_date: endDate, all_matching: true } : { paths });
      if (!imageMutationGateRef.current.acceptsMutation(mutationOwner)) return;
      toast.success(`已删除 ${data.removed} 张图片`);
      if (queryAtStart === imageQueryKey(imageQueryRef.current.startDate, imageQueryRef.current.endDate)) {
        setDeleteMode(null);
        setSelectedPaths([]);
      }
    } catch (error) {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "删除图片失败");
      }
    } finally {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner) && finishImageMutation(mutationOwner)) {
        setIsDeleting(false);
      }
    }
  };

  const handleCompress = async () => {
    const mutationOwner = beginImageMutation();
    if (!mutationOwner) return;
    try {
      const result = await compressAllImages();
      if (!imageMutationGateRef.current.acceptsMutation(mutationOwner)) return;
      setCompressResult(`已压缩${result.saved_mb}MB`);
    } catch {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) setCompressResult("压缩失败");
    } finally {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) finishImageMutation(mutationOwner);
    }
  };

  const handleDeleteToTarget = async () => {
    const mutationOwner = beginImageMutation();
    if (!mutationOwner) return;
    try {
      const result = await deleteToTarget(targetFreeMb);
      if (!imageMutationGateRef.current.acceptsMutation(mutationOwner)) return;
      toast.success(`已删除 ${result.removed} 张图片，释放 ${result.freed_mb ?? 0}MB`);
    } catch {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) toast.error("清理失败");
    } finally {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) finishImageMutation(mutationOwner);
    }
  };

  const handleDeleteByDate = async () => {
    if (!deleteStartDate) return;
    const cutoff = deleteStartDate;
    const mutationOwner = beginImageMutation();
    if (!mutationOwner) return;
    setIsDeleting(true);
    try {
      const result = await deleteManagedImages({ end_date: cutoff, all_matching: true });
      if (!imageMutationGateRef.current.acceptsMutation(mutationOwner)) return;
      toast.success(`已删除 ${result.removed} 张图片`);
      setDeleteMode(null);
    } catch {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner)) toast.error("删除失败");
    } finally {
      if (imageMutationGateRef.current.acceptsMutation(mutationOwner) && finishImageMutation(mutationOwner)) {
        setIsDeleting(false);
      }
    }
  };

  const handleBatchDownload = async () => {
    const paths = deleteMode === "filtered" ? items.map((item) => item.rel) : selectedPaths;
    if (paths.length === 0) return;
    const downloadOwner = downloadOwnerRef.current;
    const downloadAction = downloadOwner.begin();
    const isDownloadActive = () => downloadOwner.accepts(downloadAction);
    const controller = downloadAbortRegistryRef.current.begin();
    setIsDownloading(true);
    try {
      const started = await downloadImages(paths, isDownloadActive, controller.signal);
      if (isDownloadActive() && started) {
        toast.success(`已下载 ${paths.length} 张图片`);
      }
    } catch (error) {
      if (isDownloadActive()) {
        toast.error(error instanceof Error ? error.message : "下载失败");
      }
    } finally {
      downloadAbortRegistryRef.current.finish(controller);
      if (isDownloadActive()) {
        setIsDownloading(false);
      }
    }
  };

  const handleSingleDownload = async (item: ManagedImage) => {
    const downloadOwner = downloadOwnerRef.current;
    const downloadAction = downloadOwner.begin();
    const isDownloadActive = () => downloadOwner.accepts(downloadAction);
    const controller = downloadAbortRegistryRef.current.begin();
    try {
      const started = await downloadSingleImage(item.rel, isDownloadActive, controller.signal);
      if (isDownloadActive() && started) {
        toast.success("图片下载已开始");
      }
    } catch {
      if (isDownloadActive()) {
        toast.error("下载图片失败");
      }
    }
    finally {
      downloadAbortRegistryRef.current.finish(controller);
    }
  };

  useEffect(() => {
    void loadImages();
  }, [endDate, loadImages, startDate]);

  return (
    <section className="space-y-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="space-y-1">
          <div className="text-xs font-semibold tracking-[0.18em] text-stone-500 uppercase">Images</div>
          <h1 className="text-2xl font-semibold tracking-tight">图片管理</h1>
        </div>
        <div className="flex flex-wrap gap-2">
          <DateRangeFilter startDate={startDate} endDate={endDate} onChange={updateImageQuery} />
          <Button variant="outline" onClick={clearFilters} className="h-10 rounded-xl border-stone-200 bg-white px-4 text-stone-700">
            清除筛选条件
          </Button>
          <Button onClick={() => void loadImages()} disabled={isLoading || isMutating} className="h-10 rounded-xl bg-stone-950 px-4 text-white hover:bg-stone-800">
            {isLoading ? <LoaderCircle className="size-4 animate-spin" /> : <Search className="size-4" />}
            查询
          </Button>
          <Button variant="outline" onClick={() => setDeleteMode("filtered")} disabled={isDeleting || isMutating || items.length === 0 || (!startDate && !endDate)} className="h-10 rounded-xl border-rose-200 bg-white px-4 text-rose-600 hover:bg-rose-50">
            <Trash2 className="size-4" />
            删除匹配日期
          </Button>
        </div>
      </div>

      {allTags.length > 0 ? (
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs font-medium text-stone-500">
            <Tag className="mr-1 inline size-3.5" />
            标签筛选：
          </span>
          {allTags.map((tag) => {
            const isPressing = pressingTag === tag;
            return (
              <span
                key={tag}
                className="relative inline-flex items-center"
                onMouseDown={() => startTagPress(tag)}
                onMouseUp={stopTagPress}
                onMouseLeave={stopTagPress}
                onTouchStart={() => startTagPress(tag)}
                onTouchEnd={stopTagPress}
              >
                <button
                  type="button"
                  onClick={() => toggleFilterTag(tag)}
                >
                  <Badge
                    variant={selectedTags.includes(tag) ? "default" : "outline"}
                    className={`cursor-pointer rounded-md transition-all hover:opacity-80 ${isPressing ? "ring-2 ring-red-400 ring-offset-1" : ""}`}
                  >
                    {tag}
                  </Badge>
                </button>
                {isPressing ? (
                  <span className="pointer-events-none absolute inset-0 overflow-hidden rounded-md">
                    <span className="absolute inset-0 animate-[grow_800ms_linear_forwards] rounded-md bg-red-400/20" />
                  </span>
                ) : null}
              </span>
            );
          })}
          {selectedTags.length > 0 ? (
            <button type="button" onClick={() => setSelectedTags([])}>
              <Badge variant="secondary" className="cursor-pointer rounded-md">
                <X className="mr-0.5 size-3" />
                清除
              </Badge>
            </button>
          ) : null}
        </div>
      ) : null}

      {/* Storage Stats Panel */}
      <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-3 mb-4">
        {storage ? (
          <>
            <div className="rounded-xl border border-stone-200 bg-white/80 p-3">
              <div className="text-xs text-stone-500">磁盘总量</div>
              <div className="text-lg font-bold text-stone-800">{storage.disk_total_mb >= 1024 ? `${(storage.disk_total_mb / 1024).toFixed(1)} GB` : `${storage.disk_total_mb} MB`}</div>
            </div>
            <div className="rounded-xl border border-stone-200 bg-white/80 p-3">
              <div className="text-xs text-stone-500">剩余空间</div>
              <div className={`text-lg font-bold ${storage.disk_free_mb < 200 ? "text-red-500" : storage.disk_free_mb < 500 ? "text-yellow-500" : "text-green-600"}`}>{storage.disk_free_mb >= 1024 ? `${(storage.disk_free_mb / 1024).toFixed(1)} GB` : `${storage.disk_free_mb} MB`}</div>
            </div>
            <div className="rounded-xl border border-stone-200 bg-white/80 p-3">
              <div className="text-xs text-stone-500">图片数量</div>
              <div className="text-lg font-bold text-stone-800">{storage.image_count}</div>
            </div>
            <div className="rounded-xl border border-stone-200 bg-white/80 p-3">
              <div className="text-xs text-stone-500">图片占用</div>
              <div className="text-lg font-bold text-stone-800">{storage.image_size_mb >= 1024 ? `${(storage.image_size_mb / 1024).toFixed(1)} GB` : `${storage.image_size_mb} MB`}</div>
            </div>
            <div className="rounded-xl border border-stone-200 bg-white/80 p-3 col-span-2 flex items-center gap-2 flex-wrap">
              <span className="text-xs text-stone-500 w-full">快捷操作</span>
              <Button size="sm" variant="outline" className="h-7 text-xs" disabled={storageLoading || isMutating} onClick={() => { void refreshStorage(); }}>
                <RefreshCw className={`size-3 mr-1 ${storageLoading ? "animate-spin" : ""}`} />刷新
              </Button>
              <Button size="sm" variant="outline" className="h-7 text-xs" disabled={isMutating} onClick={() => void handleCompress()}>
                🗜️ 压缩优化
              </Button>
              <Button size="sm" variant="outline" className="h-7 text-xs border-rose-200 text-rose-600" disabled={isMutating}
                onClick={() => setDeleteMode("byDate")}>
                🗑️ 按日期删除
              </Button>
              <form onSubmit={(event) => { event.preventDefault(); void handleDeleteToTarget(); }} className="flex items-center gap-1">
                <Button size="sm" variant="outline" className="h-7 text-xs border-amber-200 text-amber-700" type="submit" disabled={isMutating}>
                  🧹 清理至
                </Button>
                <Input className="h-7 w-14 text-xs text-center px-1" type="number" min={50} value={targetFreeMb}
                  onChange={(e) => setTargetFreeMb(Number(e.target.value) || 500)} />
                <span className="text-xs text-stone-400">MB 剩余</span>
              </form>
              {compressResult ? <span className="text-xs text-green-600 ml-1">{compressResult}</span> : null}
            </div>
          </>
        ) : (
          <div className="rounded-xl border border-stone-200 bg-white/80 p-3 col-span-full text-center text-sm text-stone-400">
            {storageLoading ? "加载存储信息..." : "存储信息加载失败"}
          </div>
        )}
      </div>

      {/* Delete by date dialog */}
      <Dialog open={deleteMode === "byDate"} onOpenChange={() => setDeleteMode(null)}>
        <DialogContent className="sm:max-w-md rounded-2xl">
          <DialogHeader>
            <DialogTitle>按日期删除图片</DialogTitle>
            <DialogDescription className="sr-only">选择截止日期并确认永久删除此前的图片及缩略图。</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <label className="text-sm text-stone-600 shrink-0">删除</label>
              <Input className="h-9 text-sm" type="date" value={deleteStartDate} onChange={(e) => setDeleteStartDate(e.target.value)} />
              <span className="text-sm text-stone-400">之前的图片</span>
            </div>
            <p className="text-xs text-stone-500">此操作不可撤销，将永久删除所有匹配日期的图片及其缩略图。</p>
          </div>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setDeleteMode(null)}>取消</Button>
            <Button variant="destructive" disabled={!deleteStartDate || isDeleting || isMutating} onClick={() => void handleDeleteByDate()}>
              {isDeleting ? <LoaderCircle className="size-4 animate-spin" /> : null}
              确认删除
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
        <CardContent className="p-0">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-stone-100 px-5 py-4">
            <div className="flex flex-wrap items-center gap-3 text-sm text-stone-600">
              <ImageIcon className="size-4" />
              共 {filteredItems.length} 张
              {selectedTags.length > 0 ? <span className="text-stone-400">（筛选自 {items.length} 张）</span> : null}
              <label className="flex items-center gap-2">
                <Checkbox className={IMAGE_MANAGER_CHECKBOX_CLASS} checked={currentPageSelected} onCheckedChange={(checked) => togglePaths(currentRows.map(imageKey), Boolean(checked))} />
                本页全选
              </label>
              <label className="flex items-center gap-2">
                <Checkbox className={IMAGE_MANAGER_CHECKBOX_CLASS} checked={allSelected} onCheckedChange={(checked) => togglePaths(filteredItems.map(imageKey), Boolean(checked))} />
                全选结果
              </label>
              {selectedPaths.length > 0 ? <span>已选 {selectedPaths.length} 张</span> : null}
            </div>
            <div className="flex items-center gap-2">
              <Button variant="ghost" className="h-8 rounded-lg px-3 text-stone-500" onClick={() => void loadImages()} disabled={isLoading || isMutating}>
                <RefreshCw className={`size-4 ${isLoading ? "animate-spin" : ""}`} />
                刷新
              </Button>
              <button type="button" className="text-sm text-stone-500 hover:text-stone-900 disabled:text-stone-300" onClick={() => setSelectedPaths([])} disabled={selectedPaths.length === 0 || isDeleting}>
                取消选择
              </button>
              <Button variant="outline" className="h-8 rounded-lg border-stone-200 bg-white px-3 text-stone-600 hover:bg-stone-50" onClick={() => void handleBatchDownload()} disabled={selectedPaths.length === 0 || isDownloading || isDeleting}>
                {isDownloading ? <LoaderCircle className="size-4 animate-spin" /> : <Download className="size-4" />}
                下载所选
              </Button>
              <Button variant="outline" className="h-8 rounded-lg border-rose-200 bg-white px-3 text-rose-600 hover:bg-rose-50" onClick={() => setDeleteMode("selected")} disabled={selectedPaths.length === 0 || isDeleting || isMutating}>
                <Trash2 className="size-4" />
                删除所选
              </Button>
            </div>
          </div>
          <div className="grid gap-0 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {currentRows.map((item) => {
              const imageIndex = filteredItems.findIndex((row) => row.url === item.url);
              return (
              <div key={item.rel} className="group border-r border-b border-stone-100 p-4 transition hover:bg-stone-50 dark:hover:bg-white/5">
                <div className="relative">
                  <button
                    type="button"
                    className="relative block aspect-square w-full cursor-zoom-in overflow-hidden rounded-lg bg-stone-100 text-left"
                    onClick={() => {
                      setLightboxIndex(imageIndex);
                      setLightboxOpen(true);
                    }}
                  >
                    <img
                      src={item.thumbnail_url || item.url}
                      alt={item.name}
                      className="h-full w-full object-cover transition group-hover:scale-[1.02]"
                      onError={(event) => {
                        if (event.currentTarget.src !== item.url) {
                          event.currentTarget.src = item.url;
                        }
                      }}
                    />
                    <span className="absolute right-2 bottom-2 rounded-full bg-black/50 p-2 text-white opacity-100 transition sm:opacity-0 sm:group-hover:opacity-100">
                      <Maximize2 className="size-4" />
                    </span>
                  </button>
                  <button
                    type="button"
                    className="absolute top-2 right-2 z-10 inline-flex size-7 items-center justify-center rounded-full bg-black/50 text-white opacity-100 transition hover:bg-red-600 sm:opacity-0 sm:group-hover:opacity-100"
                    title="删除图片"
                    disabled={isMutating}
                    onClick={(e) => {
                      e.stopPropagation();
                      openDeleteDialog(item);
                    }}
                  >
                    <Trash2 className="size-3.5" />
                  </button>
                </div>
                <div className="mt-3 space-y-2 text-xs text-stone-500">
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-1 font-medium text-stone-700">
                      <CalendarDays className="size-3.5" />
                      {item.created_at}
                    </div>
                    <div className="flex items-center gap-1">
                      <Button
                        variant="ghost"
                        size="icon"
                        className="size-8 rounded-lg text-stone-400 hover:bg-stone-100 hover:text-stone-700"
                        onClick={() => void handleSingleDownload(item)}
                        title="下载图片"
                      >
                        <Download className="size-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="size-8 rounded-lg text-stone-400 hover:bg-stone-100 hover:text-stone-700"
                        onClick={() => {
                          void copyImageUrl(item.url);
                        }}
                      >
                        <Copy className="size-4" />
                      </Button>
                      <Checkbox className={IMAGE_MANAGER_CHECKBOX_CLASS} checked={selectedSet.has(imageKey(item))} onCheckedChange={(checked) => togglePaths([imageKey(item)], Boolean(checked))} />
                    </div>
                  </div>
                  <div className="flex items-center justify-between gap-2">
                    <span>{formatSize(item.size)}</span>
                    <span>{item.width && item.height ? `${item.width} x ${item.height}` : "-"}</span>
                  </div>
                  <div className="flex flex-wrap items-center gap-1">
                    {(item.tags ?? []).map((tag) => (
                      <Badge key={tag} variant="secondary" className="gap-0.5 rounded-md py-0 pr-0.5 text-[10px]">
                        {tag}
                        <button
                          type="button"
                          className="inline-flex size-3.5 items-center justify-center rounded-full hover:bg-stone-300"
                          disabled={isMutating}
                          onClick={() => handleRemoveTag(item, tag)}
                        >
                          <X className="size-2.5" />
                        </button>
                      </Badge>
                    ))}
                    <Popover open={tagEditTarget?.rel === item.rel} onOpenChange={(open) => { setTagEditTarget(open ? item : null); setTagInput(""); }}>
                      <PopoverTrigger asChild>
                        <button
                          type="button"
                          className="inline-flex size-5 items-center justify-center rounded-full border border-dashed border-stone-300 text-stone-400 hover:border-stone-500 hover:text-stone-600"
                          title="添加标签"
                          disabled={isMutating}
                        >
                          <Plus className="size-3" />
                        </button>
                      </PopoverTrigger>
                      <PopoverContent align="start" className="w-56 p-2">
                        <div className="space-y-2">
                          <div className="text-xs font-medium text-stone-500">添加标签</div>
                          <div className="flex gap-1">
                            <Input
                              value={tagInput}
                              onChange={(e) => setTagInput(e.target.value)}
                              disabled={isMutating}
                              placeholder="输入标签名"
                              className="h-8 text-xs"
                              onKeyDown={(e) => {
                                if (e.key === "Enter") {
                                  e.preventDefault();
                                  handleAddTag(item);
                                }
                              }}
                            />
                            <Button
                              size="icon"
                              variant="outline"
                              className="size-8 shrink-0"
                              onClick={() => handleAddTag(item)}
                              disabled={isMutating}
                            >
                              <Plus className="size-3.5" />
                            </Button>
                          </div>
                          {allTags.filter((t) => !(item.tags ?? []).includes(t)).length > 0 ? (
                            <div className="flex flex-wrap gap-1 border-t border-stone-100 pt-2">
                              {allTags.filter((t) => !(item.tags ?? []).includes(t)).map((tag) => (
                                <button
                                  key={tag}
                                  type="button"
                                  disabled={isMutating}
                                  onClick={() => {
                                    void handleSetTags(item, [...(item.tags ?? []), tag]);
                                    setTagEditTarget(null);
                                  }}
                                >
                                  <Badge variant="outline" className="cursor-pointer rounded-md text-[10px] hover:bg-stone-100">
                                    {tag}
                                  </Badge>
                                </button>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      </PopoverContent>
                    </Popover>
                  </div>
                </div>
              </div>
            )})}
          </div>
          <div className="flex items-center justify-end gap-2 border-t border-stone-100 px-4 py-3 text-sm text-stone-500">
            <span>第 {safePage} / {pageCount} 页，共 {filteredItems.length} 张</span>
            <Button variant="outline" size="icon" className="size-9 rounded-lg border-stone-200 bg-white" disabled={safePage <= 1} onClick={() => setPage((value) => Math.max(1, value - 1))}>
              <ChevronLeft className="size-4" />
            </Button>
            <Button variant="outline" size="icon" className="size-9 rounded-lg border-stone-200 bg-white" disabled={safePage >= pageCount} onClick={() => setPage((value) => Math.min(pageCount, value + 1))}>
              <ChevronRight className="size-4" />
            </Button>
          </div>
          {!isLoading && filteredItems.length === 0 ? <div className="px-6 py-14 text-center text-sm text-stone-500">没有找到图片</div> : null}
        </CardContent>
      </Card>

      <Dialog open={dialogVisible} onOpenChange={(open) => { if (!open) closeDialog(); }}>
        <DialogContent className="max-w-sm overflow-hidden rounded-2xl">
          <DialogHeader>
            <DialogTitle className="pr-8">确认删除</DialogTitle>
            <DialogDescription className="text-sm text-stone-600">
              确定要删除这张图片吗？此操作不可恢复。
            </DialogDescription>
          </DialogHeader>
          {deleteTarget ? (
            <div className="flex items-center gap-3 overflow-hidden rounded-xl border border-stone-200 bg-stone-50 p-3">
              <img
                src={deleteTarget.thumbnail_url || deleteTarget.url}
                alt=""
                className="size-16 shrink-0 rounded-lg object-cover"
                onError={(e) => { if (e.currentTarget.src !== deleteTarget.url) e.currentTarget.src = deleteTarget.url; }}
              />
              <div className="min-w-0 overflow-hidden text-xs text-stone-500">
                <div className="truncate font-medium text-stone-700">{deleteTarget.name}</div>
                <div className="truncate">{deleteTarget.created_at}</div>
                <div>{formatSize(deleteTarget.size)}</div>
              </div>
            </div>
          ) : null}
          <DialogFooter>
                <Button variant="outline" onClick={closeDialog} disabled={isDeleting || isMutating} className="rounded-xl">
              取消
            </Button>
                <Button variant="destructive" onClick={() => void handleDelete()} disabled={isDeleting || isMutating} className="rounded-xl">
              {isDeleting ? <LoaderCircle className="mr-1 size-4 animate-spin" /> : <Trash2 className="mr-1 size-4" />}
              删除
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <ImageLightbox
        images={lightboxImages}
        currentIndex={lightboxIndex}
        open={lightboxOpen}
        onOpenChange={setLightboxOpen}
        onIndexChange={setLightboxIndex}
      />
      <Dialog open={deleteMode === "selected" || deleteMode === "filtered"} onOpenChange={(open) => (!open ? setDeleteMode(null) : null)}>
        <DialogContent showCloseButton={false} className="rounded-2xl p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>{deleteMode === "filtered" ? "删除匹配日期的图片" : "删除所选图片"}</DialogTitle>
            <DialogDescription className="text-sm text-stone-600">
              确认删除 {selectedCount} 张图片吗？删除后无法恢复。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
                <Button variant="outline" className="rounded-xl" onClick={() => setDeleteMode(null)} disabled={isDeleting || isMutating}>
              取消
            </Button>
                <Button className="rounded-xl bg-rose-600 text-white hover:bg-rose-700" onClick={() => void confirmDelete()} disabled={isDeleting || isMutating || selectedCount === 0}>
              {isDeleting ? <LoaderCircle className="size-4 animate-spin" /> : null}
              确认删除
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog open={Boolean(tagDeleteTarget)} onOpenChange={(open) => { if (!open) setTagDeleteTarget(null); }}>
        <DialogContent className="max-w-sm rounded-2xl">
          <DialogHeader>
            <DialogTitle>删除标签</DialogTitle>
            <DialogDescription className="text-sm text-stone-600">
              确定要删除标签 <span className="font-semibold">&quot;{tagDeleteTarget}&quot;</span> 吗？将从所有图片中移除该标签。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" className="rounded-xl" onClick={() => setTagDeleteTarget(null)}>
              取消
            </Button>
            <Button
              variant="destructive"
              className="rounded-xl"
              disabled={isMutating}
              onClick={() => {
                if (tagDeleteTarget) void handleDeleteTag(tagDeleteTarget);
              }}
            >
              确认删除
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}

export default function ImageManagerPage() {
  const { isCheckingAuth, session } = useAuthGuard(["admin"]);
  if (isCheckingAuth || !session || session.role !== "admin") {
    return <div className="flex min-h-[40vh] items-center justify-center"><LoaderCircle className="size-5 animate-spin text-stone-400" /></div>;
  }
  return <ImageManagerContent />;
}
