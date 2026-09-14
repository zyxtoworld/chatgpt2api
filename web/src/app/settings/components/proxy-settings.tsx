"use client";

import { useEffect, useRef, useState } from "react";
import {
  LoaderCircle,
  PlugZap,
  Save,
  Wifi,
} from "lucide-react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  fetchProxy,
  testProxy,
  updateProxy,
  type ProxySettings,
  type ProxyTestResult,
} from "@/lib/api";
import { createLatestActionOwner } from "@/lib/latest-action-owner";
import { createMutationRequestGate } from "@/lib/mutation-request-gate";
import { proxySettingsWriteGate } from "@/lib/proxy-settings-write-gate";
import { createOwnedQueryLoader, scheduleOwnedMicrotask } from "@/lib/query-lifecycle";

export function ProxySettingsCard() {
  const requestGateRef = useRef(createMutationRequestGate());
  const loadOwnerRef = useRef<ReturnType<typeof createOwnedQueryLoader> | null>(null);
  const saveOwnerRef = useRef<{ epoch: number } | null>(null);
  const testOwnerRef = useRef(createLatestActionOwner());
  const [settings, setSettings] = useState<ProxySettings>({ enabled: false, url: "" });
  const [formUrl, setFormUrl] = useState("");
  const [formEnabled, setFormEnabled] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [isTesting, setIsTesting] = useState(false);
  const [testResult, setTestResult] = useState<ProxyTestResult | null>(null);

  useEffect(() => {
    const gate = requestGateRef.current;
    const testOwner = testOwnerRef.current;
    testOwner.activate();
    const loader = createOwnedQueryLoader({
      gate,
      request: fetchProxy,
      onStart: () => setIsLoading(true),
      onCommit: (data: Awaited<ReturnType<typeof fetchProxy>>) => {
        setSettings(data.proxy);
        setFormUrl(data.proxy.url);
        setFormEnabled(data.proxy.enabled);
      },
      onError: (error: unknown) => {
        toast.error(error instanceof Error ? error.message : "加载代理配置失败");
      },
      onFinish: () => setIsLoading(false),
    });
    loadOwnerRef.current = loader;
    const cancelInitialLoad = scheduleOwnedMicrotask(() => loader.run());
    return () => {
      cancelInitialLoad();
      loader.cancel();
      if (loadOwnerRef.current === loader) {
        loadOwnerRef.current = null;
      }
      saveOwnerRef.current = null;
      testOwner.cancel();
      gate.cancel();
    };
  }, []);

  const urlChanged = formUrl.trim() !== settings.url;
  const enabledChanged = formEnabled !== settings.enabled;
  const dirty = urlChanged || enabledChanged;

  const handleSave = async () => {
    if (formEnabled && !formUrl.trim()) {
      toast.error("启用代理时必须填写代理地址");
      return;
    }
    testOwnerRef.current.invalidate();
    setIsTesting(false);
    setTestResult(null);
    const writeOwner = proxySettingsWriteGate.begin();
    if (!writeOwner.accepted) {
      toast.error("代理配置操作正在进行，请稍候");
      return;
    }
    const mutationOwner = requestGateRef.current.beginMutation();
    if (!mutationOwner.accepted) {
      proxySettingsWriteGate.finish(writeOwner);
      toast.error("代理配置操作正在进行，请稍候");
      return;
    }
    loadOwnerRef.current?.clearLoadingForMutation();
    saveOwnerRef.current = mutationOwner;
    setIsSaving(true);
    try {
      const payload: { enabled?: boolean; url?: string } = {};
      if (enabledChanged) payload.enabled = formEnabled;
      if (urlChanged) payload.url = formUrl.trim();
      const data = await updateProxy(payload);
      if (requestGateRef.current.acceptsMutation(mutationOwner)) {
        setSettings(data.proxy);
        setFormUrl(data.proxy.url);
        setFormEnabled(data.proxy.enabled);
        toast.success("代理配置已保存");
      }
    } catch (error) {
      if (requestGateRef.current.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "保存失败");
      }
    } finally {
      if (saveOwnerRef.current === mutationOwner) {
        saveOwnerRef.current = null;
        setIsSaving(false);
      }
      requestGateRef.current.finishMutation(mutationOwner);
      proxySettingsWriteGate.finish(writeOwner);
    }
  };

  const handleTest = async () => {
    const candidate = formUrl.trim();
    if (!candidate) {
      toast.error("请先填写代理地址");
      return;
    }
    const testOwner = testOwnerRef.current.begin(candidate);
    setIsTesting(true);
    setTestResult(null);
    try {
      const data = await testProxy(candidate);
      if (testOwnerRef.current.accepts(testOwner, candidate)) {
        setTestResult(data.result);
        if (data.result.ok) {
          toast.success(`代理可用（${data.result.latency_ms} ms，HTTP ${data.result.status}）`);
        } else {
          toast.error(`代理不可用：${data.result.error ?? "未知错误"}`);
        }
      }
    } catch (error) {
      if (testOwnerRef.current.accepts(testOwner, candidate)) {
        toast.error(error instanceof Error ? error.message : "测试代理失败");
      }
    } finally {
      if (testOwnerRef.current.accepts(testOwner, candidate)) {
        setIsTesting(false);
      }
    }
  };

  return (
    <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
      <CardContent className="space-y-6 p-6">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="flex size-10 items-center justify-center rounded-xl bg-stone-100">
              <Wifi className="size-5 text-stone-600" />
            </div>
            <div>
              <h2 className="text-lg font-semibold tracking-tight">上游代理配置</h2>
              <p className="text-sm text-stone-500">
                为 chatgpt.com 的请求配置出网代理，适合国内服务器部署；Sub2API / CPA 请求不受影响。
              </p>
            </div>
          </div>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center py-10">
            <LoaderCircle className="size-5 animate-spin text-stone-400" />
          </div>
        ) : (
          <div className="space-y-4">
            <label className="flex cursor-pointer items-start gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3">
              <input
                type="checkbox"
                className="mt-1 size-4 rounded border-stone-300 text-stone-900 focus:ring-stone-900"
                checked={formEnabled}
                onChange={(event) => setFormEnabled(event.target.checked)}
              />
              <div className="space-y-0.5">
                <div className="text-sm font-medium text-stone-800">启用代理</div>
                <div className="text-sm text-stone-500">
                  关闭后 chatgpt.com 请求会直连。保存后立即生效，无需重启。
                </div>
              </div>
            </label>

            <div className="space-y-2">
              <label className="flex items-center gap-1.5 text-sm font-medium text-stone-700">
                <PlugZap className="size-3.5" />
                代理地址
              </label>
              <Input
                value={formUrl}
                onChange={(event) => {
                  testOwnerRef.current.invalidate();
                  setTestResult(null);
                  setIsTesting(false);
                  setFormUrl(event.target.value);
                }}
                placeholder="http://user:pass@host:port 或 socks5://host:port"
                className="h-11 rounded-xl border-stone-200 bg-white font-mono text-xs"
              />
              <div className="text-xs text-stone-400">
                支持 <code className="font-mono">http / https / socks4 / socks5 / socks5h</code>。
              </div>
            </div>

            {testResult ? (
              <div
                className={`rounded-xl border px-4 py-3 text-sm leading-6 ${
                  testResult.ok
                    ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                    : "border-rose-200 bg-rose-50 text-rose-800"
                }`}
              >
                {testResult.ok ? (
                  <>
                    代理可用：HTTP {testResult.status}，用时 {testResult.latency_ms} ms
                  </>
                ) : (
                  <>代理不可用：{testResult.error ?? "未知错误"}（用时 {testResult.latency_ms} ms）</>
                )}
              </div>
            ) : null}

            <div className="flex items-center gap-2">
              <Button
                className="h-10 rounded-xl bg-stone-950 px-5 text-white hover:bg-stone-800"
                onClick={() => void handleSave()}
                disabled={isSaving || !dirty}
              >
                {isSaving ? <LoaderCircle className="size-4 animate-spin" /> : <Save className="size-4" />}
                保存
              </Button>
              <Button
                variant="outline"
                className="h-10 rounded-xl border-stone-200 bg-white px-5 text-stone-700"
                onClick={() => void handleTest()}
                disabled={isTesting}
              >
                {isTesting ? <LoaderCircle className="size-4 animate-spin" /> : <PlugZap className="size-4" />}
                测试连通
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
