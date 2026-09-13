"use client";

import { useEffect } from "react";
import { LoaderCircle } from "lucide-react";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { createSerialPoller } from "@/lib/serial-poll";
import { useAuthGuard } from "@/lib/use-auth-guard";

import { BackupSettingsCard } from "./components/backup-settings-card";
import { ApiDocsCard } from "./components/api-docs-card";
import { CCLoadConnections } from "./components/ccload-connections";
import { ConfigCard } from "./components/config-card";
import { CPAPoolDialog } from "./components/cpa-pool-dialog";
import { CPAPoolsCard } from "./components/cpa-pools-card";
import { ImportBrowserDialog } from "./components/import-browser-dialog";
import { ProxyRuntimeCard } from "./components/proxy-runtime-card";
import { SettingsHeader } from "./components/settings-header";
import { Sub2APIConnections } from "./components/sub2api-connections";
import { ThirdPartyAppsCard } from "./components/third-party-apps-card";
import { UserKeysCard } from "./components/user-keys-card";
import { useSettingsStore } from "./store";

const settingsTabs = [
  { value: "basic", title: "基础配置" },
  { value: "backup", title: "备份" },
  { value: "keys", title: "用户密钥" },
  { value: "api-docs", title: "接口接入" },
  { value: "canvas", title: "画布入口" },
  { value: "proxy", title: "FlareSolverr" },
  { value: "cpa", title: "CPA" },
  { value: "sub2api", title: "Sub2API" },
  { value: "ccload", title: "ccLoad" },
];

function SettingsDataController() {
  const initialize = useSettingsStore((state) => state.initialize);
  const cancelInitialization = useSettingsStore((state) => state.cancelInitialization);
  const cancelConfigOperations = useSettingsStore((state) => state.cancelConfigOperations);
  const loadPools = useSettingsStore((state) => state.loadPools);
  const loadBackups = useSettingsStore((state) => state.loadBackups);
  const invalidatePoolLoads = useSettingsStore((state) => state.invalidatePoolLoads);
  const invalidateBackupLoads = useSettingsStore((state) => state.invalidateBackupLoads);
  const cancelPoolOperations = useSettingsStore((state) => state.cancelPoolOperations);
  const cancelBackupOperations = useSettingsStore((state) => state.cancelBackupOperations);
  const cancelImageStorageOperations = useSettingsStore((state) => state.cancelImageStorageOperations);
  const pools = useSettingsStore((state) => state.pools);
  const backupState = useSettingsStore((state) => state.backupState);
  const hasRunningPoolJobs = pools.some((pool) => {
    const status = pool.import_job?.status;
    return status === "pending" || status === "running";
  });

  useEffect(() => {
    void initialize();
    return () => {
      cancelInitialization();
      cancelConfigOperations();
      cancelPoolOperations();
      cancelBackupOperations();
      cancelImageStorageOperations();
    };
  }, [cancelBackupOperations, cancelConfigOperations, cancelImageStorageOperations, cancelInitialization, cancelPoolOperations, initialize]);

  useEffect(() => {
    if (!hasRunningPoolJobs) {
      return;
    }

    const poller = createSerialPoller({
      intervalMs: 1500,
      initialDelayMs: 1500,
      poll: async (signal: AbortSignal) => {
        await loadPools(true, signal);
      },
      isDone: () => false,
      onProgress: () => undefined,
    });
    void poller.start().catch(() => undefined);
    return () => {
      poller.stop();
      invalidatePoolLoads();
    };
  }, [hasRunningPoolJobs, invalidatePoolLoads, loadPools]);

  useEffect(() => {
    if (!backupState?.running) {
      return;
    }
    const poller = createSerialPoller({
      intervalMs: 3000,
      initialDelayMs: 3000,
      poll: async (signal: AbortSignal) => {
        await loadBackups(true, signal);
      },
      isDone: () => false,
      onProgress: () => undefined,
    });
    void poller.start().catch(() => undefined);
    return () => {
      poller.stop();
      invalidateBackupLoads();
    };
  }, [backupState?.running, invalidateBackupLoads, loadBackups]);

  return null;
}

function SettingsPageContent() {
  return (
    <>
      <SettingsDataController />
      <SettingsHeader />
      <Tabs defaultValue="basic" className="space-y-4">
        <div className="sticky top-3 z-20 overflow-x-auto rounded-xl border border-white/80 bg-white/90 px-3 py-2 shadow-sm backdrop-blur">
          <TabsList variant="line" className="min-w-max justify-start">
            {settingsTabs.map((tab) => (
              <TabsTrigger key={tab.value} value={tab.value} className="px-4">
                {tab.title}
              </TabsTrigger>
            ))}
          </TabsList>
        </div>
        <TabsContent value="basic">
          <ConfigCard />
        </TabsContent>
        <TabsContent value="proxy">
          <ProxyRuntimeCard />
        </TabsContent>
        <TabsContent value="backup">
          <BackupSettingsCard />
        </TabsContent>
        <TabsContent value="keys">
          <UserKeysCard />
        </TabsContent>
        <TabsContent value="canvas">
          <ThirdPartyAppsCard />
        </TabsContent>
        <TabsContent value="api-docs">
          <ApiDocsCard />
        </TabsContent>
        <TabsContent value="cpa">
          <CPAPoolsCard />
        </TabsContent>
        <TabsContent value="sub2api">
          <Sub2APIConnections />
        </TabsContent>
        <TabsContent value="ccload" forceMount className="data-[state=inactive]:hidden">
          <CCLoadConnections />
        </TabsContent>
      </Tabs>
      <CPAPoolDialog />
      <ImportBrowserDialog />
    </>
  );
}

export default function SettingsPage() {
  const { isCheckingAuth, session } = useAuthGuard(["admin"]);

  if (isCheckingAuth || !session || session.role !== "admin") {
    return (
      <div className="flex min-h-[40vh] items-center justify-center">
        <LoaderCircle className="size-5 animate-spin text-stone-400" />
      </div>
    );
  }

  return <SettingsPageContent />;
}
