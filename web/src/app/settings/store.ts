"use client";

import { create } from "zustand";
import { toast } from "sonner";

import {
  createCPAPool,
  deleteBackup,
  deleteCPAPool,
  fetchCPAPoolFiles,
  fetchCPAPools,
  fetchBackups,
  fetchSettingsConfig,
  runBackupNow,
  syncImageStorage,
  startCPAImport,
  testBackupConnection,
  testImageStorageConnection,
  updateCPAPool,
  updateSettingsConfig,
  type BackupItem,
  type BackupSettings,
  type BackupState,
  type CPAPool,
  type CPARemoteFile,
  type ImageStorageMode,
  type ImageStorageSettings,
  type ProxyRuntimeClearanceMode,
  type ProxyRuntimeEgressMode,
  type ProxyRuntimeSettings,
  type SettingsConfig,
  type ThirdPartyAppsSettings,
} from "@/lib/api";
import { createMutationRequestGate } from "@/lib/mutation-request-gate";

export const PAGE_SIZE_OPTIONS = ["50", "100", "200"] as const;

export type PageSizeOption = (typeof PAGE_SIZE_OPTIONS)[number];

const backupRequestGate = createMutationRequestGate();
const backupWriteGate = createMutationRequestGate();
const poolRequestGate = createMutationRequestGate();
const configRequestGate = createMutationRequestGate();
const configWriteGate = createMutationRequestGate();
const imageStorageOperationGate = createMutationRequestGate();
let backupLoadingOwner: unknown = null;
let poolLoadingOwner: unknown = null;
let backupRunOwner: unknown = null;
let backupDeleteOwner: unknown = null;
let backupTestOwner: unknown = null;
let backupWriteSettled: Promise<void> | null = null;
let backupOperationsGeneration = 0;
let configLoadingOwner: unknown = null;
let configWriteOwner: unknown = null;
let configWriteSettled: Promise<void> | null = null;
let settingsInitializationGeneration = 0;
let imageStoragePresentationGeneration = 0;
let poolSaveOwner: unknown = null;
let poolDeleteOwner: unknown = null;
let poolImportOwner: unknown = null;
let poolFilesOwner: unknown = null;

function beginBackupMutation() {
  const writeOwner = backupWriteGate.beginMutation();
  if (!writeOwner.accepted) {
    return null;
  }
  const queryOwner = backupRequestGate.beginMutation();
  if (!queryOwner.accepted) {
    backupWriteGate.finishMutation(writeOwner);
    return null;
  }
  let resolveSettled!: () => void;
  const settled = new Promise<void>((resolve) => {
    resolveSettled = resolve;
  });
  backupWriteSettled = settled;
  return { writeOwner, queryOwner, settled, resolveSettled };
}

function acceptsBackupMutation(owner: ReturnType<typeof beginBackupMutation>) {
  return Boolean(
    owner
      && backupWriteGate.acceptsMutation(owner.writeOwner)
      && backupRequestGate.acceptsMutation(owner.queryOwner),
  );
}

function finishBackupMutation(owner: NonNullable<ReturnType<typeof beginBackupMutation>>) {
  const queryFinished = backupRequestGate.finishMutation(owner.queryOwner);
  const writeFinished = backupWriteGate.finishMutation(owner.writeOwner);
  if (backupWriteSettled === owner.settled) {
    backupWriteSettled = null;
  }
  owner.resolveSettled();
  return { queryFinished, writeFinished };
}

function beginImageStorageOperation() {
  const owner = imageStorageOperationGate.beginMutation();
  if (!owner.accepted) {
    return null;
  }
  return {
    owner,
    presentationGeneration: imageStoragePresentationGeneration,
  };
}

function acceptsImageStoragePresentation(operation: ReturnType<typeof beginImageStorageOperation>) {
  return Boolean(
    operation
      && operation.presentationGeneration === imageStoragePresentationGeneration,
  );
}

const DEFAULT_PROXY_RUNTIME: ProxyRuntimeSettings = {
  enabled: false,
  egress_mode: "direct",
  proxy_url: "",
  resource_proxy_url: "",
  skip_ssl_verify: false,
  reset_session_status_codes: [403],
  clearance: {
    enabled: false,
    mode: "none",
    cf_cookies: "",
    cf_clearance: "",
    user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    browser: "chrome",
    flaresolverr_url: "",
    timeout_sec: 60,
    refresh_interval: 3600,
    warm_up_on_start: false,
    has_cf_cookies: false,
    has_cf_clearance: false,
  },
};

const DEFAULT_THIRD_PARTY_APPS: ThirdPartyAppsSettings = {
  infinite_canvas: {
    enabled: false,
    url: "https://canvas.best",
  },
};

function stringOrFallback(value: unknown, fallback: string): string {
  return typeof value === "string" ? value : fallback;
}

function finiteNumberOrFallback(value: unknown, fallback: number): number {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  const number = typeof value === "number" || typeof value === "string"
    ? Number(value)
    : Number.NaN;
  return Number.isFinite(number) ? number : fallback;
}

function normalizeProxyRuntime(value: unknown): ProxyRuntimeSettings {
  const source = typeof value === "object" && value !== null ? value as Partial<ProxyRuntimeSettings> : {};
  const clearanceSource = typeof source.clearance === "object" && source.clearance !== null
    ? source.clearance as Partial<ProxyRuntimeSettings["clearance"]>
    : {};
  const egressMode = source.egress_mode === "single_proxy" ? "single_proxy" : "direct";
  const clearanceMode: ProxyRuntimeClearanceMode = clearanceSource.mode === "manual" || clearanceSource.mode === "flaresolverr"
    ? clearanceSource.mode
    : "none";
  const statusCodes = Array.isArray(source.reset_session_status_codes)
    ? source.reset_session_status_codes
      .map((item) => Number(item))
      .filter((item) => Number.isInteger(item) && item >= 100 && item <= 599)
    : [];
  return {
    ...DEFAULT_PROXY_RUNTIME,
    ...source,
    enabled: Boolean(source.enabled),
    egress_mode: egressMode as ProxyRuntimeEgressMode,
    proxy_url: stringOrFallback(source.proxy_url, ""),
    resource_proxy_url: stringOrFallback(source.resource_proxy_url, ""),
    skip_ssl_verify: Boolean(source.skip_ssl_verify),
    reset_session_status_codes: statusCodes.length > 0 ? statusCodes : [403],
    clearance: {
      ...DEFAULT_PROXY_RUNTIME.clearance,
      ...clearanceSource,
      enabled: Boolean(clearanceSource.enabled),
      mode: clearanceMode,
      cf_cookies: stringOrFallback(clearanceSource.cf_cookies, ""),
      cf_clearance: stringOrFallback(clearanceSource.cf_clearance, ""),
      user_agent: stringOrFallback(clearanceSource.user_agent, DEFAULT_PROXY_RUNTIME.clearance.user_agent),
      browser: stringOrFallback(clearanceSource.browser, "chrome"),
      flaresolverr_url: stringOrFallback(clearanceSource.flaresolverr_url, ""),
      timeout_sec: finiteNumberOrFallback(clearanceSource.timeout_sec, 60),
      refresh_interval: finiteNumberOrFallback(clearanceSource.refresh_interval, 3600),
      warm_up_on_start: Boolean(clearanceSource.warm_up_on_start),
      has_cf_cookies: Boolean(clearanceSource.has_cf_cookies),
      has_cf_clearance: Boolean(clearanceSource.has_cf_clearance),
    },
  };
}

function normalizeThirdPartyApps(value: unknown): ThirdPartyAppsSettings {
  const source = typeof value === "object" && value !== null ? value as Partial<ThirdPartyAppsSettings> : {};
  const canvas: Partial<ThirdPartyAppsSettings["infinite_canvas"]> = typeof source.infinite_canvas === "object" && source.infinite_canvas
    ? source.infinite_canvas
    : {};
  return {
    infinite_canvas: {
      enabled: canvas.enabled === true,
      url: stringOrFallback(canvas.url, DEFAULT_THIRD_PARTY_APPS.infinite_canvas.url),
    },
  };
}

function normalizeConfig(config: SettingsConfig): SettingsConfig {
  const defaultThinkingEffort = typeof config.default_thinking_effort === "string"
    && ["standard", "extended", "max"].includes(config.default_thinking_effort)
    ? config.default_thinking_effort as "standard" | "extended" | "max"
    : "auto";
  const imageStorage = typeof config.image_storage === "object" && config.image_storage
    ? config.image_storage as ImageStorageSettings
    : {
      enabled: false,
      mode: "local",
      webdav_url: "",
      webdav_username: "",
      webdav_password: "",
      webdav_root_path: "chatgpt2api/images",
      public_base_url: "",
    };
  const imageStorageMode: ImageStorageMode = imageStorage.enabled && imageStorage.mode === "both"
    ? "both"
    : imageStorage.enabled && imageStorage.mode === "webdav"
      ? "webdav"
      : "local";
  const backup = typeof config.backup === "object" && config.backup
    ? config.backup as BackupSettings
    : {
      enabled: false,
      provider: "cloudflare_r2",
      account_id: "",
      access_key_id: "",
      secret_access_key: "",
      bucket: "",
      prefix: "backups",
      interval_minutes: 360,
      rotation_keep: 10,
      encrypt: false,
      passphrase: "",
      include: {
        config: true,
        cpa: true,
        sub2api: true,
        ccload: true,
        logs: true,
        image_tasks: true,
        accounts_snapshot: true,
        auth_keys_snapshot: true,
        images: false,
      },
    };
  return {
    ...config,
    refresh_account_interval_minute: finiteNumberOrFallback(config.refresh_account_interval_minute, 5),
    image_retention_days: finiteNumberOrFallback(config.image_retention_days, 30),
    image_poll_timeout_secs: finiteNumberOrFallback(config.image_poll_timeout_secs, 120),
    image_account_concurrency: finiteNumberOrFallback(config.image_account_concurrency, 3),
    image_settle_enabled: Boolean(config.image_settle_enabled !== false),
    image_check_before_hit_enabled: Boolean(config.image_check_before_hit_enabled !== false),
    image_remove_conversation_after_result: Boolean(config.image_remove_conversation_after_result),
    image_remove_conversation_always: Boolean(config.image_remove_conversation_always),
    image_settle_secs: finiteNumberOrFallback(config.image_settle_secs, 2.0),
    image_timeout_retry_secs: finiteNumberOrFallback(config.image_timeout_retry_secs, 30),
    auto_remove_invalid_accounts: Boolean(config.auto_remove_invalid_accounts),
    auto_remove_rate_limited_accounts: Boolean(config.auto_remove_rate_limited_accounts),
    auto_relogin_after_refresh: Boolean(config.auto_relogin_after_refresh),
    log_levels: Array.isArray(config.log_levels) ? config.log_levels : [],
    proxy: typeof config.proxy === "string" ? config.proxy : "",
    base_url: typeof config.base_url === "string" ? config.base_url : "",
    global_system_prompt: stringOrFallback(config.global_system_prompt, ""),
    default_upstream_model_name: stringOrFallback(config.default_upstream_model_name, "gpt-5-5"),
    default_thinking_effort: defaultThinkingEffort,
    sensitive_words: Array.isArray(config.sensitive_words) ? config.sensitive_words : [],
    ai_review: {
      enabled: Boolean(config.ai_review?.enabled),
      base_url: stringOrFallback(config.ai_review?.base_url, ""),
      api_key: stringOrFallback(config.ai_review?.api_key, ""),
      model: stringOrFallback(config.ai_review?.model, ""),
      prompt: stringOrFallback(config.ai_review?.prompt, ""),
    },
    image_storage: {
      enabled: Boolean(imageStorage.enabled),
      mode: imageStorageMode,
      webdav_url: stringOrFallback(imageStorage.webdav_url, ""),
      webdav_username: stringOrFallback(imageStorage.webdav_username, ""),
      webdav_password: stringOrFallback(imageStorage.webdav_password, ""),
      webdav_root_path: stringOrFallback(imageStorage.webdav_root_path, "chatgpt2api/images"),
      public_base_url: stringOrFallback(imageStorage.public_base_url, ""),
    },
    proxy_runtime: normalizeProxyRuntime(config.proxy_runtime),
    third_party_apps: normalizeThirdPartyApps(config.third_party_apps),
    backup: {
      ...backup,
      enabled: Boolean(backup.enabled),
      account_id: stringOrFallback(backup.account_id, ""),
      access_key_id: stringOrFallback(backup.access_key_id, ""),
      secret_access_key: stringOrFallback(backup.secret_access_key, ""),
      bucket: stringOrFallback(backup.bucket, ""),
      prefix: stringOrFallback(backup.prefix, "backups"),
      interval_minutes: finiteNumberOrFallback(backup.interval_minutes, 360),
      rotation_keep: finiteNumberOrFallback(backup.rotation_keep, 10),
      encrypt: Boolean(backup.encrypt),
      passphrase: stringOrFallback(backup.passphrase, ""),
      include: {
        config: Boolean(backup.include?.config ?? true),
        cpa: Boolean(backup.include?.cpa ?? true),
        sub2api: Boolean(backup.include?.sub2api ?? true),
        ccload: Boolean(backup.include?.ccload ?? true),
        logs: Boolean(backup.include?.logs ?? true),
        image_tasks: Boolean(backup.include?.image_tasks ?? true),
        accounts_snapshot: Boolean(backup.include?.accounts_snapshot ?? true),
        auth_keys_snapshot: Boolean(backup.include?.auth_keys_snapshot ?? true),
        images: Boolean(backup.include?.images ?? false),
      },
    },
  };
}

function normalizeFiles(items: CPARemoteFile[]) {
  const seen = new Set<string>();
  const files: CPARemoteFile[] = [];
  for (const item of items) {
    const name = stringOrFallback(item.name, "").trim();
    if (!name || seen.has(name)) {
      continue;
    }
    seen.add(name);
    files.push({
      name,
      email: stringOrFallback(item.email, "").trim(),
    });
  }
  return files;
}

type SettingsStore = {
  config: SettingsConfig | null;
  isLoadingConfig: boolean;
  isSavingConfig: boolean;
  backups: BackupItem[];
  backupState: BackupState | null;
  isLoadingBackups: boolean;
  isRunningBackup: boolean;
  deletingBackupKey: string | null;
  isTestingBackup: boolean;
  isTestingImageStorage: boolean;
  isSyncingImageStorage: boolean;

  pools: CPAPool[];
  isLoadingPools: boolean;
  deletingId: string | null;
  loadingFilesId: string | null;

  dialogOpen: boolean;
  editingPool: CPAPool | null;
  formName: string;
  formBaseUrl: string;
  formSecretKey: string;
  showSecret: boolean;
  isSavingPool: boolean;

  browserOpen: boolean;
  browserPool: CPAPool | null;
  remoteFiles: CPARemoteFile[];
  selectedNames: string[];
  fileQuery: string;
  filePage: number;
  pageSize: PageSizeOption;
  isStartingImport: boolean;

  initialize: () => Promise<void>;
  cancelInitialization: () => void;
  loadConfig: () => Promise<void>;
  cancelConfigOperations: () => void;
  saveConfig: () => Promise<boolean>;
  loadBackups: (silent?: boolean) => Promise<void>;
  invalidateBackupLoads: () => void;
  cancelBackupOperations: () => void;
  runBackup: () => Promise<void>;
  removeBackup: (key: string) => Promise<void>;
  testBackup: () => Promise<void>;
  setRefreshAccountIntervalMinute: (value: string) => void;
  setImageRetentionDays: (value: string) => void;
  setImagePollTimeoutSecs: (value: string) => void;
  setImageAccountConcurrency: (value: string) => void;
  setImageSettleEnabled: (value: boolean) => void;
  setImageCheckBeforeHitEnabled: (value: boolean) => void;
  setImageRemoveConversationAfterResult: (value: boolean) => void;
  setImageRemoveConversationAlways: (value: boolean) => void;
  setImageSettleSecs: (value: string) => void;
  setImageTimeoutRetrySecs: (value: string) => void;
  setAutoRemoveInvalidAccounts: (value: boolean) => void;
  setAutoRemoveRateLimitedAccounts: (value: boolean) => void;
  setAutoReloginAfterRefresh: (value: boolean) => void;
  setLogLevel: (level: string, enabled: boolean) => void;
  setProxy: (value: string) => void;
  setBaseUrl: (value: string) => void;
  setGlobalSystemPrompt: (value: string) => void;
  setDefaultUpstreamModelName: (value: string) => void;
  setDefaultThinkingEffort: (value: "auto" | "standard" | "extended" | "max") => void;
  setSensitiveWordsText: (value: string) => void;
  setAIReviewField: (key: "enabled" | "base_url" | "api_key" | "model" | "prompt", value: string | boolean) => void;
  setImageStorageField: (key: keyof ImageStorageSettings, value: string | boolean) => void;
  setProxyRuntimeField: <K extends keyof ProxyRuntimeSettings>(key: K, value: ProxyRuntimeSettings[K]) => void;
  setProxyRuntimeClearanceField: <K extends keyof ProxyRuntimeSettings["clearance"]>(key: K, value: ProxyRuntimeSettings["clearance"][K]) => void;
  setProxyRuntimeStatusCodesText: (value: string) => void;
  setInfiniteCanvasField: <K extends keyof ThirdPartyAppsSettings["infinite_canvas"]>(key: K, value: ThirdPartyAppsSettings["infinite_canvas"][K]) => void;
  testImageStorage: () => Promise<void>;
  syncImagesToWebDAV: () => Promise<void>;
  cancelImageStorageOperations: () => void;
  setBackupField: (key: keyof BackupSettings, value: string | boolean) => void;
  setBackupInclude: (key: keyof BackupSettings["include"], value: boolean) => void;

  loadPools: (silent?: boolean) => Promise<void>;
  invalidatePoolLoads: () => void;
  cancelPoolOperations: () => void;
  openAddDialog: () => void;
  openEditDialog: (pool: CPAPool) => void;
  setDialogOpen: (open: boolean) => void;
  setFormName: (value: string) => void;
  setFormBaseUrl: (value: string) => void;
  setFormSecretKey: (value: string) => void;
  setShowSecret: (checked: boolean) => void;
  savePool: () => Promise<void>;
  deletePool: (pool: CPAPool) => Promise<void>;

  browseFiles: (pool: CPAPool) => Promise<void>;
  setBrowserOpen: (open: boolean) => void;
  toggleFile: (name: string, checked: boolean) => void;
  replaceSelectedNames: (names: string[]) => void;
  setFileQuery: (value: string) => void;
  setFilePage: (page: number) => void;
  setPageSize: (value: PageSizeOption) => void;
  startImport: () => Promise<void>;
};

export const useSettingsStore = create<SettingsStore>((set, get) => ({
  config: null,
  isLoadingConfig: true,
  isSavingConfig: false,
  backups: [],
  backupState: null,
  isLoadingBackups: true,
  isRunningBackup: false,
  deletingBackupKey: null,
  isTestingBackup: false,
  isTestingImageStorage: false,
  isSyncingImageStorage: false,

  pools: [],
  isLoadingPools: true,
  deletingId: null,
  loadingFilesId: null,

  dialogOpen: false,
  editingPool: null,
  formName: "",
  formBaseUrl: "",
  formSecretKey: "",
  showSecret: false,
  isSavingPool: false,

  browserOpen: false,
  browserPool: null,
  remoteFiles: [],
  selectedNames: [],
  fileQuery: "",
  filePage: 1,
  pageSize: "100",
  isStartingImport: false,

  initialize: async () => {
    const initializationGeneration = ++settingsInitializationGeneration;
    if (configWriteGate.isMutationActive()) {
      const activeWrite = configWriteSettled;
      if (!activeWrite) {
        return;
      }
      await activeWrite;
      if (initializationGeneration !== settingsInitializationGeneration) {
        return;
      }
    }
    await Promise.allSettled([get().loadConfig(), get().loadPools()]);
    if (initializationGeneration !== settingsInitializationGeneration) {
      return;
    }
    const backup = get().config?.backup;
    const isConfigured = Boolean(
      stringOrFallback(backup?.account_id, "").trim()
      && stringOrFallback(backup?.access_key_id, "").trim()
      && stringOrFallback(backup?.secret_access_key, "").trim()
      && stringOrFallback(backup?.bucket, "").trim(),
    );
    if (isConfigured) {
      await get().loadBackups();
    } else if (initializationGeneration === settingsInitializationGeneration) {
      set({ backups: [], isLoadingBackups: false });
    }
  },

  cancelInitialization: () => {
    settingsInitializationGeneration += 1;
  },

  loadConfig: async () => {
    const queryOwner = configRequestGate.beginQuery();
    if (!configRequestGate.acceptsQuery(queryOwner)) {
      return;
    }
    configLoadingOwner = queryOwner;
    set({ isLoadingConfig: true });
    try {
      const data = await fetchSettingsConfig();
      const normalized = normalizeConfig(data.config);
      if (configRequestGate.acceptsQuery(queryOwner)) {
        set({
          config: normalized,
        });
      }
    } catch (error) {
      if (configRequestGate.acceptsQuery(queryOwner)) {
        toast.error(error instanceof Error ? error.message : "加载系统配置失败");
      }
    } finally {
      if (configLoadingOwner === queryOwner && configRequestGate.acceptsQuery(queryOwner)) {
        configLoadingOwner = null;
        set({ isLoadingConfig: false });
      }
    }
  },

  cancelConfigOperations: () => {
    configLoadingOwner = null;
    configRequestGate.cancel();
    set({ isLoadingConfig: false });
  },

  cancelImageStorageOperations: () => {
    imageStoragePresentationGeneration += 1;
    set({ isTestingImageStorage: false, isSyncingImageStorage: false });
  },

  saveConfig: async () => {
    const { config } = get();
    if (!config) {
      return false;
    }

    const writeOwner = configWriteGate.beginMutation();
    if (!writeOwner.accepted) {
      return false;
    }
    const queryFence = configRequestGate.beginMutation();
    if (!queryFence.accepted) {
      configWriteGate.finishMutation(writeOwner);
      return false;
    }

    let resolveWriteSettled!: () => void;
    const writeSettled = new Promise<void>((resolve) => {
      resolveWriteSettled = resolve;
    });
    configWriteSettled = writeSettled;
    configWriteOwner = writeOwner;
    configLoadingOwner = null;
    set({ isLoadingConfig: false, isSavingConfig: true });
    try {
      const data = await updateSettingsConfig({
        ...config,
        refresh_account_interval_minute: Math.max(1, Number(config.refresh_account_interval_minute) || 1),
        image_retention_days: Math.max(1, Number(config.image_retention_days) || 30),
        image_poll_timeout_secs: Math.max(1, Number(config.image_poll_timeout_secs) || 120),
        image_account_concurrency: Math.max(1, Number(config.image_account_concurrency) || 3),
        image_settle_enabled: Boolean(config.image_settle_enabled !== false),
        image_check_before_hit_enabled: Boolean(config.image_check_before_hit_enabled !== false),
        image_remove_conversation_after_result: Boolean(config.image_remove_conversation_after_result),
        image_remove_conversation_always: Boolean(config.image_remove_conversation_always),
        image_settle_secs: Math.max(0.5, Number(config.image_settle_secs) || 2.0),
        image_timeout_retry_secs: Math.max(1, Number(config.image_timeout_retry_secs) || 30),
        auto_remove_invalid_accounts: Boolean(config.auto_remove_invalid_accounts),
        auto_remove_rate_limited_accounts: Boolean(config.auto_remove_rate_limited_accounts),
        auto_relogin_after_refresh: Boolean(config.auto_relogin_after_refresh),
        proxy: config.proxy.trim(),
        base_url: stringOrFallback(config.base_url, "").trim(),
        global_system_prompt: stringOrFallback(config.global_system_prompt, "").trim(),
        default_upstream_model_name: stringOrFallback(config.default_upstream_model_name, "gpt-5-5").trim() || "gpt-5-5",
        default_thinking_effort: typeof config.default_thinking_effort === "string"
          && ["standard", "extended", "max"].includes(config.default_thinking_effort)
          ? config.default_thinking_effort
          : "auto",
        sensitive_words: (config.sensitive_words || [])
          .filter((item): item is string => typeof item === "string")
          .map((item) => item.trim())
          .filter(Boolean),
        ai_review: {
          enabled: Boolean(config.ai_review?.enabled),
          base_url: stringOrFallback(config.ai_review?.base_url, "").trim(),
          api_key: stringOrFallback(config.ai_review?.api_key, "").trim(),
          model: stringOrFallback(config.ai_review?.model, "").trim(),
          prompt: stringOrFallback(config.ai_review?.prompt, "").trim(),
        },
        image_storage: {
          enabled: Boolean(config.image_storage?.enabled),
          mode: config.image_storage?.enabled
            && (config.image_storage.mode === "webdav" || config.image_storage.mode === "both")
            ? config.image_storage.mode
            : "local",
          webdav_url: stringOrFallback(config.image_storage?.webdav_url, "").trim(),
          webdav_username: stringOrFallback(config.image_storage?.webdav_username, "").trim(),
          webdav_password: stringOrFallback(config.image_storage?.webdav_password, "").trim(),
          webdav_root_path: stringOrFallback(config.image_storage?.webdav_root_path, "chatgpt2api/images").trim(),
          public_base_url: stringOrFallback(config.image_storage?.public_base_url, "").trim(),
        },
        proxy_runtime: {
          ...normalizeProxyRuntime(config.proxy_runtime),
            proxy_url: stringOrFallback(config.proxy_runtime?.proxy_url, "").trim(),
            resource_proxy_url: stringOrFallback(config.proxy_runtime?.resource_proxy_url, "").trim(),
          reset_session_status_codes: normalizeProxyRuntime({
            reset_session_status_codes: (config.proxy_runtime?.reset_session_status_codes || [403])
              .map((item) => Number(item))
              .filter((item) => Number.isInteger(item) && item >= 100 && item <= 599),
          }).reset_session_status_codes,
          clearance: {
            ...normalizeProxyRuntime(config.proxy_runtime).clearance,
            cf_cookies: stringOrFallback(config.proxy_runtime?.clearance?.cf_cookies, "").trim(),
            cf_clearance: stringOrFallback(config.proxy_runtime?.clearance?.cf_clearance, "").trim(),
            user_agent: stringOrFallback(config.proxy_runtime?.clearance?.user_agent, DEFAULT_PROXY_RUNTIME.clearance.user_agent).trim(),
            browser: stringOrFallback(config.proxy_runtime?.clearance?.browser, "chrome").trim(),
            flaresolverr_url: stringOrFallback(config.proxy_runtime?.clearance?.flaresolverr_url, "").trim(),
            timeout_sec: Math.max(1, Number(config.proxy_runtime?.clearance?.timeout_sec) || 60),
            refresh_interval: Math.max(60, Number(config.proxy_runtime?.clearance?.refresh_interval) || 3600),
          },
        },
        third_party_apps: {
          infinite_canvas: {
            enabled: config.third_party_apps?.infinite_canvas?.enabled === true,
            url: stringOrFallback(config.third_party_apps?.infinite_canvas?.url, DEFAULT_THIRD_PARTY_APPS.infinite_canvas.url).trim(),
          },
        },
        backup: {
          ...(config.backup as BackupSettings),
          account_id: stringOrFallback(config.backup?.account_id, "").trim(),
          access_key_id: stringOrFallback(config.backup?.access_key_id, "").trim(),
          secret_access_key: stringOrFallback(config.backup?.secret_access_key, "").trim(),
          bucket: stringOrFallback(config.backup?.bucket, "").trim(),
          prefix: stringOrFallback(config.backup?.prefix, "backups").trim(),
          interval_minutes: Math.max(1, Number(config.backup?.interval_minutes) || 360),
          rotation_keep: Math.max(0, Number(config.backup?.rotation_keep) || 0),
          passphrase: stringOrFallback(config.backup?.passphrase, "").trim(),
        },
      });
      if (!configWriteGate.acceptsMutation(writeOwner)) {
        return false;
      }
      set({
        config: normalizeConfig(data.config),
      });
      if (configRequestGate.acceptsMutation(queryFence)) {
        window.dispatchEvent(new Event("third-party-apps-updated"));
        toast.success("配置已保存");
      }
      return true;
    } catch (error) {
      if (configWriteGate.acceptsMutation(writeOwner) && configRequestGate.acceptsMutation(queryFence)) {
        toast.error(error instanceof Error ? error.message : "保存系统配置失败");
      }
      return false;
    } finally {
      configRequestGate.finishMutation(queryFence);
      configWriteGate.finishMutation(writeOwner);
      if (configWriteOwner === writeOwner) {
        configWriteOwner = null;
        set({ isSavingConfig: false });
      }
      if (configWriteSettled === writeSettled) {
        configWriteSettled = null;
      }
      resolveWriteSettled();
    }
  },

  setRefreshAccountIntervalMinute: (value) => {
    set((state) => {
      if (!state.config) {
        return {};
      }
      return {
        config: {
          ...state.config,
          refresh_account_interval_minute: value,
        },
      };
    });
  },

  setImageRetentionDays: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_retention_days: value } } : {});
  },

  setImagePollTimeoutSecs: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_poll_timeout_secs: value } } : {});
  },

  setImageAccountConcurrency: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_account_concurrency: value } } : {});
  },

  setImageSettleEnabled: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_settle_enabled: value, image_check_before_hit_enabled: value } } : {});
  },

  setImageCheckBeforeHitEnabled: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_check_before_hit_enabled: value } } : {});
  },

  setImageRemoveConversationAfterResult: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_remove_conversation_after_result: value } } : {});
  },

  setImageRemoveConversationAlways: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_remove_conversation_always: value } } : {});
  },

  setImageSettleSecs: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_settle_secs: value } } : {});
  },

  setImageTimeoutRetrySecs: (value) => {
    set((state) => state.config ? { config: { ...state.config, image_timeout_retry_secs: value } } : {});
  },

  setAutoRemoveInvalidAccounts: (value) => {
    set((state) => state.config ? { config: { ...state.config, auto_remove_invalid_accounts: value } } : {});
  },

  setAutoRemoveRateLimitedAccounts: (value) => {
    set((state) => state.config ? { config: { ...state.config, auto_remove_rate_limited_accounts: value } } : {});
  },

  setAutoReloginAfterRefresh: (value) => {
    set((state) => state.config ? { config: { ...state.config, auto_relogin_after_refresh: value } } : {});
  },

  setLogLevel: (level, enabled) => {
    set((state) => {
      if (!state.config) return {};
      const levels = new Set(state.config.log_levels || []);
      if (enabled) levels.add(level);
      else levels.delete(level);
      return { config: { ...state.config, log_levels: Array.from(levels) } };
    });
  },

  setProxy: (value) => {
    set((state) => {
      if (!state.config) {
        return {};
      }
      return {
        config: {
          ...state.config,
          proxy: value,
        },
      };
    });
  },

  setBaseUrl: (value) => {
    set((state) => {
      if (!state.config) {
        return {};
      }
      return {
        config: {
          ...state.config,
          base_url: value,
        },
      };
    });
  },

  setGlobalSystemPrompt: (value) => {
    set((state) => state.config ? { config: { ...state.config, global_system_prompt: value } } : {});
  },

  setDefaultUpstreamModelName: (value) => {
    set((state) => state.config ? { config: { ...state.config, default_upstream_model_name: value } } : {});
  },

  setDefaultThinkingEffort: (value) => {
    set((state) => state.config ? { config: { ...state.config, default_thinking_effort: value } } : {});
  },

  setSensitiveWordsText: (value) => {
    set((state) => state.config ? { config: { ...state.config, sensitive_words: value.split("\n") } } : {});
  },

  setAIReviewField: (key, value) => {
    set((state) => state.config ? { config: { ...state.config, ai_review: { ...(state.config.ai_review || {}), [key]: value } } } : {});
  },

  setImageStorageField: (key, value) => {
    set((state) => {
      if (!state.config?.image_storage) {
        return {};
      }
      const next = {
        ...state.config.image_storage,
        [key]: value,
      };
      if (key === "enabled" && !value) {
        next.mode = "local";
      }
      if (key === "enabled" && value && next.mode === "local") {
        next.mode = "webdav";
      }
      return {
        config: {
          ...state.config,
          image_storage: next,
        },
      };
    });
  },

  setProxyRuntimeField: (key, value) => {
    set((state) => {
      if (!state.config) {
        return {};
      }
      const runtime = normalizeProxyRuntime(state.config.proxy_runtime);
      const nextRuntime = normalizeProxyRuntime({
        ...runtime,
        [key]: value,
      });
      return {
        config: {
          ...state.config,
          proxy_runtime: nextRuntime,
        },
      };
    });
  },

  setProxyRuntimeClearanceField: (key, value) => {
    set((state) => {
      if (!state.config) {
        return {};
      }
      const runtime = normalizeProxyRuntime(state.config.proxy_runtime);
      const nextRuntime = normalizeProxyRuntime({
        ...runtime,
        clearance: {
          ...runtime.clearance,
          [key]: value,
        },
      });
      return {
        config: {
          ...state.config,
          proxy_runtime: nextRuntime,
        },
      };
    });
  },

  setProxyRuntimeStatusCodesText: (value) => {
    const codes = value
      .split(/[,\s]+/)
      .map((item) => Number(item.trim()))
      .filter((item) => Number.isInteger(item) && item >= 100 && item <= 599);
    set((state) => {
      if (!state.config) {
        return {};
      }
      const runtime = normalizeProxyRuntime(state.config.proxy_runtime);
      return {
        config: {
          ...state.config,
          proxy_runtime: normalizeProxyRuntime({
            ...runtime,
            reset_session_status_codes: codes.length > 0 ? codes : [403],
          }),
        },
      };
    });
  },

  setInfiniteCanvasField: (key, value) => {
    set((state) => {
      if (!state.config) {
        return {};
      }
      const apps = normalizeThirdPartyApps(state.config.third_party_apps);
      return {
        config: {
          ...state.config,
          third_party_apps: {
            ...apps,
            infinite_canvas: {
              ...apps.infinite_canvas,
              [key]: value,
            },
          },
        },
      };
    });
  },

  testImageStorage: async () => {
    const operation = beginImageStorageOperation();
    if (!operation) {
      toast.error("已有 WebDAV 操作正在进行");
      return;
    }
    set({ isTestingImageStorage: true });
    try {
      const saved = await get().saveConfig();
      if (!saved) {
        return;
      }
      const data = await testImageStorageConnection();
      if (!acceptsImageStoragePresentation(operation)) {
        return;
      }
      if (data.result.ok) {
        toast.success(`WebDAV 连接可用：HTTP ${data.result.status}`);
      } else {
        toast.error(`WebDAV 连接失败：${data.result.error ?? `HTTP ${data.result.status}`}`);
      }
    } catch (error) {
      if (acceptsImageStoragePresentation(operation)) {
        toast.error(error instanceof Error ? error.message : "测试 WebDAV 失败");
      }
    } finally {
      imageStorageOperationGate.finishMutation(operation.owner);
      if (acceptsImageStoragePresentation(operation)) {
        set({ isTestingImageStorage: false });
      }
    }
  },

  syncImagesToWebDAV: async () => {
    const operation = beginImageStorageOperation();
    if (!operation) {
      toast.error("已有 WebDAV 操作正在进行");
      return;
    }
    set({ isSyncingImageStorage: true });
    try {
      const saved = await get().saveConfig();
      if (!saved) {
        return;
      }
      const data = await syncImageStorage();
      if (acceptsImageStoragePresentation(operation)) {
        toast.success(`同步完成：上传 ${data.result.uploaded}，跳过 ${data.result.skipped}，失败 ${data.result.failed}`);
      }
    } catch (error) {
      if (acceptsImageStoragePresentation(operation)) {
        toast.error(error instanceof Error ? error.message : "同步图片失败");
      }
    } finally {
      imageStorageOperationGate.finishMutation(operation.owner);
      if (acceptsImageStoragePresentation(operation)) {
        set({ isSyncingImageStorage: false });
      }
    }
  },

  setBackupField: (key, value) => {
    set((state) => {
      if (!state.config?.backup) {
        return {};
      }
      return {
        config: {
          ...state.config,
          backup: {
            ...state.config.backup,
            [key]: value,
          },
        },
      };
    });
  },

  setBackupInclude: (key, value) => {
    set((state) => {
      if (!state.config?.backup) {
        return {};
      }
      return {
        config: {
          ...state.config,
          backup: {
            ...state.config.backup,
            include: {
              ...state.config.backup.include,
              [key]: value,
            },
          },
        },
      };
    });
  },

  loadBackups: async (silent = false) => {
    const invocationGeneration = backupOperationsGeneration;
    if (backupWriteGate.isMutationActive()) {
      const settled = backupWriteSettled;
      if (!settled) {
        return;
      }
      await settled;
      if (invocationGeneration !== backupOperationsGeneration) {
        return;
      }
    }
    const queryOwner = backupRequestGate.beginQuery();
    if (!queryOwner.allowed) {
      return;
    }
    if (!silent) {
      backupLoadingOwner = queryOwner;
      set({ isLoadingBackups: true });
    }
    try {
      const data = await fetchBackups();
      if (!backupRequestGate.acceptsQuery(queryOwner)) {
        return;
      }
      set({
        backups: data.items,
        backupState: data.state,
      });
    } catch (error) {
      if (!silent && backupRequestGate.acceptsQuery(queryOwner)) {
        toast.error(error instanceof Error ? error.message : "加载备份列表失败");
      }
    } finally {
      if (!silent && backupLoadingOwner === queryOwner) {
        backupLoadingOwner = null;
        set({ isLoadingBackups: false });
      }
    }
  },

  invalidateBackupLoads: () => {
    backupRequestGate.invalidateQueries();
  },

  cancelBackupOperations: () => {
    backupOperationsGeneration += 1;
    backupRequestGate.cancel();
    backupLoadingOwner = null;
    backupRunOwner = null;
    backupDeleteOwner = null;
    backupTestOwner = null;
    set({ isLoadingBackups: false, isRunningBackup: false, deletingBackupKey: null, isTestingBackup: false });
  },

  runBackup: async () => {
    const mutationOwner = beginBackupMutation();
    if (!mutationOwner) {
      toast.error("已有备份操作正在进行，请稍候");
      return;
    }
    backupLoadingOwner = null;
    backupRunOwner = mutationOwner;
    set({ isLoadingBackups: false, isRunningBackup: true });
    let shouldReload = false;
    try {
      const saved = await get().saveConfig();
      if (!saved) {
        return;
      }
      const data = await runBackupNow();
      if (acceptsBackupMutation(mutationOwner)) {
        toast.success(`备份已完成：${data.result.key}`);
        shouldReload = true;
      }
    } catch (error) {
      if (acceptsBackupMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "执行备份失败");
      }
    } finally {
      const isOwner = backupRunOwner === mutationOwner;
      if (isOwner) {
        backupRunOwner = null;
        set({ isRunningBackup: false });
      }
      const finished = finishBackupMutation(mutationOwner);
      if (finished.queryFinished && finished.writeFinished && shouldReload) {
        await get().loadBackups(true);
      }
    }
  },

  removeBackup: async (key) => {
    const mutationOwner = beginBackupMutation();
    if (!mutationOwner) {
      toast.error("已有备份操作正在进行，请稍候");
      return;
    }
    backupLoadingOwner = null;
    backupDeleteOwner = mutationOwner;
    set({ isLoadingBackups: false, deletingBackupKey: key });
    let shouldReload = false;
    try {
      await deleteBackup(key);
      if (acceptsBackupMutation(mutationOwner)) {
        toast.success("备份已删除");
        shouldReload = true;
      }
    } catch (error) {
      if (acceptsBackupMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "删除备份失败");
      }
    } finally {
      const isOwner = backupDeleteOwner === mutationOwner;
      if (isOwner) {
        backupDeleteOwner = null;
        set({ deletingBackupKey: null });
      }
      const finished = finishBackupMutation(mutationOwner);
      if (finished.queryFinished && finished.writeFinished && shouldReload) {
        await get().loadBackups(true);
      }
    }
  },

  testBackup: async () => {
    const mutationOwner = beginBackupMutation();
    if (!mutationOwner) {
      toast.error("已有备份操作正在进行，请稍候");
      return;
    }
    backupTestOwner = mutationOwner;
    set({ isTestingBackup: true });
    try {
      const saved = await get().saveConfig();
      if (!saved) {
        return;
      }
      const data = await testBackupConnection();
      if (acceptsBackupMutation(mutationOwner)) {
        toast.success(`R2 连接正常（HTTP ${data.result.status}）`);
      }
    } catch (error) {
      if (acceptsBackupMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "测试备份连接失败");
      }
    } finally {
      if (backupTestOwner === mutationOwner) {
        backupTestOwner = null;
        set({ isTestingBackup: false });
      }
      finishBackupMutation(mutationOwner);
    }
  },

  loadPools: async (silent = false) => {
    const queryOwner = poolRequestGate.beginQuery("list");
    if (!queryOwner.allowed) {
      return;
    }
    if (!silent) {
      poolLoadingOwner = queryOwner;
      set({ isLoadingPools: true });
    }
    try {
      const data = await fetchCPAPools();
      if (!poolRequestGate.acceptsQuery(queryOwner)) {
        return;
      }
      set({ pools: data.pools });
    } catch (error) {
      if (!silent && poolRequestGate.acceptsQuery(queryOwner)) {
        toast.error(error instanceof Error ? error.message : "加载 CPA 连接失败");
      }
    } finally {
      if (!silent && poolLoadingOwner === queryOwner) {
        poolLoadingOwner = null;
        set({ isLoadingPools: false });
      }
    }
  },

  invalidatePoolLoads: () => {
    poolRequestGate.invalidateQueries("list");
  },

  cancelPoolOperations: () => {
    poolRequestGate.cancel();
    poolLoadingOwner = null;
    poolSaveOwner = null;
    poolDeleteOwner = null;
    poolImportOwner = null;
    poolFilesOwner = null;
    set({ isLoadingPools: false, isSavingPool: false, deletingId: null, isStartingImport: false });
  },

  openAddDialog: () => {
    set({
      editingPool: null,
      formName: "",
      formBaseUrl: "",
      formSecretKey: "",
      showSecret: false,
      dialogOpen: true,
    });
  },

  openEditDialog: (pool) => {
    set({
      editingPool: pool,
      formName: pool.name,
      formBaseUrl: pool.base_url,
      formSecretKey: "",
      showSecret: false,
      dialogOpen: true,
    });
  },

  setDialogOpen: (open) => {
    set({ dialogOpen: open });
  },

  setFormName: (value) => {
    set({ formName: value });
  },

  setFormBaseUrl: (value) => {
    set({ formBaseUrl: value });
  },

  setFormSecretKey: (value) => {
    set({ formSecretKey: value });
  },

  setShowSecret: (checked) => {
    set({ showSecret: checked });
  },

  savePool: async () => {
    const { editingPool, formName, formBaseUrl, formSecretKey } = get();
    if (!formBaseUrl.trim()) {
      toast.error("请输入 CPA 地址");
      return;
    }
    if (!editingPool && !formSecretKey.trim()) {
      toast.error("请输入 Secret Key");
      return;
    }

    const mutationOwner = poolRequestGate.beginMutation();
    if (!mutationOwner.accepted) {
      toast.error("已有 CPA 操作正在进行，请稍候");
      return;
    }
    poolLoadingOwner = null;
    poolSaveOwner = mutationOwner;
    set({ isLoadingPools: false, isSavingPool: true });
    try {
      if (editingPool) {
        const data = await updateCPAPool(editingPool.id, {
          name: formName.trim(),
          base_url: formBaseUrl.trim(),
          secret_key: formSecretKey.trim() || undefined,
        });
        if (poolRequestGate.acceptsMutation(mutationOwner)) {
          set({ pools: data.pools, dialogOpen: false });
          toast.success("连接已更新");
        }
      } else {
        const data = await createCPAPool({
          name: formName.trim(),
          base_url: formBaseUrl.trim(),
          secret_key: formSecretKey.trim(),
        });
        if (poolRequestGate.acceptsMutation(mutationOwner)) {
          set({ pools: data.pools, dialogOpen: false });
          toast.success("连接已添加");
        }
      }
    } catch (error) {
      if (poolRequestGate.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "保存失败");
      }
    } finally {
      if (poolSaveOwner === mutationOwner) {
        poolSaveOwner = null;
        set({ isSavingPool: false });
      }
      poolRequestGate.finishMutation(mutationOwner);
    }
  },

  deletePool: async (pool) => {
    const mutationOwner = poolRequestGate.beginMutation();
    if (!mutationOwner.accepted) {
      toast.error("已有 CPA 操作正在进行，请稍候");
      return;
    }
    poolLoadingOwner = null;
    poolDeleteOwner = mutationOwner;
    set({ isLoadingPools: false, deletingId: pool.id });
    try {
      const data = await deleteCPAPool(pool.id);
      if (poolRequestGate.acceptsMutation(mutationOwner)) {
        set({ pools: data.pools });
        toast.success("连接已删除");
      }
    } catch (error) {
      if (poolRequestGate.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "删除失败");
      }
    } finally {
      if (poolDeleteOwner === mutationOwner) {
        poolDeleteOwner = null;
        set({ deletingId: null });
      }
      poolRequestGate.finishMutation(mutationOwner);
    }
  },

  browseFiles: async (pool) => {
    const queryOwner = poolRequestGate.beginQuery("files");
    if (!queryOwner.allowed) {
      return;
    }
    poolFilesOwner = queryOwner;
    set({ loadingFilesId: pool.id });
    try {
      const data = await fetchCPAPoolFiles(pool.id);
      if (!poolRequestGate.acceptsQuery(queryOwner)) {
        return;
      }
      const currentPool = get().pools.find((item) => item.id === pool.id);
      if (!currentPool) {
        return;
      }
      const files = normalizeFiles(data.files);
      set({
        browserPool: currentPool,
        remoteFiles: files,
        selectedNames: [],
        fileQuery: "",
        filePage: 1,
        browserOpen: true,
      });
      toast.success(`读取成功，共 ${files.length} 个远程账号`);
    } catch (error) {
      if (poolRequestGate.acceptsQuery(queryOwner)) {
        toast.error(error instanceof Error ? error.message : "读取远程账号失败");
      }
    } finally {
      if (poolFilesOwner === queryOwner) {
        poolFilesOwner = null;
        set({ loadingFilesId: null });
      }
    }
  },

  setBrowserOpen: (open) => {
    set({ browserOpen: open });
  },

  toggleFile: (name, checked) => {
    set((state) => {
      if (checked) {
        return {
          selectedNames: Array.from(new Set([...state.selectedNames, name])),
        };
      }
      return {
        selectedNames: state.selectedNames.filter((item) => item !== name),
      };
    });
  },

  replaceSelectedNames: (names) => {
    set({ selectedNames: Array.from(new Set(names)) });
  },

  setFileQuery: (value) => {
    set({ fileQuery: value, filePage: 1 });
  },

  setFilePage: (page) => {
    set({ filePage: page });
  },

  setPageSize: (value) => {
    set({ pageSize: value, filePage: 1 });
  },

  startImport: async () => {
    const { browserPool, selectedNames, pools } = get();
    if (!browserPool) {
      return;
    }
    if (selectedNames.length === 0) {
      toast.error("请先选择要导入的账号");
      return;
    }

    const mutationOwner = poolRequestGate.beginMutation();
    if (!mutationOwner.accepted) {
      toast.error("已有 CPA 操作正在进行，请稍候");
      return;
    }
    poolLoadingOwner = null;
    poolImportOwner = mutationOwner;
    set({ isLoadingPools: false, isStartingImport: true });
    try {
      const result = await startCPAImport(browserPool.id, selectedNames);
      if (poolRequestGate.acceptsMutation(mutationOwner)) {
        set({
          pools: pools.map((pool) =>
            pool.id === browserPool.id ? { ...pool, import_job: result.import_job } : pool,
          ),
          browserOpen: false,
        });
        toast.success("导入任务已启动");
      }
    } catch (error) {
      if (poolRequestGate.acceptsMutation(mutationOwner)) {
        toast.error(error instanceof Error ? error.message : "启动导入失败");
      }
    } finally {
      if (poolImportOwner === mutationOwner) {
        poolImportOwner = null;
        set({ isStartingImport: false });
      }
      poolRequestGate.finishMutation(mutationOwner);
    }
  },
}));
