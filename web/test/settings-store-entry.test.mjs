import assert from "node:assert/strict";
import { register } from "node:module";
import test from "node:test";

register("./ts-source-loader.mjs", { parentURL: import.meta.url });

const [{ request }, { useSettingsStore }, { toast }] = await Promise.all([
  import("../src/lib/request.ts"),
  import("../src/app/settings/store.ts"),
  import("sonner"),
]);

function pool(id, name) {
  return {
    id,
    name,
    base_url: `https://${id}.example.test`,
    has_secret_key: true,
    import_job: null,
  };
}

test("settings config save admits one writer and reopens after it finishes", async () => {
  const originalRequest = request.request;
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve) => pending.push({ resolve }));
  };

  try {
    useSettingsStore.setState({
      config: { proxy: "", base_url: "https://first.example.test" },
      isSavingConfig: false,
    });

    const firstSave = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 1);

    useSettingsStore.setState({
      config: { proxy: "", base_url: "https://second.example.test" },
    });
    const rejectedSave = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    const callCountWhileBusy = calls.length;

    for (const item of pending.splice(0)) {
      item.resolve({ data: { config: { proxy: "", base_url: "https://server.example.test" } } });
    }
    await Promise.all([firstSave, rejectedSave]);

    assert.equal(callCountWhileBusy, 1, "a concurrent config mutation must not start another HTTP write");
    assert.equal(useSettingsStore.getState().isSavingConfig, false);

    const thirdSave = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 2, "a new config mutation is admitted after the first finishes");
    pending.shift().resolve({ data: { config: { proxy: "", base_url: "https://third.example.test" } } });
    await thirdSave;
    assert.equal(useSettingsStore.getState().isSavingConfig, false);
  } finally {
    request.request = originalRequest;
  }
});

test("a late initial config load cannot overwrite an accepted config save", async () => {
  const originalRequest = request.request;
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve) => pending.push({ config, resolve }));
  };

  try {
    useSettingsStore.setState({
      config: { proxy: "", base_url: "before.example.test" },
      isLoadingConfig: false,
      isSavingConfig: false,
    });

    const initialLoad = useSettingsStore.getState().loadConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls[0].method, "GET");

    useSettingsStore.setState({
      config: { proxy: "", base_url: "saved.example.test" },
    });
    const save = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls[1].method, "POST");

    pending[1].resolve({ data: { config: { proxy: "", base_url: "saved.example.test" } } });
    await save;
    assert.equal(useSettingsStore.getState().config.base_url, "saved.example.test");

    pending[0].resolve({ data: { config: { proxy: "", base_url: "stale.example.test" } } });
    await initialLoad;
    assert.equal(useSettingsStore.getState().config.base_url, "saved.example.test");
  } finally {
    request.request = originalRequest;
  }
});

test("config cleanup keeps the write lease and delays re-entry reads until the write settles", async () => {
  const originalRequest = request.request;
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve, reject) => pending.push({ config, resolve, reject }));
  };

  try {
    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.setState({
      config: { proxy: "", base_url: "before.example.test" },
      isLoadingConfig: false,
      isSavingConfig: false,
    });

    const saveA = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.filter(({ url }) => url === "/api/settings").length, 1);
    assert.equal(useSettingsStore.getState().isSavingConfig, true);

    useSettingsStore.getState().cancelConfigOperations();
    const initialize = useSettingsStore.getState().initialize();
    await new Promise((resolve) => queueMicrotask(resolve));

    assert.equal(
      calls.filter(({ url, method }) => url === "/api/cpa/pools" && method === "GET").length,
      0,
      "re-entry must wait before starting any initialization query",
    );
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(
      calls.filter(({ url, method }) => url === "/api/settings" && method === "GET").length,
      0,
      "re-entry must not read a stale server snapshot while the old write is pending",
    );

    useSettingsStore.setState({
      config: { proxy: "", base_url: "second.example.test" },
    });
    const saveB = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(
      calls.filter(({ url, method }) => url === "/api/settings" && method === "POST").length,
      1,
      "the second save must wait for the first server write",
    );
    assert.equal(useSettingsStore.getState().isSavingConfig, true);

    const saveARequest = pending.find((item) => item.config.url === "/api/settings" && item.config.method === "POST");
    saveARequest.resolve({ data: { config: { proxy: "", base_url: "first-response.example.test" } } });
    await saveA;
    assert.equal(useSettingsStore.getState().config.base_url, "first-response.example.test");

    const configLoad = pending.find((item) => item.config.url === "/api/settings" && item.config.method === "GET");
    assert.ok(configLoad, "re-entry may read config only after the old write settles");
    const poolLoad = pending.find((item) => item.config.url === "/api/cpa/pools" && item.config.method === "GET");
    assert.ok(poolLoad, "re-entry may load pools only after the old write settles");
    configLoad.resolve({ data: { config: { proxy: "", base_url: "reloaded.example.test" } } });
    poolLoad.resolve({ data: { pools: [] } });
    await initialize;
    await saveB;
    assert.equal(useSettingsStore.getState().config.base_url, "reloaded.example.test");
    assert.equal(useSettingsStore.getState().isSavingConfig, false);

    const saveC = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.filter(({ url, method }) => url === "/api/settings" && method === "POST").length, 2);
    const saveCRequest = pending.find((item) => item.config.url === "/api/settings" && item.config.method === "POST" && item !== saveARequest);
    saveCRequest.resolve({ data: { config: { proxy: "", base_url: "second.example.test" } } });
    await saveC;
  } finally {
    request.request = originalRequest;
    useSettingsStore.getState().cancelConfigOperations();
  }
});

test("cleanup prevents an old initialization from claiming the post-write config read", async () => {
  const originalRequest = request.request;
  const originalLoadConfig = useSettingsStore.getState().loadConfig;
  const originalToastError = toast.error;
  const originalToastSuccess = toast.success;
  const calls = [];
  const pending = [];
  const loadConfigInvocations = [];
  const toastCalls = [];
  const loadingTransitions = [];
  const unsubscribe = useSettingsStore.subscribe((state, previous) => {
    if (state.isLoadingConfig !== previous.isLoadingConfig) {
      loadingTransitions.push(state.isLoadingConfig);
    }
  });
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve, reject) => pending.push({ config, resolve, reject }));
  };
  toast.error = (...args) => toastCalls.push(["error", ...args]);
  toast.success = (...args) => toastCalls.push(["success", ...args]);
  useSettingsStore.setState({
    loadConfig: (...args) => {
      loadConfigInvocations.push(args);
      return originalLoadConfig(...args);
    },
  });

  try {
    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.setState({
      config: { proxy: "", base_url: "before.example.test" },
      isLoadingConfig: false,
      isSavingConfig: false,
    });

    const save = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.filter(({ url, method }) => url === "/api/settings" && method === "POST").length, 1);

    const oldInitialize = useSettingsStore.getState().initialize();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.filter(({ url, method }) => url === "/api/settings" && method === "GET").length, 0);

    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.getState().cancelConfigOperations();
    const newInitialize = useSettingsStore.getState().initialize();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.filter(({ url, method }) => url === "/api/settings" && method === "GET").length, 0);

    const saveRequest = pending.find(({ config }) => config.url === "/api/settings" && config.method === "POST");
    saveRequest.resolve({ data: { config: { proxy: "", base_url: "write-success.example.test" } } });
    await new Promise((resolve) => setTimeout(resolve, 0));

    const configGets = pending.filter(({ config }) => config.url === "/api/settings" && config.method === "GET");
    assert.equal(configGets.length, 1, "only the re-entered initialization may claim the post-write read");
    assert.equal(loadConfigInvocations.length, 1, "the cancelled initialization must not invoke loadConfig");
    const poolGets = pending.filter(({ config }) => config.url === "/api/cpa/pools" && config.method === "GET");
    assert.equal(poolGets.length, 1, "only the re-entered initialization may load pools");
    configGets[0].resolve({ data: { config: { proxy: "", base_url: "reloaded.example.test" } } });
    poolGets[0].resolve({ data: { pools: [] } });

    await Promise.all([save, oldInitialize, newInitialize]);
    assert.equal(useSettingsStore.getState().config.base_url, "reloaded.example.test");
    assert.equal(useSettingsStore.getState().isLoadingConfig, false);
    assert.deepEqual(
      loadingTransitions,
      [true, false],
      "only the current initialization may publish config loading transitions",
    );
    assert.deepEqual(toastCalls, [], "the cancelled initialization must not emit a toast");
  } finally {
    unsubscribe();
    request.request = originalRequest;
    toast.error = originalToastError;
    toast.success = originalToastSuccess;
    useSettingsStore.setState({ loadConfig: originalLoadConfig });
    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.getState().cancelConfigOperations();
  }
});

test("a failed write still lets only the current initialization reload authoritative config", async () => {
  const originalRequest = request.request;
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve, reject) => pending.push({ config, resolve, reject }));
  };

  try {
    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.setState({
      config: { proxy: "", base_url: "draft.example.test" },
      isLoadingConfig: false,
      isSavingConfig: false,
    });

    const save = useSettingsStore.getState().saveConfig();
    await new Promise((resolve) => queueMicrotask(resolve));
    const oldInitialize = useSettingsStore.getState().initialize();
    await new Promise((resolve) => queueMicrotask(resolve));
    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.getState().cancelConfigOperations();
    const newInitialize = useSettingsStore.getState().initialize();

    const saveRequest = pending.find(({ config }) => config.url === "/api/settings" && config.method === "POST");
    saveRequest.reject(new Error("write failed"));
    await new Promise((resolve) => setTimeout(resolve, 0));

    const configGets = pending.filter(({ config }) => config.url === "/api/settings" && config.method === "GET");
    assert.equal(configGets.length, 1);
    const poolGets = pending.filter(({ config }) => config.url === "/api/cpa/pools" && config.method === "GET");
    assert.equal(poolGets.length, 1);
    configGets[0].resolve({ data: { config: { proxy: "", base_url: "server-after-failure.example.test" } } });
    poolGets[0].resolve({ data: { pools: [] } });

    await Promise.all([save, oldInitialize, newInitialize]);
    assert.equal(useSettingsStore.getState().config.base_url, "server-after-failure.example.test");
    assert.equal(useSettingsStore.getState().isLoadingConfig, false);
  } finally {
    request.request = originalRequest;
    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.getState().cancelConfigOperations();
  }
});

test("cleanup prevents initialize from starting a later backup load", async () => {
  const originalRequest = request.request;
  const pending = [];
  request.request = (config) => new Promise((resolve) => pending.push({ config, resolve }));

  try {
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.getState().cancelPoolOperations();
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.setState({
      config: {
        proxy: "",
        backup: {
          account_id: "account",
          access_key_id: "access",
          secret_access_key: "secret",
          bucket: "bucket",
        },
      },
      isLoadingConfig: false,
      isLoadingPools: false,
      isLoadingBackups: false,
    });

    const initialize = useSettingsStore.getState().initialize();
    await new Promise((resolve) => queueMicrotask(resolve));
    const configRequest = pending.find((item) => item.config.url === "/api/settings");
    const poolRequest = pending.find((item) => item.config.url === "/api/cpa/pools");
    assert.ok(configRequest);
    assert.ok(poolRequest);

    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.getState().cancelPoolOperations();
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.getState().cancelInitialization();
    configRequest.resolve({ data: { config: { proxy: "", backup: { account_id: "account", access_key_id: "access", secret_access_key: "secret", bucket: "bucket" } } } });
    poolRequest.resolve({ data: { pools: [] } });

    await new Promise((resolve) => setTimeout(resolve, 0));
    const backupRequest = pending.find((item) => item.config.url === "/api/backups");
    if (backupRequest) {
      backupRequest.resolve({ data: { items: [], state: { running: false }, settings: {} } });
    }
    await initialize;
    assert.equal(backupRequest, undefined, "cleanup must stop initialize before its backup phase");
  } finally {
    request.request = originalRequest;
    useSettingsStore.getState().cancelInitialization();
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.getState().cancelPoolOperations();
    useSettingsStore.getState().cancelBackupOperations();
  }
});

test("CPA store admits one save, rejects the concurrent save, then admits the next one", async () => {
  const originalRequest = request.request;
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve) => pending.push({ resolve }));
  };

  try {
    useSettingsStore.setState({
      editingPool: null,
      formName: "first",
      formBaseUrl: "https://first.example.test",
      formSecretKey: "first-secret",
      isSavingPool: false,
      pools: [],
    });

    const firstSave = useSettingsStore.getState().savePool();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 1);
    assert.equal(calls[0].method, "POST");

    useSettingsStore.getState().setFormName("second");
    useSettingsStore.getState().setFormBaseUrl("https://second.example.test");
    useSettingsStore.getState().setFormSecretKey("second-secret");
    const rejectedSave = useSettingsStore.getState().savePool();
    await rejectedSave;
    assert.equal(calls.length, 1, "the rejected mutation must not start another HTTP write");
    assert.equal(useSettingsStore.getState().isSavingPool, true);

    pending.shift().resolve({
      data: { pool: pool("first", "first"), pools: [pool("first", "first")] },
    });
    await firstSave;
    assert.equal(useSettingsStore.getState().isSavingPool, false);

    const thirdSave = useSettingsStore.getState().savePool();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 2, "a new mutation is admitted after the first owner finishes");

    pending.shift().resolve({
      data: { pool: pool("second", "second"), pools: [pool("second", "second")] },
    });
    await thirdSave;
    assert.equal(useSettingsStore.getState().isSavingPool, false);
    assert.deepEqual(useSettingsStore.getState().pools.map(({ id }) => id), ["second"]);
  } finally {
    request.request = originalRequest;
  }
});

test("CPA store does not open a deleted pool from a late browse response", async () => {
  const originalRequest = request.request;
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve) => pending.push({ resolve }));
  };

  try {
    const targetPool = pool("stale", "stale");
    useSettingsStore.setState({
      pools: [targetPool],
      browserOpen: false,
      browserPool: null,
      remoteFiles: [],
      loadingFilesId: null,
      deletingId: null,
    });

    const browse = useSettingsStore.getState().browseFiles(targetPool);
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 1);
    assert.equal(calls[0].method, "GET");

    const deletion = useSettingsStore.getState().deletePool(targetPool);
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 2);
    assert.equal(calls[1].method, "DELETE");

    pending[1].resolve({ data: { pools: [] } });
    await deletion;
    assert.deepEqual(useSettingsStore.getState().pools, []);

    pending[0].resolve({ data: { files: [{ name: "stale.json", email: "stale@example.test" }] } });
    await browse;
    assert.equal(useSettingsStore.getState().browserOpen, false);
    assert.equal(useSettingsStore.getState().browserPool, null);
    assert.deepEqual(useSettingsStore.getState().remoteFiles, []);
  } finally {
    request.request = originalRequest;
  }
});

test("CPA list polling does not invalidate an unrelated in-flight file browse", async () => {
  const originalRequest = request.request;
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve) => pending.push({ resolve }));
  };

  try {
    const targetPool = pool("listed", "listed");
    useSettingsStore.setState({
      pools: [targetPool],
      browserOpen: false,
      browserPool: null,
      remoteFiles: [],
      loadingFilesId: null,
      isLoadingPools: false,
    });

    const browse = useSettingsStore.getState().browseFiles(targetPool);
    await new Promise((resolve) => queueMicrotask(resolve));
    const listLoad = useSettingsStore.getState().loadPools(true);
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 2);

    pending[1].resolve({ data: { pools: [targetPool] } });
    await listLoad;
    pending[0].resolve({ data: { files: [{ name: "listed.json", email: "listed@example.test" }] } });
    await browse;

    assert.equal(useSettingsStore.getState().browserOpen, true);
    assert.equal(useSettingsStore.getState().browserPool?.id, "listed");
  } finally {
    request.request = originalRequest;
  }
});

test("CPA browse drops a late response when the current list no longer contains the pool", async () => {
  const originalRequest = request.request;
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve) => pending.push({ resolve }));
  };

  try {
    const targetPool = pool("removed-by-list", "removed-by-list");
    useSettingsStore.setState({
      pools: [targetPool],
      browserOpen: false,
      browserPool: null,
      remoteFiles: [],
      loadingFilesId: null,
      isLoadingPools: false,
    });

    const browse = useSettingsStore.getState().browseFiles(targetPool);
    await new Promise((resolve) => queueMicrotask(resolve));
    const listLoad = useSettingsStore.getState().loadPools(true);
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 2);

    pending[1].resolve({ data: { pools: [] } });
    await listLoad;
    pending[0].resolve({ data: { files: [{ name: "removed.json", email: "removed@example.test" }] } });
    await browse;

    assert.equal(useSettingsStore.getState().browserOpen, false);
    assert.equal(useSettingsStore.getState().browserPool, null);
    assert.deepEqual(useSettingsStore.getState().remoteFiles, []);
  } finally {
    request.request = originalRequest;
  }
});

test("CPA silent poll drops a response invalidated by effect cleanup", async () => {
  const originalRequest = request.request;
  const pending = [];
  request.request = () => new Promise((resolve) => pending.push(resolve));

  try {
    const existing = pool("cleanup-old", "cleanup-old");
    useSettingsStore.getState().cancelPoolOperations();
    useSettingsStore.setState({ pools: [existing], isLoadingPools: false });

    const load = useSettingsStore.getState().loadPools(true);
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(pending.length, 1);

    useSettingsStore.getState().invalidatePoolLoads();
    pending[0]({ data: { pools: [pool("cleanup-late", "cleanup-late")] } });
    await load;

    assert.deepEqual(useSettingsStore.getState().pools.map(({ id }) => id), ["cleanup-old"]);
  } finally {
    request.request = originalRequest;
    useSettingsStore.getState().cancelPoolOperations();
  }
});

test("CPA silent poll drops a response superseded by a pool mutation", async () => {
  const originalRequest = request.request;
  const pending = [];
  request.request = (config) => new Promise((resolve) => pending.push({ config, resolve }));

  try {
    const existing = pool("mutation-old", "mutation-old");
    useSettingsStore.getState().cancelPoolOperations();
    useSettingsStore.setState({
      pools: [existing],
      isLoadingPools: false,
      editingPool: null,
      formName: "created",
      formBaseUrl: "https://created.example.test",
      formSecretKey: "created-secret",
    });

    const load = useSettingsStore.getState().loadPools(true);
    await new Promise((resolve) => queueMicrotask(resolve));
    const save = useSettingsStore.getState().savePool();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(pending.length, 2);
    assert.equal(pending[1].config.method, "POST");

    pending[0].resolve({ data: { pools: [pool("mutation-late", "mutation-late")] } });
    await load;
    assert.deepEqual(useSettingsStore.getState().pools.map(({ id }) => id), ["mutation-old"]);

    pending[1].resolve({
      data: { pool: pool("mutation-new", "mutation-new"), pools: [pool("mutation-new", "mutation-new")] },
    });
    await save;
    assert.deepEqual(useSettingsStore.getState().pools.map(({ id }) => id), ["mutation-new"]);
  } finally {
    request.request = originalRequest;
    useSettingsStore.getState().cancelPoolOperations();
  }
});

test("backup silent poll drops a response invalidated by effect cleanup", async () => {
  const originalRequest = request.request;
  const pending = [];
  request.request = () => new Promise((resolve) => pending.push(resolve));

  try {
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.setState({
      backups: [{ key: "cleanup-old", size: 1, encrypted: false }],
      backupState: { running: false },
      isLoadingBackups: false,
    });

    const load = useSettingsStore.getState().loadBackups(true);
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(pending.length, 1);

    useSettingsStore.getState().invalidateBackupLoads();
    pending[0]({ data: { items: [{ key: "cleanup-late", size: 2, encrypted: false }], state: { running: false } } });
    await load;

    assert.deepEqual(useSettingsStore.getState().backups.map(({ key }) => key), ["cleanup-old"]);
  } finally {
    request.request = originalRequest;
    useSettingsStore.getState().cancelBackupOperations();
  }
});

test("backup silent poll drops a response superseded by a backup mutation", async () => {
  const originalRequest = request.request;
  const pending = [];
  request.request = (config) => new Promise((resolve) => pending.push({ config, resolve }));

  try {
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.setState({
      backups: [{ key: "mutation-old", size: 1, encrypted: false }],
      backupState: { running: false },
      isLoadingBackups: false,
      config: null,
      isLoadingConfig: false,
      isSavingConfig: false,
    });

    const load = useSettingsStore.getState().loadBackups(true);
    await new Promise((resolve) => queueMicrotask(resolve));
    const remove = useSettingsStore.getState().removeBackup("mutation-old");
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(pending.length, 2);
    assert.equal(pending[1].config.method, "POST");

    pending[0].resolve({ data: { items: [{ key: "mutation-late", size: 2, encrypted: false }], state: { running: false } } });
    await load;
    assert.deepEqual(useSettingsStore.getState().backups.map(({ key }) => key), ["mutation-old"]);

    pending[1].resolve({ data: { items: [], state: { running: false } } });
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(pending.length, 3);
    pending[2].resolve({ data: { items: [], state: { running: false } } });
    await remove;
    assert.deepEqual(useSettingsStore.getState().backups, []);
  } finally {
    request.request = originalRequest;
    useSettingsStore.getState().cancelBackupOperations();
  }
});

test("backup connection test blocks a concurrent backup mutation", async () => {
  const originalRequest = request.request;
  const originalWindow = globalThis.window;
  globalThis.window = { dispatchEvent() {} };
  const calls = [];
  const pending = [];
  const transitions = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve, reject) => pending.push({ config, resolve, reject }));
  };
  const unsubscribe = useSettingsStore.subscribe((state) => {
    transitions.push({ testing: state.isTestingBackup, running: state.isRunningBackup });
  });

  try {
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.setState({
      config: { proxy: "", base_url: "https://backup.example.test" },
      isTestingBackup: false,
      isRunningBackup: false,
      deletingBackupKey: null,
    });

    const testing = useSettingsStore.getState().testBackup();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, "/api/settings");

    const concurrentRun = useSettingsStore.getState().runBackup();
    await concurrentRun;
    assert.equal(calls.length, 1, "a backup run must not write while connection test owns the slot");
    assert.equal(
      transitions.some(({ running }) => running),
      false,
      "a rejected backup run must not enter the running state",
    );

    pending[0].resolve({ data: { config: { proxy: "", base_url: "https://backup.example.test" } } });
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(calls[1].url, "/api/backup/test");
    pending[1].resolve({ data: { result: { ok: true, status: 200 } } });
    await testing;
    assert.equal(useSettingsStore.getState().isTestingBackup, false);
  } finally {
    unsubscribe();
    request.request = originalRequest;
    globalThis.window = originalWindow;
    useSettingsStore.getState().cancelBackupOperations();
  }
});

test("backup write admission survives page cleanup until the server write settles", async () => {
  const originalRequest = request.request;
  const originalWindow = globalThis.window;
  globalThis.window = { dispatchEvent() {} };
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve, reject) => pending.push({ config, resolve, reject }));
  };

  const responseFor = (config) => {
    if (config.url === "/api/settings") {
      return { data: { config: { proxy: "", base_url: "https://backup.example.test" } } };
    }
    if (config.url === "/api/backups/run") {
      return { data: { result: { key: "backup-key", size: 1, encrypted: false } } };
    }
    return { data: { items: [], state: { running: false } } };
  };
  const settleAll = async () => {
    let index = 0;
    while (index < pending.length) {
      const item = pending[index++];
      item.resolve(responseFor(item.config));
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  };

  try {
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.setState({
      config: { proxy: "", base_url: "https://backup.example.test" },
      isRunningBackup: false,
      isSavingConfig: false,
      isLoadingConfig: false,
    });

    const firstRun = useSettingsStore.getState().runBackup();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls[0].url, "/api/settings");
    pending[0].resolve(responseFor(pending[0].config));
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(calls[1].url, "/api/backups/run");

    useSettingsStore.getState().cancelBackupOperations();
    const secondRun = useSettingsStore.getState().runBackup();
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(
      calls.filter(({ url, method }) => url === "/api/settings" && method === "POST").length,
      1,
      "a remounted page must not start a second settings write while the first backup is pending",
    );
    assert.equal(
      calls.filter(({ url, method }) => url === "/api/backups/run" && method === "POST").length,
      1,
      "a remounted page must not start a second backup POST while the first is pending",
    );

    await settleAll();
    await Promise.all([firstRun, secondRun]);
  } finally {
    request.request = originalRequest;
    globalThis.window = originalWindow;
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.getState().cancelConfigOperations();
  }
});

test("backup cleanup drops an old waiting load but a new load waits for the writer", async () => {
  const originalRequest = request.request;
  const originalWindow = globalThis.window;
  globalThis.window = { dispatchEvent() {} };
  const calls = [];
  const pending = [];
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve, reject) => pending.push({ config, resolve, reject }));
  };
  const responseFor = (config) => {
    if (config.url === "/api/settings") {
      return { data: { config: { proxy: "", base_url: "https://backup.example.test" } } };
    }
    if (config.url === "/api/backups/run") {
      return { data: { result: { key: "backup-key", size: 1, encrypted: false } } };
    }
    return { data: { items: [{ key: "authoritative", size: 1, encrypted: false }], state: { running: false } } };
  };

  try {
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.setState({
      config: { proxy: "", base_url: "https://backup.example.test" },
      backups: [{ key: "old", size: 1, encrypted: false }],
      isLoadingBackups: false,
      isRunningBackup: false,
      isSavingConfig: false,
      isLoadingConfig: false,
    });

    const run = useSettingsStore.getState().runBackup();
    await new Promise((resolve) => queueMicrotask(resolve));
    pending[0].resolve(responseFor(pending[0].config));
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(calls[1].url, "/api/backups/run");

    const oldLoad = useSettingsStore.getState().loadBackups();
    useSettingsStore.getState().cancelBackupOperations();
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(calls.filter(({ url }) => url === "/api/backups").length, 0);

    pending[1].resolve(responseFor(pending[1].config));
    await run;
    await oldLoad;
    assert.equal(calls.filter(({ url }) => url === "/api/backups").length, 0);

    const newLoad = useSettingsStore.getState().loadBackups();
    await new Promise((resolve) => queueMicrotask(resolve));
    const listRequest = pending.find(({ config }) => config.url === "/api/backups");
    assert.ok(listRequest, "the new page load must start after the writer settles");
    listRequest.resolve(responseFor(listRequest.config));
    await newLoad;
    assert.deepEqual(useSettingsStore.getState().backups.map(({ key }) => key), ["authoritative"]);
  } finally {
    request.request = originalRequest;
    globalThis.window = originalWindow;
    useSettingsStore.getState().cancelBackupOperations();
    useSettingsStore.getState().cancelConfigOperations();
  }
});

test("WebDAV test stops publishing after settings page cleanup", async () => {
  const originalRequest = request.request;
  const originalWindow = globalThis.window;
  const originalToastSuccess = toast.success;
  const originalToastError = toast.error;
  const calls = [];
  const pending = [];
  const toasts = [];
  globalThis.window = { dispatchEvent() {} };
  toast.success = (...args) => toasts.push(["success", ...args]);
  toast.error = (...args) => toasts.push(["error", ...args]);
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve, reject) => pending.push({ config, resolve, reject }));
  };

  try {
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.getState().cancelImageStorageOperations();
    useSettingsStore.setState({
      config: {
        proxy: "",
        image_storage: { enabled: true, mode: "webdav", webdav_url: "https://dav.example.test" },
      },
      isTestingImageStorage: false,
      isSyncingImageStorage: false,
      isSavingConfig: false,
      isLoadingConfig: false,
    });

    const testing = useSettingsStore.getState().testImageStorage();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls[0].url, "/api/settings");
    pending[0].resolve({
      data: {
        config: {
          proxy: "",
          image_storage: { enabled: true, mode: "webdav", webdav_url: "https://dav.example.test" },
        },
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(calls[1].url, "/api/image-storage/test");

    toasts.length = 0;
    useSettingsStore.getState().cancelImageStorageOperations();
    pending[1].resolve({ data: { result: { ok: true, status: 200 } } });
    await testing;

    assert.deepEqual(toasts, [], "a response after settings cleanup must not toast");
    assert.equal(useSettingsStore.getState().isTestingImageStorage, false);
  } finally {
    request.request = originalRequest;
    toast.success = originalToastSuccess;
    toast.error = originalToastError;
    globalThis.window = originalWindow;
    useSettingsStore.getState().cancelImageStorageOperations?.();
    useSettingsStore.getState().cancelConfigOperations();
  }
});

test("WebDAV test and full sync share one operation admission", async () => {
  const originalRequest = request.request;
  const originalWindow = globalThis.window;
  const originalToastSuccess = toast.success;
  const originalToastError = toast.error;
  const calls = [];
  const pending = [];
  const toasts = [];
  globalThis.window = { dispatchEvent() {} };
  toast.success = (...args) => toasts.push(["success", ...args]);
  toast.error = (...args) => toasts.push(["error", ...args]);
  request.request = (config) => {
    calls.push(config);
    return new Promise((resolve, reject) => pending.push({ config, resolve, reject }));
  };

  try {
    useSettingsStore.getState().cancelConfigOperations();
    useSettingsStore.getState().cancelImageStorageOperations();
    useSettingsStore.setState({
      config: {
        proxy: "",
        image_storage: { enabled: true, mode: "both", webdav_url: "https://dav.example.test" },
      },
      isTestingImageStorage: false,
      isSyncingImageStorage: false,
      isSavingConfig: false,
      isLoadingConfig: false,
    });

    const testing = useSettingsStore.getState().testImageStorage();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(calls.length, 1);
    const sync = useSettingsStore.getState().syncImagesToWebDAV();
    await sync;
    assert.equal(calls.length, 1, "a second WebDAV operation must not start while the first owns the slot");
    assert.equal(useSettingsStore.getState().isSyncingImageStorage, false);

    pending[0].resolve({
      data: {
        config: {
          proxy: "",
          image_storage: { enabled: true, mode: "both", webdav_url: "https://dav.example.test" },
        },
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(calls[1].url, "/api/image-storage/test");
    pending[1].resolve({ data: { result: { ok: true, status: 200 } } });
    await testing;
    assert.equal(useSettingsStore.getState().isTestingImageStorage, false);
  } finally {
    request.request = originalRequest;
    toast.success = originalToastSuccess;
    toast.error = originalToastError;
    globalThis.window = originalWindow;
    useSettingsStore.getState().cancelImageStorageOperations();
    useSettingsStore.getState().cancelConfigOperations();
  }
});
