# Web capability transport release

**Goal:** 发布已验证的来源能力隔离与 transport identity fence，避免 Codex/未知来源账号进入 ChatGPT Web 端点，并保持目录按账号类型选择单一兼容代表。
**Why planning is required:** 这是需要提交、推送、构建并更新 ai-arm 的生产发布操作。
**Acceptance:** 目标 commit 的离线门禁和 Actions 多架构镜像成功；ai-arm 仅替换 chatgpt2api，健康、模型接口、唯一容器和回滚证据满足门槛。

### Outcome 1: Scope and verification
- Work: 仅纳入本批 `AccountService` 来源能力校验、按归一化账号类型选择单一代表、匿名目录一次、认证代表访问其兼容目录端点、Web transport/bootstrap 防线及相关 caller 测试；保留 Rust、Web、storage 和其他用户改动不变。
- Verify: `pytest` 离线全量、focused caller/transport 回归、`py_compile`、`git diff --check`。

### Outcome 2: Repository delivery
- Work: 审阅暂存 diff 后提交并推送 `main`；只等待该 commit 的 GitHub Actions amd64/arm64 publish 成功，并核对 GHCR `latest` manifest/digest。
- Verify: commit SHA、Actions run/head SHA、OCI 多架构 digest 可相互对应。

### Outcome 3: ai-arm rollout
- Work: 备份并校验 Compose，保持 `:latest`，只 pull/recreate `chatgpt2api`；失败立即恢复部署前镜像/Compose。
- Verify: 目标 RepoDigest、`healthy`、RestartCount=0、`/health` 与认证 `/v1/models` 200；chatgpt2api 仅一个应用容器，helper/SSH=0，其他容器 ID/StartedAt 不变。

### Stop conditions
- Actions 非目标 SHA、manifest 缺 amd64/arm64、容器不 healthy、RestartCount 非 0、模型接口非 200，或其他服务发生变化时停止并按备份回滚；不输出凭据。
