# Rust/Python Parity Audit

审计基线：`.local/public-minimal` 中的 Python 实现。审计日期：2026-09-22。
Rust 版本以当前 `main` 分支为准。这里的“已对齐”表示已经核对了输入校验、路由选择、上游请求、结果投影和持久化边界；“部分对齐”表示接口存在，但仍有可观察的行为差异；“未实现”表示 Rust 明确返回 `unsupported_capability` 或只保存配置而没有运行时逻辑。

## 已对齐的主链路

### 公共 API

- `/v1/models`：Rust 的 `AccountTypeCatalog` 会刷新公开模型目录、按账号类型合并模型，并将图片模型作为 `ModelProvenance::Image` 投影；Codex 模型不会混入网页模型列表。
- `/v1/chat/completions`：已覆盖普通文本、流式 SSE、Codex Responses 账号、网页账号、网页搜索、图片模型和输入图片；上游失败会在账号候选之间有限切换。
- `/v1/responses`：已覆盖普通 Responses、流式投影、函数工具、网页搜索工具、图片生成工具，以及 Codex Responses 原生事件校验和投影。
- `/v1/messages`：已覆盖 `Authorization`、`x-api-key`、`anthropic-version` 校验，Anthropic 文本/工具/搜索请求会转换到 Chat/Responses，再转换回 Anthropic 响应和 SSE。
- `/v1/images/generations`、`/v1/images/edits`：已覆盖 JSON、multipart、data URL、远程图片引用、mask 合成、输出格式/压缩、网页图片链路和 Codex 图片链路。
- `/v1/search`、`/v1/ppt/generations`、`/v1/psd/generations`、`/v1/editable-file-tasks`：均有 Rust 路由和对应上游/后台任务实现。

### 账号池和模型目录

- 账号快照会做规范化、文件版本校验、原子替换和并发重载；不可用状态、`invalid_count` 和模型来源会参与请求选号。
- 图片账号在真正发起图片请求前按账号单独刷新 `/backend-api/me`、conversation init、账号检查和图片能力/额度；只有验证成功的账号进入图片请求。
- 网页图片模型 `gpt-image-2` 使用配置的 `default_upstream_model_name`；其它网页图片模型使用 `auto`，与 Python `OpenAIBackendAPI._image_model_settings` 一致。
- ccLoad 导入会校验频道、OAuth 类型和 access token、计划类型及账号 ID；频道模型浏览和导入都只接受当前 access token 通过两个 canonical Web endpoint 刷新的目录与同一 token 的图片 capability/quota，Codex 模型不会误当作网页模型。

### 管理页面和持久化

- 用户密钥接口只返回 `role=user`，账号更新保留 `proxy` 字段；账号、模型、图片、标签、日志、备份、代理、CPA、Sub2API、ccLoad 的主要 CRUD 路由均已存在。
- CPA/Sub2API/ccLoad 导入均使用后台任务、幂等 job id、错误列表和账号快照合并；账号快照只保留 access token 边界，避免将 refresh/id token 重新暴露给 Rust 运行时。
- PPT/PSD 后台任务已具备任务恢复、账号类型筛选、文件下载能力哈希和受限文件读取。
- 图片任务已覆盖提交幂等、按用户隔离、`queued/running/success/error`、结果/usage/耗时、JSON/multipart 编辑输入，以及超时任务的 `resume-poll` 恢复轮询。
- 账号刷新在模型目录暂时不可用时保留最后一次成功的 Web/Codex 模型目录；quota 归零时只清理 image 模型，避免管理页面模型列表被瞬时刷新失败清空。

## 已确认的行为差异

这些不是推测，而是逐一对照 Python 路由或服务实现后确认的差异。

### P0：功能缺失或会造成误判

1. **OAuth 和密码重新登录未实现**

   Rust `src/lib.rs` 中 `/api/accounts/re-login`、re-login progress、`/api/accounts/oauth/start`、`/api/accounts/oauth/finish` 仍由 `access_token_only_disabled` 处理，返回 `unsupported_capability`。Python 对应逻辑在 `api/accounts.py`、`services/account_service.py` 和 `services/oauth_login_service.py`，包含密码登录、验证码、PKCE、授权码兑换和三件套落盘。

2. **账号导出不是 Python 的 JSON/ZIP 导出**

   Python 导出要求 `access_token + refresh_token + id_token`，JSON 单账号直接返回对象，多账号返回数组，ZIP 每账号一个 JSON 文件。Rust `api_accounts_export` 当前导出 `{"items": [...]}` 原始记录，不支持 `format=zip`，且 access-token-only 规范化会丢弃 refresh/id token。因此 Rust 不能声称与 Python 导出格式兼容。

3. **FlareSolverr 的自动覆盖仍有少数路径待核验**

   Rust 已实现 `ClearanceStore`、FlareSolverr 刷新、过期时间、single-flight、管理端测试，并已接入账号刷新、模型目录、Web conversation、Web/Codex 图片资源、搜索、editable 和 Codex Responses；普通 Chat 的 bootstrap/requirements 与线上运行行为仍需逐条核验。

4. **账号级 proxy 仍需逐路径核验**

   Rust 已为账号 lease 创建独立上游 client，并按账号代理优先级选择；模型目录、Web conversation、图片资源、搜索、editable 和 Codex 路径已接入，仍需运行时核验管理导入和边界重试路径。

### P1：配置已能保存，但运行时没有等价行为

1. `global_system_prompt`：Rust 已接入 Chat 和 Responses；仍需核对 Anthropic、搜索、图片和 editable 的消息顺序是否与 Python 完全一致。
2. `sensitive_words` 和 `ai_review`：Rust 已接入敏感词拦截、审核文本提取、base64 替换、100k 截断和审核结果处理；仍需补齐配置异常、日志和所有 API 入口的行为测试。
3. `chat_completion_cache`：Rust 已实现非流式 TTL cache、流式帧 replay、in-flight dedupe、消息规范化及递归 JSON key 排序；仍需补并发边界测试。
4. `auto_remove_invalid_accounts`、`auto_remove_rate_limited_accounts`：Rust 已实现刷新写回时的确认失效删除、刚变为限流时删除、native Codex 401 记录和 Web 图片 401 记录；仍需补边界重试测试。
5. `image_remove_conversation_after_result`、`image_remove_conversation_always`：Rust 已实现异步 PATCH 隐藏 conversation；仍需核对失败、超时、部分结果和多图路径。
6. refresh token keepalive、过期 access token 自动刷新和 `auto_relogin_after_refresh`：Rust 的 access-token-only 边界不会保存或刷新 Python 维护的 refresh/id token 三件套。

### P2：导入实现可用但不完全相同

1. CPA：Rust `execute_cpa_import` 已改为最多 16 个并发下载，并在每个文件完成后更新 job 进度；仍需补远程服务模拟测试。
2. Sub2API：Rust 管理端已按每页 200 条循环读取账号/分组，最多 5000 条；导入接口仍需补远程分页/缺失凭据模拟测试。
3. ccLoad：Rust 已实现登录、频道 editor 中的 OAuth access token 提取、逐频道 canonical 目录刷新和逐频道进度；不会从旧账号快照、其它同套餐频道或 editor 模型字段借用目录。这是当前三种导入中最接近 Python/实际页面需求的一条，但它是 Rust 扩展逻辑，Python baseline 中没有同名服务文件可直接逐行对照。

## 需要继续处理的顺序

1. 完成 clearance、账号 proxy、Web 图片 401 和搜索/editable/Codex 的逐路径验证。
2. 补齐内容审核、全局 system prompt、缓存和图片清理的行为测试。
3. 为 CPA/Sub2API 导入补远程服务模拟测试，并继续核对 ccLoad 的并发、分页和进度差异。
4. 最后决定是否突破 access-token-only 安全边界，移植 OAuth、密码重登、refresh token keepalive 和 Python 完整导出；如果不突破，接口必须继续明确返回 unsupported，而不能返回看似成功的数据。

## v1.7 之后功能清单（暂不实现）

- 账号级官方模型目录和按账号权限路由（`641e0dea`）。
- 默认上游模型名称配置（`79199cbc`）。
- 默认思考强度配置及模型后缀覆盖（`62dd0efb`）。
- 无图片结果时移除本地 conversation（`d83cef80`）。
- 过滤内部 assistant tool 消息（`6675eb33`）。
- 数据库增量同步（`ca26e54f`）。
- 图片 SSE 硬超时（`3aafaf76`）。
- 保留代码/命令标点前空格的清洗修复（`df2398fa`）。
- v1.8.0 版本与 changelog 更新（`e55aef28`）。

## 当前验证记录

- 已验证健康、模型列表、文本 Chat、Anthropic Messages、搜索、Chat 图片、Responses `image_generation` 和 `/v1/images/generations` 在线返回成功。
- Rust 单元测试和 CI 已覆盖账号快照、模型目录、native Chat/Responses/Anthropic、图片输入/mask/下载、导入快照和备份并发边界。
- 本机 `cargo check` 在 Windows 环境被 `btls-sys` 构建依赖缺少 CMake 阻断；`cargo fmt --all -- --check` 已通过，完整编译以 GitHub Actions 为准。
