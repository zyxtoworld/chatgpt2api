# Rust 生产切换能力矩阵

更新时间：2026-08-23

这份矩阵记录 Rust 二进制进入生产 Docker、主分支 `:latest` 和 ai-arm 的硬门。当前 Dockerfile/发布 workflow 已切到 Rust release 入口；本地程序门已经闭合，但同 SHA 的 Actions/GHCR、远程运行态和真实上游验收仍必须独立落盘，不能把本地 fake upstream 当成部署证据。

### 当前 Rust native 启动合同（仅黑盒/验收夹具）

Rust Docker/Compose 必须显式提供下列配置，并在绑定监听端口前完成配置与快照加载：

| 变量 | 合同 |
| --- | --- |
| `RUST_UPSTREAM_PROTOCOL` | 必须写 `chatgpt`（或 `native`）；省略时 Rust 默认 OpenAI-compatible，不能把默认值当 native 部署证明。 |
| `RUST_UPSTREAM_BASE_URL` | 必须是容器可达的 ChatGPT 上游根 URL；所有 bootstrap、模型目录、sentinel、conversation/Codex 请求从它派生。 |
| `CODEX_CLIENT_VERSION` | 必须是三段式 Codex CLI 版本（当前验收值 `0.147.0`）；Codex Responses/原生 WebSocket 请求缺失或非法时必须启动前失败或明确不可用，不能伪造请求。 |
| `RUST_AUTH_KEY` | 下游 Bearer 密钥；不能把客户端密钥转发给上游。 |
| `RUST_DATA_DIR` | `/app/data` 或等价持久卷目录；`RUST_ACCOUNTS_PATH` 至少提供隔离的账号快照，路径在进程初始化时冻结为绝对路径。 |
| `RUST_ACCOUNTS_PATH` | 只允许指向 Rust owner 的账号快照；不能与仍运行的 Python 服务共用生产热文件，跨运行时写入合同尚未关闭。 |
| `RUST_MODELS`/`RUST_MODELS_PATH` | 可选静态模型提示/快照；native 目录仍必须按账号类型代表和匿名目录合同刷新。 |
| `RUST_BIND` | 显式监听地址；未设置时非生产默认为 `127.0.0.1:8099`，设置 `RUST_PRODUCTION` 时默认为 `0.0.0.0:80`。 |

启动顺序合同是：读取配置 → 加载并校验 auth/model/account 快照 → 冻结相对路径解析 → 成功后才 bind/listen。启动失败不得先占用服务端口。当前 release 黑盒测试使用隔离目录和 fake native 上游验证模型目录；它不替代真实镜像、Compose、回滚或生产账号 owner 证明。

## 路由与运行时合同

| 能力 | Python 生产合同 | Rust 当前状态 | 发布结论 |
| --- | --- | --- | --- |
| 启动/上游 | Docker 通过 Compose 挂载 `config.json`/`data` 并启动单一主服务 | Dockerfile 的 app stage 启动 Rust release 二进制；`RUST_PRODUCTION=1`、`RUST_BIND=0.0.0.0:80`、`RUST_DATA_DIR=/app/data`、ChatGPT 上游和 Codex 版本已固定；本地 release/static 证据通过，远程同镜像启动与回滚仍待 Actions/ai-arm | 待远程验收 |
| 模型目录 | 每个规范化账号类型按顺序取一个成功代表，请求一次认证 Web `GET /backend-api/models?history_and_training_disabled=false`；代表失败才回退同类型下一账号；匿名目录单独请求一次 `GET /backend-anon/models?iim=false&is_gizmo=false`。Codex endpoint 只属于对话协议，不参与模型发现 | Rust/Python 已按单 Web 目录、同类型 fallback、last-good/失败关闭实现并有 focused/黑盒证据；仍需完整跨实现矩阵与生产启动验收 | 阻断 |
| `/v1/chat/completions` | ChatGPT 本地业务链、账号租约、工具/多模态/流生命周期 | Rust native Chat/工具/流生命周期与 Python 对照门已通过；仍需真实上游和远程启动副作用验收 | 待远程验收 |
| `POST /v1/responses` | Responses 请求、流事件、工具、搜索、历史和终态合同 | Rust native 普通/流、工具、网页搜索、图片 tool、历史和终态的本地黑盒/合同门已通过；真实上游与远程启动仍待验收 | 待远程验收 |
| `GET WebSocket /v1/responses` | 鉴权、连接容量、多个 create、cancel、错误、流式事件、会话回放和关闭语义 | Router 已注册并先鉴权后升级；Rust/Python 生命周期门已通过；真实上游和远程 release 黑盒仍待验收 | 待远程验收 |
| `POST /v1/images/generations`、`/v1/images/edits` | Python 本地 ChatGPT 图片业务处理 | Rust native 已按 web/Codex 后端分流，覆盖 options、输入像素门、WebP/JPEG/PNG 输出、流终态和 fail-closed；真实图片上游仍待远程验收 | 待远程验收 |
| `POST /v1/search` | Python 本地搜索处理、结果和引用 | Rust native 已覆盖搜索会话、轮询、引用清洗、租约 failover；真实搜索上游仍待远程验收 | 待远程验收 |
| `POST /v1/ppt/generations`、`/v1/psd/generations` | Python 本地 editable-file task 及产物生命周期 | ChatGPT native 下不支持 | 阻断 |
| `GET /v1/editable-file-tasks` | Python 任务查询 | Rust 认证后仍返回 `unsupported_capability` | 阻断 |
| `GET/HEAD /files/{file_path}` | Python 通过安全打开器返回 PPT/PSD 等产物文件流 | Rust 已注册 GET `/files/{file_path}`，读取链校验 owner scope、任务状态、能力摘要、相对路径和受检 regular file；Rust exact owner/download 回归与 Python `16 passed, 1 skipped` 通过；HEAD/Range 及完整 streaming 仍未实现 | 阻断 |
| `POST /api/images/download` | Python 接受多路径并返回 ZIP，受数量上限约束 | Rust 已接受有界多路径并生成 ZIP，同时提供单图下载；管理 flow 与 Python 合同组已通过，但全链路文件边界、并发副作用和用户验收未闭合 | 阻断 |
| 图片、日志、代理、备份、存储维护 | Python 有压缩、清理、日志读删、proxy test/runtime/clearance、backup、WebDAV test/sync 等真实副作用 | Rust 已注册并实现图片列表/删除/下载/压缩/清理、日志、proxy、backup、storage 等管理 handler；route matrix 通过，Rust management flow 与 Python 543 组通过，但安全原子写、后端 owner、跨实现细节和用户验收仍未闭合 | 阻断 |
| CPA | Python 有 files、import、进度和更新/删除等管理流程 | Rust 已实现 pools CRUD、files、import/progress 路由与远端调用；管理 flow 覆盖持久化/重启，但完整 Python↔Rust 远端状态/错误/副作用矩阵仍未闭合 | 阻断 |
| Sub2API | Python 有 groups、accounts、import、进度和持久化 | Rust 已实现 servers/groups/accounts、import/progress 及 registry 持久化；管理 flow 覆盖持久化/重启，但完整 Python↔Rust 远端状态/错误/副作用矩阵仍未闭合 | 阻断 |
| CCLoad | Python 有 channels、channel-models、import、进度和持久化 | Rust 已实现 channels、channel-models、import/progress 及 registry 持久化；管理 flow 覆盖持久化/重启，但完整 Python↔Rust 远端状态/错误/副作用矩阵仍未闭合 | 阻断 |
| `/health?format=json` 与 HTML | Python 并行、有界检查真实账号统计、storage health/backend info、proxy runtime，再计算 `ok/degraded`；HTML 仪表板展示状态、计数、类型和 JSON 入口并安全转义 | Rust 已把 account/storage probe 放入独立 bounded blocking worker，permit 直到 worker 真正退出；cumulative sidecar/snapshot 受 4 MiB 限制，未知类型/负数/非整数归入公开合同；Rust focused `running 6 tests` 全绿，覆盖 storage/account timeout 后 permit 保留、并发 deadline、腐坏/超大 account/auth/model snapshot、cumulative 正反、proxy 投影、HTML 卡片/刷新/JSON URL/转义；release blackbox 覆盖 active→`ok/healthy=true`、损坏 accounts→`degraded/healthy=false` | 已闭合（Rust JSON health 局部合同）；database/git backend/info/health 仍归存储行阻断，不得据此切换生产 |
| 存储/设置/备份 | Python 支持公开的 JSON、SQLite、Postgres、Git 后端及设置/备份合同 | Rust 只读取有限 JSON snapshots；没有后端、owner、备份和恢复 parity 证据 | 阻断 |

证据位置：Python 路由主要在 `api/ai.py`、`api/system.py`、`api/accounts.py`、`api/image_tasks.py`；Rust 路由和 `unsupported_*` 绑定在 `rust/src/lib.rs`；Rust 配置在 `rust/src/config.rs`。这些位置必须随代码变更同步复核，不能只修改矩阵文字。

## 发布硬门

以下条件全部满足之前，不能把 Rust `:latest` 视为完成交付，也不能在 ai-arm 上宣称切换已成功：

1. 矩阵中的每个“阻断”项都有 Rust 与 Python 的 method/path/auth/status/body/持久化/副作用/错误合同测试，且正反用例都通过。
2. 从干净镜像和仓库 Compose 等价环境启动，明确配置 ChatGPT native 上游；真实黑盒验证认证模型目录、匿名模型目录、`/v1/chat/completions`、`/v1/responses` 及 WebSocket 生命周期命中正确上游路径。
3. 以用户身份逐页执行 Web 管理功能，覆盖图片、文件下载、日志、代理、备份、设置、CPA、Sub2API、CCLoad 的读写、导入、更新、删除、失败和重启恢复；不能把 HTTP 200、SPA fallback 或 `unsupported_capability` 当成功。
4. `/health?format=json` 的 storage、proxy、account 字段来自真实检查；健康和降级两类结果均有部署验收证据。
5. Rust/Python focused、全量测试、fmt、Clippy `-D warnings`、构建产物和多架构镜像均在同一提交 SHA 上验证；失败任一项都不得推送或替换 `:latest`。

当前结论：Rust Docker 入口、依赖门、本地全量程序门和用户视角 fake/blackbox 门已具备提交条件；同 SHA 的多架构 Actions/GHCR、ai-arm 回滚/健康/真实请求仍是发布前必须完成的门。
