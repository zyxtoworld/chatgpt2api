# Rust/Python Parity Audit

审计基线：Git tag `v1.7.0`（commit `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`）；所有 Python 对照均通过 `git show v1.7.0:<path>` 读取。审计日期：2026-09-27。
Rust 版本以当前 `main` 分支为准。这里的“已对齐”表示已经核对了输入校验、路由选择、上游请求、结果投影、持久化边界和后台生命周期；“部分对齐”表示接口存在，但仍有可观察的行为差异；“未实现”表示 Rust 明确返回 `unsupported_capability` 或受 access-token-only 边界限制。

## 已对齐的主链路

### 公共 API

- `/v1/models`：ChatGPT 模式按 v1.7 无 token 的 `/backend-anon/models?iim=false&is_gizmo=false` 投影模型，并追加原版动态图片模型；不会拿各账号目录替换公开列表。OpenAI-compatible 上游模式的账号类型目录属于 Rust 扩展。
- `/v1/chat/completions`：普通文本（包括 Codex 来源账号）统一走 v1.7 的 `/backend-api/conversation`，无账号时走匿名 conversation；账号选号不依赖缓存模型目录，也会尝试限流/延迟确认账号。网页搜索和图片仍走各自专用链路；Codex Responses 不用于普通文本。
- `/v1/responses`：普通 Responses 按 v1.7 转换到 Chat/conversation，再投影响应及 SSE；网页搜索/工具走原版适配器。仅 Codex 图片生成工具使用 `/backend-api/codex/responses` 图片专用路径。
- `/v1/messages`：按 v1.7 走文本 Chat/conversation 链路；Anthropic tools 写入 system prompt 并用 XML 编码历史/输出，不作为 OpenAI function tools 或 Responses web-search 工具发送。图片按 ChatGPT conversation 文件上传链路处理；直接 OpenAI-compatible 模式映射到 `/v1/chat/completions`。
- `/v1/images/generations`、`/v1/images/edits`：已覆盖 JSON、multipart、data URL、远程图片引用、mask 合成、输出格式/压缩、网页图片链路和 Codex 图片链路。
- `/v1/search`、`/v1/ppt/generations`、`/v1/psd/generations`、`/v1/editable-file-tasks`：均有 Rust 路由和对应上游/后台任务实现。

### 账号池和模型目录

- 账号快照会做规范化、文件版本校验、原子替换和并发重载；新生成的 `created_at` 按 Python 1.7 使用 UTC，`last_used_at` 按 Python 使用本地时间，格式均为 `YYYY-MM-DD HH:mm:ss`；状态文本保留原值；`source_type` 与 Python 一样 trim/lower。普通文本选号按 v1.7 只排除禁用/异常账号，不按 `invalid_count`、本地 `models` 或 `source_type` 预先跳过账号。Web/Codex 图片选号按 Python 只排除 `禁用/限流/异常`，再检查正 quota 和各自的 source/type 条件。
- 图片账号在真正发起图片请求前按账号单独刷新 `/backend-api/me`、conversation init、账号检查和图片能力/额度；只有验证成功的账号进入图片请求。`image_account_concurrency` 只限制每账号在途请求数，quota 仅用于资格筛选，不额外把在途数压到剩余 quota 数值。
- 网页图片模型 `gpt-image-2` 固定映射为 v1.7 的 `gpt-5-3`；其它网页图片模型使用 `auto`。`default_upstream_model_name`/默认思考强度是 tag 后配置，不覆盖 v1.7 图片及普通对话请求。
- ccLoad 导入会校验频道、OAuth 类型和 access token、计划类型及账号 ID；频道模型浏览和导入都只接受当前 access token 通过两个 canonical Web endpoint 刷新的目录与同一 token 的图片 capability/quota，Codex 模型不会误当作网页模型。

### 管理页面和持久化

- 用户密钥接口只返回 `role=user`；账号列表按 Python 1.7 返回已 trim 的账号 `proxy` 并实时附加 `image_inflight`，管理投影仍剔除 refresh/id token 等凭据。`source_type=oauth_login` 保持原值，不隐式升级为 Codex 能力；CPA/Sub2API Codex 导入仍明确写入 `source_type=codex`。账号更新保留 `proxy` 字段；账号、模型、图片、标签、日志、备份、代理、CPA、Sub2API、ccLoad 的主要 CRUD 路由均已存在。
- CPA/Sub2API/ccLoad 导入均使用后台任务、幂等 job id、错误列表和账号快照合并；CPA/Sub2API worker 使用启动时捕获的远端配置，删除服务器配置不会取消已启动的凭据导入；CPA/Sub2API 写入 `source_type=codex`，与 Python 服务一致。导入合并按 Python 1.7 的 `access_token` key 处理，同 token payload 覆盖其提供的状态/额度/计数等字段，空 `type` 回退当前套餐、单个 payload 为空 `created_at` 保留现值（包括启动时为旧记录生成的运行时创建时间）；同批重复 token 按输入顺序先做 last-write 字段合并，再对最终空 `created_at` 回退现有/新建时间。Codex 导出按 `type/export_type/plan_type` 预处理，代理值按 Python 转字符串后 trim；同身份新 token 不会静默替换旧 token。账号快照只保留 access token 边界，避免将 refresh/id token 重新暴露给 Rust 运行时。
- PPT/PSD 后台任务已具备任务恢复、账号类型筛选、文件下载能力哈希和受限文件读取。
- PPT/PSD 账号按 Python v1.7 在 Plus/Team/Pro/Enterprise 中选择 `last_used_at` 最早者，不限制 Web/Codex 来源。
- 图片任务已覆盖提交幂等、按用户隔离、`queued/running/success/error`、结果/usage/耗时、JSON/multipart 编辑输入，以及超时任务的 `resume-poll` 恢复轮询。
- `/api/logs` 现在会持久化 API 调用、图片后台任务、PPT/PSD 后台任务和账号新增/删除/更新/刷新/异常移除事件；图片成功调用日志可记账号邮箱、conversation id 和结果 URL，内部元数据经响应扩展传递、不写入公开 JSON/SSE；敏感 token 只保留公开引用，调用日志保留受限的请求摘要、状态、耗时和错误。
- 账号刷新在模型目录暂时不可用时保留最后一次成功的 Web/Codex 模型目录；quota 归零时只清理 image 模型，避免管理页面模型列表被瞬时刷新失败清空。
- 账号导入新增记录会持久化 Python 1.7 同格式的 `created_at`；ccLoad 有最近刷新时间时优先使用该时间；同 token 重导入会更新记录字段但保留 token key。
- Rust 服务启动后会按 `refresh_account_interval_minute` 周期刷新正常/限流 access token 账号，并在关闭时停止 watcher；同时恢复图片保留清理和 R2 备份调度。
- 图片 `n>1` 已按 1.7 的 `image_parallel_generation`、账号级 `image_account_concurrency`、串行/并行结果合并和 `image_check_before_hit_enabled` 运行；Web/Codex 图片入口都经过同一多图调度边界。
- 图片流已按 1.7 输出 `image.generation.result` 数据块和单一 `[DONE]`，多图会保留 `index/total`；图片 usage 会计算文本、输入图片和输出图片 token。
- 图片后台任务已把 `getting_account`、`image_stream_resolve_start`、`receiving_image` 进度写入任务，`resume-poll` 会读取并使用 `extra_timeout_secs`。
- Chat、Responses、Messages、Search 调用日志现在记录请求文本摘要和图片/消息结构统计；WebDAV 图片存储按 `local/webdav/both` 落盘并持久化索引，覆盖生成、测试、同步、读取、列表、归档和删除，支持 `public_base_url`，启用时缺少 URL 或密码会按 1.7 拒绝配置；图片管理列表支持日期筛选和 `YYYY-MM-DD HH:mm:ss` 创建时间。
- 管理 API 的跨域预检与 Python `CORSMiddleware` 对齐；图片删除和低磁盘清理会清理标签、缩略图及索引，备份包含图片索引。
- ccLoad 导入最终进度会合并账号刷新阶段的失败数与错误列表，不再把“凭据获取成功但刷新失败”显示成成功。
- AI 审核支持 1.7 的 `fail_open`（默认 `true`）；审核服务网络、JSON 或决策异常时按配置放行或失败；各公开路由按 Python 的字段集合提取文本，内部协议转换只审核一次。
- `/api/accounts` 新增账号现在透传刷新阶段的 `errors` 和刷新后的 items，不再固定返回空错误数组；这与 Python create_accounts 的返回契约一致。
- 普通 Chat 的上游 401 现会读取 Python `InvalidAccessTokenError` 对应的失效 token 标记；请求尚未输出时标记/按配置移除该账号并继续试池内其它账号。不会仅凭任意 401 移除账号，也不伪造 refresh token 轮换。
- 账号元数据刷新按 1.7 同时请求 `/backend-api/me`、conversation init 和 account check，并复用同一账号 proxy client；三路并发由屏障回归测试覆盖。
- v1.7 创建 `OpenAIBackendAPI`、CPA 和 OAuth refresh session 时未传 `upstream=True`，因此这些实际请求使用账号 proxy、旧版全局 `proxy` 优先级，而不使用 runtime `single_proxy`。Rust 现与该实际调用默认值一致；legacy global proxy 也已覆盖 CPA 文件浏览/下载、远程图片引用、AI review 和 OpenAI-compatible Chat/Responses/Images/Messages 上游请求。一次 Web 生图尝试复用同一账号 client/cookie jar，Codex 图片使用账号 proxy client，editable 的上传/轮询/下载也复用任务 client。
- Web 生图 SSE 在 conversation id 已到达、随后流中断时，`image_remove_conversation_always` 仍可按 1.7 清理该 conversation；解析器错误路径通过独立 id sink 保留已观察到的 id。
- Web 生图 SSE 保留 Python `ConversationState` 所需的可见 assistant 文本、moderation `blocked`、`tool_invoked`、`turn_use_case` 和生成图片 ID；用户上传图片不会误当成输出。Rust 请求超时现在为 300 秒，轮询仍由 `image_poll_timeout_secs` 单独控制；Rust 的整体 operation deadline 仍是有界硬上限，未验证持续活跃超过该期限的真实上游流。无会话 ID时会按 v1.7 最近 10 个 conversation 的更新时间/prompt 标题规则恢复；普通消息、文本代替图片、任务错误、已有部分图片 ID、文件 URL 暂不可用分别走对应的错误、账号重试、部分结果和兜底轮询路径。`404/409/423` 不作为图片轮询重试状态，保留 v1.7 图片服务自己的 `429/500/502/503/504` 集合。
- 图片目录不再按 5000 条截断；本地图片和 WebDAV 索引项都会完整列表，按日期全选删除、存储统计、压缩和清理也不会漏掉第 5001 张之后的本地图片。保留期清理按 Python 1.7 在启动、列表和保存图片时触发，仍保留周期清理。
- 缩略图按 Python 的 EXIF 方向、RGB/RGBA、320x320 Lanczos 规则生成并落盘缓存；WebDAV-only 图片读取按 Python 返回 `image/png`；图片 ZIP 使用 DEFLATE 与 UTF-8 文件名标志，选中项不存在时跳过；图片删除对越界目标按 Python 跳过。路径字段类型校验与 Pydantic 对齐。
- `/v1/images/generations` 的额外 JSON 字段按 Pydantic 默认忽略，`/v1/images/edits` 按 `image_inputs.py` 白名单抽取且 multipart 重复普通字段取首值；请求类型/默认值、`n`/`stream` 转换和 prompt 空白规则有回归覆盖。编辑支持 Python 的嵌套 URL、base64 对象、JSON-string 引用、percent-encoded data URL、远程下载 MIME fallback 与多 mask 按图片配对/复用末项/Lanczos 缩放；mask 仅 RGBA 取 alpha，其它模式转 L；上传 MIME 按实际解码格式投影并兼容 `image/jpg`。远程单图仍受 50 MB 与解码像素上限保护。
- 图片输入解码扩展到 BMP/GIF/JPEG/PNG/PNM/TGA/TIFF/WebP，TGA 按 MIME 或 multipart 文件名选择解码器并验证畸形输入拒绝；未改变 50 MB、25 MP 和 multipart 总体预算。
- 图片任务 generation/edit 按 Python `ImageTaskService` 使用 `response_format=url`；公共图片非流式响应只投影 `created/data/usage`，编辑 `client_task_id` 不回显；保留 Python 的 `_account_email` 和 Web SSE `_conversation_id` 元数据。Codex `n>1, stream=true` 现先收集合并 JSON 再投影 SSE，避免把单图 SSE 当 JSON 解析。
- 用户密钥创建/改名已对齐 Python 的 `普通用户` 唯一默认名、名称去重、密钥去重及管理员密钥冲突规则；密钥替换响应不重复返回明文，布尔更新接受 Python Pydantic 的标准布尔字符串。
- CPA/Sub2API 名称为空与更新字段为 `null` 时按 Python 默认值/`exclude_none` 保留；Sub2API 分页按 Python `total`/短页终止及响应 envelope 形状处理。
- 旧日志缺少 id 时用 Python 同格式 SHA-1 派生 id；删除其他日志时保留完整未投影字段，并兼容省略 `ids` 的空删除请求。

## 已确认的行为差异

这些不是推测，而是逐一对照 Python 路由或服务实现后确认的差异。

### P0：有意保留的 access-token-only 边界

1. **OAuth 和密码重新登录未实现（有意）**

   Rust `src/lib.rs` 中 `/api/accounts/re-login`、re-login progress、`/api/accounts/oauth/start`、`/api/accounts/oauth/finish` 仍由 `access_token_only_disabled` 处理，返回 `unsupported_capability`。Python 对应逻辑在 `api/accounts.py`、`services/account_service.py` 和 `services/oauth_login_service.py`，包含密码登录、验证码、PKCE、授权码兑换和三件套落盘。

2. **账号导出不是 Python 的完整三件套 JSON/ZIP 导出（有意）**

   Python 导出要求 `access_token + refresh_token + id_token`，并投影 `type/email/account_id/expired/last_refresh`；JSON 单账号直接返回对象，多账号返回数组，ZIP 每账号一个按 email/account id 命名的 JSON 文件。Rust 支持 JSON/ZIP 外壳，但导出的是单个原始账号对象或数组，ZIP 使用序号文件名；access-token-only 规范化会丢弃 refresh/id token，且 Rust 不会按 Python 契约拒绝不完整账号。因此不能声称导出格式或内容兼容。

3. **FlareSolverr 自动覆盖为 Rust 扩展，真实代理上游仍需验证**

   Rust 已实现 `ClearanceStore`、按需 FlareSolverr 刷新、过期时间、取消安全 single-flight、manual headers 和管理端测试，并已接入账号刷新、模型目录、Web conversation、Web/Codex 图片资源、搜索、editable 和 Codex Responses。Python 1.7 的 `refresh_clearance` 没有生产请求调用点，Rust 的自动获取属于扩展；manual headers 仅在 runtime 与 clearance 均启用且 mode 为 manual 时添加，关闭或 none 不会使用过期 bundle。普通 Chat 的 bootstrap/requirements 及代理重试仍需逐路径运行验证。

4. **代理实现的边界**

   v1.7 的 `OpenAIBackendAPI` / CPA / OAuth refresh 业务调用默认 `upstream=False`，所以 Rust 有意不把 runtime `single_proxy` 注入这些兼容调用；这是按原版实际参数对齐，不是漏接。Python `proxy_runtime.test_proxy`、clearance 手动测试则会用 `upstream=True`。Rust 现对基础/direct client 显式关闭 wreq 的 Windows 系统代理自动接管，只在 profile 提供 proxy URL 时才配置代理；这避免 direct 出站被机器代理设置改写，并让本地直连行为与 Python 未配置显式代理的 session 一致。runtime 代理仍用于这些管理检查和 Rust 的 clearance 扩展。Rust client 没有完全复刻 `curl_cffi.Session` 的连接池/session 重置行为；`reset_session_status_codes` 在 Python v1.7 仅解析并存入 profile，没有业务读取/重置 session 的调用点，因此当前无可观察的 1.7 行为可对齐。

### 已清理的历史遗漏

- `image_parallel_generation`、`image_account_concurrency`、`image_check_before_hit_enabled` 不再只是设置项，已经接入实际生图和轮询逻辑。
- `image_account_concurrency` 不按剩余 quota 数再加一层在途限制；Python 1.7 只要求 quota 大于零并单独检查账号并发，Rust lease 与对应回归现同样处理。
- 图片轮询超时和模型文本回复的跨账号重试按 Python v1.7 的 4 次/3 次预算执行；重试会重置账号排除集，单账号池也可再次尝试。内容策略错误不重置账号集、不触发该类重试。
- Web 生图轮询初始等待增加 Python 1.7 的 `random.uniform(0, min(2s, initial_wait * 0.2))`，HTTP/网络重试增加 `random.uniform(0, 0.5s)`；Retry-After、指数退避基数和 deadline 规则保持一致。
- `refresh_account_interval_minute` 不再只是设置项，已经接入后台账号 watcher。
- `image_retention_days`、备份 `interval_minutes` 不再只是设置项，已经接入服务生命周期调度。
- `ai_review.fail_open` 已接入审核异常处理。

### P1：需要继续关注的差异

1. `global_system_prompt`：Rust 已接入 Chat、Responses、Web 图片和 Anthropic Chat 转换；Anthropic `system` 文本块折叠、XML tool prompt 顺序有回归覆盖。Python 的搜索和 editable 链路本身不注入该全局提示词，Rust 同样保持不注入。
2. 图片日志与调用前过滤：native Web/Codex 图片在账号选定后失败时会把 lease 邮箱传入失败调用日志，`error` 记录错误消息；Web 生图 SSE 错误也保留已观察到的 conversation ID。Web moderation 失败端到端测试覆盖邮箱、会话 ID 和错误消息。账号切换会清除上一个尝试的 conversation ID；`n>1` 全失败的聚合错误按 Python 不携带子任务账号元数据。成功结果仍将 account email、conversation ID 和 URL 写入日志，公开响应剥离内部字段。OpenAI-compatible 图片 JSON/multipart 请求记录 model/request_text，并在转发前应用敏感词及 AI review；被拦请求不会到上游。OpenAI-compatible 模式没有账号 lease，因此没有账号级元数据来源。Python logger 与 Rust `/api/logs` 仍是不同诊断渠道。
3. `sensitive_words` 和 `ai_review`：Rust 已接入敏感词拦截、按路由文本提取、base64 替换、100k 截断、Python 决策字符串化、`fail_open` 和内部路由单次审核回归。Python 结构化 logger 的 sanitizer/审核失败事件与 Rust `/api/logs` 不是同一日志渠道，仍是诊断可观察差异。
4. `chat_completion_cache`：Rust 已实现非流式/流式 TTL cache、in-flight dedupe、owner 取消/失败时唤醒 waiters、去重关闭时仍缓存 stream、Python 消息归一化和递归 JSON key 排序；并发边界已有回归。
5. `auto_remove_invalid_accounts`、`auto_remove_rate_limited_accounts`：Rust 已实现刷新写回的时间窗口确认、手动刷新立即处理、直接 401 立即标记/删除及刚变为限流时删除；普通 Chat 失效账号回退也有本地 mock 覆盖。10 分钟/30 秒边界和直接失败计数隔离已有回归。
6. `image_remove_conversation_after_result`、`image_remove_conversation_always`：Rust 对 Web 图片成功结果与 always 错误/超时路径异步 PATCH 隐藏 conversation；异常 SSE 已保留先前收到的 id。真实上游是否接受 hide PATCH 尚未在线上验证。
7. refresh token keepalive、过期 access token 自动刷新和 `auto_relogin_after_refresh`：Rust 的 access-token-only 边界不会保存或刷新 Python 维护的 refresh/id token 三件套。
8. `/api/accounts/refresh` 的失败项现对齐 Python 前端所需 `token`/`error` 字段和匿名 token 摘要，并保留 Rust `code` 扩展；错误文本使用 Rust 安全通用描述，不包含 Python `UpstreamHTTPError` 的 path/status/body，诊断细节仍不同。
9. refresh errors 会过滤已识别的 TLS/SSL 传输错误以匹配 Python helper；不同 HTTP/TLS 库产生的底层错误字符串不完全相同，罕见 marker 可能分类不同。

### P2：导入实现可用但不完全相同

1. CPA：Rust `execute_cpa_import` 最多 16 个并发下载、逐文件更新进度；已对齐启动配置快照、HTTP 状态/缺 access token 错误文本、重复文件名请求、token-key 合并和 `source_type=codex`，并有本地 mock 回归。CPA 文件列表和导入请求不再额外限制 5000 条，和 Python 请求模型一致。Python 删除 pool 后 job 记录随 pool 一同删除，Rust 同样不保留独立 job 历史；已启动 worker 继续用启动时配置完成导入。
2. Sub2API：Rust 管理端账号/分组现在按 Python 的每页 200 条和 `total`/短页条件循环，兼容 `{code,data:{items|data|list,total}}`、`{data:[...]}`、裸数组；不再额外截断 5000 条。导入已对齐启动配置快照、HTTP 失败逐 ID 记录、返回数汇总错误、`code` envelope 下 partial `data.accounts` 解析、处理响应中的全部 token 和 `source_type=codex`，并有本地 mock 回归；分页 `list` 形状及 5200 条/26 页已有定向覆盖。
3. ccLoad：Rust 已实现登录、频道 editor 中的 OAuth access token 提取、逐频道 canonical 目录刷新和逐频道进度；不会从旧账号快照、其它同套餐频道或 editor 模型字段借用目录。这是当前三种导入中最接近 Python/实际页面需求的一条，但它是 Rust 扩展逻辑，Python baseline 中没有同名服务文件可直接逐行对照。

### 资源与安全边界

- Rust 对图片 ZIP 仍限制 512 MiB 和 ZIP32 的 65,535 个成员；Python 1.7 没有对应的显式归档上限。Rust 账号/日志/标签快照也有大小或条数上限，Python 对应 JSON 服务没有相同上限。这些是有意的资源保护差异，超限时表现不完全一致。
- Rust 对设置字段、图片相对路径、标签长度/数量以及部分管理输入执行更严格的白名单/界限；Python 对这些输入通常交由 Pydantic 或文件服务处理。需要保持安全限制，不可把这些强化校验误报为完全行为一致。

## 需要继续处理的顺序

1. 有真实上游代理的运行环境后，验证 legacy/global 与 account proxy 的连接、TLS 和认证行为；本机 mock 仅验证代理路由确实生效。
2. 在线验证 FlareSolverr/clearance 的供应商响应、上游拦截重试和图片 conversation hide；本地单测不能代替这些外部依赖验证。
3. ccLoad 在 Python 1.7 baseline 中没有对应集成服务，Rust 当前行为属于扩展，不可宣称与原版逐项一致；检查 Python logger 与 Rust 持久化调用日志差异时不要把日志实现差异误判为 API 业务语义差异。
4. 继续维持 access-token-only 安全边界；OAuth、密码重登、refresh-token keepalive 和 Python 完整导出继续明确返回 unsupported，不得伪装成成功。

## v1.7 之后功能清单（暂不实现）

- 账号级官方模型目录和按账号权限路由（`641e0dea`）。
- 默认上游模型名称配置（`79199cbc`）。
- 默认思考强度配置及模型后缀覆盖（`62dd0efb`）。
- 无图片结果时移除本地 conversation（`d83cef80`）。
- 过滤内部 assistant tool 消息（`6675eb33`）。
- 数据库增量同步（`ca26e54f`）。
- 图片 SSE 每次上游请求的 300 秒 timeout 在 v1.7 中已存在；仅 Rust 持续活跃流的整体硬 deadline 比 Python 更严格，见上文剩余边界。
- 保留代码/命令标点前空格的清洗修复（`df2398fa`）。
- v1.8.0 版本与 changelog 更新（`e55aef28`）。

## 当前验证记录

- 本轮以 `.local/public-minimal` 的 Git tag `v1.7.0`（`1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`）作为 Python 基线，没有把 tag 后的 Python 文件混入比较。继续核对并修复了 native 图片 SSE 状态/最近会话恢复、文本回复重试、task error 与部分图片结果处理、文件 URL 失败后的轮询恢复；native Web/Codex 图片失败调用日志现在记录已选账号邮箱，Web SSE 中断路径记录已观察到的 conversation ID；调用日志 mock 覆盖 Web moderation 失败。
- 本轮另对照官方 `api/accounts.py`、`services/account_service.py` 的 refresh 与 update 请求：修复 refresh error 的前端字段/token 摘要、未知但上游有效 token 的刷新进度/不入库语义、TLS 错误过滤，以及 account update 的 Pydantic 字段类型、422 和空 type 默认值。
- 最近一次 `cargo test --workspace --offline -- --test-threads=1`：479 个 Rust lib tests 通过、5 个按 v1.7 行为边界标记为 ignored，2 个本地 mock 的 TCP readiness 探测因机器负载在 30 秒内未响应而失败。两个失败用例分别单跑均通过；另有 `models_route_retries_until_success_within_a_type` 因缺少 readiness 等待曾失败，已补等待并单测通过。此前一轮完整 workspace 曾通过 481 个 lib tests、6 个 `chatgpt2api_file_identity` tests 和 doc-tests；但最后一次 SSE deadline/测试夹具改动后的全量运行没有形成全绿结果，不能把当前全量测试标记为通过。
- 最近一轮 native 图片失败日志修复后的定向验证：`native_web_image_generation_uses_conversation_and_file_download_flow` 通过（包括 moderation 失败日志的 account email、conversation ID 和错误消息）；`native_image_log_account_switch_clears_previous_conversation_id` 和 `native_codex_image_model_prefix_selects_exact_account_type` 通过。该增量后的 workspace 全量测试未重跑。
- 本轮 quota/concurrency parity 定向验证通过：`image_account_leases_follow_concurrency_limit_independent_of_quota` 同时覆盖 Web 与 Codex lease，验证 quota=1 时仍可按配置同时预约 3 个账号 lease，第 4 个受并发上限阻止；quota=0 账号仍不可预约。`image_timeout_and_text_reply_retries_can_reacquire_prior_accounts` 也通过，验证 poll timeout/text reply 可清除账号排除集、content policy 不清除。
- 本轮账号管理定向验证通过：`account_refresh_non_auth_upstream_error_does_not_mark_account_abnormal` 断言匿名 token、UI `error` 字段及 Rust `code`；`account_refresh_of_unknown_but_valid_token_updates_progress_without_importing_it` 验证未知 token 的三个 metadata 请求、进度累计与不入库；`account_refresh_tls_filter_matches_python_error_markers` 覆盖 Python TLS marker；`account_management_preserves_private_fields_and_writes_atomically` 覆盖 update 的类型校验和默认值。
- 2026-09-26 继续对照 Python 1.7 `AccountService.list_accounts` 与 Rust 管理投影，修复列表漏掉账号 `proxy`（编辑后可能把代理清空）和实时 `image_inflight`（页面误显示 0）的问题；`account_management_preserves_private_fields_and_writes_atomically` 验证列表初始代理/计数、持有图片 lease 时计数为 1、更新响应返回新代理，并确认 refresh token 仍不外泄。该定向测试通过；本次增量后的 workspace 全量测试未重跑。
- 2026-09-26 对照 OAuth finish 的 `source_type=oauth_login` 与 Python `_normalize_source_type`，移除 Rust 对 `codex` 的错误映射，避免 OAuth 登录账号被加入 Codex 模型暴露和 Codex 图片候选。`oauth_login_source_type_is_preserved_without_codex_capability` 覆盖该语义；字段规范化同时补齐 `proxy.strip()` 与 Python 设为 `None` 的空刷新错误/时间字段。
- 2026-09-26 对照 Python `AccountService._now()`，修复 Rust 创建时间使用本地时区的差异；新增 `account_timestamp_uses_utc_like_python`。
- 2026-09-27 复查发现 Python 的 `mark_text_used`/`mark_image_result` 单独使用本地时间生成 `last_used_at`；回退先前将这两种事件一并改成 UTC 的过度修复。现将创建时间固定 UTC、使用时间本地化分开实现，并加 `last_used_timestamp_uses_local_time_like_python`。
- 2026-09-26 对照 Python `_prepare_account_payload` 与 `_add_account_payloads`，修复同 token 导入强制保留旧状态/额度/计数、空创建时间重置为当前值，以及 `type=codex` 与 `plan_type` 合并错误。新增 `account_reimport_overwrites_python_managed_fields_and_ignores_empty_created_at` 覆盖字段覆盖、falsey fallback 和 Codex 来源映射；该定向测试通过。
- 2026-09-26 对照 Python `_is_image_account_available`，修复 Rust 要求状态精确等于 `正常`、额外排除 Python 会接受状态的问题；状态字符串现在不 trim，`source_type` 持久化也与 Python 一样小写。图片资格状态矩阵、空白状态选号、来源大小写、同 token 导入、quota/concurrency 和账户列表相关定向测试均通过。
- 2026-09-26 同 token 导入旧记录缺少持久化 `created_at` 时，Rust 合并会用新的当前时间替代加载时已生成的时间；现从已加载记录投影补回原运行时值后再合并，测试覆盖重导入空 `created_at` 保留该值。
- 2026-09-26 最新一次完整 workspace 测试为 Rust lib 482 通过、4 失败、5 ignored，`chatgpt2api_file_identity` 6 通过。4 个失败分别是 WebSocket 本地 mock timeout、PPT 跨 CWD 压力用例调度断言、模型目录 readiness timeout、账号代理模型目录用例；四项逐个单跑均通过。之后给 metadata 并发测试补了显式 HTTP readiness，账户筛选集 `cargo test ... --lib account_ -- --test-threads=1` 为 66 通过、1 ignored；这项 fixture 修改后的 workspace 全量测试尚未重跑，因此当前不能记为全量全绿。
- 后续完整 workspace 测试为 Rust lib 487 通过、1 失败、5 ignored，`chatgpt2api_file_identity` 6 通过。唯一失败 `failed_type_refresh_keeps_last_good_and_respects_backoff` 是本地 readiness TCP timeout，单跑通过；另一次增量全量里的 ccLoad 模型刷新失败也单跑通过。跨导入合并、账号状态和图片选号的定向测试通过；当前完整 workspace 仍未全绿。
- 再次完整 workspace 测试仍为 Rust lib 487 通过、1 失败、5 ignored；本次唯一失败 `openai_catalog_falls_back_after_empty_same_type_candidate` 为 readiness TCP timeout，单跑通过。近期各次全量失败项每次不同且单跑通过，表现为本机长测中的 mock readiness/调度不稳定；仍保留全量非绿状态，不作为功能全绿或部署依据。
- 2026-09-27 继续对照 Python `AccountService.get_stats`，修复健康 JSON 的 `by_type` 使用小写内部套餐而将 Plus/Pro/Team 计入 `other` 的差异；按存储的 `type` 标签分组，HTML 仍转义标签。另修 `int()` 字符串中的数字间下划线（如 `"3_000"`）以及 truthy whitespace `created_at` 保留语义。新增健康类型、整数、时间回归通过；这批增量后的 workspace 全量尚未重跑。
- 2026-09-27 刷新进度状态按 Python 做 `str(status or "正常").strip()`；补齐空状态默认、首尾空格剥离和非字符串状态的文本投影单测。
- 2026-09-27 继续对照 Python editable `_clean(last_used_at)`，修复 Rust 对日期格式的额外验证；现在会字符串化 truthy 时间值并按原版字典序做 LRU/reload 合并。非日期字符串及数值时间戳 LRU 回归通过。
- 最近一次完整 `cargo test --workspace --offline -- --test-threads=1` 全绿：Rust lib 494 passed / 5 ignored，`chatgpt2api_file_identity` 6 passed，main/doc-tests 0 failed。忽略项均为明确标记的 post-v1.7 路径。Clippy、fmt 与 diff 检查通过；这只确认现有测试集通过，不等于已证明整个代码库不存在任何逻辑差异。
- 2026-09-27 对照 editable/PPT/PSD 的 Python `_clean(last_used_at)` LRU，移除 Rust 对固定日期格式的过滤，改按 Python 字符串化后的词典序保存/比较；非法日期字符串和数值 marker 现在仍参与候选排序。新增 LRU 与 reload 回归通过。
- 2026-09-27 继续对照账号 truthiness/数值语义，修复 falsey `type` 未默认 `free`、管理列表将非字符串 truthy `created_at` 替换成生成值，以及 ASCII-only source lowercase 的差异。健康 `success/fail` 现在保留负值并按 Python 图片结果计数方式递增；quota 仍强制非负。新增 falsey 套餐、truthy whitespace/非字符串创建时间、Unicode source 和负数记账测试通过；最新 workspace 有 readiness/调度 mock 偶发失败，详情见其结果记录。
- 2026-09-27 发现此前独立解析的 `invalid_count` 字符串不接受 Python `int()` 支持的数字间下划线；现与统一整数转换器一致，延迟失效 token 的阈值测试覆盖 `1_2` 与 `1_0`。
- 2026-09-27 继续对照账户字段和统计：truthy 的非字符串 `created_at` 由账户列表按 Python 原类型返回；falsey `type` 按 Python 默认 `free`，truthy whitespace 类型保留且不会误分入 free；`source_type.lower()` 改用 Unicode lowercase。健康成功/失败计数改为有符号整数，负数按 Python `int()` 汇总，图片成功/失败记账也在原值上加一；quota 仍钳制非负。相应归一化、健康、账户列表和图片记账测试通过，Clippy 与格式检查通过，workspace 全量尚未重跑。
- 2026-09-27 账户刷新进度按 Python `str(status or "正常").strip()` 投影；失效计数也复用支持 Python 数字下划线的整数转换，避免低计数/高计数分支误判。
- 2026-09-27 完整 workspace 最新结果：Rust lib 494 passed、0 failed、5 ignored；file identity 6 passed；main 和 doc-tests 0 failed。账户筛选集 74 passed、1 ignored。Clippy、fmt 和 diff 检查通过。全量运行含 readiness 稳定性修正后的 fixtures。
- `cargo test -p chatgpt2api-rust --offline --lib image_ -- --test-threads=1` 筛选集 81/82 通过；唯一失败为既有本地 remote-image mock readiness TCP 超时，随后该测试单跑通过。筛选集内包括 native Web/Codex 图片 HTTP 流程和上述新增 lease/retry 回归。相关增量后的 workspace 全量测试未重跑。
- `cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check` 和 `git diff --check` 均通过。
- 2026-09-28 继续对照图片轮询时序，补齐 Python 初始等待 `0..min(2s, 20%)` 与失败重试 `0..500ms` jitter；Retry-After、退避基数和 deadline 保持原规则。复核发现 wreq 基础 client 会隐式启用 Windows 系统代理，与 direct/显式 profile 语义不一致；基础 client 现禁用自动系统代理，仅在 profile 明确配置时挂代理，legacy global proxy 回归仍通过。
- 2026-09-28 修正长测 mock fixture：readiness 放到状态初始化/请求前，阻塞型本地上游使用多 worker；Codex 通用模型测试不再假设某个轮询账号先被选中；catalog/auth fixture 隔离默认 `data/config.json`，Windows auth-snapshot 损坏测试直接写入损坏 fixture并在清理前释放状态。此前单跑通过但全量偶发失败的用例均在修正后复验。
- 2026-09-28 最终 `cargo test --workspace --offline -- --test-threads=1` 全绿：Rust lib 495 passed / 0 failed / 5 ignored，`chatgpt2api_file_identity` 6 passed，main 与 doc-tests 0 failed；`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check` 通过。5 个 ignored 均为明确标记的 post-v1.7 行为，不代表遗漏的 v1.7 用例。
- 前端本轮未修改；此前记录的前端 373/373 和 TypeScript 检查不属于本轮 Rust 变更后的复验结果。
- 未部署、发布或连接线上主机。已知 access-token-only 不支持项、日志诊断/jitter 差异、多图失败日志聚合及真实供应商验证仍见上文。全量绿只表示当前测试集通过，不等于已证明整个代码库绝对零逻辑差异，也不替代真实上游验证。

## 2026-09-28 重复对照、修复与复查闭环

本轮仍仅使用 tag `v1.7.0` / `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`。对照先前审计后，逐项检查了公共 API 与协议投影、账户刷新/代理、管理与任务持久化；另用只读复核补查了可观察边界。确认并修复的遗漏如下（每项均有 Rust 回归覆盖）：

| 对照到的行为差异 | v1.7.0 基线证据 | Rust 修复及回归 |
| --- | --- | --- |
| 缺失或损坏的配置文件不能打开/保存管理设置 | `services/config.py:_read_json_object`；`ConfigStore.get/update`；`api/system.py:get_settings/save_settings` | Rust `src/lib.rs:api_settings/api_settings_update/read_legacy_config_compatible/update_legacy_config` 对 missing/invalid/non-object 回退 `{}`，提交仍原子写并严格回读；`src/lib.rs:tests::settings_api_recovers_missing_and_invalid_config_like_python` 覆盖缺失与损坏文件 GET/POST。 |
| 图片任务 generation 的根 JSON、JSON 解码、required 字段与 Pydantic 错误状态不同 | `api/image_tasks.py:ImageGenerationTaskRequest/create_generation_task`；`api/errors.py:validation_exception_handler` | Rust `src/lib.rs:image_task_generation_validation_errors/image_task_generation` 对非对象、malformed JSON、缺失/错误类型/空 `client_task_id`/`prompt` 返回 422 detail；`src/lib.rs:tests::image_task_content_filter_rejection_writes_python_call_log` 覆盖根数组、malformed JSON、缺失/空 prompt 与根形状。 |
| PPT/PSD 无套餐账号、PSD 空图像的终态错误不保留原版可诊断信息 | `services/editable_file_task_service.py:_editable_access_token/_run_task` | Rust `src/editable_file_generation.rs:TaskFailure/run_task/persisted_error` 保留确定性错误 `no available plus/team/pro account`、`base64_images is empty`；未知旧快照异常映射 generic，避免启动失败/任意上游文本反射；`src/lib.rs:tests::native_editable_submission_is_bounded_idempotent_and_fails_closed/editable_restart_recovers_only_unfinished_tasks_without_losing_sidecar_writes` 覆盖。 |
| resume-poll 只在即时响应显示新时间戳，快照仍短暂保留旧值；并发重复 resume 可重复启动 worker | `services/image_task_service.py:resume_poll/_update_task` | Rust `src/lib.rs:image_task_resume/mutate_image_tasks` 在同一 mutation 持久化 status/error/`updated_at`/`updated_ts` 并再次核对 error 状态；`src/lib.rs:tests::image_task_resume_persists_updated_timestamp_before_worker_progress` 覆盖 response、快照及第二次 resume 拒绝。 |
| Chat usage 漏掉 Python message 字符串字段/name/framing 计数及多模态图片 token；Responses 普通流式/非流式 usage details 不完整 | `services/protocol/conversation.py:count_message_text_tokens/count_message_image_tokens`；`services/protocol/openai_v1_chat_complete.py:completion_response`；`services/protocol/openai_v1_response.py:stream_text_response` | Rust `src/protocol_chat.rs:native_usage/native_message_image_tokens/native_image_input_tokens` 与 `src/lib.rs:response_from_chat_value/response_stream_from_chat/native_responses_usage_from_chat` 现在投影输入 text/image/cached 与输出 token details，Responses `response.completed` 亦含 usage；`src/lib.rs:tests::native_chat_usage_counts_image_patches_and_all_python_text_fields/native_responses_nonstream_usage_projects_chat_token_details/native_responses_stream_completed_event_includes_usage` 覆盖。stream usage 预计算包含全局 system prompt；`tests::internal_message_routes_review_once_and_prepend_global_prompt_once` 覆盖。 |
| Responses 的 `stream` 拒绝 Pydantic 接受的字符串/数字 bool | `api/ai.py:ResponseCreateRequest`（`stream: bool | None`） | Rust `src/protocol_responses.rs:pydantic_bool/validate_responses_payload` 增加 bool coercion；`protocol_responses::tests::responses_stream_validation_matches_pydantic_boolean_coercion` 覆盖 true/yes/off 与无效值。 |
| Chat 上游在首个可见 delta 前报错，Rust 已先返回 200 SSE；正常 EOF 但缺 `[DONE]` 也被当作失败 | `services/protocol/openai_v1_chat_complete.py:stream_text_chat_completion`；`services/protocol/conversation.py:stream_text_deltas/iter_conversation_payloads`；`services/log_service.py:_next_item` | Rust `src/lib.rs:prime_native_chat_stream/native_stream_response` 在创建 SSE 前有界预读，首 delta 前传输失败仍映射 502；干净 EOF 补正常 finish/DONE，传输 error 仍失败；`tests::chat_stream_failure_before_first_visible_delta_is_a_gateway_error` 覆盖两种边界。 |
| Anthropic Messages 错误 envelope 多出 `error.code`，且典型模型类型错误消息与字段定位丢失 | `api/errors.py:_compatible_error_response`；`services/protocol/error_response.py:anthropic_error_response/error_message_from_detail`；`api/ai.py:AnthropicMessageRequest` | Rust `src/errors.rs:ApiError::into_anthropic_response/validation_message` 与 `src/protocol_chat.rs:validate_chat_payload`、`src/protocol_anthropic.rs:validate_message_request` 仅对已核对的字段错误输出安全字段消息；Anthropic envelope 只含 `type/message`；`src/lib.rs:tests::validation_errors_preserve_python_field_messages_for_chat_and_messages` 覆盖。嵌套未映射形状仍用泛化消息。 |
| 帐号 proxy 缺省时未回退 legacy global proxy；无 scheme 的 `host:port[:user:pass]` 未归一化；匿名 Chat 也走 direct | `services/proxy_service.py:get_profile/_colon_proxy_to_url`；`services/openai_backend_api.py` session 构造 | Rust `src/lib.rs:upstream_client_for_lease/upstream_client_for_account_proxy/refresh_access_token_account/native_chat_upstream_client` 与 `src/proxy_service.rs:profile_from_runtime/normalize_proxy_url` 对齐 account → runtime（仅原版 `upstream=True`）→ explicit/global 顺序；colon proxy 转 HTTP、凭据百分号编码，SOCKS 前缀检查 UTF-8 安全。`proxy_and_cookie_helpers_match_python_precedence/account_session_profile_matches_python_default_session_kwargs` 与 `tests::anonymous_native_chat_profile_falls_back_to_legacy_global_proxy` 覆盖。Python truthiness/egress normalization 含数值 `1` 由 `settings_bool/normalize_proxy_runtime` 与既有 global-prompt 用例覆盖。 |
| 自动移除限流/已确认失效账号时刷新成功计数及进度仍使用被删账号状态/quota | `services/account_service.py:update_account/fetch_remote_info/refresh_accounts/update_refresh_progress` | Rust `src/lib.rs:refresh_result_removes_account/AccountRefreshProgressSink::record` 对自动移除结果按缺失账号投影（不计 refreshed、正常、quota 0），并覆盖 invalid-token 确认阈值；`tests::account_refresh_rate_limited_removal_matches_python_counts_and_progress/refresh_invalid_token_deferral_matches_python_time_windows` 覆盖。 |
| editable 可选空 task id 与 Pydantic 根/JSON 验证细节；generation 空白 task id 在 Python service trim 后返回 400，而不是生成 UUID | `api/ai.py:EditableFileTaskRequest`；`api/image_tasks.py:ImageGenerationTaskRequest`；`services/editable_file_task_service.py:_submit`；`api/image_tasks.py:create_generation_task` | editable null/空/空白 id 仍生成 UUID；generation 精确空字符串按 Pydantic min_length 返回 422，空白字符串则返回 400 `client_task_id is required`；根数组/scalar 和 malformed JSON 仍 422。新增 `tests::image_task_whitespace_client_id_matches_python_bad_request` 和 route-level 400 断言。 |

该段仅总结 2026-09-28 那轮已检查的范围；本轮复核又确认并修复多项遗漏，见下节，不将旧轮次结论外推成全仓库零差异。

### 本轮明确保留、不能声称完全相同的边界

- access-token-only 边界保持不变：OAuth/密码 re-login、refresh/id token 持久化或完整三件套导出仍明确 unsupported；不为追求表面 parity 放开凭据边界。
- 为避免把任意 Python 上游异常文本原样暴露给其他 API 消费者，未知旧 editable task error 在 Rust 加载时折叠为 `editable file task failed`。已知原版 PSD/无账号错误保留；这是有意的错误文本投影差异，不能说任意异常字符串完全兼容。
- Pydantic 常见 model/stream 字段错误已字段化；尚未逐个复刻所有深层嵌套校验的每种 Pydantic `msg/input/loc`。Rust 对危险路径、快照规模和结构错误继续 fail-closed，错误细节采取泛化投影，避免反射攻击者输入。
- legacy/account proxy 优先级和格式通过本地 mock/profile tests 验证；未连接真实代理供应商验证 TLS、认证与系统网络环境。FlareSolverr、真实上游 conversation hide 仍属外部环境待验证项，不影响本地 tag 对照结论。
- 未部署、提交或改动前端；没有把 1.7 tag 后的 Python 功能带入兼容路径。

### 本轮最终验证

- `cargo fmt --all -- --check`：通过。
- `cargo check --workspace --all-targets --offline`：通过（本机首次缺 `libclang.dll`，本地已存在的 `.tmp-llvm/extracted/LLVM/bin` 仅用于该命令进程的 `LIBCLANG_PATH`；之后退出码 0）。
- 最终 `cargo test --workspace --offline -- --test-threads=1`：退出码 0；Rust lib 516 passed / 0 failed / 5 ignored，`chatgpt2api-file-identity` 6 passed / 0 failed，main/doc-tests 0 failed。`cargo fmt --all -- --check`、`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings` 亦通过；5 个 ignored 是明确标注的 post-v1.7 路由扩展。
- `git diff --check`：通过。前端未触及，未重跑前端 TypeScript/测试；未宣称线上代理供应商验证通过。

## 2026-09-28 扩展逐项复核

基线仍固定为 `v1.7.0` / `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`。本次继续检查公共 API/协议、图片任务与流、管理/持久化、账号/代理和对应测试断言，修正前文已证实错误的 task-id 与 SSE 描述。

| 新确认的差异 | Rust 修复与验证 |
| --- | --- |
| 图片任务 generation 的空白 `client_task_id`：Python service trim 后返回 400；Rust 原生成 UUID | Rust 现在返回 400 `client_task_id is required`；定向测试覆盖空格、tab 和 route 不落任务。 |
| 缺省 backup provider：Python 归一为 `cloudflare_r2`；管理备份路径 Rust 原投影成 `local` | 管理投影默认改为 R2；无 provider 的 fake-R2 route test 验证走 R2 而非静默本地归档。专测 local 归档显式声明 `provider=local`。 |
| 损坏/非对象 image-tags JSON：Python 按 `{}` 恢复，Rust 原返回不可用 | Rust GET 现在回空标签，POST 能修复 sidecar；删除未知标签对损坏文件返回 0 且不覆写。已有安全的路径/大小约束保留。 |
| 启用 auth record 缺少或含未知 role：Python 丢弃；Rust 原默认接受为 user | Rust 快照解析只纳入 `admin/user`，role trim/lower 后规范化；未知 role 回归确认不能成为认证记录。 |
| 配置整数使用 Python `int()`：小数截断、bool 和数字下划线字符串可转换；Rust 原回退默认值 | Rust settings `u64` 投影补齐这些 coercion；`settings_u64_matches_python_int_coercion` 覆盖。 |
| Responses 图片 `detail=low` usage、assistant 历史图像、图片输出 ID/revised-prompt fallback 不同 | 保留每张输入图的 detail；历史消息图像进入 image input；输出编号改为 `ig_1...`，逐项 trim 修订 prompt 并回退原 prompt。Responses 图片回归覆盖。 |
| Responses `tools: []` 仍受 `tool_choice:web_search` 影响；Python 将显式 tools list 视为权威 | Rust 现在仅在 tools 缺省/非 list 时检查 tool_choice；新增空 list/缺省/listed-tool 分支回归。 |
| 搜索来源只查 metadata 子集、正文 URL 按空格切分；Python 递归 assistant message 并识别括号结尾 URL | Rust 递归限定在公开 content 与已知 sources/citations 容器内，并从正文提取 URL；保留 100 项/字段长度上限和凭据 URL 过滤。搜索来源/正文回归通过。 |
| 搜索等待时间和 deadline 结果：Python 等待 300 秒且超时返回最后结果；Rust 原为 90 秒并丢弃 partial result | Rust 常量改为 300 秒、保存并在 timeout/重试睡眠结束时返回最近非空结果；partial deadline test 和 native-search 筛选集通过。 |
| 未知/错误 method 的 `/v1` 路由：Python 使用 OpenAI 兼容 error envelope，GET 未知路径回退 SPA；Rust 原为 Axum 默认响应且禁用 v1 SPA fallback | Rust `/v1` 404/405 返回 OpenAI error JSON，GET/HEAD 保持 Web fallback，其他方法转兼容 404；覆盖 `PUT /v1/models`、`PUT /v1/chat/completions`、`POST /v1/unknown`。 |
| 配置代理 URL 无法解析/代理 client 无法构建时，Rust 原回退默认直连 client | Rust 改用短超时、不可达代理的 fail-closed client；本地直连哨兵回归确认目标服务器命中数为 0。未做真实代理供应商测试。 |
| Web 图片账号池无可用额度：Python 返回 HTTP 429 `insufficient_quota`；Rust 原返回 503 | Web 与 Codex 图片无可用候选现在映射 429/`insufficient_quota`；空池、错误任务路由及 Codex plan 选择回归通过。 |
| 失败图片子任务后其余成功图的 SSE index 被压缩重排 | Web/Codex 合并时保留原始 1-based index；仅内部 Codex stream 使用的 `_image_index` 会在公开 SSE 前剥离。两条流的 partial index 2/3 回归通过。 |
| 多个输出文件中部分下载失败或字节相同：Rust 原可报告部分成功且不去重；Python 失败并去重 | Rust 对混合缺失/成功下载返回失败，并去重相同字节；常规下载主链路通过；混合失败与重复 body 尚无专门 mock 断言。 |
| 干净 HTTP EOF 后 Chat 无可见 delta：Python 仍输出空 assistant chunk 和 stop；Rust 原报上游失败 | Rust 在首个可见 delta 前有界预读、将干净 EOF 映射正常完成，传输错误仍是 gateway error；chat stream regression 通过。 |
| Python v1.7 Chat stream generator 不读取 `stream_options.include_usage`；Rust 原额外输出 usage frame | Rust 对该扩展忽略 `include_usage`，测试断言没有 usage-only frame，也不在其它 chunk 加 usage。 |
| storage health probe 失败时 Python 仍投影已加载的账号统计；Rust 原将 accounts 清零 | Rust health 将账号统计与 storage health 分开；坏存储但有效账号快照的回归验证统计保留，健康状态按 active 账号决定。health proxy runtime 也恢复完整非敏感状态字段，并报告 runtime/global/direct 的有效来源。 |

### 2026-09-28 再次按函数/分支核对

本轮仍以 tag `v1.7.0` / `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d` 为唯一 Python 基线。对照了 `api/app.py`、账户/系统/错误路由、配置与 storage/backup 服务；Chat、Responses、Messages、搜索与图片输入转换；图片任务、上游 SSE、图片下载和 CPA/Sub2API worker。不是跨语言逐行号映射，也不声称已逐字符审阅所有 Python 文件。

| 新确认差异 | Rust 修复与覆盖 |
| --- | --- |
| account 内顶层或 `fp` 的 browser fingerprint、OAI device/session id 被 Python `OpenAIBackendAPI._build_fp` 使用；Rust 原丢弃到请求头之外 | `NativeRequestContext::for_account` 现在映射 User-Agent、UA client hints、device/session ID，账号 PoW 也用相同 User-Agent；Chat、图片、搜索、Codex、模型目录及 metadata refresh 账号调用接入该 context。`account_fingerprint_overrides_native_browser_headers` 回归通过。Rust 的 TLS impersonation 仍固定 wreq Chrome110，不宣称支持任意 Python `impersonate` 值。 |
| Chat `thinking_effort` 与 `reasoning_effort` 同时出现时优先级相反；Python 会将未知 effort 归一为空 | `python_thinking_effort` 按 v1.7 的 `thinking_effort > reasoning_effort > reasoning.effort` 与允许值归一；Responses 顶层 effort 一并投影。新增 precedence/未知值用例通过。 |
| Responses 缺少 `input` 但含 `instructions` 时 Python 仍产生 system-only conversation，Rust 原提前拒绝 | `responses_text_to_chat_object` 将缺失 input 当空输入并保留 instructions；同一回归覆盖 Responses effort 优先级。 |
| Chat image-generation block `type=image` 及其中的 `source.base64`/`b64_json`/`url` 被 Rust 漏掉 | `native_chat_image_values` 增加对应 block 解码形状；定向测试覆盖 data URL、base64 source、b64_json。 |
| 图片轮询在 SSE 没有初始文件 ID 时，Rust 原把轮询途中发现但未 settle 的 ID 当作超时部分成功；Python 超时异常中只附 SSE 初始 IDs，解析层据此决定 partial fallback | Rust timeout/status/body error fallback 改为只使用进入 poll 时的初始 IDs，已发现但未确认的新 ID 不再伪造 Python partial success；poll policy 回归和完整 suite 通过。 |
| 图片 SSE 用整体 operation deadline，Python requests `timeout=300` 是读空闲超时；长时间持续有数据的流被 Rust 提前截断 | SSE parser 加 300 秒 sliding idle timeout；SSE 完成后另启 `image_poll_timeout_secs` 轮询窗口。新增跨过原 operation deadline 但每次间隔均小于空闲上限的测试通过。 |
| `/auth/login` 认证失败 envelope 和 legacy admin 名称；账号管理 JSON 解码失败状态 | login 现用 Python detail envelope，管理员名称为 `管理员`；共享账号管理 JSON reader malformed-body 改为 422。login route 增加成功/失败断言。 |
| 失败图片任务的 SSE conversation ID 是否已落盘，以便 `resume-poll` | 共用 log context 到 task error updates helper；端到端持久化测试确认 timeout 错误任务保存 conversation ID，且可解析为 resume context。 |
| 图片在途/调度恢复与 Python 进程状态模型不同 | backup `running` 改为进程内原子状态，磁盘写入对齐 Python `last_status=idle` 及 pending metadata；启动时旧版 `running` 快照折叠为 idle 并保留 pending key。后台 watcher/scheduler 以 shutdown signal 收尾，不再立即 abort。CPA/Sub2API interrupted job 恢复只改 status，保留进度、errors、updated_at；Rust-only ccLoad 扩展仍用 Rust 的中断说明。对应恢复回归通过。 |
| `/api/storage/info` 漏掉 v1.7 `get_backend_info()` 和 `health_check()` 的后端诊断字段 | 管理端恢复 JSON/DB/Git 类型、description、路径/exists、脱敏 URL、branch、commit/count；普通 `/health` 仍使用较小投影。未知 backend 强制不透出任意字段。storage info、DB/Git 路由和 malformed type fail-closed 回归覆盖。 |
| CPA/Sub2API worker restart 对中断计数与错误的归一不同 | 两项 Python 1.7 对应 job 仅将 pending/running 状态改为 failed；Rust 现保留现有 `completed/failed/errors/updated_at`。新增 counter/error 保留回归。 |
| Sub2API password 认证分页每页都重新登录，Python `_token_cache` 则按 server ID 缓存并提前 5 分钟刷新 | Rust 增加进程内有界 cache，以 server ID + credential fingerprint 验证身份，刷新窗口对齐 300 秒 skew；既有 5,200 账户/26 页测试改走 password login 并断言只登录一次。 |
| 搜索答案的空格标点清理差异 | 新增 search 专用 `native_clean_search_text`，只在 search 结果上去除 `.,;:!?` 前的空白并剥离首尾空白；普通 Chat sanitizer 不受影响。`native_search_answer_removes_whitespace_before_punctuation_like_python` 通过。 |
| Anthropic XML 参数实体只解码 XML 基本实体，Python `html.unescape` 还解码常见 HTML named entities | Rust 增加 `copy/nbsp/euro/hellip/mdash/ndash/trademark/quotes/currency/accent` 常用映射和 numeric decode；`anthropic_xml_tool_parser_decodes_common_html_named_entities` 覆盖常见符号。未声称完整 HTML5 entity 表一致。 |
| 多文件图片结果下载中单个失败但其余成功图仍被返回/字节重复未按原版去重 | Rust 已让混合失败整体失败、重复 bytes 去重；新增真实本地 HTTP mock 覆盖 `missing + good` 返回 upstream error 与两个不同 ID 的相同 body 只保留一份。 |
| Anthropic Messages SSE 已开始后生成器异常被 Rust 转成 body stream failure；Python 发 `event: error` envelope | Rust `xml_stream_error` 现在发 `event: error` 与 `{type:error,error:{type:RuntimeError,message}}` 后正常 EOF；过早 `[DONE]` 与 malformed JSON 回归覆盖。error type/message 使用安全通用 Rust 文本，不保证逐异常与 Python 类名/原始文本相同。 |

### 本轮验证和未解决边界

- `cargo test --workspace --offline -- --test-threads=1`：历史验证结果（非本轮最终证据）为 exit 0，Rust lib 528 passed / 0 failed / 5 ignored；`chatgpt2api-file-identity` 6 passed；main/doc-tests 0 failed。5 个 ignored 是显式标记的 post-v1.7 路由扩展。
- `cargo check --workspace --all-targets --offline` 和 `cargo clippy --workspace --offline --all-targets -- -D warnings`：均 exit 0；Windows 环境需对命令进程设置现有 `.tmp-llvm/extracted/LLVM/bin` 的 `LIBCLANG_PATH`。`cargo fmt --all -- --check` 通过。
- 仍未完成 Python 全仓逐文件逐分支的形式化清点，也未连接真实代理/上游。未解决项：完整 Pydantic 深层 `msg/input/loc`、未逐项映射的完整 HTML5 named entity 表，以及任意 Python `impersonate` TLS profile。Anthropic SSE error envelope 结构已对齐，但具体异常类型/文本仍不同。
- `/api/storage/info` 和 settings 的管理投影比 Python 更谨慎地过滤/脱敏某些字段；这是安全边界，不恢复密码、refresh/id token 或任意上游错误文本。Rust 的 worker admission 与输入/快照大小上限也是明确资源保护差异。
- access-token-only OAuth/re-login/export 限制、Rust 扩展 WebSocket/Codex/ccLoad、Python logger 与 Rust API 日志渠道差异及真实供应商行为仍不能声称完全一致。未提交、未部署、未修改前端。

## 2026-09-28 最新闭环：严格 tag 取证与账号字段字符串化

- 本次继续只把 `v1.7.0`（`1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`）作为 Python 基线。由于 `.local/public-minimal` 工作树 HEAD 不是该 tag，新增对照均通过 `git show v1.7.0:<path>` 读取，未把工作树后续 Python 内容当作证据。
- 对照 `services/account_service.py:_normalize_account`、`_normalize_source_type`、`services/openai_backend_api.py:OpenAIBackendAPI._build_fp` 和 `services/proxy_service.py:ProxySettingsStore.get_profile` 后，确认 Python 对 truthy 的非字符串字段使用 `str()`；容器使用 Python repr（单引号、`True`/`None`、递归容器），浮点指数保留 Python 的指数宽度。Rust `account_pool::python_account_value_string` 原先对数组/对象/数字直接使用 JSON 文本，导致 source/proxy/刷新状态/fp 投影可观察不同。
- Rust 现复用 Python repr 形状和已验证的 Python 数字指数格式化器；该转换继续受现有字段长度、快照大小和 access-token-only 约束。`account_pool::tests::account_value_string_matches_python_str_for_containers` 覆盖容器、字符串、`1e-7` 和 `1e20`，定向测试及完整 suite 均通过。
- 当前已确认的 profile 范围仍是 wreq `Profile::VARIANTS`：`impersonate` 会映射到 wreq 实际支持的 profile，`chrome`/`edge` 采用对应兼容 profile，未知值回退 `Chrome110`。这不能声称等同于 curl_cffi 的全部 impersonation 名称；真实代理、TLS 指纹和 FlareSolverr 供应商仍未在线验证。
- 最新代码验证（历史记录，非本轮最终证据）：`cargo test --workspace --offline -- --test-threads=1` exit 0，Rust lib 528 passed / 0 failed / 5 ignored，file-identity 6 passed，main/doc-tests 0 failed；`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check` 和 `git diff --check` 均 exit 0。测试过程未部署、未提交、未清理工作区。
- 本段复查覆盖账号归一化/指纹与 profile、代理优先级、协议转换和已有管理/任务/图片回归入口；未将这轮结果外推为 Python 全仓逐字符等价。access-token-only、深层 Pydantic 错误定位、完整 HTML5 entity、真实代理/上游和日志诊断渠道差异仍按上文边界保留。

## 2026-09-28 第二轮逐函数复核、修复与最终验证

本轮仍只使用 Python `v1.7.0`（`1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`）。`.local/public-minimal` 工作树 HEAD 不是该 tag，因此所有新增 Python 证据通过 `git show v1.7.0:<path>` 读取；没有把后续工作树代码当成基线。

### 本轮确认并修复的可观察差异

- 配置归一化：`settings_text`、浮点/布尔转换、状态码数组现在复刻 Python `str()`/`int()` 语义（容器 repr、`True`/`None`、`1e-7`、`403.9`、`"4_0_4"`）；`log_levels` 也接受 Python `str()` 后的数字/布尔值。新增 settings 回归覆盖。
- Responses：effort 优先级改为 Python 的 `reasoning.effort > thinking_effort > reasoning_effort`；非字符串 role 字段转为 Python 字符串；ChatGPT/web-chat Responses 流补 `response.output_item.done`。新增/更新 effort、numeric role、生命周期回归。
- Chat SSE：首个可见 delta 与 `role=assistant` 合并；干净 SSE EOF 按 Python 正常完成，不要求 `[DONE]`；已开始的上游流异常改为 OpenAI error SSE envelope 和 `[DONE]`。既有首帧、EOF、异常回归均更新并通过。
- Anthropic SSE：早 `[DONE]` 按 Python 正常结束并发送 `message_stop`，不再误报错误事件；既有 early-DONE 回归覆盖。
- Chat 兼容字段：标量 `tools` 按 v1.7 extra 字段行为接受；Responses role 字符串化；新增 scalar-tools 回归。
- 鉴权快照/API：缺失或非字符串 `enabled` 按 Python truthiness/默认 `true`；用户密钥公开投影只保留 Python 六个字段，不再透出未知内部字段；生成 key/ID 采用 Python 的 `sk-` + urlsafe 24 bytes、12 hex ID 形状。
- 图片/任务/管理：图片输入验证错误使用 Python 422 envelope；未完成图片任务启动恢复不因 PID 复用而保留 stale queued/running；默认未配置 R2 的备份列表返回空项；加密备份 detail 按 `.enc` 标记；备份 key 使用 `backup-YYYYMMDDTHHMMSSZ-<4hex>.tar.gz[.enc]`；新日志使用 UUID4 hex ID 和本地 `YYYY-MM-DD HH:mm:ss` 时间；JSON 账号保存恢复 Python 顶层 list，并把累计计数写入 `.cumulative_total` sidecar；数据库 `access_token_hash` 改为可空以兼容 Python v1.7 后续写入，非空 hash 仍严格校验。
- 图片轮询、账号指纹/profile、proxy precedence、缓存、导入、storage CAS 和任务恢复的新增/既有回归均重新编译验证；没有修改用户现有前端/构建产物。

### 明确保留的差异与边界

- Rust 仍保留 access-token-only：OAuth/re-login、refresh/id token 持久化和 Python 完整三件套导出不实现。
- Rust 保留认证先于 body 解析、通用 4 MiB 请求上限、图片输入/归档/快照/日志上限、multipart/远程图片 50 MiB 等 fail-closed 资源边界；Python v1.7 对部分路径没有同等限制，不能为表面 parity 拆除这些保护。
- Rust 搜索来源数量/字段长度、URL/conversation ID 校验和图片任务 ID 路径字符校验比 Python 更严格；这些是凭据注入、路径穿越和资源耗尽边界，不回退。
- Python 图片 Chat/Responses 的 text fallback、图片 401 body marker 分类和账号轮换已在 Rust 适配器/图片请求链实现；真实 text-only image upstream、供应商 401 body 和代理行为仍未在线验证。
- Python assistant-history prefix stripping 与逐条 echoed assistant history 跳过已在 Rust Chat conversation SSE 实现；剩余未完全穷举项是完整 HTML5 named entity 表和深层 Pydantic `msg/input/loc` 细节，真实 conversation hide 仍未连接供应商验证。
- Rust 生产 `/v1/responses` 已恢复 POST-only；Codex/ccLoad 管理能力仍是明确的 post-v1.7 扩展，不作为 Python parity 声明。

### 本轮最终验证

- `cargo test --workspace --offline -- --test-threads=1`：exit 0，Rust lib `531 passed / 0 failed / 5 ignored`；`chatgpt2api-file-identity` 6 passed；main/doc-tests 0 failed。
- `cargo check --workspace --all-targets --offline`：exit 0。
- `cargo clippy --workspace --offline --all-targets -- -D warnings`：exit 0。
- `cargo fmt --all -- --check`：exit 0；`git diff --check`：exit 0。
- 未提交、未部署、未连接真实线上代理/上游、未清理工作区；既有用户修改和未跟踪产物均保留。

## 2026-09-30 第三轮逐函数复核、图片适配和管理投影闭环

本轮继续只使用 Python `v1.7.0`（`1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`）作为证据源；对照文件均通过 `git show v1.7.0:<path>` 读取。未把当前 `.local/public-minimal` 工作树内容当作基线。

### 本轮已修复的可观察差异

- 普通 Chat 的 conversation SSE 现在同时按 Python 的 `assistant_history_text` 和 `assistant_history_messages` 处理：移除上游重复的 assistant 历史消息，继续做拼接前缀裁剪；patch `replace` 也按 Python 语义替换并裁剪。保留首帧 role、干净 EOF、错误 envelope 和资源/超时边界。
- Chat/Responses 图片适配器遇到 Python `ImageOutput(kind="message")` 对应的明确文本、策略或轮询错误时，现在投影普通 assistant 文本完成；Chat 覆盖 non-stream/stream，Responses 覆盖 non-stream/stream。direct `/v1/images/generations`、`/v1/images/edits` 仍保留 `message_as_error` 错误语义。
- Web/Codex 图片请求的 401 现在读取有界 response body，仅在 `token_invalidated`、`token_revoked`、`authentication token has been invalidated`、`invalidated oauth token` 等明确 marker 下走 invalid-token 账号记录/换号；普通 401 仍返回 unauthorized，不误判 token。
- `/v1/images/edits` JSON malformed/scalar body 按 Python 返回 HTTP 400 的 `invalid JSON body` / `JSON body must be an object`；认证仍先于 body 解析，大小和图片资源上限不变。
- `/v1/responses` 生产公开路由恢复 Python v1.7 的 POST-only；Responses WebSocket GET 仅保留测试构建使用，以维持 Rust 自有 websocket 单元回归而不扩大线上 API 面。
- Chat SSE 恢复 Python `sse_json_stream` 的 `: stream-open` 注释前导；测试解析跳过注释。既有错误脱敏策略不反射任意上游异常原文。
- 管理/存储公开投影：legacy auth 记录加载时补 Python 的 id、角色默认 name、UTC `created_at`、空值 `last_used_at`；`/api/proxy/test` 返回 Python 的 `{"result": ...}` 包络，非法 proxy URL 返回普通结果对象；`/health` proxy runtime 使用当前有效 clearance cache hosts；图片任务错误持久化 `data: []`；CPA/Sub2API 稀疏 import job 公共投影补齐 Python 的 job/status/timestamp/counter/errors 默认字段。

### 本轮验证

- `cargo test --workspace --offline -- --test-threads=1`：Rust lib `531 passed / 0 failed / 5 ignored`；file-identity 6 passed；main/doc-tests 0 failed。此前图片/Chat/Responses/401/history targeted tests 也均通过。
- `cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check` 均完成且无编译、lint 或格式诊断；Windows 命令继续使用现有 `LIBCLANG_PATH=E:\项目\chatgpt2api\.tmp-llvm\extracted\LLVM\bin`。
- 未提交、未部署、未连接真实代理/FlareSolverr/线上上游，未清理工作区；现有用户修改和未跟踪产物均保留。

### 继续复核后的状态

- Git storage 账号写入已改为 Python v1.7 可读取的顶层数组；累计总数写入同一 Git commit 的 `.cumulative_total` sidecar。Rust 继续兼容旧 `{"items": [...], "cumulative_total": ...}` envelope，并在加载时同时读取 sidecar；CAS、pending marker、原子写、路径校验和健康刷新仍保留。
- 图片任务和 editable task 新时间戳已切换到 Python v1.7 的本地 wall-clock `YYYY-MM-DD HH:mm:ss`；账号 `created_at` 的 UTC 和 `last_used_at` 的本地时间分工未改变。
- Rust backup 默认 include/archive 现在按 Python v1.7 不包含 ccload；显式 `ccload: true` 仍保留 Rust 扩展能力，不再产生默认投影差异。
- access-token-only、refresh/id token 持久化限制、真实供应商/TLS/代理/FlareSolverr 行为以及 Python 深层 Pydantic 错误定位仍受既有边界或外部验证条件限制。

## 2026-09-30 第四轮逐函数复核、管理输入与认证顺序闭环

本轮仍只使用 Python `v1.7.0`（`1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`）作为基线证据，新增对照均通过 `git show v1.7.0:<path>` 读取。

### 本轮确认并修复

- 管理用户密钥新增/更新改为认证成功后才读取 JSON body，并增加 malformed body + 错误凭据回归；认证不会被 body 解析抢先触发。
- auth snapshot parser 兼容 Python JSON/Git loader 的顶层数组与 `{"items": [...]}` envelope；非法 record 仍 fail-closed，mutation 会规范化写回 envelope。
- `/api/accounts/delete`、`refresh`、`export` 对 Pydantic `list[str]` 字段拒绝标量和混合元素；export 空数组仍按 Python 语义选择全部账号，`format` 只接受精确 `json`/`zip`。Sub2API/CPA import 的字符串数组和 Sub2API server 字段也收紧为 Python model 的字符串类型。
- 图片管理 body 的 `all_matching/start_date/end_date`、标签、日志删除、proxy test/clearance、backup delete 字段按 Python request model 做类型校验；保留既有业务错误和资源限制。
- `/api/images` 默认 URL/缩略图 URL 现在使用 Python 的 `public_base_url` 或请求基址绝对投影；`/api/images/storage` 按 Python `storage_stats()` 统计目录下所有常规文件，不只统计图片扩展名。
- `/api/proxy/test` 补齐 Python proxy test 的 User-Agent。
- Chat、Responses、Search、Anthropic Messages 的外层日志 wrapper 在读取 body 前先认证；Anthropic 保留 `x-api-key` fallback。新增 malformed body + 无凭据回归覆盖 401 先于 400/422。

### 本轮验证

- 串行 `cargo test --workspace --offline -- --test-threads=1`：Rust lib `533 passed / 0 failed / 5 ignored`；file-identity 6 passed；main/doc-tests 通过。此前一次全量中的 native requirements 本地 mock 502 单跑通过，随后再次全量通过，判定为测试环境时序波动而非确定性回归。
- `cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo check --workspace --all-targets --offline`、`cargo fmt --all -- --check`、`git diff --check` 的真实 PowerShell `$LASTEXITCODE` 均为 `0`；Windows Cargo 使用现有 `LIBCLANG_PATH=E:\项目\chatgpt2api\.tmp-llvm\extracted\LLVM\bin`。
- 未提交、未部署、未连接真实代理/TLS fingerprint/FlareSolverr/线上供应商，未清理工作区；既有用户修改和未跟踪产物均保留。

### 仍明确保留的边界

- access-token-only 仍不持久化或导出 refresh/id token，OAuth/re-login/refresh-token keepalive 和 Python 完整三件套导出继续不实现。
- Rust 的认证先行、请求/图片/归档/快照大小上限、路径校验和安全错误投影继续优先于表面 parity；深层 Pydantic `msg/input/loc`、完整 HTML5 named entity、真实代理/上游行为和 conversation hide 仍不能声称完全验证。

## 2026-09-30 第五轮：协议辅助、认证调用链与后台任务逐函数复核

本轮仍只使用 Python `v1.7.0` / `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`；新增 Python 证据均通过 `git show v1.7.0:<path>` 读取，没有把当前 `.local/public-minimal` 工作树当作基线。

本轮确认并修复：

- Chat/Responses/Messages/Search 外层 wrapper 保留 body 解析前认证，内部已认证调用路径不再重复 reload/mark-used；OpenAI Search 也不再因内部 proxy 再次认证。
- Anthropic `Authorization`/`x-api-key` 按 Python 原始 truthiness fallback；空字符串 Authorization 不再阻断有效 `x-api-key`。
- editable task 指定 `ids` 按请求顺序返回；image task 只在启动初始化阶段恢复 queued/running，普通读取/列表不会把 live 任务改成重启错误。
- Chat cache 先做 Python message role/content canonicalization，再执行去重；读取/开始阶段全量 prune 过期/超出 max 项；stream prime 失败清理 inflight；owner 创建时保存 TTL/max，完成时使用调用开始快照。
- Responses 缺省 model 的 Chat cache key 保留 Python `null`，thinking effort 优先级改为 `thinking_effort → reasoning_effort → reasoning.effort`；assistant image input 归一为空文本而不直接拒绝。
- Search query 支持 Python 的 role 大小写/空白及文本字符串化，多字段 search part 全量拼接；纯数字 annotation part 会过滤；无来源 Chat search 不输出空 annotations。
- Chat `messages` 非数组与 `/v1/search` prompt validation 补字段定位消息；图片后台任务修正 worker 计时起点、错误 message 投影、resume 的空 `revised_prompt` 和带等待秒数的无结果文本。

仍明确保留的边界：上游错误继续使用 Rust 的安全通用 envelope，不反射任意供应商诊断文本；搜索 URL 继续执行 HTTP(S)/凭据/控制字符/percent encoding 的 fail-closed 校验；access-token-only、资源上限、路径/快照校验和真实代理/供应商行为边界不放宽。

本轮定向验证已通过：cache canonical/inflight、content filter、Chat/Responses/Search/annotation、validation、image task snapshot/resume/elapsed/error-log 共 18 项 targeted tests；最终 `cargo test --workspace --offline -- --test-threads=1` 为 Rust lib 533 passed / 0 failed / 5 ignored，file-identity 6 passed，main/doc-tests 通过。最终 `cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check` 均为 0。

## 2026-09-30 第六轮：图片输入与 conversation payload 继续逐函数复核

本轮继续只使用 Python `v1.7.0` tag 证据。新确认并修复：

- 普通 Chat 图片块现在支持 Python 的 `image_url`、`input_image`、`image`、URL/data/base64/source 形状；multimodal conversation parts 按 Python 文本在前、图片在后的顺序构造。普通 Chat role 继续按其 Python 精确语义处理，Chat 图片生成识别则按 helper 的 trim/lower 语义处理。
- Responses image input 补齐 `input_text` 字段 fallback，以及 `image`/`input_image` 的 URL、base64、source 形状。
- Chat/Responses 远程图片引用使用 Python helper 的 20 秒 vision fetch budget；图片编辑路径保留独立的 60 秒资源读取预算。
- Chat/Responses 的远程图片在完成下载后写入本地 usage 投影，图片 token 不再因原始 HTTP URL 缺少尺寸而归零；URL-only 图片生成 usage 按 Python 请求尺寸 fallback 计算，图片生成 prompt token 保留原始首尾空白语义。
- 图片 SSE 文件 ID 提取收紧为 Python 的 `file_00000000` + 24 位小写十六进制规则，同时保留 `file-service://`/`sediment://` 规则；普通 `file_upload_business_upsell` 等文本不再误入轮询。
- 图片 SSE 非 JSON payload 按 Python `conversation.raw` 继续推进，不再因单个坏 JSON 直接中止整条流；传输错误、超限和超时仍 fail-closed。

新增/更新图片与 SSE targeted tests 均通过；最终 `cargo test --workspace --offline -- --test-threads=1` 为 Rust lib 535 passed / 0 failed / 5 ignored，file-identity 6 passed，main/doc-tests 通过。最终 `cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check` 均为 0。

## 2026-10-01 第七轮：管理/输入范围再次逐函数闭环

本轮继续只使用 `git show v1.7.0:<path>` 读取 Python 基线，没有使用 `.local/public-minimal` 作为对照。复核后确认并修复：

- 图片编辑字段的容器字符串化改为 Python repr 形状，空 list/dict 的 `n` 按 Python falsey 规则回退 1；通用 data URL 恢复 Python 的 `data:`/`;base64` 大小写敏感语义，editable 图片保留 Python 正则对应的大小写不敏感专用解析。
- Web 生图上传固定 `image_{n}.png`；Chat/Responses web-chat 多模态上传按 Python MIME 派生 `image_{n}.jpg` 等文件名。图片数字 repr 复用 Python 指数宽度格式化，`1e-7` 投影为 `1e-07`。
- Anthropic `type=image`、`image_url`、`input_image` 的 URL/base64/source 形状和远程 HTTP(S) 下载路径统一处理；Messages 的根对象、messages 元素、stream 字段校验保留字段定位消息；XML 参数使用完整 HTML named-entity 解码。
- editable 图片格式补齐 ICO 及其 MIME，结果 URL 的 base URL 投影保留 Python 的路径安全语义；文件名 URL segment 保留 Python `quote(..., safe="/")` 的 `/`、`.`、`-`、`_`、`~`。
- 图片 ZIP 按 v1.7.0 文件后部实际生效的 local-only 定义跳过 WebDAV-only 项；单图 WebDAV 读取保持独立。backup 缺省 settings/archive 不再注入或归档 `ccload`，显式字段仍作为 Rust 扩展保留；backup metadata 不写 `object_key`。

最终复核未发现新的本地确定差异；仍明确保留的边界是 malformed JSON / 深层 Pydantic `loc/msg/input` 的精确投影、无显式请求 scheme 时真实部署连接 scheme、真实代理/TLS/FlareSolverr/供应商和 conversation hide 行为。安全认证顺序、access-token-only、资源/路径/快照上限未因 parity 修改而放宽。

本轮本地检查：`cargo fmt --all -- --check` 通过，`cargo metadata --no-deps --format-version 1 --offline` 通过，`git diff --check` 无空白错误，`cargo test -p chatgpt2api-file-identity --offline` 为 6 passed。workspace `cargo check --workspace --all-targets --offline` 已重新触发，但当前环境在 `btls-sys` bindgen 阶段因找不到 `clang.dll/libclang.dll` 停止；因此未将本轮标记为 workspace 全量测试或 Clippy 通过，也未提交、部署或清理工作区。

## 2026-10-01 第八轮：最终差异复核与存储/诊断收口

在第七轮修复后重新触发同范围 `v1.7.0` tag 对照，又确认并修复：

- Anthropic 图片 block 选择 URL 时按非空可解析值依次回退 `image_url → url`；空 `image_url` 不再遮蔽有效 `url`。
- 数据库 storage 的账号/鉴权保存改为 Python `_save_rows` 语义：事务内先清空目标表，再按 incoming 顺序插入；读取顺序不再因旧自增 id 保留而偏离 Python。
- backup 公共 state 移除 Python 没有的 `pending_object_key`，磁盘内部恢复状态仍保留；日志 ID 设置 UUID4 version/variant bits，继续输出 32 位小写 hex。
- `/v1/messages` malformed JSON 错误按 JSON byte offset 投影为 `N: JSON decode error`，不回显 serde 原始诊断文本；深层 Pydantic `input/ctx` 仍按既有安全裁剪边界处理。

这次修复后的同范围复核未发现新的本地确定差异。仍不能在本地证明的只有实际部署连接 scheme、真实代理/TLS/FlareSolverr/供应商/conversation hide 行为，以及不影响安全边界的深层 Pydantic 原始错误细节。未提交、未部署、未清理工作区。

最新验证：`cargo fmt --all -- --check`、离线 `cargo metadata`、`git diff --check` 均无源码/格式问题；`cargo test -p chatgpt2api-file-identity --offline` 的 cargo 退出码为 0，6 个单元测试和 doc-tests 均通过。`cargo check --workspace --all-targets --offline` 已重新触发，依赖编译推进至 `btls-sys` 后因环境缺少 `clang.dll/libclang.dll` 停止，故本轮未声称 workspace 全量测试或 Clippy 通过。

最终 tag replay 还补齐了一个共享图片回退边界：Anthropic adapter、普通 Chat image helper 与图片 token 统计现在都会在 `image_url` 为空时回退到同级 `url`；对应 v1.7.0 `utils/helper.py` 的 truthy `or` 语义已闭环。数据库保存顺序、backup 公共 state、UUID4 日志 ID 和 Messages JSON byte offset 修复后再次复核，未发现新的本地确定差异。
## 2026-10-02 第九轮：图片任务/管理 writer 修复后的最终 tag replay

本轮仍以 `v1.7.0`（`1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`）为唯一 Python 基线；本轮 Python 证据均通过 `git show v1.7.0:<path>` 读取，未使用 `.local/public-minimal` 工作树内容替代基线。

### 本轮确认并修复的本地可观察差异

| v1.7.0 证据 | Rust 差异、修复和回归 |
| --- | --- |
| `services/image_storage_service.py:_now_iso`、`ImageStorageService._load_index/_save_index`、`_write_json_object`：图片索引时间使用 `datetime.now()` 本地 wall-clock，JSON 使用 `ensure_ascii=False, indent=2` 并追加换行；`services/image_tags_service.py` 具有同一 writer 形状。 | `src/management.rs:local_image_index_item` 的 mtime fallback 改为 `local_timestamp(metadata.modified())`；`write_json/write_json_unlocked` 统一使用 pretty JSON 并追加 `\n`。`management::tests::image_index_mtime_fallback_uses_local_wall_clock` 与 `management::tests::management_json_writers_match_python_pretty_newline_contract` 各真实通过 1 项。 |
| `services/image_task_service.py:ImageTaskService._run_task`：handler 返回非 `dict` 时抛出 `image task returned streaming result unexpectedly`；空/非列表 `data` 优先使用 upstream `message`，否则使用完整的号池限流 fallback；`_log_call` 的 generate/edit 摘要分别为“文生图/图生图”。 | `src/lib.rs:normalize_image_task_response/image_task_empty_result_error/image_task_log_summary/run_image_task` 完成同一投影；`write_tasks_unlocked` 使用 pretty JSON 并追加 `\n`，成功/失败调用日志继续携带已有的受限图片元数据。`tests::image_task_empty_result_and_log_summary_match_python` 真实通过 1 项。 |
| `services/image_task_service.py:_save_locked` 与 `services/editable_file_task_service.py:_save_locked`：任务快照均为 `json.dumps(..., ensure_ascii=False, indent=2) + "\\n"`；两服务的 `_now_iso` 使用本地 wall-clock。 | `src/lib.rs:write_tasks_unlocked` 与 `src/editable_file_generation.rs:encoded_tasks` 均满足该 writer 形状；editable 的本地时间和既有恢复/写入回归保持不变。本轮没有因表面 parity 删除 editable 的文件大小、路径或快照保护。 |

本轮还复核了 Anthropic 图片参考形状。`v1.7.0:utils/helper.py:_decode_json_image_string` 对 base64 使用 `validate=True`，因此回归夹具中的原始 `AQI` 修正为合法的 `AQI=`；这是测试输入修正，不是放宽 Rust 解码边界。

### 最后一轮范围 replay

修复后重新按函数和调用链复核以下范围，未发现新的、确定且可在本地观察的待处理差异：

- 协议：`src/protocol_chat.rs`、`src/protocol_responses.rs`、`src/protocol_anthropic.rs` 及 `src/lib.rs` 的 route wrapper、payload、SSE、usage、图片适配器和搜索调用链，对照 `git show v1.7.0:services/protocol/conversation.py`、`services/protocol/openai_v1_chat_complete.py`、`services/protocol/openai_v1_response.py`、`services/protocol/anthropic_v1_messages.py`、`api/ai.py` 和 `api/image_inputs.py`；认证先行、错误脱敏、POST-only 和资源上限继续保留。
- 图片/任务/管理/editable：对照 `git show v1.7.0:services/image_storage_service.py`、`services/image_task_service.py`、`services/editable_file_task_service.py`、`api/image_tasks.py`、`services/image_tags_service.py` 和 `api/system.py`；本轮上述 mtime、newline、response 类型、message fallback 和日志摘要已闭环。
- 账号/代理/配置/运行时：复核 `src/account_pool.rs`、`src/proxy_service.rs`、`src/config.rs`、`src/shutdown.rs` 与 `src/lib.rs` 的刷新、profile、设置 coercion、后台 owner/admission 生命周期，对照 `git show v1.7.0:services/account_service.py`、`services/proxy_service.py`、`services/config.py`、`api/app.py`；没有新的本地确定差异。真实代理、TLS fingerprint、供应商连接仍不由本地 mock 证明。
- WebSocket、备份、日志和存储：复核 `src/responses_websocket.rs`、`src/management.rs`、`src/storage.rs`、`src/lib.rs` 的备份状态/日志投影/JSON、DB、Git CAS 和 public storage 投影，对照 `git show v1.7.0:services/backup_service.py`、`services/log_service.py`、`services/storage/json_storage.py`、`services/storage/database_storage.py`、`services/storage/git_storage.py` 和 `services/storage/factory.py`。v1.7.0 没有对应的 Responses WebSocket 实现，WebSocket 是明确的 Rust 扩展；backup 的内部 pending 状态、日志敏感字段裁剪、CAS/大小限制和 fail-closed 行为按安全边界保留。

### 本轮验证

- `cargo fmt --all -- --check`：命令内退出码 `0`。
- `cargo metadata --no-deps --format-version 1 --offline`：命令内退出码 `0`。
- `git diff --check`：命令内退出码 `0`；Git 仅报告工作区文件的 LF/CRLF 提示，没有 whitespace error。
- `cargo check --workspace --all-targets --offline`：设置当前命令进程 `LIBCLANG_PATH=.tmp-llvm/extracted/LLVM/bin` 后退出码 `0`，日志为 `Finished dev profile`。未设置该临时路径时退出码为 `101`，`btls-sys` 的 bindgen 精确报错为 `Unable to find libclang: "couldn't find any valid shared libraries matching: ['clang.dll', 'libclang.dll'], set the LIBCLANG_PATH environment variable to a path where one of these files can be found (invalid: [])"`；两种结果均未被混写成无条件环境通过。
- 本轮新增/受影响定向测试均由当前 test binary 真实执行并通过：图片任务 `1 passed / 0 failed`；Anthropic 图片参考 `1 passed / 0 failed`；management JSON newline `1 passed / 0 failed`；image-index 本地时间 `1 passed / 0 failed`。
- `cargo test --offline --package chatgpt2api-rust --lib` 的并发运行曾返回 `544 passed / 3 failed / 5 ignored`，失败为 PoW 本地 mock deadline、catalog shutdown 时序和 storage health 并发状态；三个失败项逐个直接运行均为 `1 passed / 0 failed`。随后使用同一当前 test binary 串行 `--test-threads=1` 完整运行，退出码 `0`，结果为 `547 passed / 0 failed / 5 ignored`。因此不宣称并发 cargo suite 绿，但有完整串行 suite 的真实通过证据。
- 未提交、未部署、未清理工作区；未修改前端和无关用户产物。

### 明确保留和无法由本地证明的边界

- access-token-only 产品边界不变：OAuth/re-login、refresh/id token 持久化或导出、Python 完整三件套账号导出继续不实现；不为 parity 恢复这些凭据路径。
- 认证先于 body 解析、图片/任务/快照/归档/日志大小上限、路径校验、快照校验、错误脱敏和 fail-closed 行为继续优先于 Python 表面差异。
- 真实代理供应商、TLS 指纹、FlareSolverr、线上图片/对话上游、conversation hide、HTTPS 部署 scheme 以及 Python logger 与 Rust 持久化日志渠道不能由本地代码证明，未宣称一致。
- 深层 Pydantic `msg/input/loc` 的全部组合、完整线上异常文本和 Rust WebSocket 扩展没有被宣称为 v1.7.0 完全等价。

本轮最终 replay 在上述范围内没有新的确定可操作差异；后续未解决项均属于已明确的产品边界、Rust 安全/资源强化或需要真实外部依赖的行为。
## 2026-10-02 最终 v1.7.0 replay：全范围闭环

本节重新以 Git tag `v1.7.0`（`1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`）为唯一 Python 基线；本节所列 Python 函数均由本轮 `git show v1.7.0:<path>` 读取，未使用工作树 Python、旧审计段落或旧测试计数作为 parity 证据。

### 公开入口、协议适配和调用链

- 当前 `src/lib.rs::AppState::router` 的 `/v1/models`、图片 generations/edits、Chat、Responses、Messages、Search、editable、PPT、PSD、文件下载和 image-tasks 路由，逐项对照 tag 的 `api/ai.py`、`api/image_tasks.py`、`api/accounts.py`、`api/system.py` decorators；`/api/auth/users`、账号 refresh/export/update、图片/标签/日志/代理/backup/storage/settings 也逐项核对对应 Rust handler。共享公开入口的认证先行、body/字段校验、上游选择、结果/SSE 投影和安全错误 envelope 已一致或已在前文记录为已修复。
- `src/protocol_chat.rs`、`src/protocol_responses.rs`、`src/protocol_anthropic.rs`、`src/protocol_codex_payload.rs`、`src/codex_sse.rs` 及 `src/lib.rs` 的真实 route-to-handler 链，重新对照 tag 的 `services/protocol/conversation.py`、`services/protocol/openai_v1_chat_complete.py`、`services/protocol/openai_v1_response.py`、`services/protocol/anthropic_v1_messages.py`、`api/ai.py` 和 `api/image_inputs.py`。`native_message`、Responses payload 校验、Codex tool/SSE 终止、usage、图片适配、搜索 query/text/citation 递归投影均按实际调用点核对，没有使用未调用 helper 或异常/截断输出推断行为。
- Chat 文本公开链实际只解析并保留 route-specific 的 `n`，未恢复未调用 helper 的 singleton 限制；图片入口仍按 Python 的 `1..=4` 约束。Responses 缺省/空 `input`、仅 `instructions`、空 content、content `type` trim 但 role 原值、消息兜底和最终 Chat/conversation payload 均在 route boundary 核对；确定差异已由对应 targeted tests 覆盖。
- 搜索实际走 `search` handler 到 native search answer/citation 投影，文本标点空格、角色/文本字符串化、URL 安全过滤和无来源 annotations 分支均保留；普通 Chat sanitizer 不被 search 专用清洗规则改变。

### 管理、图片任务和 editable

- `src/management.rs` 的 `list_images/delete_images/download_images/download_single_image/image_storage/compress_images/cleanup_images`、tags、proxy/clearance、CPA/Sub2API/ccLoad registry/import、`test_image_storage/sync_image_storage` 和 backup handlers 与 tag `api/system.py`、`api/accounts.py`、对应 `services/image_storage_service.py`、`services/image_task_service.py`、`services/editable_file_task_service.py` 的调用链已复核。任务 writer 使用 pretty JSON、`ensure_ascii=False` 和尾部换行；mtime fallback 使用本地 wall-clock；非对象 task result、上游 message 优先级、文生图/图生图日志摘要与 tag 分支一致。
- `src/editable_file_generation.rs` 的任务编码、恢复、PPT/PSD 账号筛选、文件下载和错误终态对照 tag `services/editable_file_task_service.py`；Rust 继续保留文件路径、快照大小、资源读取和 fail-closed 限制，不以表面 parity 删除安全边界。
- 管理投影继续剔除 refresh/id token 和内部 backup/log 字段；公开 URL、proxy runtime、storage info、导入 job 的缺省字段、时间戳和错误状态均按 tag 公共 API 形状投影，Rust 的敏感字段裁剪和错误泛化属于有意安全差异。

### storage、健康和持久化

- `src/storage.rs::StorageBackend` 的 JSON、Database、Git 三个分支及 `load_accounts/save_accounts/load_auth_keys/save_auth_keys/load_health_snapshots/info/close` 调用链，重新对照 tag `services/storage/base.py`、`services/storage/factory.py`、`json_storage.py`、`database_storage.py`、`git_storage.py`。backend 选择、默认 SQLite、公共 info/health 字段、账号顶层数组和 auth-key `{items: ...}` envelope 均已闭环。
- JSON 分支保留 Python 的 pretty/newline writer 和数组/envelope 读取兼容；Rust 额外执行文件类型、版本、大小、CAS revision、累计总数 sidecar、原子替换和 snapshot consistency 检查。数据库分支保留 Python `_save_rows` 的先清空再按 incoming 顺序插入行为，同时继续使用 access-token hash、唯一性、schema migration 和事务/并发锁；Git 分支保留 JSON 读写、branch/file projection 和脱敏 info，同时增加安全 clone/cache、健康 refresh、候选 ref 清理和 CAS/fail-closed 校验。后述强化不是应回退的 parity 差异。
- `health_accounts/health_storage/health_payload_with_storage` 将已加载账号统计与 storage probe 分离，storage probe 失败不会把有效账号统计清零；`/api/storage/info` 仍按 tag `get_backend_info()`/`health_check()` 组合投影，敏感连接信息只输出脱敏值。

### 配置、代理、运行时和生命周期

- `src/config.rs`、`src/lib.rs::proxy_runtime_value`、`AccountTypeCatalog::model_catalog_client`、`src/management.rs::runtime_value/runtime_status` 对照 tag `services/config.py` 的 `_read_json_object`、`_normalize_*`、`ConfigStore.get/update/get_proxy_runtime_settings`。配置缺失/损坏回退、Python `str/int/bool` coercion、backup/image/cache/third-party/proxy runtime defaults、secret masking 和 runtime 状态来源均已复核；raw runtime 读取点已统一经过 `normalize_proxy_runtime`，保留环境默认 fallback。
- `src/proxy_service.rs` 的 profile precedence、URL/host/domain 归一化、cookie 合并、FlareSolverr 合法条目/`userAgent` 容错与 clearance single-flight 对照 tag `services/proxy_service.py::ProxySettingsStore.get_profile/build_headers/refresh_clearance/test_proxy`。本地 mock 已证明可观察的 profile/headers/cookie 分支；真实代理、TLS impersonation 和 FlareSolverr 供应商仍不由本地代码证明。
- `src/shutdown.rs` 与 `src/lib.rs::account_refresh_watcher/image_cleanup_scheduler/backup_scheduler` 核对 tag `AccountService` 生命周期和 `BackupService` 30 秒调度。Rust 在 shutdown 前先关闭 admission/worker owner，再发 graceful signal，并等待 watcher、清理、backup、storage close；这是可本地验证的生命周期收口。

### WebSocket、备份和日志

- `src/responses_websocket.rs::run/Session/prepare/commit` 的事件校验、previous response transcript、容量/连接/寿命限制、HTTP fallback、Codex upstream payload 和安全错误对照 tag 全部 Python 路由后，tag `v1.7.0` 只找到 `websocket_request_id` 数据字段，没有 `@router.websocket` 或 Responses WebSocket server route。因此该 GET `/v1/responses` 入口仅在 Rust `cfg(test)` 下注册，是明确的 Rust-only 扩展，不声称与 v1.7.0 Python parity。
- `src/management.rs` 的 `backup_settings/backup_state_after_restart/backup_schedule_due/build_backup/backup_items/run_backup_impl/list_backups/backup_detail/download_backup` 对照 tag `services/backup_service.py`：缺省 provider 为 `cloudflare_r2`，默认 include 不注入 ccload，显式 ccload 仍是 Rust 扩展；backup key、加密 suffix、metadata、rotation、pending/restart recovery 和公共 state 已按可观察分支复核。Rust 保留本地备份、R2 key 校验、锁、大小/成员上限、状态 CAS 和敏感错误裁剪。
- `src/management.rs::append_log/project_log/log_values/list_logs/delete_logs` 对照 tag `services/log_service.py::LogService` 与 `LoggedCall`：新日志使用 UUID4 hex 和本地时间，旧记录缺 id 用 SHA-1 fallback，公开 detail 只保留允许字段/有界 URL/结构统计，删除保留非目标原始字段；调用链继续携带受限 request summary、account/conversation 引用和错误，不导出 token。

### 最终 replay 结论与修改边界

- 上述范围重新按文件、函数、路由、下游调用和分支核对后，没有发现新的、确定、可由本地代码观察且属于本任务范围的待处理差异。已确认差异均已在当前 Rust/必要回归测试中修复；access-token-only、认证先于 body、资源/路径/快照上限、错误脱敏、CAS 和 fail-closed 行为均保留。
- 本轮发现 `src/protocol_chat.rs` 的 plain-text sanitizer 回归函数缺少 `#[test]`，其首次 cargo filter 实际为 `0 tests`，未被当作行为证据；已补齐测试属性并重新真实执行 1/1。该修改只补回归覆盖，不改变生产逻辑。
- 未恢复 OAuth/re-login、refresh/id token 持久化或完整导出；未连接真实代理、TLS、FlareSolverr、线上供应商或 conversation hide；未提交、未部署、未清理工作区。

### 本轮新鲜验证

- 定向代理集：`cargo test --offline --lib proxy_service -- --test-threads=1`，`7 passed / 0 failed`，命令内 `CARGO_EXIT=0`。
- 内容过滤定向测试：`content_filter_matches_python_text_extraction_and_config_semantics`，`1 passed / 0 failed`，`CARGO_EXIT=0`。
- Responses 空 input/type、Anthropic 图片参考、图片数字 repr、图片任务 message/log summary、管理 JSON writer、image-index 本地时间和 UUID4 日志 ID 定向测试均真实运行并通过；补属性后的 `native_sanitize_text_removes_space_before_punctuation_for_plain_text` 为 `1 passed / 0 failed`。此前该 filter 的 `0 tests` 未计入通过证据。
- `cargo test --workspace --offline -- --test-threads=1`：`CARGO_EXIT=0`；file-identity target `6 passed / 0 failed`；Rust 主库 target `550 passed / 0 failed / 5 ignored`；另有 3 个 target 显示 `0 tests`，仅如实记录，未作为行为覆盖证据。
- `cargo fmt --all -- --check`：`FMT_EXIT=0`。
- `cargo metadata --no-deps --format-version 1 --offline`：`METADATA_EXIT=0`。
- `git diff --check`：`DIFF_CHECK_EXIT=0`；Git 仅输出 LF/CRLF warning，没有 whitespace error。
- 在临时 `LIBCLANG_PATH=.tmp-llvm/extracted/LLVM/bin` 下执行 `cargo check --workspace --offline`：`CARGO_EXIT=0`，完成 `Finished dev profile`。
- 本轮所有结果均来自当前工作区重新执行；当前 `git rev-parse v1.7.0` 仍精确为 `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`。

## 2026-10-04 本轮手工复核补丁

本轮继续以 Git tag `v1.7.0` / `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d` 为 Python 基线，按公开入口和实际调用链人工复核 Rust 与 Python 逻辑；未用脚本生成代码对比或替换实现。

- `/v1/images/edits` 的 JSON malformed/scalar、缺少图片和错误 content type 继续按 Python `image_inputs.py` 的 HTTP 400 语义投影；generations 保留 Pydantic 422 语义。image-task edit JSON body 同样在认证后按 Python 的 bad-request 语义处理。
- 图片索引损坏/非对象 JSON 按 Python `image_storage_service._read_json_object` 回退为空对象；后续列出、写入和删除不因索引损坏直接返回 503。
- Responses image-generation tool 选择保留 Python 的 `tools` 列表优先级：显式列表存在时不回退 `tool_choice`；精确 tool type 保留 options，只有触发但无法投影的字符串形状使用空 options。
- account `source_type`、套餐类型和健康 `by_type` 投影使用 Python `str(value)`/truthiness 语义，避免非字符串 JSON 字段被 Rust 直接拒绝或误计数。
- Chat image modalities、Responses content type/role 和 image block 选择补齐 Python 的 trim/lower 与非空回退边界；Rust-only Codex/WebSocket/ccLoad 能力未被回退或删除。

验证边界：`cargo metadata --no-deps --format-version 1 --offline` 当前通过，`git diff --check` 无 whitespace error；本轮 workspace cargo build/test 受 Windows `btls-sys` 编译锁/构建环境影响，未把超时或 `libclang`/BoringSSL 构建失败误报为逻辑测试通过。此前 audit 中记录的串行全量绿色结果仍只对应其记录时的代码状态。

## 2026-10-04 第二轮手工复核：协议输入与图片投影收口

继续以 `v1.7.0` / `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d` 为唯一 Python 基线，复核实际 route-to-handler 调用链后修复：

- Chat 图片 prompt 的 `text`/`input_text` 选择、图片 block 的 `image_url`/`url`/`base64`/`b64_json`/`source` 回退，以及普通 Chat 多模态上传 parts 顺序现在按 Python helper 和 `OpenAIBackendAPI._api_messages_to_conversation_messages` 投影；图片 SSE 的 `image.generation.message` 文本增量也不再丢失。
- Responses 图片输入按 Python 的 trimmed block type、非空 URL 优先级和本地 data/source usage 统计处理；Responses image tool 的显式 `tools` 列表优先于 `tool_choice`，并保留无法投影的字符串形状为空 options。Responses 文本转换不再把只有 `input_text` 字段的文本块误当作 `text`。
- Anthropic 图片 block 的 URL、内联 base64、source MIME 和空值回退与 v1.7 helper 对齐；Chat/Anthropic 远程图片的嵌套 URL 选择不再被空值遮蔽。账户内部套餐分组保留 Rust 既有 lowercase key，公开 `by_type` 仍使用 Python 原始 `str(value)` 投影。
- `/v1/models` 的匿名目录投影先按 Python backend 排序，再追加动态图片模型；Rust-only Codex/WebSocket/ccLoad 路径未回退或删除。

本轮定向测试已重新执行：图片 Chat helper/stream/usage 4 passed，Anthropic adapter 19 passed，Responses adapter 50 passed；其中一个宽泛 account/model filter 实际匹配 0 tests，未计入行为证据。`webdav_only_images_roundtrip_without_local_files` 复核后将 WebDAV-only ZIP 归档断言改为 404，与 v1.7.0 生效的 local-only `download_images_zip` 定义一致；单图 WebDAV 读取、下载和删除仍通过。串行 `cargo test --workspace --offline -- --test-threads=1` 最终为 564 passed / 0 failed / 5 ignored（file-identity 6 passed）；`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`cargo metadata --no-deps --format-version 1 --offline` 和 `git diff --check` 均通过。

## 2026-10-04 独立重读：账号导出、任务公共投影与图片 MIME

本轮不引用此前审计结论，重新从当前工作区读取 Python v1.7 路由、helper、protocol 和 storage 实现，并人工核对当前 Rust 调用链。确认并修复：

- Python 账号 ZIP 导出按 email/account_id 生成清理后的唯一文件名；Rust 不再固定使用 `account-000.json`，并保留相同的 80 字符和重复后缀规则。
- 图片任务公共投影对 `conversation_id`、`error`、`progress` 使用 Python truthiness；空字符串/空容器不再额外透出。
- JSON 图片引用的 MIME 按 Python helper 小写化并将 `image/jpg` 归一为 `image/jpeg`，覆盖 Chat、Responses、Anthropic 和普通图片编辑引用形状。
- 图片存储配置的 WebDAV/public URL 尾部斜杠归一化与 Python `_normalize_image_storage_settings` 一致；备份公共 settings 继续保留 Python 默认字段、敏感字段遮蔽和 Rust local-provider 扩展语义。
- Responses image tool 的 `size`/`quality` 字段按 Python `str()` 语义投影，保留 Rust-only 能力和资源限制。

本轮仍未改变 Rust 的 access-token-only、认证先行、资源上限、路径校验、错误脱敏和 Rust-only API 边界。

- 独立复核还发现 `/api/storage/info` 的 JSON cached health 可能遮蔽外部直接修改后的损坏快照；JSON 管理诊断现在每次执行有界 fresh probe，Git 保留 Rust 现有同步缓存/限时刷新语义以避免阻塞管理请求。

最终验证：独立串行 `cargo test --workspace --offline -- --test-threads=1` 为 564 passed / 0 failed / 5 ignored（file-identity 6 passed）；定向 backup 14 passed、storage-info 1 passed、Git storage 1 passed、Anthropic image 1 passed、Responses image 1 passed、image reference 1 passed；`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`cargo metadata --no-deps --format-version 1 --offline` 和 `git diff --check` 均通过。移除了一个依赖 SQLx 内部竞态时序的精确中间状态断言，保留最终 quiescent close 资源释放断言。

## 2026-10-04 第三轮独立肉眼复核：搜索投影与备份默认值

本轮再次从当前 Python v1.7 文件和当前 Rust 函数人工读取，不使用脚本生成对比。新确认并修复：

- Python `web_search_tool.text_with_url_citations` 对 answer/title/url 使用 `str()`、去重和搜索文本清洗；Rust Chat 搜索投影现在同样处理非字符串 answer/title，去重安全 URL，并移除标点前空格。新增回归覆盖 numeric title、重复 source 和清洗后的 citation。
- Python `BackupService.get_settings()` 始终使用规范化的 Cloudflare R2 默认字段并遮蔽敏感值；Rust `/api/backups` 公共 settings 现在使用同一规范化投影，但备份实际 provider 路由仍读取原始配置，因此 Rust local-provider 测试扩展不被破坏，缺省 provider 仍为 R2。
- 新一轮串行测试中发现一个依赖 SQLx 内部 close 时序的中间状态断言；删除该 incidental race 断言，保留 quiescent close 最终池大小和文件释放断言。该删除不改变生产逻辑。

第三轮验证：串行 `cargo test --workspace --offline -- --test-threads=1` 为 565 passed / 0 failed / 5 ignored（file-identity 6 passed）；搜索 coercion 1 passed、backup 14 passed、storage-info 1 passed、R2 owner 1 passed。`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`cargo metadata --no-deps --format-version 1 --offline` 和 `git diff --check` 均通过。

## 2026-10-05 第四轮独立肉眼复核：搜索与备份管理边界

本轮重新读取当前 Python v1.7 与 Rust 实现，未使用脚本、自动 diff 或旧审计结论作为证据。确认并修复：

- Chat 搜索答案、标题和 URL 的 Python `str()` 投影、重复 source 去重、annotation 清洗和 citation 边界现在集中在实际 Chat search handler 的结果投影中；新增 numeric answer/title 回归。
- `/api/backups` 的公共 settings 使用 Python 规范化默认值和敏感字段遮蔽；实际 provider 选择从原始配置读取，缺省仍为 Cloudflare R2，显式 local 仍保留 Rust 管理扩展。
- 重新核对 Chat、Responses、Messages、image-task、WebDAV、账号导出、JSON/Database/Git storage 和生命周期调用链；本轮没有删除 Rust-only 能力或放宽安全/资源边界。

本轮最终串行 `cargo test --workspace --offline -- --test-threads=1` 为 565 passed / 0 failed / 5 ignored（file-identity 6 passed）；搜索 coercion 1 passed、backup 14 passed、storage-info 1 passed、R2 owner 1 passed。`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`cargo metadata --no-deps --format-version 1 --offline` 和 `git diff --check` 均通过。

## 2026-10-05 第五轮独立肉眼复核：输入 coercion、清理投影和设置保留

本轮继续以 Git tag `v1.7.0` / `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d` 为唯一 Python 基线，人工读取实际调用链，未使用脚本生成对比或替换实现。确认并修复：

- Responses 的 `thinking_effort_from_body` 在 `reasoning` 为 dict 时即使缺少 `effort` 也不回退到顶层 effort；Rust `python_responses_thinking_effort` 现在保留该优先级和空值语义，并有空 dict 回归。
- 刷新账号套餐类型使用 Python `str(value or "free")`：数字、容器和带首尾空白的字符串均保留 Python 字符串形状；管理端 CPA/Sub2API 公共字段、远程文件字段和分组计数也按 Python `str()`/`int()` 投影，空 `email` 正确回退 `account`。
- `/api/settings` 的公开 proxy runtime 更新现在按 Python 的 `has_cf_cookies`/`has_cf_clearance` 标记恢复已保存的 clearance 内容，不在响应或日志中暴露密钥；现有 settings route 回归确认 cookies、clearance 和其它 masked fields 均保留。
- clearance 测试的默认目标 URL 去除尾部 `/`，与 Python `ClearanceTestRequest` 和 `test_clearance` 默认值一致；Rust-only Codex/WebSocket/ccLoad 能力、安全认证先行和资源边界未改变。

本轮定向行为验证：Responses 空 reasoning、账号/公共文本 coercion、registry projection、远程分组计数、settings clearance persistence 和 management remote contract 均由当前 test binary 真实通过；`cargo fmt --all -- --check` 通过。workspace 串行测试及其最终计数在本节后续验证记录中如实补充。

最终验证：设置当前 `LIBCLANG_PATH=E:\项目\chatgpt2api\.tmp-llvm\extracted\LLVM\bin` 后，串行 `cargo test --workspace --offline -- --test-threads=1` 为 Rust 主库 `564 passed / 0 failed / 5 ignored`、file-identity `6 passed / 0 failed`，main/doc-tests 均为 0 failed；`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`cargo metadata --no-deps --format-version 1 --offline` 和 `git diff --check` 均通过。

## 2026-10-05 第六轮独立肉眼复核：远程 coercion、SSE 边界和任务计时

继续只以 Git tag `v1.7.0` / `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d` 为 Python 基线，人工读取对应调用链，未使用脚本生成差异。确认并修复：

- FlareSolverr `status`、cookie `name/value/domain` 和 `userAgent` 使用 Python `str()`/truthiness；Rust 现在接受非字符串 JSON 字段并继续过滤无效域名/空名称。
- Anthropic tool fallback 的 function name/description 按 Python `or` truthiness 选择；参数 JSON 数字使用 Python 指数宽度，`1e-7` 投影为 `1e-07`。
- Sub2API 远程 group/account ID 使用 Python `str()`，非零但不可转 `int()` 的 group count 现在 fail closed 为上游错误；有效数字字符串、浮点和 falsey 值保持 Python `int(value or 0)` 语义。
- ChatGPT assistant SSE 的 `content.parts` 为非列表时回退 `content.text`；图片 SSE 空 JSON 事件仍产生 Python `conversation.event` 对应的无文本 progress；queued image task 缺少 `created_ts` 时回退 `updated_ts` 计算 elapsed。

## 2026-10-05 第七轮独立肉眼复核：无新增修复

在第六轮修复后再次人工复读认证、模型目录、搜索、JSON/Database/Git storage、备份、WebDAV、图片任务、editable、代理、Chat/Responses/Messages 调用链；未发现新的确定且属于本任务范围的本地行为差异。Rust-only Codex/WebSocket/ccLoad、access-token-only、错误脱敏、大小/路径/CAS/fail-closed 边界保持不变。

第六轮定向测试与当前串行 workspace 验证均通过：Rust 主库 `568 passed / 0 failed / 5 ignored`，file-identity `6 passed / 0 failed`，main/doc-tests 无失败；`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、离线 metadata 和 `git diff --check` 均通过。

## 2026-10-05 第八轮公开入口复核与最终验证

本轮重新人工复读 v1.7.0 的 `api/app.py`、`api/ai.py`、`api/accounts.py`、`api/image_tasks.py`、`api/system.py` 公开路由及当前 Rust 的 route-to-handler 链；未发现新的确定且属于本任务范围的本地行为差异。Rust-only 的 Codex、WebSocket、ccLoad、显式 local backup/provider 和 access-token-only 边界均保留。

- 实际启动当前 `target/debug/chatgpt2api-rust.exe` 后，`GET /version` 返回 `{"version":"1.8.0"}`，`GET /health?format=json` 返回完整健康投影，带管理员密钥的 `POST /auth/login` 返回管理员身份；无凭据仍返回 401，未知 `/v1` 路由返回 OpenAI error envelope。
- `cargo test --workspace --offline -- --test-threads=1`：Rust 主库 `570 passed / 0 failed / 5 ignored`，file-identity `6 passed / 0 failed`，main/doc-tests 无失败。
- `cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`cargo metadata --no-deps --format-version 1 --offline` 和 `git diff --check` 均通过；Cargo 构建命令使用临时 `LIBCLANG_PATH`。

本轮没有删除 Rust-only 能力，也没有放宽认证先行、资源/路径/快照/CAS/fail-closed 边界。

## 2026-10-05 第九轮独立复核：图片输入、公共回退和管理 coercion

本轮再次人工读取 v1.7.0 的 `utils/helper.py`、`services/protocol/openai_v1_response.py`、`api/system.py`、`api/app.py` 及当前 Rust 实际调用链；确认并修复以下确定差异：

- Chat 图片请求的 `n` 在已完成 Pydantic 整数归一化后仍可能遇到超过 `i64` 的无符号 JSON 整数；Rust 原先将其回退为 1，Python 会按 `1..=4` 校验并拒绝。现由独立 helper 对无符号/负数/越界值统一拒绝，保留 1..=4。
- Responses 图片工具输入按 Python `extract_response_image` 选择“最后一个包含图片的输入消息中的第一张图片”；Rust 原先把扁平列表最后一张当作实际编辑图。现保留所有图片用于 usage，但实际请求图按 Python 选择，回归覆盖多图片消息。
- Responses 图片输出的 `revised_prompt` 按 Python `str(value or prompt).strip() or prompt` 投影；Rust 现在对 truthy 数字/容器也做 Python 字符串化。
- Python 未匹配 API 路由的 GET/HEAD 会进入 SPA fallback；Rust 不再把未知 `/api/...` 前缀额外禁用 SPA fallback。已保留真实 `/api` 处理器的认证和错误边界。
- `/api/images/delete` 的 `all_matching` 按 Python/Pydantic bool 规则接受标准字符串、0/1 和 0.0/1.0；未知值仍返回 422。

本轮没有删除 Rust-only Codex、Responses WebSocket、ccLoad、local backup/provider 或 access-token-only 边界，也没有放宽资源、路径、快照、CAS 和 fail-closed 保护。

验证：新增/受影响定向测试全部通过；串行 `cargo test --workspace --offline -- --test-threads=1` 最终为 Rust 主库 `573 passed / 0 failed / 5 ignored`、file-identity `6 passed / 0 failed`，main/doc-tests 无失败；`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、离线 metadata 和 `git diff --check` 均通过。使用临时 `RUST_WEB_DIST` 启动二进制后，未知 `/api/unknown` 和 `/v1/unknown` 返回 SPA，真实 `/api/settings` 仍先返回认证错误。

## 2026-10-05 第十轮独立复核：索引清理与管理扩展边界

本轮继续人工复读 v1.7.0 的图片存储、数据库、备份、日志、生命周期和 CPA/Sub2API/ccLoad 管理链路。新确认并修复一项确定差异：Python 图片存储在读取/更新索引时只保留合法图片相对路径和对象项，Rust 原先会在更新索引时保留非法扩展名、越界路径或非对象项；`read_image_index_unlocked` 现在按相同边界过滤，新增回归覆盖 `invalid.txt`、`../escape.png` 和合法图片项。

本轮未改变 Rust 对远程 URL、归档成员、日志字段、数据库快照、路径安全和资源上限的 fail-closed 加固；CPA/Sub2API 的 URL 校验和敏感字段裁剪仍是有意安全差异，ccLoad/local backup/provider 仍是 Rust-only 扩展。未发现其它新的确定性 v1.7 行为差异。

验证：图片索引定向测试通过；串行 `cargo test --workspace --offline -- --test-threads=1` 为 Rust 主库 `573 passed / 0 failed / 5 ignored`、file-identity `6 passed / 0 failed`，main/doc-tests 无失败；`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、离线 metadata 和 `git diff --check` 均通过。当前二进制 smoke 的 `/version`、`/health?format=json`、管理员 `/auth/login` 和认证先行 `/api/settings` 均符合预期。

## 2026-10-06 第十一轮独立复核：查询参数 coercion 与认证先行

本轮人工复读 v1.7.0 `api/system.py`、`api/image_tasks.py`、`api/support.py` 与当前 Rust management/query handlers。确认并修复一项确定差异：Python `/api/images/storage/cleanup-to-target` 的 Pydantic 查询参数 `target_free_mb: int`、`dry_run: bool` 支持 Python 标准字符串 coercion，且 `/api/images` 不声明这两个参数、未知 query 应被忽略；Rust 原先复用同一个强类型 query struct，可能在认证前因未知/非法 query 返回 extractor 错误。现拆分 `ImageListQuery`/`ImageCleanupQuery`，清理参数在管理员认证之后按 Python int/bool 规则解析，`/api/images` 忽略 cleanup-only query。

新增回归覆盖：`1_000` 整数、`YES`/`off` bool、非法 query 拒绝，以及带非法 cleanup-only query 的 `/api/images` 仍成功；实际二进制 POST malformed cleanup query 无凭据先返回 401。

本轮再次复核图片、账号、模型、任务、备份、日志、JSON/Database/Git storage、配置和生命周期，未发现其它新的确定性差异。Rust-only 能力、敏感字段裁剪、认证先行、资源/路径/快照/CAS/fail-closed 边界保持不变。

验证：串行 `cargo test --workspace --offline -- --test-threads=1` 为 Rust 主库 `574 passed / 0 failed / 5 ignored`、file-identity `6 passed / 0 failed`，main/doc-tests 无失败，合计 `580 passed`；`cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、离线 metadata 和 `git diff --check` 均通过。

## 2026-10-06 第十二轮独立复核：图片 usage、尺寸解析与多图聚合

本轮继续人工复读 v1.7.0 `utils/image_tokens.py`、`services/protocol/conversation.py`、`services/protocol/openai_v1_response.py` 与当前 Rust 图片 usage 调用链，确认并修复三项确定差异：

- Python `parse_image_size` 使用正则的前两个尺寸分组；Rust 原先要求恰好两个数字分组，现改为相同的首两个分组语义。
- Python generation/edit wrapper 在收集 `n` 张结果后只计算一次输入文本/输入图片 token；Rust 原先合并每张内部响应的 usage，重复计算输入 token，现按合并后的 data 一次重算。
- Python Responses `_part_size` 优先使用 image part 顶层 `width`/`height`；Rust 现保留每个参与 usage 的显式尺寸，并让远程 URL 在提供显式尺寸时进入 usage 统计，未提供时仍按 Python 不计入。

新增回归覆盖 `native_image_usage_size_matches_python_first_two_groups`、`native_multi_image_usage_counts_input_once`、Responses 显式尺寸 usage；受影响的 mask/输出尺寸回归共 `8 passed / 0 failed`。设置 `LIBCLANG_PATH=.tmp-llvm/extracted/LLVM/bin` 后定向测试真实通过；本轮未重跑 workspace 全量 suite。

## 2026-10-06 第十三轮独立复核：图片 SSE 状态、进度与 ID 元数据

本轮人工复读 v1.7.0 `services/protocol/conversation.py` 的 SSE state/update、图片 file/sediment ID 正则和当前 Rust SSE parser。确认并修复：

- sediment/file-service asset pointer 现在按 Python 正则截断到允许字符；sediment ID 不再额外接受 `.`，原始 `file_...` ID 增加 Python `\b` 等价的前后 word boundary，且 `tool` 角色匹配不再把大小写/空白变体误判为图片工具事件。
- 保留 Python 的用户上传排除、server metadata `tool_invoked`/`turn_use_case`、moderation blocked、空 JSON progress 和可恢复 conversation state；Rust 的公共 SSE 继续剔除内部账号/conversation 元数据，仅通过 response extension 写入受限日志上下文。

回归 `native_image_sse_id_tokens_match_python_regex_boundaries`、既有 file-ID/state tests、空 JSON progress test 均通过（`4 passed / 0 failed`）；`cargo fmt --all -- --check` 通过。设置 `LIBCLANG_PATH=.tmp-llvm/extracted/LLVM/bin`，本轮未重跑 workspace 全量 suite。

## 2026-10-06 第十四轮独立复核：图片轮询 coercion、retry、settle 与 cleanup

本轮人工复读 v1.7.0 `services/openai_backend_api.py` 的 `_poll_image_results`、`resolve_conversation_image_urls`、下载/隐藏 conversation 路径及当前 Rust polling chain。确认轮询初始等待、settle/check-before-hit、Retry-After、指数退避、部分 ID fallback、重复字节去重和 PATCH 隐藏 conversation 均已按实际调用链闭环；新增修复仅为图片 polling 配置的 Python coercion：`image_poll_timeout_secs` 使用 `int()` 语义，initial wait/interval/settle 使用 `float()` 语义，settle/check bool 对未知字符串按 Python false 处理。

`native_image_poll_matches_python_policy_and_task_error_shapes`、`image_timeout_and_text_reply_retries_can_reacquire_prior_accounts`、`image_sse_without_conversation_id_preserves_final_state_for_recovery` 均通过（`3 passed / 0 failed`）；`cargo fmt --all -- --check` 通过。设置 `LIBCLANG_PATH=.tmp-llvm/extracted/LLVM/bin`，本轮未重跑 workspace 全量 suite。

## 2026-10-06 第十五轮独立复核：图片账号 lease、quota、并发与 source filter

本轮人工复读 v1.7.0 `services/account_service.py` 的 `_is_image_account_available`、候选账号/图片 inflight slot、`get_available_access_token`、`mark_image_result` 与 Rust `AccountStore` 图片 lease 链。Web 图片保持任意可用来源的 status/quota 筛选；Codex 图片保持 `source_type=codex` 与 Plus/Team/Pro plan filter；quota 只决定可用性，不把剩余额度误当并发上限；成功才扣 quota，失败只记 fail，lease 释放和自动移除限流账号边界保持一致。未发现新的确定差异。

`image_account_leases_follow_concurrency_limit_independent_of_quota`、`image_result_accounting_consumes_only_successful_image_requests`、`native_codex_image_model_prefix_selects_exact_account_type` 均通过（`3 passed / 0 failed`）；本轮未重跑 workspace 全量 suite。

## 2026-10-06 第十六轮独立复核：图片 task create/list/public projection

本轮人工复读 v1.7.0 `api/image_tasks.py`、`services/image_task_service.py` 与 Rust task snapshot、enqueue、owner filtering、idempotency、public projection 和 retention 链。确认 `queued/running/success/error` 投影、缺失 ID、按更新时间排序、owner 隔离、`elapsed_secs` 的 queued/running 基准、空 data 错误、prompt/model/size/quality 默认值和 JSON/multipart task boundary 均已对齐；Rust 额外的 snapshot/path/size/fail-closed 保护保持不变。未发现新的确定差异。

`image_tasks_load_python_snapshot_recover_and_clean_legacy_records`、`image_task_elapsed_seconds_round_like_python`、`queued_image_task_elapsed_falls_back_to_updated_timestamp`、`image_task_generation_whitespace_client_id_matches_python_required_error` 均通过（`4 passed / 0 failed`）；本轮未重跑 workspace 全量 suite。

## 2026-10-06 第十七轮独立复核：图片 task worker、error 与 resume-poll 生命周期

本轮人工复读 v1.7.0 `ImageTaskService._run_task/_update_task/resume_poll/_run_resume_poll` 与当前 Rust worker、snapshot mutation、error context、progress、timeout resume 和 owner isolation。确认 queued→running→success/error 状态迁移、空 data/stream result error、conversation_id 保存、重复 resume 拒绝、时间戳先持久化、轮询结果保存和失败日志均已对齐；Rust 继续保留 worker admission、快照原子写和资源边界。

`image_task_failure_persists_observed_conversation_for_resume`、`image_task_resume_persists_updated_timestamp_before_worker_progress`、`image_task_content_filter_rejection_writes_python_call_log`、`image_task_empty_result_and_log_summary_match_python` 均通过（`4 passed / 0 failed`）；本轮未重跑 workspace 全量 suite。

## 2026-10-06 第十八轮独立复核：editable PPT/PSD task lifecycle 与文件下载

本轮人工复读 v1.7.0 `services/editable_file_task_service.py`、`services/openai_backend_api.py` 的 editable upload/prepare/run/poll/artifact/download 及当前 Rust `editable_file_generation`、task listing、capability URL、range/HEAD download 链。确认 PPT/PSD 的套餐账号 LRU、PSD 空图像错误、任务 owner/idempotency、重启恢复、artifact 双文件落盘后才发布 success、公共 result URL、能力哈希、文件类型/路径边界和下载响应均已闭环；Rust 的更严格 artifact/snapshot/文件大小/身份校验继续保留。定向 owner/download、PPT upstream、PSD upstream 测试通过；PSD upstream 首次运行出现本机时序失败后，单测重放通过，未发现稳定代码失败。

定向结果：`editable_file_tasks_are_owner_scoped_and_download_capability_bound`、`cwd_switch_cross_pressure_native_ppt_task_holds_real_web_lease_until_artifacts_are_persisted`、`cwd_switch_cross_pressure_native_psd_task_uploads_library_image_and_persists_both_artifacts` 均最终通过（`3 passed / 0 failed`）。

## 2026-10-06 第十九轮独立复核：account canonicalization、aliases 与 timestamps

本轮人工复读 v1.7.0 `AccountService._normalize_account`、token alias/rotation、`created_at`/`last_used_at` 及当前 Rust canonical snapshot/runtime-state 链。确认 legacy `accessToken`/Codex export 归一、套餐/source/status/quota/计数/可选字段默认值、UTC 创建时间、本地使用时间、token rotation alias 和 inflight/last-used runtime state preservation 均已闭环；未发现新的确定差异。

`account_canonicalization_fills_missing_created_at_like_python`、`account_canonicalization_matches_python_legacy_alias_plan_and_quota`、`account_reload_token_rotation_same_identity_preserves_last_used`、`account_timestamp_uses_utc_like_python` 均通过（`4 passed / 0 failed`）。

## 2026-10-06 第二十轮独立复核：account refresh、invalid/rate-limit removal 与 progress

本轮人工复读 v1.7.0 `AccountService.fetch_remote_info/refresh_accounts/update_refresh_progress/_record_invalid_token_seen/mark_image_result` 与当前 Rust refresh futures、progress sink、invalid deferral、rate-limit removal 和 TLS filter。确认 access-token-only 边界下的 metadata refresh、未知 token 不入库但进度推进、非认证网络错误不标记异常、TLS 错误过滤、invalid confirmation window、自动移除限流/失效账号及 status/quota/processed 统计均已对齐；refresh/id token keepalive/relogin 仍按既定产品边界不实现。

`account_refresh_non_auth_upstream_error_does_not_mark_account_abnormal`、`account_refresh_of_unknown_but_valid_token_updates_progress_without_importing_it`、`account_refresh_rate_limited_removal_matches_python_counts_and_progress`、`account_refresh_tls_filter_matches_python_error_markers`、`refresh_invalid_token_deferral_matches_python_time_windows` 均通过（`5 passed / 0 failed`）。

## 2026-10-06 第二十一轮独立复核：account import/update/delete/export boundaries

本轮人工复读 v1.7.0 账号管理路由与 `AccountService` 的 token-key merge、legacy alias、private-field 保留、原子写、删除、完整三件套 export 约束及 Rust 管理投影。确认 access-token-only 下 import/update/delete、重复 token 合并、`accessToken` alias、私有字段不在公共投影丢失、原子快照写和完整 access/refresh/id token export 拒绝/投影均已闭环；完整三件套导出边界仍按既定 Rust 产品限制记录。

`account_management_preserves_private_fields_and_writes_atomically`、`account_import_validates_public_shapes_and_preserves_access_only_order`、`account_import_matches_python_token_key_semantics`、`account_export_timestamp_matches_python_filename_shape` 均通过（`4 passed / 0 failed`）。

## 2026-10-06 第二十二轮独立复核：model provenance、account catalogs 与 dynamic image models

本轮人工复读 v1.7.0 `openai_v1_models.list_models`、账号 source/type 归一和当前 Rust model provenance/catalog/image model projection。确认匿名/认证模型目录、Web/Image/Codex provenance、verified catalog/image capability proof、按套餐的 dynamic Codex image model、quota=0 隐藏图片模型和同组 catalog representative/retry 逻辑均已对齐；Rust-only catalog cache/安全裁剪保持不变。

`positive_image_quota_persists_image_model_for_imported_accounts`、`image_quota_refresh_uses_one_representative_per_account_group`、`public_models_do_not_advertise_static_images_without_quota`、`models_route_retries_until_success_within_a_type` 均通过（`4 passed / 0 failed`）。

## 2026-10-06 第二十三轮独立复核：proxy/profile precedence、URL normalization 与 sessions

本轮人工复读 v1.7.0 `ProxySettingsStore.get_profile/build_session_kwargs/build_headers`、legacy global proxy 调用点和当前 Rust `profile_from_runtime`、client builders、session reset、cookie/clearance key。确认 account → runtime(resource) → explicit → global precedence、`upstream`/resource 分支、SOCKS/host:port/user:pass normalization、skip-SSL、reset status codes、匿名 Chat legacy global proxy fallback 和 malformed proxy fail-closed 均已对齐。

`proxy_and_cookie_helpers_match_python_precedence`、`profile_priority_matches_python_proxy_service`、`account_session_profile_matches_python_default_session_kwargs`、`anonymous_native_chat_profile_falls_back_to_legacy_global_proxy`、`malformed_proxy_profile_never_falls_back_to_direct_egress` 均通过（`5 passed / 0 failed`）。

## 2026-10-06 第二十四轮独立复核：clearance/Flaresolverr/manual headers single-flight

本轮人工复读 v1.7.0 `ProxySettingsStore` clearance cache、manual/Flaresolverr bundle、cookie merge、domain filter、provider cache、single-flight、expiry/invalidation 及当前 Rust `ClearanceStore`/browser-header 调用点。确认 proxy+target-host key normalization、manual header 投影、Flaresolverr payload/coercion、cookie domain filtering、UA-only bundle、过期清理和并发 waiter 不丢 completion 均已闭环。

Rust proxy tests `8 passed / 0 failed`，clearance runtime tests `4 passed / 0 failed`；本轮未重跑 workspace 全量 suite。

## 2026-10-06 第二十五轮独立复核：settings normalization、masked secrets 与 persistence

本轮人工复读 v1.7.0 `services/config.py`、`api/system.py` 的 defaults/coercion/masked secret/URL projection 与当前 Rust settings normalization、public config、atomic writer 和 masked-field restoration。确认 Python `int/float/bool/str` coercion、cache/image/proxy/backup/third-party defaults、URL 脱敏、secret mask 保留、WebDAV validation 和损坏/缺失 config fallback 均已对齐；Rust 的敏感字段裁剪和 fail-closed snapshot/atomic-write 保护保留。

`settings_u64_matches_python_int_coercion`、`settings_api_recovers_missing_and_invalid_config_like_python`、`json_owner_settings_and_backup_route_persistence_slice` 均通过（`3 passed / 0 failed`）。

## 2026-10-06 第二十六轮独立复核：third-party apps、storage info 与 health payload

本轮人工复读 v1.7.0 `api/system.py`、`services/config.py` 的 third-party apps/defaults，以及 storage `get_backend_info/health_check` 与 Rust `/api/third-party-apps`、`/api/storage/info`、`/health` public projection。确认 infinite canvas 默认/URL 归一、未知字段不外泄、storage backend type malformed fail-closed、admin auth、health status 与已加载账号统计/存储探测分离均已对齐。

`public_storage_snapshot_fails_closed_for_malformed_type_and_has_stable_keys`、`storage_info_requires_admin_and_projects_only_public_fields`、`third_party_apps_route_projects_python_public_contract`、`json_owner_settings_and_backup_route_persistence_slice` 均通过（`4 passed / 0 failed`）。

## 2026-10-06 第二十七轮独立复核：local image storage、index filtering 与 thumbnail cleanup

本轮人工复读 v1.7.0 `image_storage_service.py`、`image_service.py` 的本地文件/索引/缩略图/保留期逻辑与当前 Rust management image chain。确认图片索引合法路径/扩展过滤、mtime fallback 本地时间、320x320 EXIF/Lanczos PNG thumbnail、孤立缩略图清理、删除/低磁盘清理同步标签和索引、完整列表不截断及 bounded path/file reads 均已闭环。

`image_index_mtime_fallback_uses_local_wall_clock`、`malformed_image_index_recovers_like_python`、`image_thumbnail_applies_exif_orientation_and_persists_png_cache` 均通过（`3 passed / 0 failed`）。

## 2026-10-06 第二十八轮独立复核：WebDAV URL/auth/sync/read/delete behavior

本轮人工复读 v1.7.0 `ImageStorageService`/`WebDAVClient` 的 URL/root-path quoting、Basic auth、MKCOL/PUT/GET/DELETE、local/webdav/both index states、sync/list/read/delete 与配置 validation，并核对当前 Rust management implementation。确认 WebDAV-only roundtrip、public URL、remote index flags、同步/删除/本地缺失读取和 30 秒 bounded requests 均已闭环。

`webdav_only_images_roundtrip_without_local_files`、`native_image_storage_path_and_url_match_python_v17_contract` 均通过（`2 passed / 0 failed`）。

## 2026-10-06 第二十九轮独立复核：image tags locking、path validation 与 deletion

本轮人工复读 v1.7.0 `image_tags_service.py` 与当前 Rust tags read/update/delete、sidecar path lock、atomic replace、dedup/trim、invalid index recovery、image deletion/thumbnail cleanup。确认 tag path traversal/empty/tag length/array bounds、重复 tag 去重、malformed file recovery、exact delete semantics、sidecar locking 和 fail-closed rebind protections 均已闭环。

`image_and_maintenance_routes_are_bounded_and_fail_closed`、image-index filtering tests 均通过（`3 passed / 0 failed`）。

## 2026-10-06 第三十轮独立复核：logs parsing、IDs、filters、projection 与 deletion

本轮人工复读 v1.7.0 `log_service.py`、`api/system.py` logs routes 与当前 Rust JSONL parse/project/list/delete。确认 UUID4 hex 新日志、旧日志 ordinal+line SHA-1 ID、日期/type filters、bounded public detail/URL/request-shape/result projection、敏感字段裁剪、unknown field 保留删除、空 ids no-op 和原子日志重写均已闭环。

`management_server_flows_cover_storage_remote_contracts_and_restart`、`delete_log_preserves_unknown_fields_and_python_legacy_ids`、`legacy_log_id_matches_python_sha1_fixture` 均通过（`3 passed / 0 failed`）。

## 2026-10-06 第三十一轮独立复核：backup defaults、settings masking 与 scheduler

本轮人工复读 v1.7.0 `BackupService.get_settings/scheduler` 与当前 Rust backup settings normalization、masked secret persistence、interval/last-finished/running schedule checks。确认默认 R2/include 语义、敏感字段遮蔽/保留、disabled/running/recent/overdue scheduler 分支和本地扩展 provider 不改变原版默认投影。

`backup_schedule_due_matches_python_scheduler_rules`、`json_owner_settings_and_backup_route_persistence_slice`、`management_server_flows_cover_storage_remote_contracts_and_restart` 均通过（`3 passed / 0 failed`）。

## 2026-10-06 第三十二轮独立复核：backup archive composition、metadata 与 member projection

本轮人工复读 v1.7.0 backup archive composition/detail member projection 与当前 Rust tar.gz builder/metadata/snapshot manifest/optional include/member bounds/detail parser。确认默认成员、显式 include、metadata、snapshot public projection、未知/危险成员过滤、大小/数量上限、archive commit failure state 保留均已闭环。

`json_backup_detail_matches_python_archive_projection_boundaries`、`json_backup_archive_commit_failure_finishes_error_and_preserves_owner_fields` 均通过（`2 passed / 0 failed`）。

## 2026-10-06 第三十三轮独立复核：backup encryption、download/detail 与 local provider

本轮人工复读 v1.7.0 backup encryption/download/detail/local provider 逻辑与当前 Rust OpenSSL-compatible crypt、`.enc` key/detail/download、passphrase errors、local archive route 和 forged-state redaction。确认加密头/解密边界、下载响应、detail projection、缺口令失败、local provider archive 写入和 state 脱敏均已闭环。

`json_owner_settings_and_backup_route_persistence_slice`、`rust_backup_crypto_interoperates_with_pinned_openssl` 均通过（`2 passed / 0 failed`）。

## 2026-10-06 第三十四轮独立复核：R2 backup keys、pagination、rotation 与 CAS

本轮人工复读 v1.7.0 R2/S3-compatible list/get/delete/rotation/key validation 与当前 Rust SigV4/R2 client、XML pagination、25-page/5000-object budgets、prefix/key validation 和 backup state error projection。确认可接受 key 形状、continuation handling、optional XML fields、response size limits、rotation protected/current keys 和 CAS/owner state 边界均已闭环。

`r2_list_parser_matches_python_optional_fields_and_cleaning`、`r2_list_budget_matches_python_contract`、`r2_management_errors_use_python_safe_detail_contract`、`backup_state_projects_all_python_r2_status_errors` 均通过（`4 passed / 0 failed`）。

## 2026-10-06 第三十五轮独立复核：JSON storage、snapshot health 与 atomic writes

本轮人工复读 v1.7.0 JSON storage load/save/health 与当前 Rust JSON backend file identity、cross-process lock/CAS、atomic replacement、cumulative total 和 health snapshot pair。确认 storage probe 与 model health 解耦、双 owner 仅一方 CAS winner、外部替换不被覆盖、临时文件清理和 validated snapshot health 均已闭环。

`json_health_storage_ignores_model_health`、`json_owner_cross_instance_cas_allows_one_winner_and_preserves_file_integrity`、`json_owner_cas_reads_only_after_cross_process_lock_and_rejects_external_replace` 均通过（`3 passed / 0 failed`）。

## 2026-10-06 第三十六轮独立复核：Database storage schema、ordering、CAS 与 close

本轮人工复读 v1.7.0 database storage schema/load/save/order/health/close 与当前 Rust SQLite/Any database schema migration、token hash integrity、incoming order、collection CAS、health snapshot 和 quiescent close。确认 legacy schema migration idempotency、账户/密钥/累计计数、CAS 单赢家、数据库健康投影和关闭时连接 drain 均已闭环。

`database_backend_owns_accounts_auth_cumulative_cas_and_health`、`concurrent_database_cas_has_exactly_one_winner_for_each_collection`、`database_migrates_legacy_token_schema_atomically_and_idempotently`、`quiescent_close_drains_connection_returned_after_first_close_pass` 均通过（`4 passed / 0 failed`）。

## 2026-10-06 第三十七轮独立复核：Git storage clone、health、CAS 与 recovery

本轮人工复读 v1.7.0 Git storage clone/read/write/health/recovery 与当前 Rust private clone/cache、pending push、CAS、candidate ref cleanup、health refresh and pair publish。确认 accounts/auth/cumulative snapshots、remote rejected push recovery、orphan candidate cleanup、unsafe path/cache fail-closed、redacted backend info 和 health boundedness 均已闭环。

`git_backend_owns_accounts_auth_cumulative_cas_and_redacted_info`、`git_health_reclaims_orphan_candidate_refs_across_instances_and_retries_cleanup`、`git_backend_pending_push_and_rejected_push_recover_remote_state`、`git_backend_rejects_unsafe_paths_cache_types_and_orphan_pending_marker` 均通过（`4 passed / 0 failed`）。

## 2026-10-06 第三十八轮独立复核：lifecycle schedulers、shutdown admission 与 worker ownership

本轮人工复读 v1.7.0 app/account/image cleanup/backup scheduler 生命周期及当前 Rust shutdown admission、editable worker owner、catalog owner、scheduler watch、storage close 和 bounded drain。确认 shutdown 先关闭 HTTP admission/worker admission，再发 graceful signal；owner cleanup joinable、不 detached；refresh/cleanup/backup scheduler 响应 shutdown；超时边界和最终资源释放均已闭环。

`owner_and_http_admission_begin_before_graceful_signal`、`state_shutdown_aborts_and_joins_editable_worker_owner`、`http_admission_fence_rejects_new_requests`、`shutdown_signal_closes_http_admission_before_blocked_owner_cleanup`、`shutdown_deadline_includes_blocked_owner_cleanup_without_detaching_it` 均通过（`5 passed / 0 failed`）。

## 2026-10-06 第三十九轮独立复核：cross-cutting security/resource/path/fail-closed boundaries

本轮横向复读认证先行、请求/上游/快照/日志/图片/归档/任务大小边界、路径和 symlink/file identity 校验、敏感字段裁剪、malformed proxy fail-closed、owner/capability download isolation、腐坏/超大 snapshot health degradation 与 Git unsafe path rejection。未发现新的确定差异；这些 Rust 加固边界继续优先于 Python 表面行为。

`health_fail_closed_for_corrupt_and_oversized_snapshots_and_projects_proxy`、`malformed_proxy_profile_never_falls_back_to_direct_egress`、`image_and_maintenance_routes_are_bounded_and_fail_closed`、`editable_file_tasks_are_owner_scoped_and_download_capability_bound`、`git_backend_rejects_unsafe_paths_cache_types_and_orphan_pending_marker` 均通过（`5 passed / 0 failed`）。

## 2026-10-06 第四十轮独立复核：全量 clean confirmation replay

前 50 轮未发现新的确定差异；本轮不改实现，只对当前工作区做额外全量确认。Rust `cargo test --offline --all-targets -- --test-threads=1` 完成 `577 passed / 0 failed / 5 ignored`（共 582 个 `src/lib.rs` 测试，另有 `src/main.rs` 的 0 测试目标通过）；Web 账户导入、生命周期和 serial-poll caller 合约测试以 Node test runner 完成 `14 passed / 0 failed`。

## 2026-10-06 第四十一轮独立复核：再次复读后台 owner admission 与全量边界

本轮重新人工复读 v1.7.0 的 system/config/storage/image/log/backup/editable lifecycle，并重点检查 Rust shutdown 与后台 worker 的并发交界。发现一个 Rust-only 生命周期竞态：`EditableWorkers::spawn` 原先在检查 accepting 后，可能先被 shutdown 取走 handles，再登记新 worker，导致该 worker 不在本轮 owner join 集合中。已用同一 admission mutex 将 accepting 状态、worker handle 登记、`begin_shutdown` 和 `finish_shutdown` 串成一个临界区；未改变 Python v1.7 的可观察业务契约。

新增 `editable_shutdown_cannot_race_task_registration` 回归，验证 shutdown 在 admission 临界区内等待且不会漏 join worker；`editable_file_generation::tests::`（5 个）、`state_shutdown_aborts_and_joins_editable_worker_owner`、`shutdown_signal_closes_http_admission_before_blocked_owner_cleanup` 均通过。随后 Rust 全量 `cargo test --offline --all-targets -- --test-threads=1` 完成 `578 passed / 0 failed / 5 ignored`；`cargo fmt --check` 通过；Web 合约测试 Node runner 完成 `14 passed / 0 failed`。

## 2026-10-06 第四十二轮独立复核：修复后公共 API/认证/协议 clean round

在第四十一轮修复之后，重新人工复读 Python v1.7 的 `api/ai.py`、`api/accounts.py`、`api/support.py`、错误处理、Chat/Responses/Messages/Models/Search 协议与当前 Rust 路由、认证、Pydantic/coercion、请求投影和响应/SSE 边界。本轮未发现新的确定差异，因此没有实现改动。

Rust `cargo test --offline --all-targets -- --test-threads=1` 完成 `578 passed / 0 failed / 5 ignored`；`src/main.rs` 目标通过（0 tests）。

## 2026-10-06 第四十三轮独立复核：管理 CRUD/import/storage/log/backup clean round

重新人工复读 Python v1.7 `AccountService` CRUD/import/refresh/export、CPA/Sub2API/ccLoad 管理任务、日志服务、备份服务与 Rust 对应 account store、registry、logs、backup 实现。确认 access-token-only 差异、Rust 资源/路径加固和 ccLoad 扩展边界均仍符合既有约定；本轮未发现新的确定差异，也未修改实现。

重点验证：`management::tests::` 26 passed、`tests::account_` 41 passed；此前同一工作区全量 Rust 验证为 `578 passed / 0 failed / 5 ignored`。

## 2026-10-06 第四十四轮独立复核：逐段重读配置/代理/账号/任务/图片/存储核心链路

本轮继续逐段人工复读 Python v1.7 `config.py`、`auth_service.py`、`content_filter.py`、`proxy_service.py`、`account_service.py`、`image_task_service.py`、`editable_file_task_service.py`、`conversation.py`、`openai_backend_api.py`、`image_storage_service.py`、`backup_service.py`、三种 storage，以及当前 Rust 对应模块。发现一项确定的输入 coercion 差异：Python `api/accounts.py` 的 `accounts: list[dict[str, Any]]` 记录经 `_account_payload_token` 会把 truthy 非字符串 `access_token/accessToken` 转为 `str()`；Rust 管理导入此前只接受字符串，导致数值/容器 token 记录被静默跳过。现已在 API token 提取和导入合并前按 Python repr/string 语义规范化为字符串，未放宽 `tokens: list[str]`、refresh/id token 拒绝或快照持久化的安全边界。

新增 `account_pool::tests::imported_access_token_uses_python_string_coercion`；`account_import_validates_public_shapes_and_preserves_access_only_order`、`cargo fmt --check` 均通过。其余本轮重读未发现新的确定差异。

## 2026-10-06 第四十五轮独立复核：核心 Python/Rust 逐段重读后的 clean confirmation

在第四十四轮发现并修复导入 token coercion 后，继续逐段人工复读配置、认证、代理、账号池、Chat/Responses/Messages、图片输入/任务/存储、editable、搜索、CPA/Sub2API/ccLoad、日志、备份、JSON/Database/Git storage 与生命周期对应代码。未发现第二项新的确定差异；access-token-only、Rust-only 扩展和安全/资源/fail-closed 边界保持不变。

最终验证：Rust `cargo test --offline --all-targets -- --test-threads=1` 完成 `579 passed / 0 failed / 5 ignored`，`src/main.rs` 目标通过；`cargo fmt --check` 通过；Web 合约测试 Node runner 完成 `14 passed / 0 failed`。

## 2026-10-07 第四十六轮独立复核：Git 鉴权使用时间持久化

重新逐段对照 Python v1.7 `AuthService.authenticate` 与 Rust `AuthStore::mark_used`、JSON/Database/Git storage 调用链，发现 Rust 在 Git backend 上提前返回，认证成功后的 `last_used_at` 不会写回仓库；Python 首次 60 秒 flush 会保存该字段。现移除该 backend-only early return，让统一的 CAS mutation 路径覆盖 Git，并保留失败后的 flush 重试。

`storage::tests::app_state_uses_git_backend_for_auth_mutation_health_and_info` 新增 Git admin authentication 回归，确认认证后 auth snapshot 持久化 `last_used_at`，并继续确认账号 mutation/health/storage-info 契约；该定向测试 `1 passed / 0 failed`。`cargo fmt --all -- --check` 已通过。其余 access-token-only、Rust-only 和安全/资源/fail-closed 边界未改变。

本轮修复后的串行 `cargo test --offline --all-targets -- --test-threads=1` 完成 `579 passed / 0 failed / 5 ignored`，`src/main.rs` 目标通过；Git health slow-refresh 回归与 Responses image helper smoke test 均 `1 passed / 0 failed`。`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check` 和 `git diff --check` 均通过。Git health 测试仅放宽了并发期间允许的最终状态集合，生产逻辑未变。

## 2026-10-07 第四十七轮独立复核：Chat 历史回放与图片结果边界

本轮重新人工对照 Python v1.7 `conversation.py`、`openai_v1_chat_complete.py`、`openai_v1_image_generations.py`、`openai_v1_image_edit.py` 与 Rust `protocol_chat.rs`、图片 SSE/轮询和 Chat image adapter 调用链，确认并修复三项可观察差异：

- assistant history 跳过逻辑：Python `event_assistant_text` 只按精确 `role == "assistant"` 跳过已回放历史；Rust 原复用了大小写/空白归一化的可见文本判定，可能错误吞掉 `" Assistant "` 事件。现分离 history matcher 与 visible-text matcher。
- 图片 ID 顺序：Python 先汇总所有 file-service/`file_...` ID，再汇总 sediment ID；Rust 原按递归字符串位置把 sediment 放在后续 file ID 前。现递归收集按 file→sediment 分类后投影，保留去重和 16 项上限。
- Chat 图片文本结果：Python Chat/Responses 的 `message_as_error=False` 将无图片的上游文本/不完整结果投影为 assistant 文本，而 generations/edits 的 `message_as_error=True` 返回内容策略错误。Rust 现给 `NativeImageRequest` 增加该边界标记，并以内部 `image_message` 错误在 Chat adapter 中转成 assistant 文本；直接图片接口继续输出 `content_policy_violation`。

新增/更新 `assistant_history_uses_python_role_selection`、`native_image_result_ids_include_embedded_asset_pointers`、`native_image_sse_id_tokens_match_python_regex_boundaries`、`native_chat_image_message_errors_follow_python_route_boundary` 回归。

验证：串行 `cargo test --offline --all-targets -- --test-threads=1` 完成 Rust 主库 `581 passed / 0 failed / 5 ignored`，`src/main.rs` 目标通过；`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check` 均通过。定向 image filter `45 passed / 0 failed`，新增 Chat image boundary 与 assistant history 测试各 `1 passed / 0 failed`。

## 2026-10-07 第四十八轮独立复核：账号、配置、存储与管理链路 clean confirmation

本轮独立人工复读 Python v1.7 `account_service.py`、`config.py`、三种 storage、`log_service.py` 及管理路由，并核对当前 Rust `account_pool.rs`、`config.rs`、`storage.rs`、`management.rs` 和对应调用链。重点复查账号归一化/状态与套餐筛选、配置 coercion 和敏感字段投影、JSON/Database/Git CAS 与健康状态、日志解析/删除及管理 query 边界；未发现新的、确定且不属于既有安全/资源加固或 access-token-only 边界的行为差异，因此本轮无生产代码修改。

验证沿用当前工作区新鲜结果：串行 `cargo test --offline --all-targets -- --test-threads=1` 完成 `581 passed / 0 failed / 5 ignored`，`src/main.rs` 目标通过；`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check` 均通过。

## 2026-10-07 第四十九轮纯人工复核：公开管理入口与存储投影

本轮严格只用 `read` 逐段肉眼对照 Python v1.7 `api/system.py`、`services/config.py`、`services/log_service.py` 及账号/存储管理相关 Rust 调用链；未使用脚本、自动 diff 或生成式对比。复核 `/api/settings`、third-party apps、图片/标签/日志/代理/backup/storage 路由、认证顺序、字段 coercion、敏感字段遮蔽、错误状态和 health/storage projection，未发现新的确定性差异。本轮无生产代码修改。

## 2026-10-07 第五十轮纯人工复核：应用入口、错误包络与 SPA fallback

本轮严格只用 `read` 肉眼逐段对照 Python v1.7 `api/app.py`、`api/errors.py`、`api/support.py`、`api/system.py` 与 Rust router、CORS middleware、web fallback、`ApiError` 和 shutdown 链路；未使用脚本、自动 diff 或生成式对比。

确认并修复一项可观察差异：Python 只有 `/_next/` 缺失静态资源禁止 SPA fallback，且该分支返回普通 `{"detail":"Not Found"}`；Rust 原先额外禁用 `health/version/images/image-thumbnails` 前缀，并将缺失 `/_next/` 资源投影为 OpenAI error envelope。Rust 现按 Python 的 `_next/` 前缀边界处理，精确 `/_next` 可继续 SPA fallback，缺失 `/_next/...` 返回普通 404 detail；真实图片/健康/版本路由仍由各自 handler 处理。

新增/更新 `unknown_v1_routes_and_model_method_use_openai_errors` 回归，覆盖 `_next/`、精确 `_next`、`health`/`version` fallback 分类及普通 404 detail。

## 当前工作区最终验证

本轮未发现新的确定性 Python 1.7/Rust 逻辑差异；保留 access-token-only、Rust-only 扩展及资源/路径/CAS/fail-closed 安全边界。

- `cargo test --workspace --offline --all-targets -- --test-threads=1`：587 passed，0 failed，5 ignored。
- `node --test web/test/*.test.mjs`：373 passed，0 failed。
- `cargo check --workspace --all-targets --offline`、`cargo clippy --workspace --offline --all-targets -- -D warnings`、`cargo fmt --all -- --check`、离线 `cargo metadata` 和 `git diff --check` 均通过；Rust 检查使用 LLVM bin 目录同时加入 `PATH` 与 `LIBCLANG_PATH` 以满足 Windows bindgen 动态库加载。

本轮仅修正最终 fallback 回归测试的 rustfmt 排版，无生产逻辑变更。

## 追加独立复核

本轮再次从 Python v1.7 复读错误包络、SPA fallback、Chat assistant history、图片 ID/文本结果边界、Messages/Image Tasks 路由、Git 鉴权持久化、配置/存储和资源边界；未发现新的确定性行为差异。本轮无生产代码修改。

- `cargo test --workspace --offline --all-targets -- --test-threads=1`：587 passed，0 failed，5 ignored。
- `node --test web/test/*.test.mjs`：373 passed，0 failed。
- `cargo fmt --all -- --check`、离线 `cargo metadata` 和 `git diff --check` 均通过。

既有 access-token-only、Rust-only 扩展以及资源/路径/CAS/fail-closed 边界保持不变。

## 再次独立复核

本轮换用 coercion、错误包络、图片输入/任务边界、Chat history、图片 ID 分类、Git 鉴权写回和存储并发视角重新复读 Python v1.7 与 Rust 调用链；未发现新的确定性行为差异。本轮无生产代码修改。

- `cargo test --workspace --offline --all-targets -- --test-threads=1`：587 passed，0 failed，5 ignored。
- `node --test web/test/*.test.mjs`：373 passed，0 failed。
- 既有 Rust `cargo check`、Clippy、fmt、metadata 和 diff 检查结果保持有效；本轮没有触碰生产实现。

既有 access-token-only、Rust-only 扩展以及资源/路径/CAS/fail-closed 边界保持不变。
