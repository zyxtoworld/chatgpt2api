# Rust canary

这是 chatgpt2api Rust 渐进重构的独立 canary，不是当前生产服务的替换品。

当前源码按职责拆分为 `config.rs`（配置与初始化错误）、`account_pool.rs`（账号快照、租约与并发池）、`model_pool.rs`（模型快照、目录刷新与公开投影）、`errors.rs`（公共错误映射）、`shutdown.rs`（服务启动与有界关闭）、`protocol_chat.rs`（Chat 消息/会话 payload、usage、SSE 文本归一化与 reasoning 校验）、`protocol_responses.rs`（Responses 输入校验与文本适配）、`protocol_codex_payload.rs`（Codex Responses 工具、输入历史与请求 payload 适配）、`codex_sse.rs`（Codex SSE 帧、非流事件归一化与 Chat frame）、`codex_upstream.rs`（Codex 版本、请求头与无状态 payload 适配），`lib.rs` 保留路由组合和尚未拆出的协议实现。拆分只改变模块归属，不改变运行时合同；每次拆分都必须通过独立 CI 的 fmt、Clippy `-D warnings` 和 `cargo test --locked --all-targets`。

`RUST_AUTH_KEYS_PATH`、`RUST_MODELS_PATH` 与 `RUST_ACCOUNTS_PATH` 只能指向人工导出的隔离快照，严禁指向生产热文件。Rust canary 对 auth、模型和账号快照提供有界的 generation-aware reload：每次请求最多允许一个受控读取，并发请求等待当前 leader 完成；合法且指纹变化的完整快照才会原子替换当前内存代，坏代清空并 fail-closed，下一份合法快照才能恢复。账号格式为 `[{"access_token","status","type","source_type","chatgpt_account_id","models"}]` 或 `{"items":[...]}`，缺失 `type` 默认 `free`，缺失 `source_type` 默认 `web`，显式值会先规范化；旧 `oauth_login` 记录归一为 Codex 能力；目录分组严格只按归一化 `account_type`，`source_type` 不创建额外目录组，只用于选择兼容的目录端点并保留给实际对话 transport；`chatgpt_account_id` 只作为认证目录请求和同一 Codex lease 的可选身份头；在途租约保留自己的不可变 slot 直到请求结束，不会被新代 Drop 误释放；这不是 Python 生产账号/模型权威热 reload、token rotation 或撤销合同。

当前已实现并测试：

- `/health`：默认 HTML，`?format=json` 返回固定公开字段；
- 停机：Unix 下响应 SIGTERM 与 Ctrl-C，其他平台响应 Ctrl-C，并交给 Axum graceful shutdown 排空连接；
- `/v1/models`：要求 Bearer 鉴权，读取显式 `RUST_MODELS` 或隔离 `models.json` 快照；文件模式按指纹 generation-aware reload，坏快照返回固定 503，按 Python 公开白名单投影，不读取 Python 生产数据。OpenAI-compatible canary 同时支持隔离上游匿名目录和按规范化 `account_type` 的目录刷新：每个 active group 只选一个确定性代表，失败按稳定顺序继续尝试到首个成功、候选耗尽或 90 秒总截止；匿名目录全局只请求一次；认证代表只访问自身 `source_type` 兼容的端点——Web/password/password-oauth 访问 Web `/backend-api/models?history_and_training_disabled=false`，Codex 访问 `/backend-api/codex/models?client_version=...`——所有成功组最终做全局 union，不能把 source 拆成目录组，也不对一个代表双发认证请求。Codex 请求的 `supported_in_api` 过滤、可选 `ChatGPT-Account-ID` 和严格 `CODEX_MODELS_CLIENT_VERSION` 都是单独合同；类型级 single-flight、5 秒退避和 90 秒 catalog 总预算保留 last-good 完整目录，匿名目录独立退避；刷新最多 4 个在途 group 任务，截止时不再启动 pending 任务，并为所有 active/pending 任务设置退避；公共目录请求按 Python `wait_for_cold=False` 只启动一个后台 owner 并立即返回静态/last-good/partial 快照，chat requested-model 路径才使用冷态等待；已有 ready 目录的同组账号增删/替换只更新 live token membership，未 ready group 的新代表不会继承旧退避；已消失 group 立即从公开能力剪枝；返回数据按 model id 稳定排序；路由只取“group 目录支持 + 当前 live token”交集；服务 shutdown 会取消并等待后台 catalog owner，不把挂起的上游请求遗留到 server 返回之后；
- native ChatGPT 目录刷新在隔离 fake/local upstream 上复现 Python 的 bootstrap→models 路径：目录请求不执行仅属于对话的 sentinel requirements 握手。每个规范化账号类型只选择一个成功代表；Web/password/password-oauth 代表访问 Web 认证目录，Codex 代表访问 Codex 目录，匿名访问 `/backend-anon/models?iim=false&is_gizmo=false`，结果全局按 slug/id 去重，响应中的 `slug` 投影为公开 `id`。真正的对话请求才按 bootstrap→sentinel requirements→conversation/Responses 执行握手。静态配置且账号快照明确提供同一模型提示时走显式静态合同；没有预填能力的静态同名模型仍先确认动态目录，动态模型请求只有在仍有 live token 的对应 group 目录未 ready 时才等待，不会被已删除 group 的 last-good 快照误满足。该路径只证明本地协议夹具，不代表真实 ChatGPT 上游 parity；
- Bearer 鉴权：严格比较本地 `RUST_AUTH_KEY` 或隔离的 `auth_keys.json` 快照中的启用 SHA-256 `key_hash`，失败响应不反射输入；
- `/v1/chat/completions`：严格检查 `model`、`prompt`、`stream`、`messages`、文本后端选项、工具容器和单一 reasoning effort 来源，并要求至少提供非空 `prompt` 或一条消息；多模态 `image_url`/`input_image`/`input_audio` 仅允许出现在 `user` 消息中；
- `/v1/responses`：在隔离 ChatGPT/Codex 上游夹具中已覆盖纯文本，以及 function tool 和 Codex web-search 的非流/流式 Responses。请求按 Codex Responses 形状转换：function schema、`tool_choice=auto`、web-search 选项、function call/output 历史的 ID/arguments 均严格保留，未知字段 fail-closed；`response.created`/文本 delta/工具事件/搜索引用/`response.completed` 终态、`[DONE]` 不提前终止和 lease 生命周期都有 route-level 回归。该切片仍只转发已验证的标准 Codex Responses 事件，不声称完整覆盖所有未来事件、图片、复杂多模态或 Anthropic Messages；
- 可选的 OpenAI-compatible 上游转发：`RUST_UPSTREAM_BASE_URL` 和独立的
  `RUST_UPSTREAM_AUTH`，不会把客户端 API key 转发给上游；非流式和 SSE body 都有固定大小上限，通用 SSE 转发还对每次上游读取设置有界 deadline。
- 隔离的 native ChatGPT canary：设置 `RUST_UPSTREAM_PROTOCOL=chatgpt` 后，优先选取可用账号租约；目录明确允许匿名且没有可用账号时，则使用空 token 走 `/backend-anon`，不发送空 Authorization。随后按 bootstrap →
  `sentinel/chat-requirements/prepare` → 本地 PoW/Turnstile proof → `finalize` → 对应的 `backend-api/conversation`、`backend-anon/conversation` 或 Codex `/backend-api/codex/responses` 顺序请求，并把同一 `AccountLease`（source、规范化套餐、Codex account id）持有到非流式结果或 SSE body 结束/取消。Codex Chat 只支持纯文本 Responses 转换；工具/复杂输入仍 fail-closed。PoW 使用隔离快照中解析出的脚本资源生成 legacy `p`，PoW/Turnstile token 同时提交到 finalize 和 conversation header；Arkose challenge 仍 fail-closed。native SSE 只接受显式 `[DONE]` 和可见 assistant 文本；成功流恰好先发送一次 `assistant` role chunk，再发送内容、finish、可选 usage 和 `[DONE]`。截断、隐藏/工具消息、畸形事件和超限会 fail-closed；上游 body、错误和 token 不进入公开响应。

  native canary 尚未实现 ChatGPT conversation 的 tool message/tool-call 转换；assistant `tool_calls` 与 `tool` 消息会在请求边界明确 fail-closed，不会静默丢弃调用元数据。

  native canary 的账号租约在上游会话返回 429/5xx 且尚未交付正文时，最多按请求模型过滤切换一次到另一个当前账号；4xx、第二次瞬态失败和正文已开始后的流错误都不继续轮换。切换前释放旧租约，失败不修改账号状态，测试锁定两次上游尝试和最终租约归还。这只是隔离快照的单次 failover 合同，不是 Python 动态账号池、刷新、禁用/限流回写或跨进程 owner parity。

  bootstrap 的 5xx、requirements prepare 的 429/5xx、requirements finalize 的 5xx 也按同一条“尚未交付正文、最多一次切换”合同分类；阶段 400、畸形/超限 body、网络错误和超时保持 fail-closed 且不切换。阶段状态只影响本地 failover 判定，不把响应 body、凭据或上游细节投影到公开错误。

  Python 会把 `developer` 消息路由到 Codex Responses；Rust canary 没有该后端实现，因此同类消息也在 native ChatGPT 边界明确 fail-closed，不伪装成 ChatGPT conversation 合同。

  native conversation 文本还对齐了 Python 的直接 append、嵌套 patch 列表和无路径字符串追加，以及私有 URL/citation/entity 标记的公开清洗；这些向量由 Python `assistant_raw_text`/`apply_text_patch`/`sanitize_output_text` 权威入口和 Rust 测试共同固定。Python `replace` patch 依赖调用方持有的 `history_text`，当前 native canary 没有可靠的 history owner，因此该边界明确 fail-closed，不宣称 replacement parity。

Turnstile canary solver 目前对 Python 当前最小成功程序（固定 opcode 3 dx）、普通 JSON `json.dumps` 格式/插入顺序、JSON 原生值在 opcode 1/5 中的有限 Python repr、数字指数宽度、CPython presence-based 引号和非可打印 Unicode 类别以及固定布尔字符串化黄金向量证明同 token；`dx` 长度上限为 2 MiB，程序最多 100,000 条指令，非法结构、未知 opcode、类型错和超时都 fail-closed。依赖进程随机性或实时钟的 `window.Math.random`/`window.performance.now` 也 fail-closed，不用固定值伪造 parity；`Reflect.set` 仅接受已建模的 OrderedMap，普通 JSON 对象也 fail-closed；`Object.keys` 仅接受 `window.localStorage` 参数。复杂的槽 9/10/16、Reflect.set、OrderedMap、未证实的 opcode 20/23 夹具已与 Python 权威 solver 对照并同样拒绝；Rust 不声称实现完整 VM 或生产 parity。它仍只证明隔离 fake transport 的协议切片，不代表浏览器脚本随版本变化后的生产 parity。
其中已锁定 opcode 23 的 `None` guard 语义和 Python 的 raw-argument 行为；这不扩大 Rust 对复杂 VM 的生产 parity 声明。

PoW 配置 builder 已按 Python 的 25 项索引布局建模，包含 index 3 的固定占位值，求解时只替换 counter；当前 canary 使用固定的 2024 时间、浏览器指纹和随机值来保证协议夹具可复现，这些值是测试夹具，不是浏览器环境 parity 或生产 PoW 实现声明。PoW 求解在有界 `spawn_blocking` 池中运行，单次 requirements deadline 同时约束 permit 获取、求解和握手 body；超时/取消会协作停止循环，permit 保持到 blocking worker 真正结束。

native canary 仍不是 Python 生产等价实现：当前不实现 Arkose 求解；PoW 的 runtime 时间/指纹输入仍是可复现夹具，进入真实 canary 流量前必须替换为受控但真实的当前时间/随机输入。非流式 `usage` 已使用 `tiktoken-rs` 的模型映射与 `o200k_base` fallback，按 Python `tiktoken` 的消息 framing 生成文本 usage，但 native 多模态、图片 token 和真实上游 usage 仍未覆盖。Rust 仍没有 Python 动态 token 刷新、禁用/限流回写、缓存合并、图片、任务、导入、持久化或生产 Compose 合同；按类型目录只覆盖隔离快照 + fake/local OpenAI-compatible upstream。它只能用于本地 fake transport 的纵切合同与资源生命周期验证，不能接生产文件、不能切 ai-arm、不能作为 Python-vs-Rust 资源胜出结论。

运行：

```powershell
$env:RUST_AUTH_KEY = "LOCAL_ONLY"
$env:RUST_MODELS = "auto"
$env:RUST_AUTH_KEYS_PATH = "C:\\path\\to\\isolated\\immutable-snapshot\\auth_keys.json"
$env:RUST_MODELS_PATH = "C:\\path\\to\\isolated\\immutable-snapshot\\models.json"
$env:RUST_ACCOUNTS_PATH = "C:\\path\\to\\isolated\\immutable-snapshot\\accounts.json"
$env:RUST_UPSTREAM_BASE_URL = "http://127.0.0.1:8088"
$env:RUST_UPSTREAM_AUTH = "Bearer UPSTREAM_ONLY"
$env:RUST_UPSTREAM_PROTOCOL = "openai" # native canary: chatgpt
cargo run
```

当前明确未覆盖：Python 的动态 token 刷新/状态回写权威链、生产浏览器输入驱动的 PoW/Turnstile parity、Arkose 求解、Responses/Anthropic、图片、任务、导入、持久化和生产 Compose。按类型目录只证明隔离快照中的代表选择、失败回退、last-good 与 live membership，不代表生产目录/账号服务 parity。没有这些合同与回滚证据前，不得把该二进制部署到 ai-arm。
