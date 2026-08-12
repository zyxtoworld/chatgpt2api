<h1 align="center">ChatGPT2API</h1>


<p align="center">ChatGPT2API 主要是对 ChatGPT 官网相关能力进行逆向整理与封装，提供面向 ChatGPT 图片生成、图片编辑、多图组图编辑场景的 OpenAI 兼容图片 API / 代理，并集成在线画图、号池管理、多种账号导入方式与 Docker 自托管部署能力。</p>

> [!WARNING]
> 免责声明：
>
> 本项目涉及对 ChatGPT 官网文本生成、图片生成与图片编辑等相关接口的逆向研究，仅供个人学习、技术研究与非商业性技术交流使用。
>
> - 严禁将本项目用于任何商业用途、盈利性使用、批量操作、自动化滥用或规模化调用。
> - 严禁将本项目用于破坏市场秩序、恶意竞争、套利倒卖、二次售卖相关服务，以及任何违反 OpenAI 服务条款或当地法律法规的行为。
> - 严禁将本项目用于生成、传播或协助生成违法、暴力、色情、未成年人相关内容，或用于诈骗、欺诈、骚扰等非法或不当用途。
> - 使用者应自行承担全部风险，包括但不限于账号被限制、临时封禁或永久封禁以及因违规使用等所导致的法律责任。
> - 使用本项目即视为你已充分理解并同意本免责声明全部内容；如因滥用、违规或违法使用造成任何后果，均由使用者自行承担。
> - 本项目基于对 ChatGPT 官网相关能力的逆向研究实现，存在账号受限、临时封禁或永久封禁的风险。请勿使用你自己的重要账号、常用账号或高价值账号进行测试。


## 赞助商

<table>
  <tr>
    <td width="190" align="center">
      <a href="https://www.atlascloud.ai/zh?utm_source=github&utm_medium=link&utm_campaign=chatgpt2api"><img src="assets/atlascloud.svg" width="163" alt="Atlas Cloud"></a>
    </td>
    <td>
      <a href="https://www.atlascloud.ai/zh?utm_source=github&utm_medium=link&utm_campaign=chatgpt2api">Atlas Cloud</a> is a full-modal AI inference platform that gives developers a single AI API to access video generation, image generation, and LLM APIs. Instead of managing multiple vendor integrations, you connect once and get unified access to 300+ curated models across all modalities. Check out <a href="https://www.atlascloud.ai/console/coding-plan">Atlas Cloud's new coding plan promotion</a> for more budget-friendly API access.
    </td>
  </tr>
</table>

## 快速开始

### Docker 运行

```bash
git clone git@github.com:basketikun/chatgpt2api.git
cd chatgpt2api
chmod 600 config.json
docker compose up -d
```

启动前请先在 `config.json` 中设置 `auth-key`，也可以在 `docker-compose.yml` 中通过 `CHATGPT2API_AUTH_KEY` 覆盖。Compose 使用固定的 `/app/config.json`，不需要额外的配置目录或路径环境变量。

- Web 面板：`http://localhost:3000`
- API 地址：`http://localhost:3000/v1`
- 数据目录：`./data`

### WARP / FlareSolverr 稳定代理部署

如果图片链路经常遇到 Cloudflare 拦截，可以启用附带的 WARP + Privoxy + FlareSolverr 方案：

```bash
cp .env.example .env
docker compose -f docker-compose.warp.yml up -d --build
```

该 compose 会启动：

- `warp-proxy`：提供 WARP SOCKS5 出口。
- `privoxy`：把 WARP SOCKS5 转成 HTTP 代理。
- `flaresolverr`：刷新 Cloudflare clearance。
- `init-config`：幂等写入 `proxy_runtime` 默认配置。
- `app`：启动 ChatGPT2API 主服务。

默认只让上游 OpenAI / ChatGPT 请求走稳定代理，账号邮箱、CPA 等辅助链路不会被强制接管。账号自身配置的代理优先级最高，其次是稳定代理运行时，再其次是显式代理和旧版全局代理。

可在 `.env` 中调整端口和代理运行时参数，也可在后台设置页的「稳定代理运行时」面板手动保存、测试代理和测试 clearance。

### 本地开发

启动后端：

```bash
git clone git@github.com:basketikun/chatgpt2api.git
cd chatgpt2api
uv sync
uv run main.py
```

启动前端：

```bash
cd chatgpt2api/web
bun install
bun run dev
```

后续更新新版本：

```bash
docker pull ghcr.io/basketikun/chatgpt2api:latest
docker-compose down
docker-compose up -d

```

### 存储后端配置

支持通过环境变量 `STORAGE_BACKEND` 切换存储方式：

- `json` - 本地 JSON 文件（默认）
- `sqlite` - 本地 SQLite 数据库
- `postgres` - 外部 PostgreSQL（需配置 `DATABASE_URL`）
- `git` - Git 私有仓库（需配置 `GIT_REPO_URL` 和 `GIT_TOKEN`）

示例：使用 PostgreSQL

```yaml
environment:
  - STORAGE_BACKEND=postgres
  - DATABASE_URL=postgresql://user:password@host:5432/dbname
```

## 功能

### API 兼容能力

- 兼容 `POST /v1/images/generations` 图片生成接口
- 兼容 `POST /v1/images/edits` 图片编辑接口
- 兼容面向图片场景的 `POST /v1/chat/completions`
- 兼容面向图片场景的 `POST /v1/responses`
- Chat Completions 与 Responses 支持官方函数工具调用格式；函数调用和显式网页搜索通过 Codex Responses 上游原生执行，需要可用的 Codex OAuth 账号
- 兼容 Responses WebSocket 模式：连接 `ws(s)://<host>/v1/responses` 后连续发送 `response.create`；`previous_response_id` 仅引用当前连接内最近一次完成响应
- 图片生成与编辑支持 `png`、`jpeg`、`webp` 输出；流式响应分别使用官方 `image_generation.completed` / `image_edit.completed` SSE 事件，不发送 `[DONE]`
- `GET /v1/models` 返回 `gpt-image-2`、`codex-gpt-image-2`、`auto`、`gpt-5`、`gpt-5-1`、`gpt-5-2`、`gpt-5-3`、`gpt-5-3-mini`、
  `gpt-5-mini`
- 支持通过 `n` 返回多张生成结果
- 支持生成可编辑 PPT 文件
- 支持生成可编辑 PSD 文件
- 支持 Codex 中的画图接口逆向，仅 `Plus` / `Team` / `Pro` 订阅可用，模型别名为 `codex-gpt-image-2`，如有需要可自行在其他场景映射回
  `gpt-image-2`，用于和官网画图区分；也就意味着同一账号会同时有官网和 Codex 两份生图额度

### 在线画图功能

- 内置在线画图工作台，支持生成、图片编辑与多图组图编辑
- 支持 `gpt-image-2`、`codex-gpt-image-2`、`auto`、`gpt-5`、`gpt-5-1`、`gpt-5-2`、`gpt-5-3`、`gpt-5-3-mini`、`gpt-5-mini` 模型选择
- 编辑模式支持参考图上传
- 前端支持多图生成交互
- 本地保存图片会话历史，支持回看、删除和清空
- 支持服务端缓存图片URL
- 图片生成进度追踪，超时后可继续等待
- 图片懒加载与滚动位置记忆，优化大量图片场景性能

### 号池管理功能

- 自动刷新账号邮箱、类型、额度和恢复时间（异步进度追踪）
- 轮询可用账号执行图片生成与图片编辑
- 遇到 Token 失效类错误时自动剔除无效 Token
- 定时检查限流账号并自动刷新
- 支持密码重新登录恢复异常账号，刷新后可自动重登
- 支持网页端配置全局 HTTP / HTTPS / SOCKS5 / SOCKS5H 代理
- 支持 WARP / FlareSolverr 稳定代理运行时
- 支持搜索、筛选、批量刷新、导出、手动编辑和清理账号
- 支持五种导入方式：本地 CPA JSON 文件导入、远程 CPA 服务器导入、`sub2api` 服务器导入、ccLoad Codex OAuth 渠道导入、`access_token` 导入
- 支持在设置页配置 `sub2api` 服务器，筛选并批量导入其中的 OpenAI OAuth 账号
- 支持连接 ccLoad，读取 `codex_oauth` 渠道并导入 access/refresh token 及其可选 id token；管理员密码、临时会话令牌和 OAuth 凭据不会返回浏览器

### 实验性 / 规划中

- 详细状态说明见：[功能清单](./docs/feature-status.en.md)

## 效果展示

<table width="100%">
  <tr>
    <td width="50%"><img src="https://i.ibb.co/Jj8nfwwP/image.png" alt="image" border="0"></td>
    <td width="50%"><img src="https://i.ibb.co/pqf235v/image-edit.png" alt="image edit" border="0"></td>
  </tr>
  <tr>
    <td width="50%"><img src="https://i.ibb.co/tPcqtVfd/chery-studio.png" alt="chery studio" border="0"></td>
    <td width="50%"><img src="https://i.ibb.co/PsT9YHBV/account-pool.png" alt="account pool" border="0"></td>
  </tr>
  <tr>
    <td width="50%"><img src="https://i.ibb.co/rRWLG08q/new-api.png" alt="new api" border="0"></td>
  </tr>
</table>

## API

所有 AI 接口都需要请求头：

```http
Authorization: Bearer <auth-key>
```

<details>
<summary><code>GET /v1/models</code></summary>
<br>

返回当前暴露的图片模型列表。

```bash
curl http://localhost:8000/v1/models \
  -H "Authorization: Bearer <auth-key>"
```

<details>
<summary>说明</summary>
<br>

| 字段   | 说明                                                                                                         |
|:-----|:-----------------------------------------------------------------------------------------------------------|
| 返回模型 | `gpt-image-2`、`codex-gpt-image-2`、`auto`、`gpt-5`、`gpt-5-1`、`gpt-5-2`、`gpt-5-3`、`gpt-5-3-mini`、`gpt-5-mini` |
| 接入场景 | 可接入 Cherry Studio、New API 等上游或客户端                                                                          |

<br>
</details>
</details>

<details>
<summary><code>WS /v1/responses</code></summary>
<br>

使用与 HTTP API 相同的 Bearer 凭据建立 WebSocket，在一个连接内连续发送官方 `response.create` 事件。普通文本、图片、函数工具和网页搜索请求都会优先复用同一条 Codex 原生上游 WebSocket；账号凭据或传输路径变化、失败终态会关闭旧上游连接，并在需要时用当前连接内的受控 transcript 重建请求。没有可用 Codex OAuth 账号或上游握手在发送请求前失败时，普通生成回合才安全回退到既有 HTTP/SSE 链路；`generate:false` 预热不回退，发送后的异常不会自动重放。

服务端逐条返回既有 Responses 流事件；需要延续上一轮时，把该连接最近一次成功终态的 `response.id` 作为下一轮 `previous_response_id`。连接外或不匹配的响应 ID 会返回 `previous_response_not_found`，不会读取其他连接的状态。引用上一响应的回合失败后，该 ID 会立即从连接状态逐出；单连接最长保留 60 分钟，到期返回 `websocket_connection_limit_reached` 并关闭，客户端需新建连接。

Codex 原生上游支持连接预热：发送 `generate:false`、`tools:[]` 的 `response.create` 后，使用返回的 `response.id` 作为下一条事件的 `previous_response_id`。`generate` 省略表示正常生成；显式值只接受布尔值 `false`。预热和后续请求会保持在同一条上游 WebSocket 上，`stream` 与 `background` 是传输层字段，不转发到上游请求体。

```json
{"type":"response.create","model":"gpt-5.5","input":[],"tools":[],"generate":false}
```

```json
{"type":"response.create","model":"gpt-5","input":"第一轮"}
```

```json
{"type":"response.create","model":"gpt-5","previous_response_id":"resp_...","input":"继续"}
```

</details>

<details>
<summary><code>POST /v1/images/generations</code></summary>
<br>

OpenAI 兼容图片生成接口，用于文生图。

```bash
curl http://localhost:8000/v1/images/generations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <auth-key>" \
  -d '{
    "model": "gpt-image-2",
    "prompt": "一只漂浮在太空里的猫",
    "n": 1,
    "response_format": "b64_json",
    "output_format": "webp",
    "output_compression": 90
  }'
```

<details>
<summary>字段说明</summary>
<br>

| 字段                   | 说明                                                                  |
|:---------------------|:--------------------------------------------------------------------|
| `model`              | 图片模型，当前可用值以 `/v1/models` 返回结果为准，推荐使用 `gpt-image-2`          |
| `prompt`             | 图片生成提示词                                                             |
| `n`                  | 生成数量，遵循官方图片接口范围 `1-10`                                             |
| `size`               | `auto`、标准尺寸，或 `gpt-image-2` 的 `WIDTHxHEIGHT`；宽高须为 16 的倍数、比例在 `1:3-3:1`，最大边界为 `3840x2160` |
| `quality`            | `auto`（默认）、`low`、`medium` 或 `high`                                 |
| `response_format`    | `b64_json`（默认）或 `url`；流式请求只支持 `b64_json`                            |
| `output_format`      | `png`（默认）、`jpeg` 或 `webp`                                            |
| `output_compression` | `jpeg` / `webp` 的压缩质量，范围 `0-100`                                    |
| `stream`             | 为 `true` 时返回 `image_generation.completed` 类型化 SSE，包含输出元数据与 usage，不发送 `[DONE]` |
| `partial_images`     | 当前上游没有真实局部位图能力，只接受省略或 `0`；大于 `0` 会明确返回 `400`                    |
| `background`         | 当前上游只支持默认 `auto`；`opaque` / `transparent` 会明确返回 `400`              |
| `moderation`         | 当前上游只支持默认 `auto`；`low` 会明确返回 `400`                                  |
| `style`              | 当前配置模型不支持，传入时明确返回 `400`                                               |
| `user`               | 当前上游没有终端用户标识透传能力，传入时明确返回 `400`                                      |

<br>
</details>
</details>

<details>
<summary><code>POST /v1/images/edits</code></summary>
<br>

OpenAI 兼容图片编辑接口，可上传图片文件，也可按 JSON 格式传入 data URL、base64 图片内容或公开的 `http://` / `https://` 图片 URL 并生成编辑结果。远程 URL 只允许无凭据的 HTTP(S)，服务端会逐跳校验公网解析地址、重定向、大小、响应类型和图片格式。

```bash
curl http://localhost:8000/v1/images/edits \
  -H "Authorization: Bearer <auth-key>" \
  -F "model=gpt-image-2" \
  -F "prompt=把这张图改成赛博朋克夜景风格" \
  -F "n=1" \
  -F "image=@./input.png"
```

也可以传入 data URL：

```bash
curl http://localhost:8000/v1/images/edits \
  -H "Authorization: Bearer <auth-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-image-2",
    "prompt": "把这张图改成赛博朋克夜景风格",
    "images": [
      {"image_url": "data:image/png;base64,<base64-image-data>"}
    ]
  }'
```

公开 HTTPS 图片 URL 使用同一 JSON 结构：

```json
{
  "model": "gpt-image-2",
  "prompt": "把这张图改成赛博朋克夜景风格",
  "images": [
    {"image_url": "https://cdn.example.com/input.png"}
  ]
}
```

<details>
<summary>字段说明</summary>
<br>

| 字段                   | 说明                                                          |
|:---------------------|:------------------------------------------------------------|
| `model`              | 图片模型，`gpt-image-2`                                           |
| `prompt`             | 图片编辑提示词                                                     |
| `n`                  | 生成数量，遵循官方图片接口范围 `1-10`                                 |
| `image`              | 需要编辑的图片文件，使用 multipart/form-data 上传                         |
| `images`             | JSON 图片引用数组，支持 data URL、base64 图片内容或安全下载的 HTTP(S) 图片 URL，最多 16 张 |
| `image_url`          | 表单模式下可传 data URL 或安全下载的公开 HTTP(S) 图片 URL，支持重复字段传多张图          |
| `mask`               | 可选单张遮罩；必须与第一张输入图格式、尺寸一致且包含 alpha 通道，只作用于第一张图         |
| `size`               | `auto`、`1024x1024`、`1536x1024` 或 `1024x1536`                    |
| `quality`            | `auto`（默认）、`low`、`medium` 或 `high`                              |
| `response_format`    | `b64_json`（默认）或 `url`；流式请求只支持 `b64_json`                    |
| `output_format`      | `png`（默认）、`jpeg` 或 `webp`                                    |
| `output_compression` | `jpeg` / `webp` 的压缩质量，范围 `0-100`                            |
| `stream`             | 为 `true` 时返回 `image_edit.completed` 类型化 SSE，包含输出元数据与 usage，不发送 `[DONE]` |
| `input_fidelity`     | 当前上游不支持，传入时明确返回 `400`                                       |
| `user`               | 当前上游没有终端用户标识透传能力，传入时明确返回 `400`                              |
| `client_task_id`     | 仅 `/api/image-tasks/edits` 异步任务接口支持；同步编辑接口传入时返回 `400`           |

<br>
</details>
</details>

<details>
<summary><code>POST /v1/chat/completions</code></summary>
<br>

面向文本、网页搜索与图片场景的 Chat Completions 兼容接口，不是完整通用聊天代理。
消息中的 \`image_url\` HTTP(S) 图片引用与图片编辑接口共用同一安全下载器。
普通文本链路支持消息历史、流式 usage 与思考强度；官方 `developer` 消息及用户 `input_audio`（`wav` / `mp3` base64）使用 Codex Responses 原生链路。固定 Codex wire 无法表达的消息级 `name` 及未知消息字段会返回 400，不会静默丢弃。服务会读取上游模型目录的 `thinking_efforts` / `supported_reasoning_efforts`：请求强度不在所选模型支持范围内时，自动使用该模型公布的最强档；未公布能力时保留原有兼容映射。采样、输出上限等当前上游无法兑现的字段及未知字段同样返回 400。

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <auth-key>" \
  -d '{
    "model": "gpt-image-2",
    "messages": [
      {
        "role": "user",
        "content": "生成一张雨夜东京街头的赛博朋克猫"
      }
    ],
    "n": 1
  }'
```

<details>
<summary>字段说明</summary>
<br>

| 字段                   | 说明                                                                           |
|:---------------------|:-----------------------------------------------------------------------------|
| `model`              | 文本、搜索或图片模型；搜索模型会触发网页搜索兼容逻辑                                                   |
| `messages`           | 消息数组，支持文本、搜索、图片、`developer` 与 `input_audio`；原生能力需要可用的 Codex OAuth 账号 |
| `n`                  | 图片生成数量，按当前实现解析为图片数量                                                          |
| `stream`             | 文本、搜索和图片场景均支持                                                                     |
| `stream_options`     | 流式请求支持 `include_usage=true` 返回终态 usage chunk，并接受官方 `include_obfuscation=false`；当前后端不能启用流混淆                 |
| `tools`              | 支持 Chat 官方嵌套 `function` 定义；也兼容 `web_search` 系列工具；工具结果续轮可仅提交历史 `tool_calls` / `tool` 消息，不必重复工具定义 |
| `tool_choice`        | 固定 Codex 上游当前只发送并接受 `auto`；省略同样按 `auto`，其他字符串或对象明确返回 `400`             |
| `parallel_tool_calls`| 原生工具链严格接受布尔值并透传；省略时默认 `true`                                                  |
| `web_search_options` | 按 Chat 官方字段映射为原生 `web_search` 工具；需可用的 Codex OAuth 账号                                 |
| `response_format`    | 支持 `text` 与可映射的 `json_schema`；传入时直接走 Codex Responses，无需同时声明工具                              |
| `verbosity`          | 支持 `low`、`medium`、`high`，映射为 Codex Responses `text.verbosity`                                  |
| `prompt_cache_key` / `service_tier` | 传入时走 Codex Responses；仅转发上游请求结构可表达的缓存键和服务层级                                      |

<br>
</details>
</details>

<details>
<summary><code>POST /v1/responses</code></summary>
<br>

面向文本、原生函数工具、网页搜索和图片生成的 Responses API 兼容接口，不是完整通用 Responses API 代理。
输入中的 \`input_image.image_url\` HTTP(S) 图片引用与图片编辑接口共用同一安全下载器。
函数工具与网页搜索使用 Codex Responses 上游，要求存在可用的 Codex OAuth 账号；不支持的上游参数会明确返回 400，不会静默忽略。
普通文本和图片工具链路使用同一显式参数边界；未知顶层字段或未实现的图片工具选项同样返回 400。

```bash
curl http://localhost:8000/v1/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <auth-key>" \
  -d '{
    "model": "gpt-5",
    "input": "生成一张未来感城市天际线图片",
    "tools": [
      {
        "type": "image_generation"
      }
    ]
  }'
```

<details>
<summary>字段说明</summary>
<br>

| 字段       | 说明                                                                                      |
|:---------|:----------------------------------------------------------------------------------------|
| `model`  | Responses 编排模型并原样回显；图片工具的图片模型由 `tools[].model` 指定，省略时使用 `gpt-image-2`                           |
| `input`  | 支持字符串、单个输入对象或对象数组；EasyInputMessage 可省略 `type`，支持 `user/assistant/system/developer` 角色，`phase` 仅用于 assistant。Codex 原生链严格验证并保留音频、四种图片 detail 及 reasoning/tool/web/image/compaction 历史项，顶层原生多模态 part 会规范化为用户 message；图片生成需能解析出提示词。固定上游没有 `input_file`、`file_id`、caller 或 content part 级缓存断点，这些字段及未知/畸形 item/part 明确返回 `400` |
| `instructions` | 可选顶层指令；文本及 Codex 原生工具链按对应上游合同传递，图片生成链路传入时明确返回 `400`                             |
| `context_management` | Codex 文本/原生工具链支持官方 compaction 数组；当前只接受 `type=compaction`，`compact_threshold` 至少为 `1000` |
| `tools`  | 支持官方扁平 `function`、`image_generation`、`web_search`、`web_search_2025_08_26`、`web_search_preview`、`web_search_preview_2025_03_11`；工具输出续轮无需重复定义 tools，函数/自定义工具输出支持字符串或固定 Codex 可表达的文本、图片、音频、加密内容数组，图片工具支持独立模型与渲染参数 |
| `web_search` 控制 | 支持 `search_context_size`、`user_location`、最多 100 个无协议前缀的 `filters.allowed_domains`、`external_web_access` 与 `search_content_types`；固定 Codex 上游无法表达的 `blocked_domains`、`return_token_budget`、`image_settings` 明确返回 `400` |
| `include` | 支持 `web_search_call.action.sources` 与 `web_search_call.results`；请求会同时保留续接所需的 `reasoning.encrypted_content` |
| `tool_choice` | 固定 Codex 上游当前只发送并接受 `auto`；省略同样按 `auto`，其他字符串或对象明确返回 `400`                         |
| `parallel_tool_calls` | 原生工具链严格接受布尔值并透传；省略时默认 `true`                                                     |
| `reasoning` | `effort` 按实际选中账号的模型能力归一化；`summary` / `context` 会独立触发 Codex 原生链并按固定上游结构透传                   |
| `text` | 支持 `verbosity` 与可映射的 `json_schema` 格式；单独传入即可触发 Codex 原生链，无法表示的格式明确返回 400                    |
| `prompt_cache_key` / `service_tier` | 单独传入即可走 Codex Responses；仅转发固定上游请求结构可表达的缓存键和服务层级                              |
| `stream` | 支持原生 Responses SSE 事件；函数参数增量使用 `response.function_call_arguments.delta`                              |
| `stream_options` | 接受 `include_obfuscation=false`；`reasoning_summary_delivery=sequential_cutoff` 会走原生链，其他值明确返回 400            |

<br>
</details>
</details>

## 社区支持

学 AI , 上 L 站：[LinuxDO](https://linux.do)

## Contributors

感谢所有为本项目做出贡献的开发者：

<a href="https://github.com/basketikun/chatgpt2api/graphs/contributors">
  <img alt="Contributors" src="https://contrib.rocks/image?repo=basketikun/chatgpt2api" />
</a>

## Star History

[![Star History Chart](https://api.star-history.com/chart?repos=basketikun/chatgpt2api&type=date&legend=top-left)](https://www.star-history.com/?repos=basketikun%2Fchatgpt2api&type=date&legend=top-left)
