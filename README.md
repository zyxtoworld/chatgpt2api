# ChatGPT2API

ChatGPT2API 提供面向图片生成、图片编辑和多参考图场景的 OpenAI 兼容接口，并包含一个可选的 Web 管理面板。

## Docker

先准备外部配置文件。配置文件只存在于本地部署目录，不应提交到 Git 或构建进镜像：

```bash
cp config.example.json config.json
```

编辑 `config.json` 中的 `auth-key`，或通过 `CHATGPT2API_AUTH_KEY` 环境变量提供认证密钥，然后启动：

```bash
docker compose up -d --build
```

默认地址：

- Web 面板：`http://localhost:3000`
- API：`http://localhost:3000/v1`
- 数据目录：`./data`

使用已发布镜像时，将 `CHATGPT2API_IMAGE` 设置为实际镜像地址，再执行 `docker compose up -d`。

## WARP 代理

复制环境变量模板并按需调整，然后启动完整代理方案：

```bash
cp .env.example .env
docker compose -f docker-compose.warp.yml up -d --build
```

该方案包含 WARP、Privoxy、FlareSolverr 和主服务。代理运行时配置写入外部 `config.json`。

## 源码开发

后端需要 Python 3.13 或更高版本：

```bash
uv sync
uv run main.py
```

前端：

```bash
cd web
bun install
bun run dev
```

## 配置

支持以下存储后端：

- `json`：默认，数据保存在 `data/`。
- `sqlite`：使用 `DATABASE_URL` 指定 SQLite 文件。
- `postgres`：使用 `DATABASE_URL` 指定 PostgreSQL。
- `git`：使用 `GIT_REPO_URL`、`GIT_TOKEN`、`GIT_BRANCH` 和 `GIT_FILE_PATH` 指定私有 Git 存储。

所有 AI 接口使用以下请求头：

```http
Authorization: Bearer <auth-key>
```

核心接口：

- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /v1/responses`
- `POST /v1/images/generations`
- `POST /v1/images/edits`
- `POST /v1/search`

图片生成和编辑支持 `gpt-image-2`、多图结果以及 `png`、`jpeg`、`webp` 输出。完整请求字段以接口返回的 OpenAPI 描述为准。

## 安全边界

- 不要提交 `config.json`、`.env`、`data/` 或任何账号文件。
- 不要把访问令牌、刷新令牌、密码、API 密钥或代理凭据写入源码、注释、测试和日志。
- 生产部署应使用独立的外部数据目录，并在更新前完成备份。

## License

MIT
