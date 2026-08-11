# 主分支多架构 Docker 镜像发布

**Goal:** 让推送到 `main` 时由 GitHub Actions 构建并发布 `linux/amd64`、`linux/arm64` 多架构镜像，同时保留版本标签发布。
**Why planning is required:** 这是修改 CI/CD 并向 GitHub 主分支推送的跨系统外部状态变更。
**Acceptance:** 工作流仅在目标分支/版本标签触发，GHCR 权限和镜像标签明确，Dockerfile 可通过静态与本地可执行校验，提交后远端主分支指向该提交且 Action 已被触发；不把 Action 成功假定为本地验证结果。

### Outcome 1: 核对发布目标与现有构建合同
- Work: 确认当前分支、远端、Dockerfile 构建阶段、镜像仓库命名及既有工作树改动。
- Risks/open questions: 主分支推送会触发外部 Action；保留用户已有改动，不回滚、不强推。
- Verify: `git status --short`, `git branch -vv`, `git remote -v`, `Get-Content Dockerfile`, `Get-Content .github/workflows/docker-publish.yml`

### Outcome 2: 让 main 推送发布多架构镜像
- Work: 修改 `.github/workflows/docker-publish.yml`，加入 `main` push 触发，给默认分支稳定镜像标签，保留 `v*` 版本标签和现有 GHCR 登录、Buildx、缓存、多平台配置。
- Verify: YAML/文本审查、`git diff --check`，确认平台集合和权限配置。

### Outcome 3: 交付前验证与远端确认
- Work: 运行适用的本地测试/构建检查，审阅最终 diff，提交所有当前授权交付范围的工作树改动并推送 `main`。
- Risks/open questions: GitHub Action 的实际构建结果属于远端证据；若推送或 Action 失败，保留原始状态并报告，不强推、不回滚、不拉取合并。
- Verify: 提交前 `git diff --check` 与必要构建检查；提交后 `git rev-parse HEAD`, `git ls-remote origin refs/heads/main`，并检查 Action 触发状态（若本地工具可用）。
