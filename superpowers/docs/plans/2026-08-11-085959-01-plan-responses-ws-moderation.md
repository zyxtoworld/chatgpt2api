# Responses WebSocket 内容审核错误合同修复

**Goal:** 让 Responses WebSocket 在内容审核拒绝时返回正确的可恢复请求错误，同时保持 5xx 脱敏、连接可继续处理合法回合，并确保上游与会话资源不泄漏。
**Why planning is required:** 这是公开 WebSocket API、认证请求链和长连接资源生命周期的高风险合同修复。
**Acceptance:** 审核 4xx 在任何上游调用前投影为固定 `invalid_request_error`；同一连接可继续处理下一条合法 `response.create`；审核 5xx 使用固定公共服务错误；日志状态与 iterator、slot、session 均正确收口；不提交、不推送、不触碰真实账号或外部网络。

### Outcome 1: 固定可复现红态
- Work: 在 `test/test_responses_websocket.py` 通过真实 WebSocket 路由注入审核 `HTTPException(400)`，断言现状错误类型、上游未调用以及当前连接行为。
- Risks/open questions: 需要沿现有 `filter_or_log`、`LoggedCall` 和 WS 外层异常边界复用既有安全消息，不公开审核异常 detail 原文。
- Verify: `.venv\\Scripts\\python.exe -m pytest -q test/test_responses_websocket.py -k moderation`

### Outcome 2: 修正审核异常的公共边界
- Work: 在 `api/ai.py` 的 WS 单回合边界区分审核产生的 4xx 与未知/5xx 异常；4xx 发送固定 `invalid_request_error` 并继续读下一回合，非 4xx 维持固定脱敏错误；保持 `LoggedCall` 失败日志和上游未调用语义。
- Risks/open questions: 不复制新的协议错误格式，不改变同步 `/v1/responses` 的已有合同，不把任意 `HTTPException` detail 透传给客户端。
- Verify: focused WS、内容过滤、公共错误和资源生命周期测试。

### Outcome 3: 回归与完成前核验
- Work: 检查最终 diff、编译和受影响本地全套；将真实账号/上游及未执行检查与本地通过证据分开记录。
- Risks/open questions: 外部服务和真实凭据保持 missing，不伪造集成通过。
- Verify: `.venv\\Scripts\\python.exe -m pytest ...`、compileall、`git diff --check`。

### Outcome 4: Git push 失败后的远端一致性
- Work: 在 `test/test_git_storage_recovery_contract.py` 用真实本地 bare remote 制造 push 前远端分叉，覆盖 accounts/auth_keys 的失败保存、后续读取/health、进程重建和同值重试；`services/storage/git_storage.py` 失败时恢复到当前可验证远端状态并继续 fail-closed，不能把本地领先提交当成已持久化。
- Risks/open questions: 不用重克隆掩盖状态；保留 pull 失败时最后有效缓存不被删除的既有合同。
- Verify: Git storage recovery focused、storage propagation/public error、编译和本地全套。
