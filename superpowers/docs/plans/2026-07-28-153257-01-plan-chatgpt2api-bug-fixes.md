# ChatGPT2API bug fixes

**Goal:** Fix the confirmed routing, availability, security, protocol, and test-gate defects in the integrated ChatGPT2API branch.
**Why planning is required:** The work changes authentication transport security, server-side URL fetching, model-routing contracts, and public API error behavior.
**Acceptance:** Model requests route only to accounts that advertise the requested model; catalog outages do not serially block on the whole account pool; anonymous fallback and unavailable-model status behavior are correct; remote image fetching rejects unsafe targets and enforces a streaming byte limit; OAuth/password traffic verifies TLS; request messages are not silently removed; repeatable offline tests and CI checks pass; the final worktree contains only scoped changes.

### Outcome 1: Route models by account capability without blocking readers
- Work: Store discovered models per account token, derive the public union and routes from those capabilities, scan every active account with bounded concurrency and per-request timeouts, and serve the last successful catalog while refresh is in progress.
- Risks/open questions: Token rotation must preserve capability ownership without logging or persisting raw credentials outside existing account storage.
- Verify: `.venv\\Scripts\\python.exe -m unittest -v test.test_model_catalog_service test.test_text_model_routing`

### Outcome 2: Preserve fallback and public API error semantics
- Work: Represent anonymous selection separately from missing routing, retry anonymous after an invalid authenticated token, and map unavailable requested models to a client error instead of an upstream 502.
- Verify: `.venv\\Scripts\\python.exe -m unittest -v test.test_text_model_routing test.test_model_error_response`

### Outcome 3: Secure outbound image and authentication traffic
- Work: Validate every remote image destination and redirect against private, loopback, link-local, reserved, and unsupported addresses; stream downloads with a hard byte cap; enable TLS certificate verification for password and OAuth token flows with existing proxy support.
- Risks/open questions: Test doubles must avoid external DNS/network access, and custom interception proxies require a trusted CA rather than disabled verification.
- Verify: `.venv\\Scripts\\python.exe -m unittest -v test.test_image_inputs_security test.test_account_tls`

### Outcome 4: Preserve request content and establish a repeatable gate
- Work: Stop deleting adjacent messages by default and avoid mutating upstream payloads for cache normalization; isolate offline tests from local credentials and live HTTP tests; declare test dependencies; add PR/push CI and deterministic frontend dependency installation.
- Verify: `.venv\\Scripts\\python.exe -m pytest -m "not live" -q` and `bun run --cwd web build`

### Outcome 5: Verify the integrated result
- Work: Run focused regressions, the complete offline suite, Python compilation, TypeScript checking, Docker build, diff checks, and inspect the final changes against every acceptance claim.
- Verify: `git diff --check && git status --short --branch`
