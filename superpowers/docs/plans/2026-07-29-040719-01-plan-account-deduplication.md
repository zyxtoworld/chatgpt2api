# Account pool deduplication

**Goal:** Prevent refreshed credentials for the same ChatGPT account from creating duplicate pool entries, then safely remove the confirmed duplicates from ai-arm.
**Why planning is required:** This changes the shared account import path and rewrites production account-pool data.
**Acceptance:** Fork `main` keeps one current token per stable account identity without merging separate workspaces; all relevant tests pass; ai-arm is backed up before mutation; the production pool changes only from 1577 rows with 70 two-row duplicate groups to 1507 unique rows; the service returns healthy; no credentials are printed.

### Outcome 1: Shared import deduplication
- Work: Update `services/account_service.py` so CPA, Sub2API, manual token/JSON, and OAuth imports deduplicate at their common write path by ChatGPT account ID, with subject and normalized email only as fallbacks. Keep the newest JWT, preserve existing operational metadata, and retain aliases for rotated tokens.
- Risks/open questions: A user can own multiple workspaces, so account ID must take precedence over subject or email. Opaque tokens have no expiry ordering and require deterministic last-import behavior.
- Verify: `uv run python -m unittest test.test_account_deduplication test.test_account_export test.test_account_image_capabilities`

### Outcome 2: Repository delivery
- Work: Run the full Python suite and static syntax/lock checks, inspect the final diff, commit only this task, and push the verified commit to `origin/main` without rebasing or force-pushing.
- Verify: `uv run python -m unittest discover -s test && uv lock --check && git diff --check`

### Outcome 3: Production data cleanup
- Work: Target only `152.70.243.22:/opt/services/chatgpt2api/data/accounts.json`. Confirm the expected dry-run counts and container identity, stop only `chatgpt2api`, create a timestamped byte-for-byte backup, atomically write the deduplicated file, and restart the service. Preserve the latest-expiry credential, earliest creation time, and accumulated counters.
- Risks/open questions: Abort before mutation if the row/group shape differs from 1577 rows and 70 pairs. Keep the backup for rollback; restart the original container even if cleanup fails.
- Verify: Recount 1507 rows and zero duplicate account IDs, verify the backup exists, require Docker health `healthy`, and inspect startup logs for errors.
