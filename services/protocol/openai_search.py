from __future__ import annotations

import re

from services.account_service import account_service
from services.openai_backend_api import OpenAIBackendAPI, SEARCH_MODEL
from services.protocol.web_search_tool import normalized_sources

MODEL = SEARCH_MODEL
_PUBLIC_SEARCH_STATUSES = {"finished_successfully", "finished_partial_completion"}
_PUBLIC_SEARCH_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}\Z")


def _public_search_result(result: object) -> dict[str, object]:
    if not isinstance(result, dict):
        raise RuntimeError("invalid search result")

    projected: dict[str, object] = {
        "answer": result.get("answer") if isinstance(result.get("answer"), str) else "",
        "sources": normalized_sources(result),
    }
    conversation_id = result.get("conversation_id")
    if isinstance(conversation_id, str) and _PUBLIC_SEARCH_ID_RE.fullmatch(conversation_id.strip()):
        projected["conversation_id"] = conversation_id.strip()
    status = result.get("status")
    if isinstance(status, str) and status in _PUBLIC_SEARCH_STATUSES:
        projected["status"] = status
    return projected


def handle(body: dict[str, object]) -> dict[str, object]:
    token = account_service.get_text_access_token(model=MODEL)
    expected_account = None
    get_account_lease = getattr(account_service, "_get_account_lease", None)
    if callable(get_account_lease):
        _, expected_account = get_account_lease(token)
    backend = OpenAIBackendAPI(token)
    try:
        result = backend.search(str(body["prompt"]))
    finally:
        backend.close()
    if expected_account is None:
        account_service.mark_text_used(token)
    else:
        account_service.mark_text_used(token, expected_account=expected_account)
    return _public_search_result(result)
