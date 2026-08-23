from __future__ import annotations

from typing import Any

from services.account_service import account_service
from services.model_service import ModelCatalogPendingError, model_catalog_service
from services.openai_backend_api import _parse_model_created
from utils.helper import CODEX_IMAGE_MODEL


_PUBLIC_MODEL_TEXT_MAX_LENGTH = 256


def _model_id(item: object) -> str:
    if not isinstance(item, dict) or not isinstance(item.get("id"), str):
        return ""
    model_id = item["id"].strip()
    return model_id if model_id and len(model_id) <= _PUBLIC_MODEL_TEXT_MAX_LENGTH else ""


def _public_model_item(item: object) -> dict[str, Any] | None:
    """Project internal catalog data into the fixed public model schema."""
    model_id = _model_id(item)
    if not model_id or not isinstance(item, dict):
        return None

    owned_by = item.get("owned_by")
    if not isinstance(owned_by, str) or not owned_by.strip() or len(owned_by.strip()) > _PUBLIC_MODEL_TEXT_MAX_LENGTH:
        owned_by = "chatgpt"
    else:
        owned_by = owned_by.strip()

    root = item.get("root")
    if not isinstance(root, str) or not root.strip() or len(root.strip()) > _PUBLIC_MODEL_TEXT_MAX_LENGTH:
        root = model_id
    else:
        root = root.strip()

    parent = item.get("parent")
    if isinstance(parent, str):
        parent = parent.strip() or None
        if isinstance(parent, str) and len(parent) > _PUBLIC_MODEL_TEXT_MAX_LENGTH:
            parent = None
    else:
        parent = None

    efforts = item.get("supported_reasoning_efforts")
    public_efforts: list[str] = []
    if isinstance(efforts, list):
        for value in efforts:
            if not isinstance(value, str):
                continue
            normalized = value.strip().lower()
            if normalized and len(normalized) <= 64 and normalized not in public_efforts:
                public_efforts.append(normalized)

    account_types = item.get("supported_account_types")
    public_account_types: list[str] = []
    if isinstance(account_types, list):
        for value in account_types:
            if not isinstance(value, str):
                continue
            normalized = value.strip().lower()
            if normalized and len(normalized) <= 64 and normalized not in public_account_types:
                public_account_types.append(normalized)
    public_account_types.sort()

    projected: dict[str, Any] = {
        "id": model_id,
        "object": "model",
        "created": _parse_model_created(item.get("created")),
        "owned_by": owned_by,
        "permission": [],
        "root": root,
        "parent": parent,
        "allow_anonymous": item.get("allow_anonymous") if isinstance(item.get("allow_anonymous"), bool) else False,
        "supported_account_types": public_account_types,
    }
    if public_efforts:
        projected["supported_reasoning_efforts"] = public_efforts
    return projected


def list_models() -> dict[str, Any]:
    # Cold discovery must not turn an empty catalog into a successful response.
    # If one account type fails while another type (or anonymous discovery) is
    # already usable, publish that safe partial snapshot instead of hiding the
    # working type behind the failed representative.  The service still raises
    # when there is no usable snapshot at all.
    try:
        result = model_catalog_service.list_models(wait_for_cold=True)
    except ModelCatalogPendingError:
        partial = model_catalog_service.list_models(wait_for_cold=False)
        partial_data = partial.get("data") if isinstance(partial, dict) else None
        if not isinstance(partial_data, list) or not partial_data:
            raise
        result = partial
    if not isinstance(result, dict):
        return {"object": "list", "data": []}
    data = result.get("data")
    if not isinstance(data, list):
        return {"object": "list", "data": []}
    seen = {_model_id(item) for item in data if _model_id(item)}
    dynamic_models: set[str] = set()
    accounts = account_service.list_accounts()
    active_accounts = [
        account
        for account in accounts
        if account_service._is_image_account_available(account)
    ]
    web_image_types = {
        normalized.lower()
        for account in active_accounts
        if (normalized := account_service._normalize_account_type(account.get("type")))
    }
    codex_types = {
        normalized
        for account in active_accounts
        if isinstance(account, dict)
           and account_service._normalize_source_type(account.get("source_type")) == "codex"
           and (normalized := account_service._normalize_account_type(account.get("type")))
    }

    if active_accounts:
        dynamic_models.add("gpt-image-2")
    if codex_types & {"Plus", "Team", "Pro"}:
        dynamic_models.add(CODEX_IMAGE_MODEL)
    if "Plus" in codex_types:
        dynamic_models.add(f"plus-{CODEX_IMAGE_MODEL}")
    if "Team" in codex_types:
        dynamic_models.add(f"team-{CODEX_IMAGE_MODEL}")
    if "Pro" in codex_types:
        dynamic_models.add(f"pro-{CODEX_IMAGE_MODEL}")

    def is_public_catalog_item(item: object) -> bool:
        model_id = _model_id(item)
        return bool(model_id) and ("image" not in model_id.lower() or model_id in dynamic_models)

    data[:] = [item for item in data if is_public_catalog_item(item)]

    def supported_account_types_for(model: str) -> set[str]:
        if model == CODEX_IMAGE_MODEL:
            return {
                account_type.lower()
                for account_type in codex_types & {"Plus", "Team", "Pro"}
            }
        if model.startswith(("plus-", "team-", "pro-")):
            return {model.split("-", 1)[0]}
        return web_image_types

    for item in data:
        if not isinstance(item, dict):
            continue
        model_id = _model_id(item)
        if model_id in dynamic_models:
            item["allow_anonymous"] = False
            item["supported_account_types"] = sorted(supported_account_types_for(model_id))

    for model in sorted(dynamic_models):
        if model not in seen:
            data.append({
                "id": model,
                "object": "model",
                "created": 0,
                "owned_by": "chatgpt2api",
                "permission": [],
                "root": model,
                "parent": None,
                "allow_anonymous": False,
                "supported_account_types": sorted(supported_account_types_for(model)),
            })
    projected_data = [
        projected
        for item in data
        if (projected := _public_model_item(item)) is not None
    ]
    return {"object": "list", "data": projected_data}
