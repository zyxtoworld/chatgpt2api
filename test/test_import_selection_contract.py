from __future__ import annotations

from unittest import mock

import pytest

import services.ccload_service as ccload_module
import services.cpa_service as cpa_module
import services.sub2api_service as sub2api_module


@pytest.mark.parametrize(
    ("module", "service_factory", "args", "message"),
    (
        (
            cpa_module,
            lambda: cpa_module.CPAImportService(mock.Mock()),
            ({"id": "pool-1"}, [f"file-{index}" for index in range(cpa_module.CPA_MAX_REMOTE_FILES + 1)]),
            "selected files limit exceeded",
        ),
        (
            sub2api_module,
            lambda: sub2api_module.Sub2APIImportService(mock.Mock()),
            ({"id": "server-1"}, [str(index) for index in range(sub2api_module.SUB2API_MAX_REMOTE_ITEMS + 1)]),
            "account ids limit exceeded",
        ),
        (
            ccload_module,
            lambda: ccload_module.CCLoadImportService(mock.Mock()),
            ({"id": "server-1"}, [str(index) for index in range(1, ccload_module.CCLOAD_MAX_CHANNELS + 2)]),
            "channel ids limit exceeded",
        ),
    ),
)
def test_import_selection_limit_is_rejected_before_background_scheduling(
    module,
    service_factory,
    args,
    message: str,
) -> None:
    service = service_factory()
    with mock.patch.object(
        module,
        "reserve_background_task",
        side_effect=AssertionError("background task must not start"),
    ):
        with pytest.raises(module.PublicSafeValueError, match=message):
            service.start_import(*args)
