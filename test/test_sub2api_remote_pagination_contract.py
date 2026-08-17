from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import json
from threading import Event

import pytest

import services.ccload_service as ccload_module
import services.sub2api_service as sub2api_module


def test_sub2api_config_rejects_unsafe_base_urls_before_persisting(tmp_path):
    unsafe_urls = (
        "file:///private/sub2api",
        "https://user:password@sub2api.example.test",
        "https://sub2api.example.test/root?token=management-secret",
        "https://sub2api.example.test/root#fragment",
    )

    for index, base_url in enumerate(unsafe_urls):
        store_file = tmp_path / f"sub2api-{index}.json"
        config = sub2api_module.Sub2APIConfig(store_file)

        with pytest.raises(sub2api_module.PublicSafeValueError):
            config.add_server(
                name="preview",
                base_url=base_url,
                email="",
                password="password-secret",
                api_key="api-key-secret",
                group_id="",
            )

        assert config.list_servers() == []
        if store_file.exists():
            assert "password-secret" not in store_file.read_text(encoding="utf-8")


def test_sub2api_config_rejects_persisted_unsafe_base_url(tmp_path):
    store_file = tmp_path / "sub2api.json"
    store_file.write_text(
        json.dumps([{
            "id": "server-1",
            "name": "preview",
            "base_url": "https://user:password@sub2api.example.test",
            "email": "",
            "password": "password-secret",
            "api_key": "api-key-secret",
            "group_id": "",
            "import_job": None,
        }]),
        encoding="utf-8",
    )

    with pytest.raises(sub2api_module.StorageDataError):
        sub2api_module.Sub2APIConfig(store_file)


def test_sub2api_account_browse_passes_shared_deadline_into_login(monkeypatch):
    session = type("Session", (), {
        "get": lambda self, *_args, **_kwargs: _Response({
            "code": 0,
            "data": {"items": []},
        }),
        "close": lambda self: None,
    })()
    deadlines = []

    def fake_login(_base_url, _email, _password, *, deadline=None):
        deadlines.append(deadline)
        return "jwt-token", 4_000_000_000.0

    sub2api_module._token_cache.clear()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_login", fake_login)
    monkeypatch.setattr(sub2api_module.time, "monotonic", lambda: 100.0)

    sub2api_module.list_remote_accounts({
        "id": "server-1",
        "base_url": "https://sub2api.example.test",
        "email": "owner@example.test",
        "password": "password",
    })

    assert deadlines == [100.0 + sub2api_module.SUB2API_REMOTE_BROWSE_TIMEOUT_SECS]


class _Response:
    ok = True

    def __init__(self, payload):
        self._payload = payload
        self.closed = False
        self.content = json.dumps(payload).encode("utf-8")

    def json(self):
        return self._payload

    def close(self):
        self.closed = True


class _NeverEndingPageSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def get(self, _url, **_kwargs):
        self.calls += 1
        if self.calls > 2:
            raise AssertionError("pagination did not honor its deadline")
        return _Response(self.payload)

    def close(self):
        pass


@pytest.mark.parametrize(
    ("reader", "payload"),
    (
        (
            sub2api_module.list_remote_accounts,
            {
                "code": 0,
                "data": {
                    "items": [
                        {"id": str(index), "credentials": {}}
                        for index in range(200)
                    ],
                        "total": 5_000,
                }
            },
        ),
        (
            sub2api_module.list_remote_groups,
            {
                "code": 0,
                "data": {
                    "items": [
                        {"id": str(index)}
                        for index in range(200)
                    ],
                        "total": 5_000,
                }
            },
        ),
    ),
)
def test_remote_pagination_stops_when_absolute_deadline_expires(
    monkeypatch,
    reader,
    payload,
):
    session = _NeverEndingPageSession(payload)
    clock_values = iter((0.0, 0.0, 0.0, 2.0))

    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})
    monkeypatch.setattr(
        sub2api_module,
        "SUB2API_REMOTE_BROWSE_TIMEOUT_SECS",
        1.0,
        raising=False,
    )
    monkeypatch.setattr(sub2api_module.time, "monotonic", lambda: next(clock_values))

    with pytest.raises(RuntimeError, match="remote browse timed out"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == 2


@pytest.mark.parametrize("reader", (sub2api_module.list_remote_accounts, sub2api_module.list_remote_groups))
def test_sub2api_remote_pagination_rejects_unbounded_declared_total(monkeypatch, reader):
    session = _NeverEndingPageSession({
        "code": 0,
        "data": {
            "items": [{"id": str(index), "credentials": {}} for index in range(200)],
            "total": 1_000_000,
        },
    })
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="remote item limit exceeded"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == 1


@pytest.mark.parametrize("reader", (sub2api_module.list_remote_accounts, sub2api_module.list_remote_groups))
def test_sub2api_remote_pagination_rejects_missing_items_array(monkeypatch, reader):
    session = _NeverEndingPageSession({
        "code": 0,
        "data": {"unexpected": "not-a-page"},
    })
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="invalid sub2api pagination payload"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == 1


@pytest.mark.parametrize(
    ("reader", "item"),
    (
        (
            sub2api_module.list_remote_accounts,
            {
                "id": "1",
                "name": "n" * 257,
                "status": "s" * 257,
                "credentials": {
                    "email": "e" * 257,
                    "plan_type": "p" * 257,
                    "expires_at": "t" * 257,
                },
            },
        ),
        (
            sub2api_module.list_remote_groups,
            {
                "id": "1",
                "name": "n" * 257,
                "description": "d" * 257,
                "platform": "p" * 257,
                "status": "s" * 257,
            },
        ),
    ),
)
def test_sub2api_public_metadata_bounds_text_fields(monkeypatch, reader, item):
    session = _NeverEndingPageSession({
        "code": 0,
        "data": {"items": [item], "total": 1},
    })
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    result = reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert all(len(str(value)) <= 256 for row in result for value in row.values())
    assert "n" * 257 not in repr(result)
    assert "s" * 257 not in repr(result)


@pytest.mark.parametrize(
    ("reader", "item_factory"),
    (
        (sub2api_module.list_remote_accounts, lambda index: {"id": str(index), "credentials": {}}),
        (sub2api_module.list_remote_groups, lambda index: {"id": str(index)}),
    ),
)
def test_sub2api_remote_pagination_uses_declared_total_for_short_pages(
    monkeypatch,
    reader,
    item_factory,
):
    pages = [
        [item_factory(index) for index in range(start, min(start + 50, 250))]
        for start in range(0, 250, 50)
    ]

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            page = pages[self.calls]
            self.calls += 1
            return _Response({
                "code": 0,
                "data": {"items": page, "total": 250},
            })

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    result = reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert len(result) == 250
    assert session.calls == 5


@pytest.mark.parametrize("reader", (sub2api_module.list_remote_accounts, sub2api_module.list_remote_groups))
def test_sub2api_remote_pagination_rejects_empty_page_before_declared_total(monkeypatch, reader):
    session = _NeverEndingPageSession({
        "code": 0,
        "data": {"items": [], "total": 100},
    })
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="invalid sub2api pagination payload"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == 1


@pytest.mark.parametrize(
    ("reader", "item_factory"),
    (
        (sub2api_module.list_remote_accounts, lambda index: {"id": str(index), "credentials": {}}),
        (sub2api_module.list_remote_groups, lambda index: {"id": str(index)}),
    ),
)
def test_sub2api_remote_pagination_continues_full_page_when_total_is_missing(
    monkeypatch,
    reader,
    item_factory,
):
    pages = [
        [item_factory(index) for index in range(200)],
        [item_factory(index) for index in range(200, 250)],
    ]

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            page = pages[min(self.calls, len(pages) - 1)]
            self.calls += 1
            return _Response({"code": 0, "data": {"items": page}})

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    result = reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert len(result) == 250
    assert session.calls == 2


@pytest.mark.parametrize(
    ("reader", "item_factory"),
    (
        (sub2api_module.list_remote_accounts, lambda index: {"id": str(index), "credentials": {}}),
        (sub2api_module.list_remote_groups, lambda index: {"id": str(index)}),
    ),
)
def test_sub2api_remote_pagination_rejects_total_that_moves_before_current_page(
    monkeypatch,
    reader,
    item_factory,
):
    pages = [
        {"items": [item_factory(index) for index in range(200)], "total": 500},
        {"items": [item_factory(index) for index in range(200, 250)], "total": 100},
    ]

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            payload = pages[min(self.calls, len(pages) - 1)]
            self.calls += 1
            return _Response({"code": 0, "data": payload})

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="invalid sub2api pagination payload"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == 2


@pytest.mark.parametrize(
    ("reader", "item_factory"),
    (
        (sub2api_module.list_remote_accounts, lambda index: {"id": str(index), "credentials": {}}),
        (sub2api_module.list_remote_groups, lambda index: {"id": str(index)}),
    ),
)
def test_sub2api_remote_pagination_rejects_declared_total_rollback(
    monkeypatch,
    reader,
    item_factory,
):
    pages = [
        {"items": [item_factory(index) for index in range(50)], "total": 500},
        {"items": [item_factory(index) for index in range(50, 100)], "total": 100},
    ]

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            payload = pages[self.calls]
            self.calls += 1
            return _Response({"code": 0, "data": payload})

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="invalid sub2api pagination payload"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == 2


@pytest.mark.parametrize(
    ("reader", "item_factory", "pages"),
    (
        (
            sub2api_module.list_remote_accounts,
            lambda index: {"id": str(index), "credentials": {}},
            [
                {"items": [{"id": str(index), "credentials": {}} for index in range(200)]},
                {"items": [{"id": str(index), "credentials": {}} for index in range(200, 250)], "total": 250},
            ],
        ),
        (
            sub2api_module.list_remote_groups,
            lambda index: {"id": str(index)},
            [
                {"items": [{"id": str(index)} for index in range(200)]},
                {"items": [{"id": str(index)} for index in range(200, 250)], "total": 250},
            ],
        ),
    ),
)
def test_sub2api_remote_pagination_rejects_total_mode_drift_from_missing_to_declared(
    monkeypatch,
    reader,
    item_factory,
    pages,
):
    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            payload = pages[self.calls]
            self.calls += 1
            return _Response({"code": 0, "data": payload})

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="invalid sub2api pagination payload"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == 2


@pytest.mark.parametrize("reader", (sub2api_module.list_remote_accounts, sub2api_module.list_remote_groups))
def test_sub2api_remote_pagination_rejects_total_mode_drift_from_declared_to_missing(monkeypatch, reader):
    first_page = [
        {"id": str(index), "credentials": {}}
        if reader is sub2api_module.list_remote_accounts
        else {"id": str(index)}
        for index in range(200)
    ]
    second_page = [
        {"id": str(index), "credentials": {}}
        if reader is sub2api_module.list_remote_accounts
        else {"id": str(index)}
        for index in range(200, 250)
    ]
    pages = [
        {"items": first_page, "total": 250},
        {"items": second_page},
    ]

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            payload = pages[self.calls]
            self.calls += 1
            return _Response({"code": 0, "data": payload})

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="invalid sub2api pagination payload"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == 2


@pytest.mark.parametrize("reader", (sub2api_module.list_remote_accounts, sub2api_module.list_remote_groups))
def test_sub2api_remote_pagination_caps_missing_total_full_pages(monkeypatch, reader):
    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            self.calls += 1
            return _Response({
                "code": 0,
                "data": {
                    "items": [
                        {"id": str(index), "credentials": {}}
                        for index in range(200)
                    ],
                },
            })

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="remote item limit exceeded"):
        reader({"id": "server-1", "base_url": "https://sub2api.example.test"})

    assert session.calls == sub2api_module.SUB2API_MAX_REMOTE_PAGES


def test_ccload_channel_pagination_continues_when_count_is_omitted(monkeypatch):
    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **kwargs):
            self.calls += 1
            offset = kwargs["params"]["offset"]
            if offset == 0:
                ids = range(1, 201)
            elif offset == 200:
                ids = range(201, 401)
            else:
                ids = range(401, 451)
            return _Response({
                "success": True,
                "data": [
                    {
                        "id": index,
                        "auth_type": "codex_oauth",
                        "enabled": True,
                        "codex_plan_type": "free",
                    }
                    for index in ids
                ],
            })

    session = Session()

    @contextmanager
    def admin_session(_server, *, deadline=None):
        yield session, "https://ccload.example.test", {}

    monkeypatch.setattr(ccload_module, "_admin_session", admin_session)

    channels = ccload_module.list_remote_channels({
        "base_url": "https://ccload.example.test",
        "password": "secret",
    })

    assert len(channels) == 450
    assert [channel["id"] for channel in channels[:2]] == ["1", "2"]
    assert [channel["id"] for channel in channels[-2:]] == ["449", "450"]
    assert session.calls == 3


def test_ccload_channel_pagination_uses_declared_count_for_short_pages(monkeypatch):
    pages = [list(range(start, min(start + 50, 251))) for start in range(1, 251, 50)]

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **kwargs):
            page = pages[self.calls]
            self.calls += 1
            return _Response({
                "success": True,
                "data": [
                    {
                        "id": index,
                        "auth_type": "codex_oauth",
                        "enabled": True,
                        "codex_plan_type": "free",
                    }
                    for index in page
                ],
                "count": 250,
            })

    session = Session()

    @contextmanager
    def admin_session(_server, *, deadline=None):
        yield session, "https://ccload.example.test", {}

    monkeypatch.setattr(ccload_module, "_admin_session", admin_session)

    channels = ccload_module.list_remote_channels({
        "base_url": "https://ccload.example.test",
        "password": "secret",
    })

    assert len(channels) == 250
    assert session.calls == 5


def test_ccload_channel_pagination_rejects_empty_page_before_count(monkeypatch):
    class Session:
        calls = 0

        def get(self, _url, **_kwargs):
            self.calls += 1
            return _Response({"success": True, "data": [], "count": 100})

    session = Session()

    @contextmanager
    def admin_session(_server, *, deadline=None):
        yield session, "https://ccload.example.test", {}

    monkeypatch.setattr(ccload_module, "_admin_session", admin_session)

    with pytest.raises(ccload_module.CCLoadError, match="channel list failed"):
        ccload_module.list_remote_channels({
            "base_url": "https://ccload.example.test",
            "password": "secret",
        })

    assert session.calls == 1


def test_ccload_channel_pagination_rejects_count_rollback(monkeypatch):
    pages = [
        {"data": [{"id": index, "auth_type": "codex_oauth", "enabled": True, "codex_plan_type": "free"}
                  for index in range(start, start + 50)], "count": total}
        for start, total in ((1, 500), (51, 100))
    ]

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            payload = pages[self.calls]
            self.calls += 1
            return _Response({"success": True, **payload})

    session = Session()

    @contextmanager
    def admin_session(_server, *, deadline=None):
        yield session, "https://ccload.example.test", {}

    monkeypatch.setattr(ccload_module, "_admin_session", admin_session)

    with pytest.raises(ccload_module.CCLoadError, match="channel list failed"):
        ccload_module.list_remote_channels({
            "base_url": "https://ccload.example.test",
            "password": "secret",
        })

    assert session.calls == 2


def test_ccload_channel_pagination_rejects_count_mode_drift(monkeypatch):
    pages = [
        {
            "data": [
                {"id": index, "auth_type": "codex_oauth", "enabled": True, "codex_plan_type": "free"}
                for index in range(1, 201)
            ],
        },
        {
            "data": [
                {"id": index, "auth_type": "codex_oauth", "enabled": True, "codex_plan_type": "free"}
                for index in range(201, 251)
            ],
            "count": 250,
        },
    ]

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            payload = pages[self.calls]
            self.calls += 1
            return _Response({"success": True, **payload})

    session = Session()

    @contextmanager
    def admin_session(_server, *, deadline=None):
        yield session, "https://ccload.example.test", {}

    monkeypatch.setattr(ccload_module, "_admin_session", admin_session)

    with pytest.raises(ccload_module.CCLoadError, match="channel list failed"):
        ccload_module.list_remote_channels({
            "base_url": "https://ccload.example.test",
            "password": "secret",
        })

    assert session.calls == 2


def test_ccload_channel_pagination_rejects_count_above_cap(monkeypatch):
    class Session:
        calls = 0

        def get(self, _url, **_kwargs):
            self.calls += 1
            if self.calls > 25:
                return _Response({"success": True, "data": []})
            return _Response({
                "success": True,
                "data": [],
                "count": 5001,
            })

    session = Session()

    @contextmanager
    def admin_session(_server, *, deadline=None):
        yield session, "https://ccload.example.test", {}

    monkeypatch.setattr(ccload_module, "_admin_session", admin_session)

    with pytest.raises(ccload_module.CCLoadError, match="channel list limit exceeded"):
        ccload_module.list_remote_channels({
            "base_url": "https://ccload.example.test",
            "password": "secret",
        })
    assert session.calls == 1


def test_ccload_channel_pagination_rejects_unbounded_full_pages(monkeypatch):
    class Session:
        calls = 0

        def get(self, _url, **_kwargs):
            self.calls += 1
            if self.calls > 25:
                return _Response({"success": True, "data": []})
            return _Response({
                "success": True,
                "data": [
                    {
                        "id": index,
                        "auth_type": "codex_oauth",
                        "enabled": True,
                        "codex_plan_type": "free",
                    }
                    for index in range(1, 201)
                ],
            })

    session = Session()

    @contextmanager
    def admin_session(_server, *, deadline=None):
        yield session, "https://ccload.example.test", {}

    monkeypatch.setattr(ccload_module, "_admin_session", admin_session)

    with pytest.raises(ccload_module.CCLoadError, match="channel list limit exceeded"):
        ccload_module.list_remote_channels({
            "base_url": "https://ccload.example.test",
            "password": "secret",
        })
    assert session.calls == 25


def test_sub2api_import_rejects_container_account_id_before_scheduling():
    canary = "sub2api-import-account-container-canary"
    service = sub2api_module.Sub2APIImportService(object())

    with pytest.raises(sub2api_module.PublicSafeValueError):
        service.start_import(
            {"id": "server-1"},
            [{"secret": canary}],
        )


@pytest.mark.parametrize("invalid_total", (-1, True, 1.5, "200"))
def test_remote_pagination_rejects_noncanonical_total(monkeypatch, invalid_total):
    session = _NeverEndingPageSession({
        "code": 0,
        "data": {
            "items": [{"id": "1", "credentials": {}}],
            "total": invalid_total,
        },
    })
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})
    monkeypatch.setattr(sub2api_module, "SUB2API_REMOTE_BROWSE_TIMEOUT_SECS", 1.0)

    with pytest.raises(ValueError, match="invalid sub2api pagination payload"):
        sub2api_module.list_remote_accounts({
            "id": "server-1",
            "base_url": "https://sub2api.example.test",
        })

    assert session.calls == 1


def test_sub2api_remote_accounts_do_not_stringify_container_fields(monkeypatch):
    canary = "sub2api-account-container-canary"

    class Session:
        def __init__(self, **_kwargs):
            self.closed = False

        def get(self, *_args, **_kwargs):
            return _Response({
                "code": 0,
                "data": {
                    "items": [
                        {"id": {"secret": canary}, "name": "skip"},
                        {
                            "id": "safe-account",
                            "name": {"secret": canary},
                            "status": [canary],
                            "credentials": {
                                "email": {"secret": canary},
                                "plan_type": [canary],
                                "expires_at": {"secret": canary},
                                "refresh_token": [canary],
                            },
                        },
                    ],
                    "total": 2,
                },
            })

        def close(self):
            self.closed = True

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    accounts = sub2api_module.list_remote_accounts({
        "id": "server-1",
        "base_url": "https://sub2api.example.test",
    })

    assert accounts == [{
        "id": "safe-account",
        "name": "",
        "email": "",
        "plan_type": "",
        "status": "",
        "expires_at": "",
        "has_refresh_token": False,
    }]
    assert canary not in repr(accounts)
    assert session.closed


def test_sub2api_remote_groups_do_not_stringify_container_fields(monkeypatch):
    canary = "sub2api-group-container-canary"

    class Session:
        def __init__(self, **_kwargs):
            self.closed = False

        def get(self, *_args, **_kwargs):
            return _Response({
                "code": 0,
                "data": {
                    "items": [
                        {"id": {"secret": canary}, "name": "skip"},
                        {
                            "id": "safe-group",
                            "name": {"secret": canary},
                            "description": [canary],
                            "platform": {"secret": canary},
                            "status": [canary],
                            "account_count": 2,
                            "active_account_count": 1,
                        },
                    ],
                    "total": 2,
                },
            })

        def close(self):
            self.closed = True

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    groups = sub2api_module.list_remote_groups({
        "id": "server-1",
        "base_url": "https://sub2api.example.test",
    })

    assert groups == [{
        "id": "safe-group",
        "name": "",
        "description": "",
        "platform": "",
        "status": "",
        "account_count": 2,
        "active_account_count": 1,
    }]
    assert canary not in repr(groups)
    assert session.closed


def test_sub2api_export_rejects_container_tokens_and_error_names(monkeypatch):
    canary = "sub2api-export-container-canary"

    class Session:
        def __init__(self, **_kwargs):
            self.closed = False

        def get(self, *_args, **_kwargs):
            return _Response({
                "accounts": [
                    {"id": "bad-token", "credentials": {"access_token": {"secret": canary}}},
                    {"id": {"secret": canary}, "name": {"secret": canary}, "credentials": {}},
                ],
            })

        def close(self):
            self.closed = True

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    tokens, errors = sub2api_module._fetch_access_tokens_for_accounts(
        {"base_url": "https://sub2api.example.test"},
        ["bad-token", "missing-token"],
    )

    assert tokens == []
    assert errors == [
        {"name": "bad-token", "error": "missing access_token"},
        {"name": "Sub2API", "error": "missing 1 selected accounts"},
    ]
    assert canary not in repr((tokens, errors))
    assert session.closed


def test_sub2api_export_rejects_nonstring_selected_ids_before_network(monkeypatch):
    canary = "sub2api-selected-id-container-canary"
    monkeypatch.setattr(
        sub2api_module,
        "Session",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("network must not start")),
    )

    with pytest.raises(ValueError, match="invalid account ids"):
        sub2api_module._fetch_access_tokens_for_accounts(
            {"base_url": "https://sub2api.example.test"},
            ["safe-id", {"secret": canary}],
        )


def test_sub2api_export_ignores_accounts_outside_selected_ids(monkeypatch):
    class Session:
        def get(self, *_args, **_kwargs):
            return _Response({
                "accounts": [
                    {"id": "selected-id", "credentials": {"access_token": "selected-token"}},
                    {"id": "unselected-id", "credentials": {"access_token": "unselected-token"}},
                ],
            })

        def close(self):
            pass

    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: Session())
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    tokens, errors = sub2api_module._fetch_access_tokens_for_accounts(
        {"base_url": "https://sub2api.example.test"},
        ["selected-id"],
    )

    assert tokens == ["selected-token"]
    assert errors == []


@pytest.mark.parametrize("invalid_count", (-1, True, 1.5, "200"))
def test_ccload_channel_pagination_rejects_noncanonical_count(monkeypatch, invalid_count):
    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, _url, **_kwargs):
            self.calls += 1
            return _Response({
                "success": True,
                "data": [
                    {
                        "id": str(index),
                        "auth_type": "codex_oauth",
                        "enabled": True,
                        "codex_plan_type": "free",
                    }
                    for index in range(1, 201)
                ],
                "count": invalid_count,
            })

    session = Session()

    @contextmanager
    def admin_session(_server, *, deadline=None):
        yield session, "https://ccload.example.test", {}

    monkeypatch.setattr(ccload_module, "_admin_session", admin_session)

    with pytest.raises(ccload_module.CCLoadError, match="channel list failed"):
        ccload_module.list_remote_channels({"base_url": "https://ccload.example.test", "password": "secret"})

    assert session.calls == 1


@pytest.mark.parametrize("field", ("account_count", "active_account_count"))
@pytest.mark.parametrize("invalid_value", (-1, True, 1.5, "2"))
def test_sub2api_group_counts_reject_noncanonical_values(monkeypatch, field, invalid_value):
    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, *_args, **_kwargs):
            self.calls += 1
            group = {"id": "group-1", "account_count": 2, "active_account_count": 1}
            group[field] = invalid_value
            return _Response({"code": 0, "data": {"items": [group], "total": 1}})

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)
    monkeypatch.setattr(sub2api_module, "_auth_headers", lambda _server, **_kwargs: {})

    with pytest.raises(ValueError, match="invalid sub2api group payload"):
        sub2api_module.list_remote_groups({"base_url": "https://sub2api.example.test"})

    assert session.calls == 1


@pytest.mark.parametrize("invalid_expires_in", (True, 1.5, "3600", -1))
def test_sub2api_login_rejects_noncanonical_expiry(monkeypatch, invalid_expires_in):
    class Session:
        def __init__(self, **_kwargs):
            self.closed = False

        def post(self, *_args, **_kwargs):
            return _Response({
                "code": 0,
                "data": {"access_token": "jwt-token", "expires_in": invalid_expires_in},
            })

        def close(self):
            self.closed = True

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)

    with pytest.raises(RuntimeError, match="sub2api login payload is invalid"):
        sub2api_module._login("https://sub2api.example.test", "user", "password")


def test_sub2api_login_rejects_container_access_token(monkeypatch):
    canary = "sub2api-login-token-container-canary"

    class Session:
        def __init__(self, **_kwargs):
            self.closed = False

        def post(self, *_args, **_kwargs):
            return _Response({
                "code": 0,
                "data": {
                    "access_token": {"secret": canary},
                    "expires_in": 3600,
                },
            })

        def close(self):
            self.closed = True

    session = Session()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)

    with pytest.raises(RuntimeError, match="sub2api login did not return access_token"):
        sub2api_module._login("https://sub2api.example.test", "user", "password")

    assert session.closed


def test_sub2api_login_closes_response_after_json_parse(monkeypatch):
    response = _Response({"code": 0, "data": {"access_token": "jwt-token", "expires_in": 3600}})
    fake_session = type("Session", (), {
        "post": lambda self, *_args, **_kwargs: response,
        "close": lambda self: None,
    })()
    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: fake_session)

    token, expires_at = sub2api_module._login(
        "https://sub2api.example.test",
        "user",
        "password",
    )

    assert token == "jwt-token"
    assert expires_at > 0
    assert response.closed


def test_sub2api_login_passes_streaming_request_to_session(monkeypatch):
    response = _Response({"code": 0, "data": {"access_token": "jwt-token", "expires_in": 3600}})
    calls = []

    class Session:
        def post(self, *args, **kwargs):
            calls.append((args, kwargs))
            return response

        def close(self):
            pass

    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: Session())
    sub2api_module._login("https://sub2api.example.test", "user", "password")

    assert calls[0][1]["stream"] is True


def test_sub2api_server_update_cannot_publish_an_inflight_old_login(monkeypatch, tmp_path):
    config = sub2api_module.Sub2APIConfig(tmp_path / "sub2api.json")
    server = config.add_server(
        name="sub2api",
        base_url="https://sub2api.example.test",
        email="owner@example.test",
        password="old-password",
        api_key="",
    )
    login_started = Event()
    release_old_login = Event()
    calls = []

    def fake_login(_base_url, _email, password, *, deadline=None):
        calls.append(password)
        if password == "old-password":
            login_started.set()
            assert release_old_login.wait(2)
            return "old-token", 4_000_000_000.0
        return "new-token", 4_000_000_000.0

    sub2api_module._token_cache.clear()
    monkeypatch.setattr(sub2api_module, "_login", fake_login)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            old_request = executor.submit(sub2api_module._auth_headers, dict(server))
            assert login_started.wait(1)

            updated = config.update_server(server["id"], {"password": "new-password"})
            assert updated is not None
            release_old_login.set()
            assert old_request.result(timeout=2)["Authorization"] == "Bearer old-token"
            assert server["id"] not in sub2api_module._token_cache

            current_headers = sub2api_module._auth_headers(updated)

        assert current_headers["Authorization"] == "Bearer new-token"
        assert calls == ["old-password", "new-password"]
    finally:
        sub2api_module._token_cache.clear()


def test_sub2api_auth_failure_invalidates_cached_password_token(monkeypatch):
    server = {
        "id": "server-auth-failure",
        "base_url": "https://sub2api.example.test",
        "email": "owner@example.test",
        "password": "password",
        "api_key": "",
    }
    generation = 0
    sub2api_module._token_cache.clear()
    sub2api_module._token_cache_generations.clear()

    sub2api_module._token_cache[server["id"]] = ("stale-token", 4_000_000_000.0, generation)

    class UnauthorizedResponse:
        ok = False
        status_code = 401

        def __init__(self):
            self.closed = False

        def iter_content(self, *, chunk_size):
            raise AssertionError("401 response body must not be read")

        def close(self):
            self.closed = True

    response = UnauthorizedResponse()

    class Session:
        def get(self, *_args, **_kwargs):
            return response

        def close(self):
            pass

    monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: Session())

    with pytest.raises(RuntimeError, match="HTTP 401"):
        sub2api_module.list_remote_accounts(server)

    assert response.closed
    assert server["id"] not in sub2api_module._token_cache

    fresh_calls = []

    def fresh_login(*_args, **_kwargs):
        fresh_calls.append(True)
        return "fresh-token", 4_000_000_000.0

    monkeypatch.setattr(sub2api_module, "_login", fresh_login)
    headers = sub2api_module._auth_headers(server)

    assert headers["Authorization"] == "Bearer fresh-token"
    assert fresh_calls == [True]
    sub2api_module._token_cache.clear()
    sub2api_module._token_cache_generations.clear()


def test_sub2api_auth_failure_invalidates_cache_for_all_password_reads(monkeypatch):
    server = {
        "id": "server-auth-failure-all-reads",
        "base_url": "https://sub2api.example.test",
        "email": "owner@example.test",
        "password": "password",
        "api_key": "",
    }

    class UnauthorizedResponse:
        ok = False
        status_code = 403

        def __init__(self):
            self.closed = False

        def iter_content(self, *, chunk_size):
            raise AssertionError("403 response body must not be read")

        def close(self):
            self.closed = True

    class Session:
        def __init__(self):
            self.response = UnauthorizedResponse()

        def get(self, *_args, **_kwargs):
            return self.response

        def close(self):
            pass

    readers = (
        (sub2api_module.list_remote_accounts, (server,)),
        (sub2api_module.list_remote_groups, (server,)),
        (sub2api_module._fetch_access_tokens_for_accounts, (server, ["account-1"])),
    )
    for reader, args in readers:
        sub2api_module._token_cache.clear()
        sub2api_module._token_cache_generations.clear()
        sub2api_module._token_cache[server["id"]] = ("stale-token", 4_000_000_000.0, 0)
        session = Session()
        monkeypatch.setattr(sub2api_module, "Session", lambda **_kwargs: session)

        with pytest.raises(RuntimeError, match="HTTP 403"):
            reader(*args)

        assert session.response.closed
        assert server["id"] not in sub2api_module._token_cache
