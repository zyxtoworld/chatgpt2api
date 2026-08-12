from __future__ import annotations

from pathlib import Path


SKILL_PANEL = Path(__file__).parents[1] / "web/src/app/debug/components/skill-panel.tsx"
API_DOCS_CARD = Path(__file__).parents[1] / "web/src/app/settings/components/api-docs-card.tsx"
TOP_NAV = Path(__file__).parents[1] / "web/src/components/top-nav.tsx"
THIRD_PARTY_URL = Path(__file__).parents[1] / "web/src/lib/third-party-url.js"
THIRD_PARTY_CARD = Path(__file__).parents[1] / "web/src/app/settings/components/third-party-apps-card.tsx"


def test_skill_install_prompts_never_interpolate_session_credentials() -> None:
    source = SKILL_PANEL.read_text(encoding="utf-8")

    assert 'getStoredAuthSession' not in source
    assert "${authKey}" not in source
    assert source.count("Authorization: Bearer ${API_KEY_PLACEHOLDER}") == 2
    assert source.count("${API_KEY_PLACEHOLDER}") >= 4
    assert "请将 ${API_KEY_PLACEHOLDER} 替换为你自己的 API key" in source
    assert "Replace ${API_KEY_PLACEHOLDER} with your own API key" in source


def test_api_docs_never_reads_or_displays_session_credentials() -> None:
    source = API_DOCS_CARD.read_text(encoding="utf-8")

    assert 'getStoredAuthSession' not in source
    assert "session?.key" not in source
    assert "setAuthKey" not in source
    assert "const displayKey = API_KEY_PLACEHOLDER;" in source
    assert "const API_KEY_PLACEHOLDER = \"<YOUR_API_KEY>\";" in source


def test_third_party_handoff_never_puts_session_credentials_in_url_or_dom() -> None:
    source = TOP_NAV.read_text(encoding="utf-8")
    helper = THIRD_PARTY_URL.read_text(encoding="utf-8")

    assert "session.key" not in source
    assert "apiKey" not in source
    assert "buildThirdPartyHref(canvas.url, baseUrl)" in source
    assert "buildThirdPartyHref(appUrl, baseUrl)" in helper
    assert "apiKey" not in helper
    assert "thirdPartyState?.owner === session" in source
    assert "手工输入独立 API key" in source


def test_third_party_settings_describe_secret_free_handoff() -> None:
    source = THIRD_PARTY_CARD.read_text(encoding="utf-8")

    assert "当前密钥" not in source
    assert "apiKey" not in source
    assert "独立 API key" in source
