"""公开 API 的 Python↔Rust 路由硬门。

这里只检查路由集合和 method/动态段；响应 schema、副作用和错误状态由同名
focused server-flow 测试覆盖。Python 路由只从真实 router/app decorator
提取，前端路由只从实际 HTTP 调用提取，避免字段访问或手工清单制造假闭包。
``/api/ccload/servers/{server_id}/channel-models`` 的真实合同是 POST，来源
为 ``api/accounts.py`` 和 ``web/src/lib/api.ts``，不是 GET。
"""

from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _normalize(path: str) -> str:
    path = re.sub(r"\$\{[^}]*\}", "{param}", path)
    # A nested template literal in a query expression may make the tiny
    # scanner stop before the outer ``}``; the route itself is the literal
    # prefix and the query is deliberately outside this contract.
    path = re.sub(r"\$\{.*$", "", path)
    path = path.split("?", 1)[0]
    return re.sub(r"\{\*?[^}]+\}", "{param}", path)


def _python_routes() -> set[tuple[str, str]]:
    routes: set[tuple[str, str]] = set()
    for source in (ROOT / "api").glob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call):
                    continue
                if not isinstance(decorator.func, ast.Attribute):
                    continue
                if not isinstance(decorator.func.value, ast.Name):
                    continue
                if decorator.func.value.id not in {"router", "app"}:
                    continue
                if decorator.func.attr not in {"get", "post", "delete", "put", "patch"}:
                    continue
                if not decorator.args or not isinstance(decorator.args[0], ast.Constant):
                    continue
                path = decorator.args[0].value
                if isinstance(path, str):
                    routes.add((decorator.func.attr.upper(), _normalize(path)))
    return routes


def _read_ts_string(source: str, start: int) -> tuple[str, int] | None:
    quote = source[start]
    if quote not in {'"', "'", "`"}:
        return None
    cursor = start + 1
    value: list[str] = []
    while cursor < len(source):
        char = source[cursor]
        if char == "\\" and cursor + 1 < len(source):
            value.append(source[cursor + 1])
            cursor += 2
            continue
        if char == quote:
            return "".join(value), cursor + 1
        value.append(char)
        cursor += 1
    return None


def _call_body(source: str, open_paren: int) -> str | None:
    depth = 0
    quote: str | None = None
    cursor = open_paren
    while cursor < len(source):
        char = source[cursor]
        if quote is not None:
            if char == "\\":
                cursor += 2
                continue
            if char == quote:
                quote = None
            cursor += 1
            continue
        if char in {'"', "'", "`"}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return source[open_paren + 1 : cursor]
        cursor += 1
    return None


def _frontend_routes() -> set[tuple[str, str]]:
    source = (ROOT / "web" / "src" / "lib" / "api.ts").read_text(encoding="utf-8")
    call_re = re.compile(
        r"(?P<name>httpRequest(?:<[^;\n]+?>)?|request\.(?:get|post|delete|put|patch))\s*\("
    )
    routes: set[tuple[str, str]] = set()
    for match in call_re.finditer(source):
        body = _call_body(source, match.end() - 1)
        if body is None:
            continue
        first = len(body) - len(body.lstrip())
        parsed = _read_ts_string(body, first)
        if parsed is None:
            continue
        path, _ = parsed
        if not path.startswith(("/", "http://", "https://")):
            continue
        method_match = re.search(r"\bmethod\s*:\s*[\"'](GET|POST|DELETE|PUT|PATCH)[\"']", body)
        if match.group("name").startswith("request."):
            method = match.group("name").split(".", 1)[1].upper()
        else:
            method = method_match.group(1) if method_match else "GET"
        routes.add((method, _normalize(path)))
    return routes


def _rust_routes() -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    source = (ROOT / "rust" / "src" / "lib.rs").read_text(encoding="utf-8")
    route_re = re.compile(
        r'\.route\(\s*"(?P<path>[^"]+)",(?P<body>.*?)(?=\n\s*\.route\(|\n\s*\.layer\()',
        re.DOTALL,
    )
    routes: set[tuple[str, str]] = set()
    unsupported: set[tuple[str, str]] = set()
    for match in route_re.finditer(source):
        path = _normalize(match.group("path"))
        methods = re.findall(r"\b(get|post|delete|put|patch)\s*\(", match.group("body"))
        for method in methods:
            item = (method.upper(), path)
            routes.add(item)
            if "unsupported_management_" in match.group("body"):
                unsupported.add(item)
    return routes, unsupported


def test_rust_covers_web_management_route_contract() -> None:
    python = _python_routes()
    frontend = _frontend_routes()
    rust, unsupported = _rust_routes()
    assert python <= rust, f"Rust route set missing Python routes: {sorted(python - rust)}"
    assert frontend <= python, f"Python route set missing frontend calls: {sorted(frontend - python)}"
    assert frontend <= rust, f"Rust route set missing frontend calls: {sorted(frontend - rust)}"
    assert not (python & unsupported), (
        "Rust public routes still terminate in unsupported_management_*: "
        f"{sorted(python & unsupported)}"
    )
