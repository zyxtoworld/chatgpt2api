from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RUST_MANIFEST = ROOT / "rust" / "Cargo.toml"
EXPECTED_ROOT = ("chatgpt2api-rust", "0.1.0")
EXPECTED_SQLX = ("sqlx", "0.8.6")
EXPECTED_MYSQL = ("sqlx-mysql", "0.8.6")
EXPECTED_RSA = ("rsa", "0.9.10")
EXPECTED_REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
EXPECTED_CHECKSUMS = {
    EXPECTED_SQLX: "1fefb893899429669dcdd979aff487bd78f4064e5e7907e4269081e0ef7d97dc",
    EXPECTED_MYSQL: "aa003f0038df784eb8fecbbac13affe3da23b45194bd57dba231c8f48199c526",
    EXPECTED_RSA: "b8573f03f5883dcaebdfcf4725caa1ecb9c15b2ef50c43a07b816e06799bb12d",
}
EXPECTED_AUTH_SHA256 = "DE00396DECE908E052EA99735EB3F4079C098B553137F28E9194307C21003C77"
CHECKSUM_RE = re.compile(r"^[0-9a-f]{64}$")
FORBIDDEN_PRIVATE = re.compile(r"\bRsaPrivateKey\b")
FORBIDDEN_DECRYPT = re.compile(r"\bdecrypt\s*\(")


class VerificationError(RuntimeError):
    """The advisory exception cannot be proven safe for the current graph."""


@dataclass(frozen=True)
class VerificationResult:
    sqlx_manifest: Path
    auth_source: Path
    auth_sha256: str
    lock_sha256: str


def _run(command: list[str], cwd: Path) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise VerificationError(
            f"command failed ({completed.returncode}): {' '.join(command)}: {detail}"
        )
    return completed.stdout


def _package(packages: list[dict[str, Any]], expected: tuple[str, str]) -> dict[str, Any]:
    matches = [
        package
        for package in packages
        if package.get("name") == expected[0] and package.get("version") == expected[1]
    ]
    if len(matches) != 1:
        raise VerificationError(
            f"locked graph must contain exactly one {expected[0]} {expected[1]} package"
        )
    if not isinstance(matches[0].get("id"), str) or not matches[0]["id"]:
        raise VerificationError(f"{expected[0]} package has no exact metadata package id")
    return matches[0]


def _dependency(package: dict[str, Any], name: str) -> dict[str, Any]:
    dependencies = package.get("dependencies")
    if not isinstance(dependencies, list):
        raise VerificationError(f"{package.get('name')} metadata dependencies are missing")
    matches = [dependency for dependency in dependencies if dependency.get("name") == name]
    if len(matches) != 1:
        raise VerificationError(
            f"{package.get('name')} must have exactly one dependency edge to {name}"
        )
    return matches[0]


def _metadata_nodes(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    resolve = metadata.get("resolve")
    nodes = resolve.get("nodes") if isinstance(resolve, dict) else None
    if not isinstance(nodes, list) or not nodes:
        raise VerificationError("cargo metadata resolve.nodes is missing")

    result: dict[str, dict[str, Any]] = {}
    for node in nodes:
        if not isinstance(node, dict) or not isinstance(node.get("id"), str):
            raise VerificationError("cargo metadata contains a malformed resolve node")
        node_id = node["id"]
        if node_id in result:
            raise VerificationError(f"cargo metadata repeats resolve node {node_id}")
        deps = node.get("deps")
        if not isinstance(deps, list):
            raise VerificationError(f"resolve node {node_id} has no structured dependency edges")
        for dependency in deps:
            if not isinstance(dependency, dict) or not isinstance(dependency.get("pkg"), str):
                raise VerificationError(f"resolve node {node_id} has a malformed dependency edge")
        result[node_id] = node

    for node_id, node in result.items():
        for dependency in node["deps"]:
            if dependency["pkg"] not in result:
                raise VerificationError(
                    f"resolve node {node_id} points to an unknown package id {dependency['pkg']}"
                )
    return result


def _node_dependencies(node: dict[str, Any]) -> set[str]:
    return {dependency["pkg"] for dependency in node["deps"]}


def _require_direct_edge(
    nodes: dict[str, dict[str, Any]], from_id: str, to_id: str, label: str
) -> None:
    edges = [dependency for dependency in nodes[from_id]["deps"] if dependency["pkg"] == to_id]
    if len(edges) != 1:
        raise VerificationError(f"resolve graph must contain exactly one {label} edge")


def _reachable(nodes: dict[str, dict[str, Any]], start: str, target: str) -> bool:
    seen: set[str] = set()
    pending = [start]
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        if current == target:
            return True
        pending.extend(_node_dependencies(nodes[current]) - seen)
    return False


def _lock_package(
    lock_data: dict[str, Any],
    expected: tuple[str, str],
    expected_checksums: dict[tuple[str, str], str],
) -> dict[str, Any]:
    packages = lock_data.get("package")
    if not isinstance(packages, list):
        raise VerificationError("Cargo.lock has no package table")
    matches = [
        package
        for package in packages
        if package.get("name") == expected[0] and package.get("version") == expected[1]
    ]
    if len(matches) != 1:
        raise VerificationError(
            f"Cargo.lock must contain exactly one {expected[0]} {expected[1]} entry"
        )
    package = matches[0]
    if package.get("source") != EXPECTED_REGISTRY:
        raise VerificationError(f"Cargo.lock source changed for {expected[0]}")
    checksum = package.get("checksum")
    if not isinstance(checksum, str) or CHECKSUM_RE.fullmatch(checksum) is None:
        raise VerificationError(f"Cargo.lock checksum missing or malformed for {expected[0]}")
    expected_checksum = expected_checksums.get(expected)
    if not isinstance(expected_checksum, str) or CHECKSUM_RE.fullmatch(expected_checksum) is None:
        raise VerificationError(f"authoritative checksum is missing for {expected[0]}")
    if checksum != expected_checksum:
        raise VerificationError(f"Cargo.lock checksum changed for {expected[0]}; re-audit the exception")
    return package


def _regular_source_tree(source_root: Path, label: str) -> list[Path]:
    if source_root.is_symlink() or not source_root.is_dir():
        raise VerificationError(f"{label} source root is not a regular directory: {source_root}")
    for path in source_root.rglob("*"):
        if path.is_symlink():
            raise VerificationError(f"{label} source tree contains a symlink: {path}")
        if not path.is_dir() and not path.is_file():
            raise VerificationError(f"{label} source tree contains a non-regular entry: {path}")
    files = sorted(path for path in source_root.rglob("*.rs") if path.is_file())
    if not files:
        raise VerificationError(f"{label} source tree is empty: {source_root}")
    return files


def _read_source(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise VerificationError(f"Rust source is not valid UTF-8: {path}") from exc


def _verify_no_private_or_decrypt(files: list[Path], label: str) -> None:
    for path in files:
        text = _read_source(path)
        if FORBIDDEN_PRIVATE.search(text) or FORBIDDEN_DECRYPT.search(text):
            raise VerificationError(f"{label} source contains a forbidden private/decrypt path: {path}")


def _registry_manifest(package: dict[str, Any], expected: tuple[str, str]) -> tuple[Path, Path, Path]:
    manifest_value = package.get("manifest_path")
    if not isinstance(manifest_value, str) or not manifest_value:
        raise VerificationError(f"metadata did not locate {expected[0]} Cargo.toml")
    manifest = Path(manifest_value)
    if manifest.is_symlink() or not manifest.is_file() or manifest.name != "Cargo.toml":
        raise VerificationError(f"{expected[0]} manifest is not a regular file: {manifest}")
    package_dir = manifest.parent
    if package_dir.name != f"{expected[0]}-{expected[1]}":
        raise VerificationError(f"{expected[0]} manifest is not the locked registry source")
    registry_index = package_dir.parent
    if (
        registry_index.is_symlink()
        or not registry_index.is_dir()
        or not registry_index.name.startswith("index.crates.io-")
        or registry_index.parent.name != "src"
        or registry_index.parent.parent.name != "registry"
    ):
        raise VerificationError(f"{expected[0]} source is outside the expected crates.io registry")
    source_root = package_dir / "src"
    crate_path = (
        registry_index.parent.parent
        / "cache"
        / registry_index.name
        / f"{expected[0]}-{expected[1]}.crate"
    )
    return manifest, source_root, crate_path


def _verify_registry_crate(
    package: dict[str, Any], expected: tuple[str, str], expected_checksum: str
) -> None:
    _, _, crate_path = _registry_manifest(package, expected)
    cache_root = crate_path.parent
    if cache_root.is_symlink() or not cache_root.is_dir():
        raise VerificationError(f"registry cache is missing for {expected[0]}")
    if crate_path.is_symlink() or not crate_path.is_file():
        raise VerificationError(f"locked registry crate is missing for {expected[0]}")
    digest = hashlib.sha256()
    with crate_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected_checksum:
        raise VerificationError(f"registry crate checksum changed for {expected[0]}")


def _verify_graph(
    metadata: dict[str, Any],
    tree: str,
    *,
    lock_data: dict[str, Any],
    root_manifest: Path,
    expected_checksums: dict[tuple[str, str], str] | None = None,
    expected_auth_sha256: str = EXPECTED_AUTH_SHA256,
) -> Path:
    packages = metadata.get("packages")
    if not isinstance(packages, list):
        raise VerificationError("cargo metadata did not return packages")

    root = _package(packages, EXPECTED_ROOT)
    sqlx = _package(packages, EXPECTED_SQLX)
    mysql = _package(packages, EXPECTED_MYSQL)
    rsa = _package(packages, EXPECTED_RSA)
    package_by_id = {package["id"]: package for package in packages}
    if len(package_by_id) != len(packages):
        raise VerificationError("cargo metadata repeats a package id")
    if root.get("source") is not None:
        raise VerificationError("application root is no longer a path package")
    if Path(root.get("manifest_path", "")).resolve() != root_manifest.resolve():
        raise VerificationError("metadata root manifest does not match the verified Cargo.toml")
    authoritative_checksums = EXPECTED_CHECKSUMS if expected_checksums is None else expected_checksums
    for package, expected in ((sqlx, EXPECTED_SQLX), (mysql, EXPECTED_MYSQL), (rsa, EXPECTED_RSA)):
        if package.get("source") != EXPECTED_REGISTRY:
            raise VerificationError(f"{expected[0]} is not the expected crates.io package")
        lock_package = _lock_package(lock_data, expected, authoritative_checksums)
        _verify_registry_crate(package, expected, lock_package["checksum"])
        metadata_checksum = package.get("checksum")
        if metadata_checksum is not None and metadata_checksum != lock_package["checksum"]:
            raise VerificationError(f"metadata checksum disagrees with Cargo.lock for {expected[0]}")

    sqlx_edge = _dependency(root, "sqlx")
    features = sqlx_edge.get("features")
    if not isinstance(features, list) or not {"any", "mysql"}.issubset(set(features)):
        raise VerificationError("root sqlx dependency no longer enables both any and mysql")
    mysql_edge = _dependency(sqlx, "sqlx-mysql")
    if mysql_edge.get("req") != "=0.8.6":
        raise VerificationError("sqlx no longer resolves the exact sqlx-mysql 0.8.6 package")
    rsa_edge = _dependency(mysql, "rsa")
    if rsa_edge.get("req") not in {"^0.9", "0.9"}:
        raise VerificationError("sqlx-mysql rsa requirement changed; re-audit the exception")

    nodes = _metadata_nodes(metadata)
    root_id = root["id"]
    sqlx_id = sqlx["id"]
    mysql_id = mysql["id"]
    rsa_id = rsa["id"]
    expected_ids = {root_id, sqlx_id, mysql_id, rsa_id}
    if not expected_ids.issubset(nodes):
        raise VerificationError("resolve graph is missing an expected chain node")
    _require_direct_edge(nodes, root_id, sqlx_id, "root-to-sqlx")
    _require_direct_edge(nodes, sqlx_id, mysql_id, "sqlx-to-sqlx-mysql")
    _require_direct_edge(nodes, mysql_id, rsa_id, "sqlx-mysql-to-rsa")
    if not _reachable(nodes, root_id, rsa_id):
        raise VerificationError("root cannot reach rsa through the verified chain")
    reverse_rsa = {
        node_id
        for node_id, node in nodes.items()
        if rsa_id in _node_dependencies(node)
    }
    if reverse_rsa != {mysql_id}:
        raise VerificationError(
            "rsa@0.9.10 must have exactly one direct reverse dependency: sqlx-mysql@0.8.6"
        )
    sqlx_features = nodes[sqlx_id].get("features")
    if not isinstance(sqlx_features, list) or not {"any", "mysql"}.issubset(set(sqlx_features)):
        raise VerificationError("resolved sqlx features no longer include any and mysql")

    tree_fragments = (
        "rsa v0.9.10",
        "sqlx-mysql v0.8.6",
        "sqlx feature \"mysql\"",
        "sqlx feature \"any\"",
        "chatgpt2api-rust v0.1.0",
    )
    missing = [fragment for fragment in tree_fragments if fragment not in tree]
    if missing:
        raise VerificationError(f"cargo tree no longer agrees with the locked chain: {missing}")

    _, source_root, _ = _registry_manifest(mysql, EXPECTED_MYSQL)
    auth_source = source_root / "connection" / "auth.rs"
    rust_files = _regular_source_tree(source_root, "sqlx-mysql")
    if not auth_source.is_file() or auth_source.is_symlink():
        raise VerificationError(f"sqlx-mysql authentication source is missing: {auth_source}")
    auth_sha256 = hashlib.sha256(auth_source.read_bytes()).hexdigest().upper()
    if auth_sha256 != expected_auth_sha256.upper():
        raise VerificationError("sqlx-mysql auth source checksum changed; re-audit the exception")
    auth_text = _read_source(auth_source)
    if "RsaPublicKey" not in auth_text or ".encrypt(" not in auth_text:
        raise VerificationError("sqlx-mysql auth path no longer uses public-key encryption")
    _verify_no_private_or_decrypt(rust_files, "sqlx-mysql")
    if "RsaPublicKey::from_public_key_pem" not in auth_text:
        raise VerificationError("sqlx-mysql auth key parsing layout changed; re-audit it")

    for package in packages:
        if package.get("source") is not None:
            continue
        manifest_value = package.get("manifest_path")
        if not isinstance(manifest_value, str) or not manifest_value:
            raise VerificationError("path package source is not locatable")
        package_source = Path(manifest_value).parent / "src"
        files = _regular_source_tree(package_source, str(package.get("name")))
        _verify_no_private_or_decrypt(files, str(package.get("name")))

    return auth_source


def verify_current(root: Path = ROOT) -> VerificationResult:
    rust_manifest = root / "rust" / "Cargo.toml"
    lock_path = root / "rust" / "Cargo.lock"
    metadata = json.loads(
        _run(
            ["cargo", "metadata", "--locked", "--format-version", "1", "--manifest-path", str(rust_manifest)],
            root,
        )
    )
    tree = _run(
        [
            "cargo",
            "tree",
            "--locked",
            "--manifest-path",
            str(rust_manifest),
            "--edges",
            "all",
            "-i",
            "rsa@0.9.10",
        ],
        root,
    )
    lock_bytes = lock_path.read_bytes()
    lock_data = tomllib.loads(lock_bytes.decode("utf-8"))
    auth_source = _verify_graph(
        metadata,
        tree,
        lock_data=lock_data,
        root_manifest=rust_manifest,
    )
    return VerificationResult(
        sqlx_manifest=auth_source.parents[2] / "Cargo.toml",
        auth_source=auth_source,
        auth_sha256=hashlib.sha256(auth_source.read_bytes()).hexdigest().upper(),
        lock_sha256=hashlib.sha256(lock_bytes).hexdigest().upper(),
    )


def main() -> int:
    try:
        result = verify_current()
    except (OSError, ValueError, json.JSONDecodeError, VerificationError) as exc:
        print(f"RSA advisory reachability verification failed closed: {exc}", file=sys.stderr)
        return 1
    print(f"locked sqlx-mysql auth source: {result.auth_source}")
    print(f"auth source sha256: {result.auth_sha256}")
    print(f"Cargo.lock sha256: {result.lock_sha256}")
    print("reachable operation: RsaPublicKey::encrypt only; private/decrypt paths absent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
