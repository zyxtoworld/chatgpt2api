from __future__ import annotations

import copy
import hashlib
import importlib.util
import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_rsa_advisory_exception_is_narrow_and_documented() -> None:
    workflow = (ROOT / ".github" / "workflows" / "docker-publish.yml").read_text(
        encoding="utf-8"
    )
    policy = (ROOT / "deny.toml").read_text(encoding="utf-8")
    explanation = (ROOT / "docs" / "rust-dependency-audit.md").read_text(
        encoding="utf-8"
    )

    command = (
        "cargo audit --deny warnings --ignore RUSTSEC-2023-0071 "
        "--file rust/Cargo.lock"
    )
    assert workflow.count("--ignore RUSTSEC-2023-0071") == 1
    assert command in workflow
    assert "uv run python scripts/verify_rsa_advisory.py" in workflow
    assert policy.count("RUSTSEC-2023-0071") == 1
    for required_text in (
        "RUSTSEC-2023-0071",
        "rsa 0.9.10",
        "sqlx-mysql 0.8.6",
        "RsaPublicKey",
        "RsaPrivateKey",
        "decrypt",
        "ssl-mode=REQUIRED",
        "移除条件",
    ):
        assert required_text in explanation


def test_application_rust_sources_do_not_add_rsa_private_or_decrypt_paths() -> None:
    rust_sources = sorted(
        path
        for source_root in (
            ROOT / "rust" / "src",
            ROOT / "rust" / "file_identity" / "src",
        )
        for path in source_root.rglob("*.rs")
    )
    assert rust_sources

    for path in rust_sources:
        source = path.read_text(encoding="utf-8")
        assert "RsaPrivateKey" not in source, path
        assert re.search(r"\brsa\s*::", source) is None, path
        assert re.search(r"\.decrypt\s*\(", source) is None, path


def _load_dependency_verifier():
    import sys

    spec = importlib.util.spec_from_file_location(
        "verify_rsa_advisory_test_module", ROOT / "scripts" / "verify_rsa_advisory.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _positive_rsa_fixture(tmp_path: Path):
    registry = "registry+https://github.com/rust-lang/crates.io-index"
    root_id = "path+file:///fixture/root#chatgpt2api-rust@0.1.0"
    sqlx_id = f"{registry}#sqlx@0.8.6"
    mysql_id = f"{registry}#sqlx-mysql@0.8.6"
    rsa_id = f"{registry}#rsa@0.9.10"

    root = tmp_path / "root"
    registry_root = tmp_path / "registry" / "src" / "index.crates.io-fixture"
    sqlx = registry_root / "sqlx-0.8.6"
    mysql = registry_root / "sqlx-mysql-0.8.6"
    rsa = registry_root / "rsa-0.9.10"
    for package_root in (root, sqlx, mysql, rsa):
        (package_root / "src").mkdir(parents=True)
        (package_root / "Cargo.toml").write_text("[package]\n", encoding="utf-8")
    (root / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
    (sqlx / "src" / "lib.rs").write_text("pub fn sqlx() {}\n", encoding="utf-8")
    (mysql / "src" / "lib.rs").write_text("pub mod connection;\n", encoding="utf-8")
    auth_source = mysql / "src" / "connection" / "auth.rs"
    auth_source.parent.mkdir()
    auth_source.write_text(
        "use rsa::RsaPublicKey;\n"
        "fn f() { RsaPublicKey::from_public_key_pem(pem); key.encrypt(&mut rng, padding, data); }\n",
        encoding="utf-8",
    )
    (rsa / "src" / "lib.rs").write_text(
        "pub struct RsaPrivateKey;\nfn decrypt() {}\n", encoding="utf-8"
    )
    cache_root = tmp_path / "registry" / "cache" / "index.crates.io-fixture"
    cache_root.mkdir(parents=True)
    crate_bytes = {
        "sqlx": b"sqlx fixture crate\n",
        "sqlx-mysql": b"sqlx-mysql fixture crate\n",
        "rsa": b"rsa fixture crate\n",
    }
    crate_checksums = {}
    for name, version in (
        ("sqlx", "0.8.6"),
        ("sqlx-mysql", "0.8.6"),
        ("rsa", "0.9.10"),
    ):
        payload = crate_bytes[name]
        (cache_root / f"{name}-{version}.crate").write_bytes(payload)
        crate_checksums[(name, version)] = hashlib.sha256(payload).hexdigest()

    def package(name, version, package_id, manifest_path, source, dependencies):
        return {
            "name": name,
            "version": version,
            "id": package_id,
            "manifest_path": str(manifest_path),
            "source": source,
            "dependencies": dependencies,
        }

    def node(package_id, deps, features=()):
        return {
            "id": package_id,
            "dependencies": [package_id2 for _, package_id2 in deps],
            "deps": [{"name": name, "pkg": package_id2, "dep_kinds": [{"kind": None, "target": None}]} for name, package_id2 in deps],
            "features": list(features),
        }

    metadata = {
        "packages": [
            package(
                "chatgpt2api-rust",
                "0.1.0",
                root_id,
                root / "Cargo.toml",
                None,
                [{"name": "sqlx", "req": "^0.8.6", "features": ["any", "mysql"]}],
            ),
            package(
                "sqlx",
                "0.8.6",
                sqlx_id,
                sqlx / "Cargo.toml",
                registry,
                [{"name": "sqlx-mysql", "req": "=0.8.6", "features": []}],
            ),
            package(
                "sqlx-mysql",
                "0.8.6",
                mysql_id,
                mysql / "Cargo.toml",
                registry,
                [{"name": "rsa", "req": "^0.9", "features": []}],
            ),
            package("rsa", "0.9.10", rsa_id, rsa / "Cargo.toml", registry, []),
        ],
        "resolve": {
            "nodes": [
                node(root_id, [("sqlx", sqlx_id)]),
                node(sqlx_id, [("sqlx-mysql", mysql_id)], ("any", "mysql")),
                node(mysql_id, [("rsa", rsa_id)], ("any",)),
                node(rsa_id, []),
            ]
        },
    }
    lock_data = {
        "package": [
            {
                "name": "sqlx",
                "version": "0.8.6",
                "source": registry,
                "checksum": crate_checksums[("sqlx", "0.8.6")],
            },
            {
                "name": "sqlx-mysql",
                "version": "0.8.6",
                "source": registry,
                "checksum": crate_checksums[("sqlx-mysql", "0.8.6")],
            },
            {
                "name": "rsa",
                "version": "0.9.10",
                "source": registry,
                "checksum": crate_checksums[("rsa", "0.9.10")],
            },
        ]
    }
    tree = (
        'rsa v0.9.10 -> sqlx-mysql v0.8.6 -> sqlx feature "mysql" '
        '-> sqlx feature "any" -> chatgpt2api-rust v0.1.0'
    )
    return metadata, lock_data, tree, root / "Cargo.toml", auth_source, rsa_id, mysql_id


def _fixture_authority(lock_data, auth_source):
    expected_checksums = {
        (package["name"], package["version"]): package["checksum"]
        for package in lock_data["package"]
    }
    expected_auth_sha256 = hashlib.sha256(auth_source.read_bytes()).hexdigest().upper()
    return expected_checksums, expected_auth_sha256


def _verify_fixture(
    verifier,
    metadata,
    lock_data,
    tree,
    root_manifest,
    auth_source,
    *,
    expected_checksums=None,
    expected_auth_sha256=None,
):
    if expected_checksums is None or expected_auth_sha256 is None:
        default_checksums, default_auth_sha256 = _fixture_authority(lock_data, auth_source)
        expected_checksums = expected_checksums or default_checksums
        expected_auth_sha256 = expected_auth_sha256 or default_auth_sha256
    return verifier._verify_graph(
        metadata,
        tree,
        lock_data=lock_data,
        root_manifest=root_manifest,
        expected_checksums=expected_checksums,
        expected_auth_sha256=expected_auth_sha256,
    )


def test_dependency_verifier_accepts_only_the_positive_fixture(tmp_path: Path) -> None:
    verifier = _load_dependency_verifier()
    metadata, lock_data, tree, root_manifest, auth_source, _, _ = _positive_rsa_fixture(tmp_path)

    assert _verify_fixture(verifier, metadata, lock_data, tree, root_manifest, auth_source) == auth_source


def test_dependency_verifier_rejects_extra_rsa_consumer_and_edge_mutations(tmp_path: Path) -> None:
    verifier = _load_dependency_verifier()
    metadata, lock_data, tree, root_manifest, auth_source, rsa_id, _ = _positive_rsa_fixture(tmp_path)
    extra_root = tmp_path / "extra-crypto-0.1.0"
    (extra_root / "src").mkdir(parents=True)
    (extra_root / "Cargo.toml").write_text("[package]\n", encoding="utf-8")
    extra_id = "registry+https://github.com/rust-lang/crates.io-index#extra-crypto@0.1.0"
    metadata["packages"].append(
        {
            "name": "extra-crypto",
            "version": "0.1.0",
            "id": extra_id,
            "manifest_path": str(extra_root / "Cargo.toml"),
            "source": "registry+https://github.com/rust-lang/crates.io-index",
            "dependencies": [],
        }
    )
    metadata["resolve"]["nodes"].append(
        {
            "id": extra_id,
            "dependencies": [rsa_id],
            "deps": [{"name": "rsa", "pkg": rsa_id, "dep_kinds": [{"kind": None, "target": None}]}],
            "features": [],
        }
    )
    with pytest.raises(verifier.VerificationError, match="reverse"):
        _verify_fixture(verifier, metadata, lock_data, tree, root_manifest, auth_source)

    changed = copy.deepcopy(metadata)
    changed["packages"] = [p for p in changed["packages"] if p["name"] != "extra-crypto"]
    changed["resolve"]["nodes"] = [n for n in changed["resolve"]["nodes"] if n["id"] != extra_id]
    changed["packages"][-1]["version"] = "0.9.11"
    with pytest.raises(verifier.VerificationError):
        _verify_fixture(verifier, changed, lock_data, tree, root_manifest, auth_source)


def test_dependency_verifier_rejects_crypto_source_and_lock_mutations(tmp_path: Path) -> None:
    verifier = _load_dependency_verifier()
    metadata, lock_data, tree, root_manifest, auth_source, _, _ = _positive_rsa_fixture(tmp_path)

    auth_source.write_text("use rsa::RsaPublicKey;\nfn f() { key.sign(data); }\n", encoding="utf-8")
    with pytest.raises(verifier.VerificationError, match="encryption"):
        _verify_fixture(verifier, metadata, lock_data, tree, root_manifest, auth_source)

    metadata, lock_data, tree, root_manifest, auth_source, _, _ = _positive_rsa_fixture(tmp_path / "second")
    root_secret = tmp_path / "second" / "root" / "src" / "secret.rs"
    root_secret.write_text("use rsa::RsaPrivateKey; fn x() { key.decrypt(value); }\n", encoding="utf-8")
    with pytest.raises(verifier.VerificationError, match="forbidden"):
        _verify_fixture(verifier, metadata, lock_data, tree, root_manifest, auth_source)

    metadata, lock_data, tree, root_manifest, auth_source, _, _ = _positive_rsa_fixture(tmp_path / "third")
    expected_checksums, expected_auth_sha256 = _fixture_authority(lock_data, auth_source)
    lock_data["package"][-1]["checksum"] = "4" * 64
    with pytest.raises(verifier.VerificationError, match="checksum"):
        _verify_fixture(
            verifier,
            metadata,
            lock_data,
            tree,
            root_manifest,
            auth_source,
            expected_checksums=expected_checksums,
            expected_auth_sha256=expected_auth_sha256,
        )
    lock_data["package"][-1]["checksum"] = expected_checksums[("rsa", "0.9.10")]
    lock_data["package"][-1]["source"] = "registry+https://example.invalid/index"
    with pytest.raises(verifier.VerificationError, match="source"):
        _verify_fixture(
            verifier,
            metadata,
            lock_data,
            tree,
            root_manifest,
            auth_source,
            expected_checksums=expected_checksums,
            expected_auth_sha256=expected_auth_sha256,
        )


def test_dependency_verifier_rejects_auth_source_byte_mutation(tmp_path: Path) -> None:
    verifier = _load_dependency_verifier()
    metadata, lock_data, tree, root_manifest, auth_source, _, _ = _positive_rsa_fixture(tmp_path)
    expected_checksums, expected_auth_sha256 = _fixture_authority(lock_data, auth_source)
    auth_source.write_text(
        auth_source.read_text(encoding="utf-8") + "// token-preserving byte mutation\n",
        encoding="utf-8",
    )
    with pytest.raises(verifier.VerificationError, match="auth source checksum"):
        _verify_fixture(
            verifier,
            metadata,
            lock_data,
            tree,
            root_manifest,
            auth_source,
            expected_checksums=expected_checksums,
            expected_auth_sha256=expected_auth_sha256,
        )
