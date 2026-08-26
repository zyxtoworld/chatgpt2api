# Rust dependency audit exception

当前唯一的 advisory 例外是 `RUSTSEC-2023-0071`，不是全局关闭 warning。锁定依赖链为：

`chatgpt2api-rust -> sqlx (mysql/any) 0.8.6 -> sqlx-mysql 0.8.6 -> rsa 0.9.10`。

## 可达性审计

审计使用项目隔离的 Cargo registry 中锁定源码。`sqlx-mysql-0.8.6/src/connection/auth.rs` 只导入
`RsaPublicKey`，`encrypt_rsa` 在非 TLS 分支解析服务器发来的公钥并调用
`RsaPublicKey::encrypt`；TLS 分支直接在已建立的 TLS 流中发送密码。该 SQLx 客户端路径没有构造
`RsaPrivateKey`，也没有调用私钥 `decrypt`。应用自己的 `rust/src` 与 `rust/file_identity/src`
同样不导入 RSA、不创建私钥、不调用解密。

因此该 advisory 所针对的私钥解密 timing/key-recovery 路径不在本应用进程的可达调用图中。
`rsa` crate 自身仍包含私钥实现，这不是把它标成安全，而是记录本次例外的严格边界。

## MySQL 传输边界

当前 `DatabaseStorage::connect` 保留用户提供的 MySQL URL。SQLx MySQL 的默认 `ssl-mode` 是
`PREFERRED`，它在服务器不支持 TLS 时允许降级到明文；本例外不把该行为解释成安全，也不把
RSA 公钥加密当作 TLS。生产连接应明确使用 `ssl-mode=REQUIRED`，有可信 CA/主机名时使用
`VERIFY_CA` 或 `VERIFY_IDENTITY`。本轮不擅自把默认值改成强制 TLS，以免破坏现有已承诺的
MariaDB/MySQL 后端连接合同；这仍是部署配置的安全要求。

## 例外与移除条件

`.github/workflows/docker-publish.yml` 和 `deny.toml` 只忽略这个完整 advisory ID，仍然执行
`--deny warnings` 及其余 bans/licenses/sources 检查。禁止新增 RSA 私钥或解密调用；静态合同测试
会在应用 Rust 源码出现 `RsaPrivateKey`、`rsa::` 或 `.decrypt(...)` 时失败。

依赖审计阶段随后运行 `uv run python scripts/verify_rsa_advisory.py`。该 verifier 使用当前锁定的
`cargo metadata.resolve.nodes` 与 `cargo tree`，按精确 package ID 验证完整的
`chatgpt2api-rust -> sqlx -> sqlx-mysql -> rsa` 可达链，并要求 `rsa@0.9.10` 的直接反向依赖集合
严格等于 `{sqlx-mysql@0.8.6}`；新增 RSA consumer、版本或边变化都会 fail-closed。它还校验 root
的 `any`/`mysql` feature、解析后的 feature、三段 registry package 的当前 crates.io source 和
Cargo.lock 中绑定的精确 checksum，并拒绝 registry 源/布局漂移。

verifier 定位实际 registry 下载目录中的 `sqlx-mysql/src/connection/auth.rs`，扫描整个该 crate
以及所有应用 path package 的 Rust 源码；缺少 metadata/tree 边、源码文件、版本/source/layout 变化，
缺少 `RsaPublicKey::encrypt`，或出现 `RsaPrivateKey`/私钥 `decrypt`，都会 fail-closed。RSA crate
自身的实现被明确排除在 consumer 扫描之外；这不是依赖说明文字测试，而是当前锁定图和源码的可达性门。

`cargo-deny` 对仓库内不可发布的 `rust/file_identity` path dependency 保留 wildcard 警告；这条边
不是外部 registry/git 依赖，真正的 registry、git、license 和 advisory 检查仍为错误级门。

一旦 `sqlx-mysql` 发布不再携带该 advisory、项目可以移除 MySQL 支持，或应用需要接收/解密
RSA 私钥，必须先移除例外并重新审计/升级依赖，不能继续沿用本说明。喵
