#!/usr/bin/env bash
set -euo pipefail

version="3.5.5"
archive_sha256="b28c91532a8b65a1f983b4c28b7488174e4a01008e29ce8e69bd789f28bc2a89"
root="${RUNNER_TEMP:?RUNNER_TEMP must be set}/chatgpt2api-openssl-${version}"
archive="${RUNNER_TEMP}/openssl-${version}.tar.gz"
source_dir="${RUNNER_TEMP}/openssl-${version}"
url="https://github.com/openssl/openssl/releases/download/openssl-${version}/openssl-${version}.tar.gz"

rm -rf "$root" "$source_dir"
curl --fail --location --retry 3 --retry-all-errors --max-time 180 "$url" --output "$archive"
printf '%s  %s\n' "$archive_sha256" "$archive" | sha256sum --check --status
tar --extract --gzip --file "$archive" --directory "$RUNNER_TEMP"
cd "$source_dir"
./Configure --prefix="$root" --openssldir="$root/ssl" no-shared no-tests
make -j2
make install_sw

test -x "$root/bin/openssl"
test "$("$root/bin/openssl" version)" = "OpenSSL 3.5.5 27 Jan 2026 (Library: OpenSSL 3.5.5 27 Jan 2026)"
printf 'CHATGPT2API_TEST_OPENSSL_PATH=%s\n' "$root/bin/openssl" >> "${GITHUB_ENV:?GITHUB_ENV must be set}"
"$root/bin/openssl" version
