ARG BUILDPLATFORM
ARG TARGETPLATFORM
ARG TARGETARCH

FROM --platform=$BUILDPLATFORM oven/bun:1-alpine AS web-build

WORKDIR /app/web

COPY web/package.json web/bun.lock ./
RUN bun install --frozen-lockfile

COPY VERSION /app/VERSION
COPY CHANGELOG.md /app/CHANGELOG.md
COPY web ./
RUN NEXT_PUBLIC_APP_VERSION="$(cat /app/VERSION)" bun run build


FROM --platform=$TARGETPLATFORM rust:1.89-bookworm AS rust-build

WORKDIR /app/rust

COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/file_identity ./file_identity
COPY rust/src ./src
RUN cargo build --release --locked --bin chatgpt2api-rust


FROM --platform=$TARGETPLATFORM debian:bookworm-slim AS app

ARG TARGETPLATFORM
ARG TARGETARCH
ARG CODEX_CLIENT_VERSION

ENV CODEX_CLIENT_VERSION=${CODEX_CLIENT_VERSION} \
    RUST_PRODUCTION=1 \
    RUST_BIND=0.0.0.0:80 \
    RUST_DATA_DIR=/app/data \
    RUST_UPSTREAM_PROTOCOL=chatgpt \
    RUST_UPSTREAM_BASE_URL=https://chatgpt.com

WORKDIR /app

# Rust runtime needs CA roots for ChatGPT and a small HTTP client for the
# container health check. Build-only compilers and Python are not shipped.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY config.json ./
COPY VERSION ./
COPY --from=web-build /app/web/out ./web_dist
COPY --from=rust-build /app/rust/target/release/chatgpt2api-rust /usr/local/bin/chatgpt2api-rust

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD wget --quiet --output-document=- 'http://127.0.0.1:80/health?format=json' > /dev/null || exit 1

CMD ["/usr/local/bin/chatgpt2api-rust"]
