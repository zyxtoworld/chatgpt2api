ARG BUILDPLATFORM
ARG TARGETPLATFORM
ARG TARGETARCH

FROM --platform=$BUILDPLATFORM oven/bun:1.4.0-alpine@sha256:07235578f79ef8c6f97d94aee7938e76f5cdba5f21ae5dbfdd3d3d38058437eb AS web-build

WORKDIR /app/web

COPY web/package.json web/bun.lock ./
RUN bun install --frozen-lockfile

COPY VERSION /app/VERSION
COPY CHANGELOG.md /app/CHANGELOG.md
COPY web ./
RUN NEXT_PUBLIC_APP_VERSION="$(cat /app/VERSION)" bun run build


FROM --platform=$TARGETPLATFORM rust:1.98-bookworm@sha256:e70e2eec3d495fd5c8e0be74adda86507dfac7f51a724fbf9813ff59b2b247c7 AS rust-build

WORKDIR /app/rust

COPY account_snapshot_contract.json /app/account_snapshot_contract.json
COPY services/protocol/codex_public_item_manifest.json /app/services/protocol/codex_public_item_manifest.json
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/file_identity ./file_identity
COPY rust/src ./src
RUN cargo build --release --locked --bin chatgpt2api-rust


FROM --platform=$TARGETPLATFORM debian:bookworm-slim AS rust-app-candidate

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
# public JSON health contract. Build-only compilers and Python are not shipped.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY config.json ./
COPY VERSION ./
COPY --from=web-build /app/web/out ./web_dist
COPY --from=rust-build /app/rust/target/release/chatgpt2api-rust /usr/local/bin/chatgpt2api-rust

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD wget --quiet --output-document=- 'http://127.0.0.1:80/health?format=json' | grep --quiet '"healthy":true' || exit 1

CMD ["/usr/local/bin/chatgpt2api-rust"]


FROM --platform=$TARGETPLATFORM python:3.13-slim AS app

ARG TARGETPLATFORM
ARG TARGETARCH
ARG CODEX_CLIENT_VERSION

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    CODEX_CLIENT_VERSION=${CODEX_CLIENT_VERSION}

WORKDIR /app

# git: Git storage backend; libpq-dev/gcc: PostgreSQL client build;
# wget: public JSON health contract used by Docker.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    gcc \
    openssl \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY main.py ./
COPY config.json ./
COPY VERSION ./
COPY api ./api
COPY services ./services
COPY utils ./utils
COPY scripts ./scripts
COPY --from=web-build /app/web/out ./web_dist

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD wget --quiet --output-document=- 'http://127.0.0.1:80/health?format=json' | grep --quiet '"healthy":true' || exit 1

CMD ["uv", "run", "--no-sync", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--no-access-log"]
