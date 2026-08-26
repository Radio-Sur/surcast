FROM oven/bun:1 AS frontend-builder
WORKDIR /app
COPY frontend/package.json frontend/bun.lock ./
RUN bun install --frozen-lockfile
COPY frontend/ .
RUN bun run build

FROM rust:slim-bookworm AS backend-builder
RUN apt-get update \
  && apt-get install -y --no-install-recommends pkg-config libssl-dev libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/workspace/target

COPY autocue-rs/ ./autocue-rs/

COPY backend/Cargo.toml backend/Cargo.lock ./backend/
RUN mkdir -p backend/src && echo "fn main() {}" > backend/src/main.rs
RUN cargo build --release --manifest-path backend/Cargo.toml 2>/dev/null || true

RUN rm -rf backend/src
COPY backend/ ./backend/
RUN cargo build --release --manifest-path backend/Cargo.toml

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates icecast2 ffmpeg curl gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav \
  && rm -rf /var/lib/apt/lists/* \
  && (groupadd -r icecast2 || true) \
  && usermod -g icecast2 icecast2
WORKDIR /workspace/backend

COPY --from=backend-builder /workspace/target/release/surcast-backend ./
COPY --from=backend-builder /workspace/backend/migrations ./migrations
COPY --from=frontend-builder /app/dist ../frontend/dist

EXPOSE 3001
CMD ["./surcast-backend"]