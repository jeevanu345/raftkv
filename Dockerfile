# Multi-stage build for raftkv-server.
#
# Stage 1: build the static-ish release binary.
FROM rust:1.81-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release -p raftkv-server -p raftkv-cli

# Stage 2: minimal runtime.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 raftkv
COPY --from=builder /src/target/release/raftkv-server /usr/local/bin/raftkv-server
COPY --from=builder /src/target/release/raftkv-cli    /usr/local/bin/raftkv-cli
RUN mkdir -p /data && chown raftkv:raftkv /data
USER raftkv
WORKDIR /data
EXPOSE 7001 6379 9100
ENTRYPOINT ["tini", "--", "/usr/local/bin/raftkv-server"]
