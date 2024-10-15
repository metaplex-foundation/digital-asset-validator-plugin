FROM rust:1.75.0 AS builder

RUN apt-get update && apt-get install -y git curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Clone geyser repo
RUN git clone --depth 1 --branch v0.1.11 https://github.com/extrnode/solana-geyser-zmq

COPY ./solana-snapshot-etl solana-snapshot-etl
COPY Cargo.* ./

# Build geyser zmq
RUN cargo build --release -p solana-geyser-plugin-scaffold

# Build snapshot etl
RUN cargo build --release --features=standalone --bin=solana-snapshot-etl && \
    rm -rf /app/target/release/.fingerprint /app/target/release/deps /usr/local/cargo/registry

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 \
    libc6 \
    libgcc1 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and group with large UID and GID for security
RUN groupadd -g 10001 appgroup && useradd -u 10001 -r -g appgroup -m -d /home/appuser -s /bin/bash appuser

WORKDIR /app

# Copy compiled binaries and other required files and set the correct ownership during the copy
COPY --from=builder --chown=appuser:appgroup /app/target/release/solana-snapshot-etl /app/
COPY --from=builder --chown=appuser:appgroup /app/target/release/libsolana_geyser_plugin_scaffold.so /app/target/release/
COPY --chown=appuser:appgroup geyser-conf.json process_snapshots.sh /app/
RUN chown -R appuser:appgroup /app && chmod -R u+w /app

USER appuser

ENV PATH="/app:${PATH}"
ENV GEYSER_CONFIG=/app/geyser-conf.json
ENV TCP_PORT=3000
ENV TCP_MIN_SUBSCRIBERS=1
ENV BASEDIR=/snapshots/*.tar.zst

ENTRYPOINT ["process_snapshots.sh"]