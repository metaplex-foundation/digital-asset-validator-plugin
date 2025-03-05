# This Dockerfile describes the build process for the snapshot
# ETL tool.

FROM rust:1.83.0-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN apt update && apt install -y git curl protobuf-compiler
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin solana-snapshot-etl -F standalone

FROM debian:trixie-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/solana-snapshot-etl .
COPY --from=builder /app/scripts/docker-entrypoint.sh .
COPY ./accounts-selector-config.json .
RUN groupadd appuser && useradd -g appuser appuser
USER appuser

ENTRYPOINT ["./docker-entrypoint.sh"]
STOPSIGNAL SIGINT
