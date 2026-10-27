ARG RUST_VERSION=1.89.0

FROM --platform=$TARGETPLATFORM rust:${RUST_VERSION}-bullseye AS builder

RUN apt-get update \
      && apt-get install -y --no-install-recommends \
           build-essential \
           ca-certificates \
           cmake \
           libelf-dev \
           libsasl2-dev \
           libssl-dev \
           libudev-dev \
           libzstd-dev \
           pkg-config \
           protobuf-compiler \
      && rm -rf /var/lib/apt/lists/*

WORKDIR /rust
COPY Cargo.toml Cargo.lock ./
COPY plerkle ./plerkle
COPY plerkle_messenger ./plerkle_messenger
COPY plerkle_serialization ./plerkle_serialization

RUN cargo build --release --locked -p plerkle

FROM --platform=$TARGETPLATFORM debian:bullseye-slim

LABEL org.opencontainers.image.title="Plerkle Geyser Plugin"
LABEL org.opencontainers.image.description="Plerkle Geyser plugin artifact for DAS e2e validator images"
LABEL org.opencontainers.image.source="https://github.com/metaplex-foundation/digital-asset-validator-plugin"

COPY --from=builder /rust/target/release/libplerkle.so /plugin/plugin.so
