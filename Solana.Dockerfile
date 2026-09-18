ARG SOLANA_VERSION=v4.2.2
ARG RUST_VERSION=1.96.1
FROM rust:$RUST_VERSION-bookworm AS builder
RUN apt-get update \
      && apt-get -y install \
           wget \
           curl \
           build-essential \
           software-properties-common \
           lsb-release \
           libelf-dev \
           linux-headers-generic \
           pkg-config \
           curl \
           cmake \
           protobuf-compiler
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /rust/
COPY plerkle_serialization /rust/plerkle_serialization
COPY plerkle_messenger /rust/plerkle_messenger
COPY plerkle /rust/plerkle
COPY Cargo.toml /rust/
COPY Cargo.lock /rust/
WORKDIR /rust
RUN cargo build --release --locked

# Anza stopped publishing anzaxyz/agave Docker images after v3.1.14, so
# install the official prebuilt release binaries on the same distro the
# plugin is built on (bookworm), keeping glibc/OpenSSL in sync (see #103).
FROM debian:bookworm-slim
ARG SOLANA_VERSION
RUN apt-get update \
      && apt-get -y install --no-install-recommends \
           curl \
           ca-certificates \
           bzip2 \
           bash \
           libssl3 \
      && rm -rf /var/lib/apt/lists/*
RUN curl -sSfL "https://release.anza.xyz/${SOLANA_VERSION}/solana-release-x86_64-unknown-linux-gnu.tar.bz2" \
      | tar -xj -C /usr/local \
      && for bin in /usr/local/solana-release/bin/*; do \
           [ -f "$bin" ] && ln -s "$bin" /usr/local/bin/; \
         done
RUN mkdir -p /so /plugin-config
COPY --from=builder /rust/target/release/libplerkle.so /plugin/plugin.so
COPY ./docker .
RUN chmod +x ./*.sh
ENTRYPOINT [ "./runs.sh" ]
CMD [""]
