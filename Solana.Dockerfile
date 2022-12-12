FROM rust:1.64-bullseye as builder
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
RUN cargo build --release

FROM solanalabs/solana:v1.14.10
COPY --from=builder /rust/target/release/libplerkle.so /plugin/plugin.so
COPY ./docker .
RUN chmod +x ./*.sh
ENTRYPOINT [ "./runs.sh" ]
CMD [""]
