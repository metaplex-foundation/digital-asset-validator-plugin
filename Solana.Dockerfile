FROM solanalabs/solana:v1.10.34 as builder
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
           curl
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /rust/
COPY deps /rust/deps
COPY lib /rust/lib
COPY plerkle_serialization /rust/plerkle_serialization
COPY messenger /rust/messenger
COPY plerkle /rust/plerkle
WORKDIR /rust/plerkle
RUN cargo build

FROM solanalabs/solana:v1.10.34
COPY --from=builder /rust/plerkle/target/debug/libplerkle.so /plugin/plugin.so
COPY --from=builder /so/ /so/

COPY ./docker .
RUN chmod +x ./*.sh
ENTRYPOINT [ "./runs.sh" ]
CMD [""]
