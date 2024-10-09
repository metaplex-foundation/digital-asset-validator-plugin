FROM rust:1.70.0

RUN apt-get update && apt install -y git curl

WORKDIR /app

COPY . ./

RUN cargo build --release -p plerkle

ENTRYPOINT ["cargo", "r", "--features=standalone", "--package=plerkle_snapshot", "--bin=solana-snapshot-etl"]
