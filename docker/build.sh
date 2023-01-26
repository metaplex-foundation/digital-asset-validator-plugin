#!/usr/bin/env sh
# builds the Solana.Dockerfile image and outputs build artefacts to docker-output directory, you can use this
# to get the geyser plugin build and solana validator build to run on a node
set -eux

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

docker build -t das-geyser/build
docker create --name temp das-geyser/build
mkdir -p $SCRIPT_DIR/geyser-outputs
mkdir -p $SCRIPT_DIR/solana-outputs
# copy plugin .so file
docker container cp temp:/plugin $SCRIPT_DIR/geyser-outputs
# copy solana executables
docker container cp temp:/usr/bin $SCRIPT_DIR/solana-outputs
docker rm temp
