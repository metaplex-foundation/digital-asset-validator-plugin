#!/usr/bin/env bash

# Prints the Solana version.

set -e

cd "$(dirname "$0")/.."
source ci/rust-version.sh stable

cd "$(dirname "$0")/../plerkle"
cargo +"$rust_stable" read-manifest | jq -r '.dependencies[] | select(.name == "solana-geyser-plugin-interface") | .req'
