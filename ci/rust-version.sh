#!/usr/bin/env bash

# Source:
# https://github.com/solana-labs/solana-accountsdb-plugin-postgres/blob/master/ci/rust-version.sh

#
# This file maintains the rust versions for use by CI.
#
# Obtain the environment variables without any automatic toolchain updating:
#   $ source ci/rust-version.sh
#
# Obtain the environment variables updating both stable and nightly, only stable, or
# only nightly:
#   $ source ci/rust-version.sh all
#   $ source ci/rust-version.sh stable
#   $ source ci/rust-version.sh nightly

# Then to build with either stable or nightly:
#   $ cargo +"$rust_stable" build
#   $ cargo +"$rust_nightly" build
#

if [[ -n $RUST_VERSION ]]; then
  stable_version="$RUST_VERSION"
else
  stable_version=latest
fi

if [[ -n $RUST_NIGHTLY_VERSION ]]; then
  nightly_version="$RUST_NIGHTLY_VERSION"
else
  nightly_version=latest
fi


export rust_stable="$stable_version"
export rust_stable_docker_image=rust:"$stable_version"

export rust_nightly_docker_image=shepmaster/rust-nightly:"$nightly_version"

[[ -z $1 ]] || (
  rustup_install() {
    declare toolchain=$1
    if ! cargo +"$toolchain" -V > /dev/null; then
      echo "$0: Missing toolchain? Installing...: $toolchain" >&2
      rustup install "$toolchain"
      cargo +"$toolchain" -V
    fi
  }

  set -e
  cd "$(dirname "${BASH_SOURCE[0]}")"
  case $1 in
  stable)
    rustup_install "$rust_stable"
    ;;
  nightly)
    rustup_install "nightly"
    ;;
  all)
    rustup_install "$rust_stable"
    rustup_install "nightly"
    ;;
  *)
    echo "$0: Note: ignoring unknown argument: $1" >&2
    ;;
  esac
)
