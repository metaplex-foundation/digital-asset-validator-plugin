#!/bin/bash
#Function definition
readCargoVariable() {
  declare variable="$1"
  declare Cargo_toml="$2"

  while read -r name equals value _; do
    if [[ $name = "$variable" && $equals = = ]]; then
      echo "${value//\"/}"
      return
    fi
  done < <(cat "$Cargo_toml")
  echo "Unable to locate $variable in $Cargo_toml" 1>&2
}

# Variable definition
plugin_name="$(readCargoVariable name plerkle/Cargo.toml)"
plugin_lib_name=plerkle
plugin_version="$(readCargoVariable version plerkle/Cargo.toml)"
targets="$(readCargoVariable targets plerkle/Cargo.toml | sed 's/\[\(.*\)\]/\1/')"

# This mas be a dot separeted value to identify pre-release/build
rust_version="$(readCargoVariable channel rust-toolchain.toml)"
rust_profile="$(readCargoVariable profile rust-toolchain.toml)"
rust_components="$(grep components rust-toolchain.toml | awk -F = '{print $2}' | sed "s/\[//" | sed "s/\]//" | sed "s/\"//g")"

# Validation of solana_version
solana_version="$(grep solana-sdk plerkle/Cargo.toml | awk -F = '{print $4}' | sed 's/\"//g' | sed 's/}//' | sed 's/\=//' | sed 's/ *$//' | sed 's/^[ \t]*//')"
validate=${#solana_version}
if [[ validate -lt 5 ]]
then
    solana_version="$(grep solana-sdk plerkle/Cargo.toml | awk -F = '{print $3}' | sed 's/\"//g' | sed 's/}//' | sed 's/\~//' | sed 's/ *$//' | sed 's/^[ \t]*//')"
fi


build_meta=solana"$solana_version"
release=$plugin_version
