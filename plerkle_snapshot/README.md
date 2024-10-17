# Solana Snapshot ETL 📸

[![license](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](#license)

**`solana-snapshot-etl` efficiently extracts all accounts in a snapshot** to load them into an external system.

> [!IMPORTANT]  
> This code is a fork of the [original repository](https://github.com/riptl/solana-snapshot-etl.git) and has been modified to diverge from the behavior of the original implementation.

## Motivation

Solana nodes periodically backup their account database into a `.tar.zst` "snapshot" stream.
If you run a node yourself, you've probably seen a snapshot file such as this one already:

```
snapshot-139240745-D17vR2iksG5RoLMfTX7i5NwSsr4VpbybuX1eqzesQfu2.tar.zst
```

A full snapshot file contains a copy of all accounts at a specific slot state (in this case slot `139240745`).

Historical accounts data is relevant to blockchain analytics use-cases and event tracing.
Despite archives being readily available, the ecosystem was missing an easy-to-use tool to access snapshot data.

## Usage

Instruction of usage we can find in [this section](../README.md#snapshot-etl).

## Changes

The following changes were made to the original Solana Snapshot ETL tool:

- The solana-opcode-stats binary has been removed.
- The current version of the ETL only streams data to the Geyser plugin. CSV and SQLite support have been removed.
- The ETL now assigns a slot number to the account data, extracting the slot number from the snapshot file name.