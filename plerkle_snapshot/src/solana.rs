// Copyright 2022 Solana Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains code vendored from https://github.com/solana-labs/solana

use std::{
    collections::{HashMap, HashSet},
    io::Read,
};

use bincode::Options;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use solana_accounts_db::{
    account_storage::meta::StoredMetaWriteVersion, accounts_db::stats::BankHashStats,
    ancestors::AncestorsForSerialization, blockhash_queue::BlockhashQueue,
};
use solana_frozen_abi_macro::AbiExample;
use solana_runtime::{epoch_stakes::EpochStakes, stakes::Stakes};
use solana_sdk::{
    clock::{Epoch, UnixTimestamp},
    deserialize_utils::default_on_eof,
    epoch_schedule::EpochSchedule,
    fee_calculator::{FeeCalculator, FeeRateGovernor},
    hard_forks::HardForks,
    hash::Hash,
    inflation::Inflation,
    pubkey::Pubkey,
    rent_collector::RentCollector,
    slot_history::Slot,
    stake::state::Delegation,
};

const MAX_STREAM_SIZE: u64 = 32 * 1024 * 1024 * 1024;

pub fn deserialize_from<R, T>(reader: R) -> bincode::Result<T>
where
    R: Read,
    T: DeserializeOwned,
{
    bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from::<R, T>(reader)
}

#[derive(Default, PartialEq, Eq, Debug, Deserialize)]
struct UnusedAccounts {
    unused1: HashSet<Pubkey>,
    unused2: HashSet<Pubkey>,
    unused3: HashMap<Pubkey, u64>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct DeserializableVersionedBank {
    pub blockhash_queue: BlockhashQueue,
    pub ancestors: AncestorsForSerialization,
    pub hash: Hash,
    pub parent_hash: Hash,
    pub parent_slot: Slot,
    pub hard_forks: HardForks,
    pub transaction_count: u64,
    pub tick_height: u64,
    pub signature_count: u64,
    pub capitalization: u64,
    pub max_tick_height: u64,
    pub hashes_per_tick: Option<u64>,
    pub ticks_per_slot: u64,
    pub ns_per_slot: u128,
    pub genesis_creation_time: UnixTimestamp,
    pub slots_per_year: f64,
    pub accounts_data_len: u64,
    pub slot: Slot,
    pub epoch: Epoch,
    pub block_height: u64,
    pub collector_id: Pubkey,
    pub collector_fees: u64,
    pub fee_calculator: FeeCalculator,
    pub fee_rate_governor: FeeRateGovernor,
    pub collected_rent: u64,
    pub rent_collector: RentCollector,
    pub epoch_schedule: EpochSchedule,
    pub inflation: Inflation,
    pub stakes: Stakes<Delegation>,
    #[allow(dead_code)]
    unused_accounts: UnusedAccounts,
    pub epoch_stakes: HashMap<Epoch, EpochStakes>,
    pub is_delta: bool,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, AbiExample)]
pub struct BankHashInfo {
    pub hash: Hash,
    pub snapshot_hash: Hash,
    pub stats: BankHashStats,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct AccountsDbFields<T>(
    pub HashMap<Slot, Vec<T>>,
    pub StoredMetaWriteVersion,
    pub Slot,
    pub BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<(Slot, Hash)>,
);

pub type SerializedAppendVecId = usize;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct SerializableAccountStorageEntry {
    pub id: SerializedAppendVecId,
    pub accounts_current_len: usize,
}
