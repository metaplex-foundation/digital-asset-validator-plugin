use solana_sdk::{clock::UnixTimestamp, signature::Signature, transaction::SanitizedTransaction};
use solana_transaction_status::TransactionStatusMeta;
#[derive(Debug, Clone, PartialEq, Eq)]
/// Information about an account being updated
/// (extended with transaction signature doing this update)
pub struct ReplicaAccountInfoV2<'a> {
    /// The Pubkey for the account
    pub pubkey: &'a [u8],

    /// The lamports for the account
    pub lamports: u64,

    /// The Pubkey of the owner program account
    pub owner: &'a [u8],

    /// This account's data contains a loaded program (and is now read-only)
    pub executable: bool,

    /// The epoch at which this account will next owe rent
    pub rent_epoch: u64,

    /// The data held in this account.
    pub data: &'a [u8],

    /// A global monotonically increasing atomic number, which can be used
    /// to tell the order of the account update. For example, when an
    /// account is updated in the same slot multiple times, the update
    /// with higher write_version should supersede the one with lower
    /// write_version.
    pub write_version: u64,

    /// First signature of the transaction caused this account modification
    pub txn_signature: Option<&'a Signature>,
}

#[derive(Clone, Debug)]
pub struct ReplicaTransactionInfoV2<'a> {
    /// The first signature of the transaction, used for identifying the transaction.
    pub signature: &'a Signature,

    /// Indicates if the transaction is a simple vote transaction.
    pub is_vote: bool,

    /// The sanitized transaction.
    pub transaction: &'a SanitizedTransaction,

    /// Metadata of the transaction status.
    pub transaction_status_meta: &'a TransactionStatusMeta,

    /// The transaction's index in the block
    pub index: usize,
}

#[derive(Clone, Debug)]
pub struct ReplicaBlockInfoV2<'a> {
    pub parent_slot: u64,
    pub parent_blockhash: &'a str,
    pub slot: u64,
    pub blockhash: &'a str,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatus {
    /// The highest slot of the heaviest fork processed by the node. Ledger state at this slot is
    /// not derived from a confirmed or finalized block, but if multiple forks are present, is from
    /// the fork the validator believes is most likely to finalize.
    Processed,

    /// The highest slot having reached max vote lockout.
    Rooted,

    /// The highest slot that has been voted on by supermajority of the cluster, ie. is confirmed.
    Confirmed,
}

impl SlotStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SlotStatus::Confirmed => "confirmed",
            SlotStatus::Processed => "processed",
            SlotStatus::Rooted => "rooted",
        }
    }
}
