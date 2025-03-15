use crate::{
    error::PlerkleSerializationError,
    solana_geyser_plugin_interface_shims::{
        ReplicaAccountInfoV2, ReplicaBlockInfoV2, ReplicaTransactionInfoV2, SlotStatus,
    },
    AccountInfo, AccountInfoArgs, BlockInfo, BlockInfoArgs, CompiledInnerInstruction,
    CompiledInnerInstructionArgs, CompiledInnerInstructions, CompiledInnerInstructionsArgs,
    CompiledInstruction, CompiledInstructionArgs, Pubkey as FBPubkey, Pubkey, SlotStatusInfo,
    SlotStatusInfoArgs, Status as FBSlotStatus, TransactionInfo, TransactionInfoArgs,
    TransactionVersion,
};
use chrono::Utc;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use solana_sdk::{
    message::{SanitizedMessage, VersionedMessage},
    transaction::VersionedTransaction,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta, UiInstruction,
    UiTransactionStatusMeta,
};

pub fn serialize_account<'a>(
    mut builder: FlatBufferBuilder<'a>,
    account: &ReplicaAccountInfoV2,
    slot: u64,
    is_startup: bool,
) -> FlatBufferBuilder<'a> {
    // Serialize vector data.
    let pubkey: Pubkey = account.pubkey.into();
    let owner: Pubkey = account.owner.into();
    let data = builder.create_vector(account.data);

    // Serialize everything into Account Info table.
    let seen_at = Utc::now();
    let account_info = AccountInfo::create(
        &mut builder,
        &AccountInfoArgs {
            pubkey: Some(&pubkey),
            lamports: account.lamports,
            owner: Some(&owner),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: Some(data),
            write_version: account.write_version,
            slot,
            is_startup,
            seen_at: seen_at.timestamp_millis(),
        },
    );

    // Finalize buffer and return to caller.
    builder.finish(account_info, None);
    builder
}

pub fn serialize_slot_status(
    mut builder: FlatBufferBuilder<'_>,
    slot: u64,
    parent: Option<u64>,
    status: SlotStatus,
) -> FlatBufferBuilder<'_> {
    // Convert to flatbuffer enum.
    let status = match status {
        SlotStatus::Confirmed => FBSlotStatus::Confirmed,
        SlotStatus::Processed => FBSlotStatus::Processed,
        SlotStatus::Rooted => FBSlotStatus::Rooted,
    };

    // Serialize everything into Slot Status Info table.
    let seen_at = Utc::now();
    let slot_status = SlotStatusInfo::create(
        &mut builder,
        &SlotStatusInfoArgs {
            slot,
            parent,
            status,
            seen_at: seen_at.timestamp_millis(),
        },
    );

    // Finalize buffer and return to caller.
    builder.finish(slot_status, None);
    builder
}

pub fn serialize_transaction<'a>(
    mut builder: FlatBufferBuilder<'a>,
    transaction_info: &ReplicaTransactionInfoV2,
    slot: u64,
) -> FlatBufferBuilder<'a> {
    // Flatten and serialize account keys.
    let account_keys = transaction_info.transaction.message().account_keys();
    let atl_keys = &transaction_info.transaction_status_meta.loaded_addresses;

    let account_keys = {
        let mut account_keys_fb_vec = vec![];
        for key in account_keys.iter() {
            account_keys_fb_vec.push(FBPubkey(key.to_bytes()));
        }

        for i in &atl_keys.writable {
            let pubkey = FBPubkey(i.to_bytes());
            account_keys_fb_vec.push(pubkey);
        }

        for i in &atl_keys.readonly {
            let pubkey = FBPubkey(i.to_bytes());
            account_keys_fb_vec.push(pubkey);
        }

        if !account_keys_fb_vec.is_empty() {
            Some(builder.create_vector(&account_keys_fb_vec))
        } else {
            None
        }
    };

    // Serialize log messages.
    let log_messages =
        if let Some(log_messages) = &transaction_info.transaction_status_meta.log_messages {
            let mut log_messages_fb_vec = Vec::with_capacity(log_messages.len());
            for message in log_messages {
                log_messages_fb_vec.push(builder.create_string(message));
            }
            Some(builder.create_vector(&log_messages_fb_vec))
        } else {
            None
        };

    // Serialize inner instructions.
    let inner_instructions = if let Some(inner_instructions_vec) = transaction_info
        .transaction_status_meta
        .inner_instructions
        .as_ref()
    {
        let mut overall_fb_vec = Vec::with_capacity(inner_instructions_vec.len());
        for inner_instructions in inner_instructions_vec.iter() {
            let index = inner_instructions.index;
            let mut instructions_fb_vec = Vec::with_capacity(inner_instructions.instructions.len());
            for compiled_instruction in inner_instructions.instructions.iter() {
                let program_id_index = compiled_instruction.instruction.program_id_index;
                let accounts =
                    Some(builder.create_vector(&compiled_instruction.instruction.accounts));
                let data = Some(builder.create_vector(&compiled_instruction.instruction.data));
                let compiled = CompiledInstruction::create(
                    &mut builder,
                    &CompiledInstructionArgs {
                        program_id_index,
                        accounts,
                        data,
                    },
                );
                instructions_fb_vec.push(CompiledInnerInstruction::create(
                    &mut builder,
                    &CompiledInnerInstructionArgs {
                        compiled_instruction: Some(compiled),
                        stack_height: 0, // Desperatley need this when it comes in 1.15
                    },
                ));
            }

            let instructions = Some(builder.create_vector(&instructions_fb_vec));
            overall_fb_vec.push(CompiledInnerInstructions::create(
                &mut builder,
                &CompiledInnerInstructionsArgs {
                    index,
                    instructions,
                },
            ))
        }

        Some(builder.create_vector(&overall_fb_vec))
    } else {
        None
    };
    let message = transaction_info.transaction.message();
    let version = match message {
        SanitizedMessage::Legacy(_) => TransactionVersion::Legacy,
        SanitizedMessage::V0(_) => TransactionVersion::V0,
    };

    // Serialize outer instructions.
    let outer_instructions = message.instructions();
    let outer_instructions = if !outer_instructions.is_empty() {
        let mut instructions_fb_vec = Vec::with_capacity(outer_instructions.len());
        for compiled_instruction in outer_instructions.iter() {
            let program_id_index = compiled_instruction.program_id_index;
            let accounts = Some(builder.create_vector(&compiled_instruction.accounts));
            let data = Some(builder.create_vector(&compiled_instruction.data));
            instructions_fb_vec.push(CompiledInstruction::create(
                &mut builder,
                &CompiledInstructionArgs {
                    program_id_index,
                    accounts,
                    data,
                },
            ));
        }
        Some(builder.create_vector(&instructions_fb_vec))
    } else {
        None
    };
    let seen_at = Utc::now();
    let txn_sig = transaction_info.signature.to_string();
    let signature_offset = builder.create_string(&txn_sig);
    let slot_idx = format!("{}_{}", slot, transaction_info.index);
    let slot_index_offset = builder.create_string(&slot_idx);
    // Serialize everything into Transaction Info table.
    let transaction_info_ser = TransactionInfo::create(
        &mut builder,
        &TransactionInfoArgs {
            is_vote: transaction_info.is_vote,
            account_keys,
            log_messages,
            inner_instructions: None,
            outer_instructions,
            slot,
            slot_index: Some(slot_index_offset),
            seen_at: seen_at.timestamp_millis(),
            signature: Some(signature_offset),
            compiled_inner_instructions: inner_instructions,
            version,
        },
    );

    // Finalize buffer and return to caller.
    builder.finish(transaction_info_ser, None);
    builder
}

pub fn serialize_block<'a>(
    mut builder: FlatBufferBuilder<'a>,
    block_info: &ReplicaBlockInfoV2,
) -> FlatBufferBuilder<'a> {
    // Serialize blockash.
    let blockhash = Some(builder.create_string(block_info.blockhash));

    // Serialize rewards.
    let rewards = None;

    // Serialize everything into Block Info table.
    let seen_at = Utc::now();
    let block_info = BlockInfo::create(
        &mut builder,
        &BlockInfoArgs {
            slot: block_info.slot,
            blockhash,
            rewards,
            block_time: block_info.block_time,
            block_height: block_info.block_height,
            seen_at: seen_at.timestamp_millis(),
        },
    );

    // Finalize buffer and return to caller.
    builder.finish(block_info, None);
    builder
}

/// Serialize a `EncodedConfirmedTransactionWithStatusMeta` from RPC into a FlatBuffer.
/// The Transaction must be base54 encoded.
pub fn seralize_encoded_transaction_with_status(
    mut builder: FlatBufferBuilder<'_>,
    tx: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<FlatBufferBuilder<'_>, PlerkleSerializationError> {
    let meta: UiTransactionStatusMeta =
        tx.transaction
            .meta
            .ok_or(PlerkleSerializationError::SerializationError(
                "Missing meta data for transaction".to_string(),
            ))?;
    // Get `UiTransaction` out of `EncodedTransactionWithStatusMeta`.
    let ui_transaction: VersionedTransaction = tx.transaction.transaction.decode().ok_or(
        PlerkleSerializationError::SerializationError("Transaction cannot be decoded".to_string()),
    )?;
    let msg = ui_transaction.message;
    let atl_keys = msg.address_table_lookups();
    let account_keys = msg.static_account_keys();
    let sig = ui_transaction.signatures[0].to_string();
    let account_keys = {
        let mut account_keys_fb_vec = vec![];
        for key in account_keys.iter() {
            account_keys_fb_vec.push(FBPubkey(key.to_bytes()));
        }
        if atl_keys.is_some() {
            if let OptionSerializer::Some(ad) = meta.loaded_addresses {
                for i in ad.writable {
                    let mut output: [u8; 32] = [0; 32];
                    bs58::decode(i).into(&mut output).map_err(|e| {
                        PlerkleSerializationError::SerializationError(e.to_string())
                    })?;
                    let pubkey = FBPubkey(output);
                    account_keys_fb_vec.push(pubkey);
                }

                for i in ad.readonly {
                    let mut output: [u8; 32] = [0; 32];
                    bs58::decode(i).into(&mut output).map_err(|e| {
                        PlerkleSerializationError::SerializationError(e.to_string())
                    })?;
                    let pubkey = FBPubkey(output);
                    account_keys_fb_vec.push(pubkey);
                }
            }
        }
        if !account_keys_fb_vec.is_empty() {
            Some(builder.create_vector(&account_keys_fb_vec))
        } else {
            None
        }
    };

    // Serialize log messages.
    let log_messages = if let OptionSerializer::Some(log_messages) = &meta.log_messages {
        let mut log_messages_fb_vec = Vec::with_capacity(log_messages.len());
        for message in log_messages {
            log_messages_fb_vec.push(builder.create_string(message));
        }
        Some(builder.create_vector(&log_messages_fb_vec))
    } else {
        None
    };

    // Serialize inner instructions.
    let inner_instructions = if let OptionSerializer::Some(inner_instructions_vec) =
        meta.inner_instructions.as_ref()
    {
        let mut overall_fb_vec = Vec::with_capacity(inner_instructions_vec.len());
        for inner_instructions in inner_instructions_vec.iter() {
            let index = inner_instructions.index;
            let mut instructions_fb_vec = Vec::with_capacity(inner_instructions.instructions.len());
            for ui_instruction in inner_instructions.instructions.iter() {
                if let UiInstruction::Compiled(ui_compiled_instruction) = ui_instruction {
                    let program_id_index = ui_compiled_instruction.program_id_index;
                    let accounts = Some(builder.create_vector(&ui_compiled_instruction.accounts));
                    let data = bs58::decode(&ui_compiled_instruction.data)
                        .into_vec()
                        .map_err(|e| {
                            PlerkleSerializationError::SerializationError(e.to_string())
                        })?;
                    let data = Some(builder.create_vector(&data));
                    let compiled = CompiledInstruction::create(
                        &mut builder,
                        &CompiledInstructionArgs {
                            program_id_index,
                            accounts,
                            data,
                        },
                    );
                    instructions_fb_vec.push(CompiledInnerInstruction::create(
                        &mut builder,
                        &CompiledInnerInstructionArgs {
                            compiled_instruction: Some(compiled),
                            stack_height: 0, // Desperatley need this when it comes in 1.15
                        },
                    ));
                }
            }

            let instructions = Some(builder.create_vector(&instructions_fb_vec));
            overall_fb_vec.push(CompiledInnerInstructions::create(
                &mut builder,
                &CompiledInnerInstructionsArgs {
                    index,
                    instructions,
                },
            ));
        }

        Some(builder.create_vector(&overall_fb_vec))
    } else {
        let empty: Vec<WIPOffset<CompiledInnerInstructions>> = Vec::new();
        Some(builder.create_vector(empty.as_slice()))
    };

    // Serialize outer instructions.
    let outer_instructions = &msg.instructions();
    let outer_instructions = if !outer_instructions.is_empty() {
        let mut instructions_fb_vec = Vec::with_capacity(outer_instructions.len());
        for ui_compiled_instruction in outer_instructions.iter() {
            let program_id_index = ui_compiled_instruction.program_id_index;
            let accounts = Some(builder.create_vector(&ui_compiled_instruction.accounts));

            let data = Some(builder.create_vector(&ui_compiled_instruction.data));
            instructions_fb_vec.push(CompiledInstruction::create(
                &mut builder,
                &CompiledInstructionArgs {
                    program_id_index,
                    accounts,
                    data,
                },
            ));
        }
        Some(builder.create_vector(&instructions_fb_vec))
    } else {
        None
    };
    let version = match msg {
        VersionedMessage::Legacy(_) => TransactionVersion::Legacy,
        VersionedMessage::V0(_) => TransactionVersion::V0,
    };

    // Serialize everything into Transaction Info table.

    let sig_db = builder.create_string(&sig);
    let transaction_info = TransactionInfo::create(
        &mut builder,
        &TransactionInfoArgs {
            is_vote: false,
            account_keys,
            log_messages,
            inner_instructions: None,
            outer_instructions,
            slot: tx.slot,
            seen_at: 0,
            slot_index: None,
            signature: Some(sig_db),
            compiled_inner_instructions: inner_instructions,
            version,
        },
    );

    // Finalize buffer and return to caller.
    builder.finish(transaction_info, None);
    Ok(builder)
}
