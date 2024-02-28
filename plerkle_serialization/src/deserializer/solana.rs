use crate::{
    CompiledInnerInstructions as FBCompiledInnerInstructions,
    CompiledInstruction as FBCompiledInstruction, InnerInstructions as FBInnerInstructions,
    Pubkey as FBPubkey,
};
use flatbuffers::{ForwardsUOffset, Vector};
use solana_sdk::{instruction::CompiledInstruction, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{InnerInstruction, InnerInstructions};

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SolanaDeserializerError {
    #[error("Deserialization error")]
    DeserializationError,
    #[error("Not found")]
    NotFound,
    #[error("Invalid FlatBuffer key")]
    InvalidFlatBufferKey,
}

pub type SolanaDeserializeResult<T> = Result<T, SolanaDeserializerError>;

pub fn parse_pubkey(pubkey: &FBPubkey) -> SolanaDeserializeResult<Pubkey> {
    Pubkey::try_from(pubkey.0.as_slice())
        .map_err(|_error| SolanaDeserializerError::InvalidFlatBufferKey)
}

pub fn parse_slice(data: Vector<'_, u8>) -> SolanaDeserializeResult<&[u8]> {
    Ok(data.bytes())
}

pub fn parse_signature(data: &str) -> SolanaDeserializeResult<Signature> {
    data.parse()
        .map_err(|_error| SolanaDeserializerError::DeserializationError)
}

pub fn parse_account_keys(
    public_keys: Vector<'_, FBPubkey>,
) -> SolanaDeserializeResult<Vec<Pubkey>> {
    public_keys
        .iter()
        .map(|key| {
            Pubkey::try_from(key.0.as_slice())
                .map_err(|_error| SolanaDeserializerError::InvalidFlatBufferKey)
        })
        .collect::<SolanaDeserializeResult<Vec<Pubkey>>>()
}

pub fn parse_compiled_instructions(
    vec_cix: Vector<'_, ForwardsUOffset<FBCompiledInstruction>>,
) -> SolanaDeserializeResult<Vec<CompiledInstruction>> {
    let mut message_instructions = vec![];

    for cix in vec_cix {
        message_instructions.push(CompiledInstruction {
            program_id_index: cix.program_id_index(),
            accounts: cix
                .accounts()
                .ok_or(SolanaDeserializerError::NotFound)?
                .bytes()
                .to_vec(),
            data: cix
                .data()
                .ok_or(SolanaDeserializerError::NotFound)?
                .bytes()
                .to_vec(),
        })
    }

    Ok(message_instructions)
}

pub fn parse_compiled_inner_instructions(
    vec_ixs: Vector<'_, ForwardsUOffset<FBCompiledInnerInstructions>>,
) -> SolanaDeserializeResult<Vec<InnerInstructions>> {
    let mut meta_inner_instructions = vec![];

    for ixs in vec_ixs {
        let mut instructions = vec![];
        for ix in ixs
            .instructions()
            .ok_or(SolanaDeserializerError::NotFound)?
        {
            let cix = ix
                .compiled_instruction()
                .ok_or(SolanaDeserializerError::NotFound)?;
            instructions.push(InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: cix.program_id_index(),
                    accounts: cix
                        .accounts()
                        .ok_or(SolanaDeserializerError::NotFound)?
                        .bytes()
                        .to_vec(),
                    data: cix
                        .data()
                        .ok_or(SolanaDeserializerError::NotFound)?
                        .bytes()
                        .to_vec(),
                },
                stack_height: Some(ix.stack_height() as u32),
            });
        }
        meta_inner_instructions.push(InnerInstructions {
            index: ixs.index(),
            instructions,
        })
    }

    Ok(meta_inner_instructions)
}

pub fn parse_inner_instructions(
    vec_ixs: Vector<'_, ForwardsUOffset<FBInnerInstructions>>,
) -> SolanaDeserializeResult<Vec<InnerInstructions>> {
    vec_ixs
        .iter()
        .map(|iixs| {
            let instructions = iixs
                .instructions()
                .ok_or(SolanaDeserializerError::NotFound)?
                .iter()
                .map(|cix| {
                    Ok(InnerInstruction {
                        instruction: CompiledInstruction {
                            program_id_index: cix.program_id_index(),
                            accounts: cix
                                .accounts()
                                .ok_or(SolanaDeserializerError::NotFound)?
                                .bytes()
                                .to_vec(),
                            data: cix
                                .data()
                                .ok_or(SolanaDeserializerError::NotFound)?
                                .bytes()
                                .to_vec(),
                        },
                        stack_height: Some(0),
                    })
                })
                .collect::<SolanaDeserializeResult<Vec<InnerInstruction>>>()?;
            Ok(InnerInstructions {
                index: iixs.index(),
                instructions,
            })
        })
        .collect::<SolanaDeserializeResult<Vec<InnerInstructions>>>()
}
