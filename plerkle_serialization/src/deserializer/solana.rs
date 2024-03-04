use std::convert::TryFrom;

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

impl<'a> TryFrom<&FBPubkey> for Pubkey {
    type Error = SolanaDeserializerError;

    fn try_from(pubkey: &FBPubkey) -> SolanaDeserializeResult<Self> {
        Pubkey::try_from(pubkey.0.as_slice())
            .map_err(|_error| SolanaDeserializerError::InvalidFlatBufferKey)
    }
}

pub struct PlerkleOptionalU8Vector<'a>(pub Option<Vector<'a, u8>>);

impl<'a> TryFrom<PlerkleOptionalU8Vector<'a>> for Vec<u8> {
    type Error = SolanaDeserializerError;

    fn try_from(data: PlerkleOptionalU8Vector<'a>) -> SolanaDeserializeResult<Self> {
        Ok(data
            .0
            .ok_or(SolanaDeserializerError::NotFound)?
            .bytes()
            .to_vec())
    }
}

pub struct PlerkleOptionalStr<'a>(pub Option<&'a str>);

impl<'a> TryFrom<PlerkleOptionalStr<'a>> for Signature {
    type Error = SolanaDeserializerError;

    fn try_from(data: PlerkleOptionalStr<'a>) -> SolanaDeserializeResult<Self> {
        data.0
            .ok_or(SolanaDeserializerError::NotFound)?
            .parse::<Signature>()
            .map_err(|_error| SolanaDeserializerError::DeserializationError)
    }
}

pub struct PlerkleOptionalPubkeyVector<'a>(pub Option<Vector<'a, FBPubkey>>);

impl<'a> TryFrom<PlerkleOptionalPubkeyVector<'a>> for Vec<Pubkey> {
    type Error = SolanaDeserializerError;

    fn try_from(public_keys: PlerkleOptionalPubkeyVector<'a>) -> SolanaDeserializeResult<Self> {
        public_keys
            .0
            .ok_or(SolanaDeserializerError::NotFound)?
            .iter()
            .map(|key| {
                Pubkey::try_from(key.0.as_slice())
                    .map_err(|_error| SolanaDeserializerError::InvalidFlatBufferKey)
            })
            .collect::<SolanaDeserializeResult<Vec<Pubkey>>>()
    }
}

pub struct PlerkleCompiledInstructionVector<'a>(
    pub Vector<'a, ForwardsUOffset<FBCompiledInstruction<'a>>>,
);

impl<'a> TryFrom<PlerkleCompiledInstructionVector<'a>> for Vec<CompiledInstruction> {
    type Error = SolanaDeserializerError;

    fn try_from(vec_cix: PlerkleCompiledInstructionVector<'a>) -> SolanaDeserializeResult<Self> {
        let mut message_instructions = vec![];

        for cix in vec_cix.0 {
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
}

pub struct PlerkleCompiledInnerInstructionVector<'a>(
    pub Vector<'a, ForwardsUOffset<FBCompiledInnerInstructions<'a>>>,
);
impl<'a> TryFrom<PlerkleCompiledInnerInstructionVector<'a>> for Vec<InnerInstructions> {
    type Error = SolanaDeserializerError;

    fn try_from(
        vec_ixs: PlerkleCompiledInnerInstructionVector<'a>,
    ) -> SolanaDeserializeResult<Self> {
        let mut meta_inner_instructions = vec![];

        for ixs in vec_ixs.0 {
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
}

pub struct PlerkleInnerInstructionsVector<'a>(
    pub Vector<'a, ForwardsUOffset<FBInnerInstructions<'a>>>,
);

impl<'a> TryFrom<PlerkleInnerInstructionsVector<'a>> for Vec<InnerInstructions> {
    type Error = SolanaDeserializerError;

    fn try_from(vec_ixs: PlerkleInnerInstructionsVector<'a>) -> SolanaDeserializeResult<Self> {
        vec_ixs
            .0
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
}
