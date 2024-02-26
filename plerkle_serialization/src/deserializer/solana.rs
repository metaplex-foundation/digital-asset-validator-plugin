use super::{
    OptionalCompiledInnerInstructionsVector, OptionalCompiledInstructionVector, OptionalPubkey,
    OptionalPubkeyVector, OptionalStr, OptionalU8Vector,
};
use crate::Pubkey as FBPubkey;
use solana_sdk::{
    instruction::CompiledInstruction,
    pubkey::Pubkey,
    signature::{ParseSignatureError, Signature},
};
use solana_transaction_status::{InnerInstruction, InnerInstructions};

#[derive(Debug, thiserror::Error)]
pub enum SolanaDeserializerError {
    #[error("solana pubkey deserialization error")]
    Pubkey,
    #[error("solana signature deserialization error")]
    Signature,
    #[error("expected data not found in FlatBuffer")]
    NotFound,
}

impl<'a> TryFrom<OptionalPubkey<'a>> for Pubkey {
    type Error = SolanaDeserializerError;

    fn try_from(value: OptionalPubkey<'a>) -> Result<Self, Self::Error> {
        value
            .0
            .ok_or(SolanaDeserializerError::NotFound)
            .and_then(|data| Pubkey::try_from(data))
    }
}

impl TryFrom<&FBPubkey> for Pubkey {
    type Error = SolanaDeserializerError;

    fn try_from(value: &FBPubkey) -> Result<Self, Self::Error> {
        (value.0)
            .try_into()
            .map_err(|_| SolanaDeserializerError::Pubkey)
    }
}

impl TryFrom<FBPubkey> for Pubkey {
    type Error = SolanaDeserializerError;

    fn try_from(value: FBPubkey) -> Result<Self, Self::Error> {
        Pubkey::try_from(value.0).map_err(|_| SolanaDeserializerError::Pubkey)
    }
}

impl<'a> TryFrom<OptionalU8Vector<'a>> for &'a [u8] {
    type Error = SolanaDeserializerError;

    fn try_from(value: OptionalU8Vector<'a>) -> Result<Self, Self::Error> {
        value
            .0
            .map(|data| data.bytes())
            .ok_or(SolanaDeserializerError::NotFound)
    }
}

impl<'a> TryFrom<OptionalStr<'a>> for Signature {
    type Error = SolanaDeserializerError;

    fn try_from(value: OptionalStr<'a>) -> Result<Self, Self::Error> {
        value
            .0
            .ok_or(ParseSignatureError::Invalid)
            .and_then(|data| data.parse())
            .map_err(|_| SolanaDeserializerError::Signature)
    }
}

impl<'a> TryFrom<OptionalPubkeyVector<'a>> for Vec<Pubkey> {
    type Error = SolanaDeserializerError;

    fn try_from(value: OptionalPubkeyVector<'a>) -> Result<Self, Self::Error> {
        value
            .0
            .ok_or(SolanaDeserializerError::Pubkey)?
            .iter()
            .map(|key| Pubkey::try_from(key))
            .collect::<Result<Self, Self::Error>>()
    }
}

impl TryFrom<OptionalCompiledInstructionVector<'_>> for Vec<CompiledInstruction> {
    type Error = SolanaDeserializerError;

    fn try_from(value: OptionalCompiledInstructionVector<'_>) -> Result<Self, Self::Error> {
        let mut message_instructions = vec![];
        for cix in value.0.ok_or(SolanaDeserializerError::NotFound)? {
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

impl TryFrom<OptionalCompiledInnerInstructionsVector<'_>> for Vec<InnerInstructions> {
    type Error = SolanaDeserializerError;

    fn try_from(value: OptionalCompiledInnerInstructionsVector<'_>) -> Result<Self, Self::Error> {
        let mut meta_inner_instructions = vec![];
        for ixs in value.0.ok_or(SolanaDeserializerError::NotFound)? {
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
