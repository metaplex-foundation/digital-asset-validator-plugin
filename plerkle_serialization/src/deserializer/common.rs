use crate::{
    CompiledInnerInstructions as FBCompiledInnerInstructions,
    CompiledInstruction as FBCompiledInstruction, Pubkey as FBPubkey,
};
use flatbuffers::{ForwardsUOffset, Vector};

/// Represents an optional reference to a FBPubkey.
pub struct OptionalPubkey<'a>(pub Option<&'a FBPubkey>);

/// Represents an optional Vector of u8 values.
pub struct OptionalU8Vector<'a>(pub Option<Vector<'a, u8>>);

/// Represents an optional reference to a string.
pub struct OptionalStr<'a>(pub Option<&'a str>);

/// Represents an optional Vector of FBPubkey values.
pub struct OptionalPubkeyVector<'a>(pub Option<Vector<'a, FBPubkey>>);

/// Represents an optional Vector of compiled instructions.
pub struct OptionalCompiledInstructionVector<'a>(
    pub Option<Vector<'a, ForwardsUOffset<FBCompiledInstruction<'a>>>>,
);

/// Represents an optional Vector of compiled inner instructions.
pub struct OptionalCompiledInnerInstructionsVector<'a>(
    pub Option<Vector<'a, ForwardsUOffset<FBCompiledInnerInstructions<'a>>>>,
);
