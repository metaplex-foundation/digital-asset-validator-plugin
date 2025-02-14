use thiserror::Error;
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlerkleSerializationError {
    #[error("Serialization error: {0}")]
    SerializationError(String),
}
