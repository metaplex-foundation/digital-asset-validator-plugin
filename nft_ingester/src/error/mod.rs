use sea_orm::{DbErr, TransactionError};
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use crate::BgTask;

#[derive(Error, Debug)]
pub enum IngesterError {
    #[error("ChangeLog Event Malformed")]
    ChangeLogEventMalformed,
    #[error("Compressed Asset Event Malformed")]
    CompressedAssetEventMalformed,
    #[error("Error downloading batch files")]
    BatchInitNetworkingError,
    #[error("Error writing batch files")]
    BatchInitIOError,
    #[error("Storage Write Error {0}")]
    StorageWriteError(String),
    #[error("NotImplemented")]
    NotImplemented,
    #[error("Deserialization Error {0}")]
    DeserializationError(String),
    #[error("Task Manager Error {0}")]
    TaskManagerError(String),
    #[error("Missing or invalid configuration: ({msg})")]
    ConfigurationError { msg: String },

}

impl From<reqwest::Error> for IngesterError {
    fn from(_err: reqwest::Error) -> Self {
        IngesterError::BatchInitNetworkingError
    }
}

impl From<std::io::Error> for IngesterError {
    fn from(_err: std::io::Error) -> Self {
        IngesterError::BatchInitIOError
    }
}

impl From<DbErr> for IngesterError {
    fn from(e: DbErr) -> Self {
        IngesterError::StorageWriteError(e.to_string())
    }
}

impl From<TransactionError<IngesterError>> for IngesterError {
    fn from(e: TransactionError<IngesterError>) -> Self {
        IngesterError::StorageWriteError(e.to_string())
    }
}

impl From<SendError<Box<dyn BgTask>>> for IngesterError {
    fn from(err: SendError<Box<dyn BgTask>>) -> Self {
        IngesterError::TaskManagerError(format!("Could not create task: {:?}", err.to_string()))
    }
}

