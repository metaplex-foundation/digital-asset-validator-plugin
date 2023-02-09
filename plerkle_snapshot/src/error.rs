use solana_snapshot_etl::SnapshotError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SnappError {
    #[error("Error reading config file: ({msg})")]
    ConfigFileReadError { msg: String },
    #[error("Error Snapshot Download Error: ({msg})")]
    SnapshotDownloadError { msg: String },
    #[error("General Error: ({msg})")]
    GeneralError { msg: String },
    #[error("Plugin Load Error: ({msg})")]
    PluginLoadError { msg: String },
}

impl From<SnapshotError> for SnappError {
    fn from(error: SnapshotError) -> Self {
        match error {
            SnapshotError::IOError(e) => SnappError::GeneralError { msg: e.to_string() },
            SnapshotError::BincodeError(e) => SnappError::GeneralError { msg: e.to_string() },
            SnapshotError::NoStatusCache => SnappError::GeneralError {
                msg: "No Status Cache".to_string(),
            },
            SnapshotError::NoSnapshotManifest => SnappError::GeneralError {
                msg: "No Manifest".to_string(),
            },
            SnapshotError::UnexpectedAppendVec => SnappError::GeneralError {
                msg: "Unexpected Append Vec".to_string(),
            },
        }
    }
}
