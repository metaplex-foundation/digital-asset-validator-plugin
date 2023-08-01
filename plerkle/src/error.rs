use thiserror::Error;

use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;

#[derive(Error, Debug)]
pub enum PlerkleError {
    #[error("General Plugin Config Error ({msg})")]
    GeneralPluginConfigError { msg: String },

    #[error("Error connecting to the backend data store. Error message: ({msg})")]
    DataStoreConnectionError { msg: String },

    #[error("Error preparing data store schema. Error message: ({msg})")]
    DataSchemaError { msg: String },

    #[error("Error preparing data store schema. Error message: ({msg})")]
    ConfigurationError { msg: String },

    #[error("Malformed Anchor Event")]
    EventError {},

    #[error("Unable to Send Event to Stream ({msg})")]
    EventStreamError { msg: String },

    #[error("Unable to acquire lock for updating slots seen. Error message: ({msg})")]
    SlotsSeenLockError { msg: String },
}

// Implement the From trait for the PlerkleError to convert it into GeyserPluginError
impl From<PlerkleError> for GeyserPluginError {
    fn from(err: PlerkleError) -> Self {
        GeyserPluginError::Custom(Box::new(err))
    }
}
