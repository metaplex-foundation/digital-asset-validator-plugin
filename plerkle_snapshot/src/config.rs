use std::str;

use figment::{Figment, providers::Env};
use tracing::Level;

use crate::error::SnappError;
use serde::{Deserialize};

#[derive(Default, Debug, Deserialize)]
pub struct Config {
    pub snapshot_url: String,
    pub snapshot_storage_path: String,
    pub plugin_path: String,
    #[serde(skip)]
    pub level: Option<Level>,
}

pub fn extract_config() -> Result<Config, SnappError> {
    Figment::new()
    .join(Env::prefixed("SNAP_"))
    .extract::<Config>()
    .map(|mut op|{
        op.level = Some(Level::INFO);
        op
    })
    .map_err(|config_error| SnappError::ConfigFileReadError {
        msg: config_error.to_string(),
    })
}