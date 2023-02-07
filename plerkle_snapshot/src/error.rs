use thiserror::Error;

#[derive(Error, Debug)]
pub enum SnappError {
    #[error("Error reading config file: ({msg})")]
    ConfigFileReadError { msg: String }
}