pub mod redis_messenger;
pub mod redis_pool_messenger;

// Redis stream values.
pub const GROUP_NAME: &str = "plerkle";
pub const DATA_KEY: &str = "data";
pub const DEFAULT_RETRIES: usize = 3;
pub const DEFAULT_MSG_BATCH_SIZE: usize = 10;
pub const MESSAGE_WAIT_TIMEOUT: usize = 10;
pub const IDLE_TIMEOUT: usize = 5000;
pub const REDIS_MAX_BYTES_COMMAND: usize = 536870912;
pub const PIPELINE_SIZE_BYTES: usize = REDIS_MAX_BYTES_COMMAND / 100;
pub const PIPELINE_MAX_TIME: u64 = 10;

pub(crate) const REDIS_CON_STR: &str = "redis_connection_str";
