use std::env;

use tracing_subscriber::fmt;

pub fn init_logger() {
    // tracing doesn't seem to load RUST_LOG even though its supposed to, set it
    // manually
    let env_filter = env::var("RUST_LOG")
        .or::<Result<String, ()>>(Ok("info".to_string()))
        .unwrap();
    fmt()
        .with_env_filter(env_filter)
        .event_format(fmt::format::json())
        .init();
}
