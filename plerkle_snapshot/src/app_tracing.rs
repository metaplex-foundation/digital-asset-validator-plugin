use crate::config::Config;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

pub fn enable_tracing(config: Config) {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(config.level.unwrap_or(Level::INFO))
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
