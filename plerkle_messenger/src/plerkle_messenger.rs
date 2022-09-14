use crate::error::MessengerError;
use async_trait::async_trait;
use figment::value::{Dict, Value};
use serde::Deserialize;
use std::collections::BTreeMap;

#[cfg(feature = "pulsar")]
use crate::PulsarMessenger;
#[cfg(feature = "redis")]
use crate::RedisMessenger;

/// Some constants that can be used as stream key values.
pub const ACCOUNT_STREAM: &str = "ACC";
pub const SLOT_STREAM: &str = "SLT";
pub const TRANSACTION_STREAM: &str = "TXN";
pub const BLOCK_STREAM: &str = "BLK";

#[async_trait]
pub trait Messenger: Sync + Send {
    async fn new(config: MessengerConfig) -> Result<Self, MessengerError>
    where
        Self: Sized;
    fn messenger_type(&self) -> MessengerType;
    async fn add_stream(&mut self, stream_key: &'static str) -> Result<(), MessengerError>;
    async fn set_buffer_size(&mut self, stream_key: &'static str, max_buffer_size: usize);
    async fn send(&mut self, stream_key: &'static str, bytes: &[u8]) -> Result<(), MessengerError>;
    async fn recv(&mut self, stream_key: &'static str)
        -> Result<Vec<(i64, &[u8])>, MessengerError>;
}

pub async fn select_messenger(
    config: MessengerConfig,
) -> Result<Box<dyn Messenger>, MessengerError> {
    match config.messenger_type {
        #[cfg(feature = "pulsar")]
        MessengerType::Pulsar => {
            PulsarMessenger::new(config).await.map(|a| Box::new(a) as Box<dyn Messenger>)
        }
        #[cfg(feature = "redis")]
        MessengerType::Redis => {
            RedisMessenger::new(config).await.map(|a| Box::new(a) as Box<dyn Messenger>)
        }
        _ => Err(MessengerError::ConfigurationError {
            msg: "This Messenger type is not valid, unimplemented or you dont have the right crate features on.".to_string()
        })
    }
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub enum MessengerType {
    Redis,
    Pulsar,
    Invalid,
}

impl Default for MessengerType {
    fn default() -> Self {
        MessengerType::Redis
    }
}

#[derive(Deserialize, Debug, Default, PartialEq)]
pub struct MessengerConfig {
    pub messenger_type: MessengerType,
    pub connection_config: Dict,
}

impl Clone for MessengerConfig {
    fn clone(&self) -> Self {
        let mut d: BTreeMap<String, Value> = BTreeMap::new();
        for (k, i) in self.connection_config.iter() {
            d.insert(k.clone(), i.clone());
        }
        MessengerConfig {
            messenger_type: self.messenger_type.clone(),
            connection_config: d,
        }
    }
}

impl MessengerConfig {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.connection_config.get(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::{MessengerConfig, MessengerType};
    use figment::{providers::Env, value::Dict, Figment, Jail};
    use serde::Deserialize;

    #[derive(Deserialize, Debug, PartialEq)]
    struct Container {
        messenger_config: MessengerConfig,
    }

    #[test]
    fn test_config_deser() {
        Jail::expect_with(|jail| {
            jail.set_env("MESSENGER_CONFIG.messenger_type", "Redis");
            jail.set_env(
                "MESSENGER_CONFIG.connection_config",
                r#"{redis_connection_str="redis://redis"}"#,
            );

            let config: Container = Figment::from(Env::raw()).extract()?;
            let mut expected_dict = Dict::new();
            expected_dict.insert("redis_connection_str".to_string(), "redis://redis".into());
            assert_eq!(
                config.messenger_config,
                MessengerConfig {
                    messenger_type: MessengerType::Redis,
                    connection_config: expected_dict,
                }
            );
            Ok(())
        });
    }
}
