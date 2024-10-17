use crate::{error::MessengerError, redis_pool_messenger::RedisPoolMessenger};
use async_trait::async_trait;
use blake3::OUT_LEN;
use figment::value::{Dict, Value};
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::redis_messenger::RedisMessenger;

/// Some constants that can be used as stream key values.
pub const ACCOUNT_STREAM: &str = "ACC";
pub const ACCOUNT_BACKFILL_STREAM: &str = "ACCFILL";
pub const SLOT_STREAM: &str = "SLT";
pub const TRANSACTION_STREAM: &str = "TXN";
pub const TRANSACTION_BACKFILL_STREAM: &str = "TXNFILL";
pub const BLOCK_STREAM: &str = "BLK";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecvData {
    pub id: String,
    pub tries: usize,
    pub data: Vec<u8>,
}

impl RecvData {
    pub fn new(id: String, data: Vec<u8>) -> Self {
        RecvData { id, data, tries: 0 }
    }

    pub fn new_retry(id: String, data: Vec<u8>, tries: usize) -> Self {
        RecvData { id, data, tries }
    }

    pub fn hash(&mut self) -> [u8; OUT_LEN] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.data);
        let hash = hasher.finalize();
        hash.as_bytes().to_owned()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumptionType {
    New,
    Redeliver,
    All,
}

#[async_trait]
pub trait Messenger: Sync + Send {
    fn messenger_type(&self) -> MessengerType;
    async fn recv(
        &mut self,
        stream_key: &'static str,
        consumption_type: ConsumptionType,
    ) -> Result<Vec<RecvData>, MessengerError>;
    async fn stream_size(&mut self, stream_key: &'static str) -> Result<u64, MessengerError>;

    // Ack-ing messages is made a bit awkward by the current interface layout because
    // the sequence of msgs returned by `recv` will mutably borrow `self`, and calling
    // `ack_msg` need to do the same thing, which isn't possible while that returned `Vec`
    // is alive or the borrow checker complains. We can do stuff like making `recv` and `ack`
    // require interior mutability, but that or other alternatives are non-trivial refactoring
    // efforts best applied after we get more data about how the system performs and what
    // changes we'd like to do overall.
    //
    // For now, the flow is that `recv` returns a `Vec` of items where ids are owned `Strings`
    // for convenience, which can be kept until going through all data items, and then
    // passed to `ack_msg` together. Right now, we're reading a single messages via `recv`
    // anyway, but at some point we might want to get more in a single shot if talking
    // to the backing channel becomes a bottleneck.
    async fn ack_msg(
        &mut self,
        stream_key: &'static str,
        ids: &[String],
    ) -> Result<(), MessengerError>;
}

#[async_trait]
pub trait MessageStreamer: Sync + Send {
    fn messenger_type(&self) -> MessengerType;
    async fn add_stream(&mut self, stream_key: &'static str) -> Result<(), MessengerError>;
    async fn set_buffer_size(&mut self, stream_key: &'static str, max_buffer_size: usize);
    async fn send(&mut self, stream_key: &'static str, bytes: &[u8]) -> Result<(), MessengerError>;
}

pub async fn select_messenger_read(
    config: MessengerConfig,
) -> Result<Box<dyn Messenger>, MessengerError> {
    match config.messenger_type {
        MessengerType::Redis => {
            RedisMessenger::new(config).await.map(|a| Box::new(a) as Box<dyn Messenger>)
        }
        _ => Err(MessengerError::ConfigurationError {
            msg: "This Messenger type is not valid or not unimplemented.".to_string()
        })
    }
}

pub async fn select_messenger_stream(
    config: MessengerConfig,
) -> Result<Box<dyn MessageStreamer>, MessengerError> {
    match config.messenger_type {
        MessengerType::Redis => {
            RedisMessenger::new(config).await.map(|a| Box::new(a) as Box<dyn MessageStreamer>)
        }
        MessengerType::RedisPool => {
            RedisPoolMessenger::new(config).await.map(|a| Box::new(a) as Box<dyn MessageStreamer>)
        }
        _ => Err(MessengerError::ConfigurationError {
            msg: "This Messenger type is not valid, unimplemented or you don't have the right crate features on.".to_string()
        })
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum MessengerType {
    // Connect to one Redis instance
    Redis,
    // Connect to few different Redis instances
    // Not a cluster
    RedisPool,
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
