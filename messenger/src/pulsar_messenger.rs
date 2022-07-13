#![cfg(feature = "pulsar")]

use std::fmt::format;
use {
    crate::{error::MessengerError, Messenger, MessengerConfig},
    async_trait::async_trait,
    log::*,
    pulsar::{
        message::proto, producer, Authentication, Consumer, DeserializeMessage,
        Error as PulsarError, Payload, Pulsar, PulsarBuilder, SerializeMessage, SubType,
        TokioExecutor,
    },
    solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    std::{
        collections::HashMap,
        fmt::{Debug, Formatter},
        pin::Pin,
    },
};

#[derive(Default)]
pub struct PulsarMessenger {
    connection: Pulsar<TokioExecutor>,
    topics: HashMap<&'static str, Producer<TokioExecutor>>,
}

const PULSAR_CON_STR: &str = "pulsar_connection_str";
const PULSAR_AUTH_TOKEN: &str = "pulsar_auth_token";

#[async_trait]
impl Messenger for PulsarMessenger {
    /// Create new Pulsar connection for future topics
    async fn new(config: MessengerConfig) -> Result<Self, MessengerError> {
        let uri = config
            .get(&*PULSAR_CON_STR)
            .and_then(|u| u.clone().into_string())
            .ok_or(MessengerError::ConfigurationError {
                msg: format!("Connection String Missing: {}", PULSAR_CON_STR),
        })?;

        let mut builder = Pulsar::builder(uri, TokioExecutor);

        if let Some(token) = config.get(&*PULSAR_AUTH_TOKEN) {
            builder = builder.with_auth(token.clone().into_string().unwrap());
        }

        let pulsar: Pulsar<_> = builder.build().await.unwrap();

        Ok(Self {
            connection: pulsar,
            topics: HashMap::<&'static str, Producer<TokioExecutor>>::default(),
        })
    }

    /// Create new Producer for Pulsar topic
    async fn add_stream(&mut self, stream_key: &'static str) {
        let mut producer = pulsar
            .producer()
            .with_topic(stream_key)
            .build()
            .await
            .unwrap();

        let result = self.topics.try_insert(stream_key, producer);

        if !result.is_ok() {
            error!("Stream {stream_key} already exists");
        }
    }

    // TODO
    async fn set_buffer_size(&mut self, stream_key: &'static str, max_buffer_size: usize) {
        todo!();
    }

    /// Send message to the Pulsar topic
    async fn send(&mut self, stream_key: &'static str, bytes: &[u8]) -> Result<(), MessengerError> {
        // Check if topic is configured
        let topic = if let Some(topic) = self.topic.get(stream_key) {
            topic
        } else {
            error!("Cannot send data for topic {stream_key}, it is not configured");
            return Ok(());
        };

        let result = topic
            .send(bytes)
            .await
            .unwrap()
            .await;
        
        if let Err(e) = result {
            error!("Pulsar send error: {e}");
            return Err(Messenger::SendError {msg: e.to_string()});
        } else {
            info!("Data sent");  // TODO: maybe change to debug!, not sure we need this info in logs
        }

        Ok(())
    }

    // TODO
    async fn recv(&mut self, stream_key: &'static str,) -> Result<Vec<(i64, &[u8])>, MessengerError> {
        todo!();
    }
}

impl Debug for PulsarMessenger {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
