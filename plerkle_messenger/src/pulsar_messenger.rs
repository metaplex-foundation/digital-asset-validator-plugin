#![cfg(feature = "pulsar")]
use crate::{error::MessengerError, Messenger, MessengerConfig, MessengerType};
use async_mutex::Mutex;
use async_trait::async_trait;
use futures::TryStreamExt;
use log::*;
use metaplex_pulsar::{
    authentication::oauth2::{OAuth2Authentication, OAuth2Params},
    consumer::Message,
    Authentication, Consumer, Producer, Pulsar, TokioExecutor,
};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Default)]
pub struct PulsarMessenger {
    connection: Option<Pulsar<TokioExecutor>>,
    producers: HashMap<&'static str, Producer<TokioExecutor>>,
    pub consumers: HashMap<&'static str, Arc<Mutex<Consumer<Vec<u8>, TokioExecutor>>>>,
    max_buffer_size: HashMap<&'static str, usize>,
    pub acquired_messages: HashMap<&'static str, Vec<Message<Vec<u8>>>>,
    pub create_consumer_by_default: bool,
}

const PULSAR_CON_STR: &str = "pulsar_connection_str";
const PULSAR_AUTH_TOKEN: &str = "pulsar_auth_token";
const PULSAR_OAUTH_CREDENTIALS_URL: &str = "oauth_credentials_url";
const PULSAR_ISSUER_URL: &str = "issuer_url";
const PULSAR_AUDIENCE: &str = "audience";
const PULSAR_SCOPE: &str = "scope";
const PULSAR_CONSUMER_BY_DEFAULT: &str = "create_consumer_by_default";

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
            let authentication = Authentication {
                name: String::from("token"),
                data: token.clone().into_string().unwrap().into_bytes(),
            };
            builder = builder.with_auth(authentication);
        }

        if let Some(credentials) = config.get(&*PULSAR_OAUTH_CREDENTIALS_URL) {
            let issuer_url = config
                .get(&*PULSAR_ISSUER_URL)
                .and_then(|u| u.clone().into_string())
                .ok_or(MessengerError::ConfigurationError {
                    msg: format!("Issuer url Missing: {}", PULSAR_ISSUER_URL),
                })?;
            let audience = config
                .get(&*PULSAR_AUDIENCE)
                .map_or(None, |v| v.clone().into_string());
            let scope = config
                .get(&*PULSAR_SCOPE)
                .map_or(None, |v| v.clone().into_string());

            let oauth_params = OAuth2Params {
                issuer_url: issuer_url,
                credentials_url: credentials.clone().into_string().unwrap(),
                audience: audience,
                scope: scope,
            };

            let authentication = OAuth2Authentication::client_credentials(oauth_params);

            builder = builder.with_auth_provider(authentication);
        }

        let pulsar: Pulsar<_> = builder.build().await.unwrap();

        let create_consumer_by_default = config
            .get(&*PULSAR_CONSUMER_BY_DEFAULT)
            .and_then(|u| u.clone().to_bool())
            .or(Some(false))
            .unwrap();

        Ok(Self {
            connection: Some(pulsar),
            producers: HashMap::<&'static str, Producer<TokioExecutor>>::default(),
            consumers:
                HashMap::<&'static str, Arc<Mutex<Consumer<Vec<u8>, TokioExecutor>>>>::default(),
            max_buffer_size: HashMap::<&'static str, usize>::default(),
            acquired_messages: HashMap::<&'static str, Vec<Message<Vec<u8>>>>::default(),
            create_consumer_by_default,
        })
    }

    fn messenger_type(&self) -> MessengerType {
        MessengerType::Pulsar
    }

    /// Create new Producer for Pulsar topic
    async fn add_stream(&mut self, stream_key: &'static str) -> Result<(), MessengerError> {
        if self.producers.contains_key(stream_key) {
            info!("Stream {stream_key} already exists");
            return Ok(());
        }

        self.create_producer(stream_key).await?;

        if self.create_consumer_by_default {
            if self.consumers.contains_key(stream_key) {
                info!("Consumer for {stream_key} already exists");
                return Ok(());
            }

            self.create_consumer(stream_key).await?;
        }

        Ok(())
    }

    /// Set max buffer size for the stream
    async fn set_buffer_size(&mut self, stream_key: &'static str, max_buffer_size: usize) {
        if self.max_buffer_size.contains_key(stream_key) {
            error!("Max buffer size already set for {stream_key} topic");
            return;
        }

        self.max_buffer_size.insert(stream_key, max_buffer_size);
    }

    /// Send message to the Pulsar topic
    async fn send(&mut self, stream_key: &'static str, bytes: &[u8]) -> Result<(), MessengerError> {
        if let Some(max_buffer_size) = self.max_buffer_size.get(stream_key) {
            if bytes.len() > *max_buffer_size {
                error!("Cannot send data for topic {stream_key}, buffer size is exaggerated");
                return Err(MessengerError::SendError {
                    msg: String::from("Buffer size is exaggerated"),
                });
            }
        }

        // Check if topic is configured
        let producer = if let Some(producer) = self.producers.get_mut(stream_key) {
            producer
        } else {
            error!("Cannot send data for topic {stream_key}, it is not configured");
            return Err(MessengerError::SendError {
                msg: String::from(format!(
                    "Cannot send data for topic {:?}, it is not configured",
                    stream_key
                )),
            });
        };

        let result = producer
            .send(bytes)
            .await
            .map_err(|e| MessengerError::SendError {
                msg: String::from(format!(
                    "Error while sending message to the Pulsar: {:?}",
                    e
                )),
            })?
            .await;

        if let Err(e) = result {
            error!("Did not get message receipt: {e}");
            return Err(MessengerError::SendError { msg: e.to_string() });
        } else {
            info!("Data sent");
        }

        Ok(())
    }

    /// Receive message from the Pulsar topic
    async fn recv(
        &mut self,
        stream_key: &'static str,
    ) -> Result<Vec<(i64, &[u8])>, MessengerError> {
        // Check if consumer is exists
        let mut consumer = if let Some(consumer) = self.consumers.get(stream_key) {
            consumer.lock().await
        } else {
            self.create_consumer(stream_key).await?;
            self.consumers.get(stream_key).unwrap().lock().await
        };

        let next_message = consumer
            .try_next()
            .await
            .map_err(|e| MessengerError::ReceiveError { msg: e.to_string() })?;

        if let Some(msg) = next_message {
            if let Some(messages) = self.acquired_messages.get_mut(stream_key) {
                messages.push(msg);
            } else {
                self.acquired_messages.insert(stream_key, vec![msg]);
            }
            let data = self
                .acquired_messages
                .get(stream_key)
                .unwrap()
                .last()
                .unwrap()
                .payload
                .data
                .as_ref(); // safe to use unwrap because we just pushed data to the HashMap

            return Ok(vec![(0, data)]); // TODO: it is not universal data type
        } else {
            return Err(MessengerError::ReceiveError {
                msg: String::from("No data in requested topic found"),
            });
        }
    }
}

impl PulsarMessenger {
    /// Creates new Pulsar Producer and saves it to the HashMap
    async fn create_producer(&mut self, topic: &'static str) -> Result<(), MessengerError> {
        let producer = self
            .connection
            .as_mut()
            .unwrap()
            .producer()
            .with_topic(topic)
            .build()
            .await
            .map_err(|e| MessengerError::ConnectionError {
                msg: String::from(format!("Cannot create Pulsar producer: {:?}", e)),
            })?;

        self.producers.insert(topic, producer);

        Ok(())
    }

    /// Creates new Pulsar Consumer and saves it to the HashMap
    async fn create_consumer(&mut self, topic: &'static str) -> Result<(), MessengerError> {
        let consumer: Consumer<Vec<u8>, _> = self
            .connection
            .as_mut()
            .unwrap()
            .consumer()
            .with_topic(topic)
            .build()
            .await
            .map_err(|e| MessengerError::ConnectionError {
                msg: String::from(format!("Cannot create Pulsar consumer: {:?}", e)),
            })?;

        self.consumers.insert(topic, Arc::new(Mutex::new(consumer)));

        Ok(())
    }

    /// Acknowledge reading message from the Pulsar by removing it from the HashMap and calling Pulsar.ack()
    /// Should be called after `recv()` method
    pub async fn ack_message(&mut self, key: &str) -> Result<(), MessengerError> {
        let key_messages =
            self.acquired_messages
                .get_mut(key)
                .ok_or(MessengerError::ReceiveError {
                    msg: String::from("No message for requested key found"),
                })?;

        // Check if consumer is exists
        let mut consumer = if let Some(consumer) = self.consumers.get(key) {
            consumer.lock().await
        } else {
            error!("Cannot get data from topic {key}, consumer is not configured");
            return Err(MessengerError::ReceiveError {
                msg: String::from("Consumer for the requested topic wasn't created"),
            });
        };

        for data in key_messages.iter() {
            consumer
                .ack(data)
                .await
                .map_err(|e| MessengerError::ReceiveError { msg: e.to_string() })?;
        }

        self.acquired_messages.remove(key);

        Ok(())
    }
}

impl Debug for PulsarMessenger {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
