use crate::{
    error::MessengerError, redis_messenger::RedisMessengerStream, MessageStreamer, MessengerConfig,
    MessengerType, DATA_KEY,
};
use async_trait::async_trait;

use log::*;
use redis::{aio::ConnectionManager, streams::StreamMaxlen, AsyncCommands, RedisResult};

use std::{
    collections::{HashMap, LinkedList},
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

use super::{
    GROUP_NAME, MESSAGE_WAIT_TIMEOUT, PIPELINE_MAX_TIME, PIPELINE_SIZE_BYTES, REDIS_CON_STR,
};

/// A Redis Messenger capable of streaming data to multiple separate Redis instances.
pub struct RedisPoolMessenger {
    connections_pool: Vec<ConnectionManager>,
    streams: HashMap<&'static str, RedisMessengerStream>,
    _message_wait_timeout: usize,
    consumer_group_name: String,
    pipeline_size: usize,
    pipeline_max_time: u64,
}

impl RedisPoolMessenger {
    pub async fn new(config: MessengerConfig) -> Result<Self, MessengerError> {
        let uris = config
            .get(REDIS_CON_STR)
            .and_then(|u| u.clone().into_array())
            .ok_or(MessengerError::ConfigurationError {
                msg: format!("Connection String Missing: {}", REDIS_CON_STR),
            })?;

        let mut connections_pool = vec![];

        for uri in uris {
            // Setup Redis client.
            let client = redis::Client::open(uri.into_string().ok_or(
                MessengerError::ConfigurationError {
                    msg: format!("Connection String Missing: {}", REDIS_CON_STR),
                },
            )?)
            .unwrap();

            // Get connection.
            connections_pool.push(client.get_connection_manager().await.map_err(|e| {
                error!("{}", e.to_string());
                MessengerError::ConnectionError { msg: e.to_string() }
            })?);
        }

        let message_wait_timeout = config
            .get("message_wait_timeout")
            .and_then(|r| r.clone().to_u128().map(|n| n as usize))
            .unwrap_or(MESSAGE_WAIT_TIMEOUT);

        let consumer_group_name = config
            .get("consumer_group_name")
            .and_then(|r| r.clone().into_string())
            .unwrap_or_else(|| GROUP_NAME.to_string());

        let pipeline_size = config
            .get("pipeline_size_bytes")
            .and_then(|r| r.clone().to_u128().map(|n| n as usize))
            .unwrap_or(PIPELINE_SIZE_BYTES);

        let pipeline_max_time = config
            .get("local_buffer_max_window")
            .and_then(|r| r.clone().to_u128().map(|n| n as u64))
            .unwrap_or(PIPELINE_MAX_TIME);

        Ok(Self {
            connections_pool,
            streams: HashMap::<&'static str, RedisMessengerStream>::default(),
            _message_wait_timeout: message_wait_timeout,
            consumer_group_name,
            pipeline_size,
            pipeline_max_time,
        })
    }
}

#[async_trait]
impl MessageStreamer for RedisPoolMessenger {
    fn messenger_type(&self) -> MessengerType {
        MessengerType::RedisPool
    }

    async fn add_stream(&mut self, stream_key: &'static str) -> Result<(), MessengerError> {
        // Add to streams hashmap.
        let _result = self.streams.insert(
            stream_key,
            RedisMessengerStream {
                max_len: None,
                local_buffer: LinkedList::new(),
                local_buffer_total: 0,
                local_buffer_last_flush: Instant::now(),
            },
        );

        for connection in &mut self.connections_pool {
            // Add stream to Redis.
            let result: RedisResult<()> = connection
                .xgroup_create_mkstream(stream_key, self.consumer_group_name.as_str(), "$")
                .await;

            if let Err(e) = result {
                info!("Group already exists: {:?}", e)
            }
        }

        Ok(())
    }

    async fn set_buffer_size(&mut self, stream_key: &'static str, max_buffer_size: usize) {
        // Set max length for the stream.
        if let Some(stream) = self.streams.get_mut(stream_key) {
            stream.max_len = Some(StreamMaxlen::Approx(max_buffer_size));
        } else {
            error!("Stream key {stream_key} not configured");
        }
    }

    async fn send(&mut self, stream_key: &'static str, bytes: &[u8]) -> Result<(), MessengerError> {
        // Check if stream is configured.
        let stream = if let Some(stream) = self.streams.get_mut(stream_key) {
            stream
        } else {
            error!("Cannot send data for stream key {stream_key}, it is not configured");
            return Ok(());
        };

        // Get max length for the stream.
        let maxlen = if let Some(maxlen) = stream.max_len {
            maxlen
        } else {
            error!("Cannot send data for stream key {stream_key}, buffer size not set.");
            return Ok(());
        };
        stream.local_buffer.push_back(bytes.to_vec());
        stream.local_buffer_total += bytes.len();
        // Put serialized data into Redis.
        if stream.local_buffer_total < self.pipeline_size
            && stream.local_buffer_last_flush.elapsed()
                <= Duration::from_millis(self.pipeline_max_time as u64)
        {
            debug!(
                "Redis local buffer bytes {} and message pipeline size {} elapsed time {}ms",
                stream.local_buffer_total,
                stream.local_buffer.len(),
                stream.local_buffer_last_flush.elapsed().as_millis()
            );
            return Ok(());
        } else {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for bytes in stream.local_buffer.iter() {
                pipe.xadd_maxlen(stream_key, maxlen, "*", &[(DATA_KEY, &bytes)]);
            }

            let mut tasks = JoinSet::new();

            for connection in &self.connections_pool {
                let mut connection = connection.clone();
                let pipe = pipe.clone();
                tasks.spawn(async move {
                    let result: Result<Vec<String>, redis::RedisError> =
                        pipe.query_async(&mut connection).await;
                    if let Err(e) = result {
                        error!("Redis send error: {e}");
                        return Err(MessengerError::SendError { msg: e.to_string() });
                    }

                    Ok(())
                });
            }

            while let Some(task) = tasks.join_next().await {
                match task {
                    Ok(_) => {
                        debug!("One of the message send tasks was finished")
                    }
                    Err(err) if err.is_panic() => {
                        let msg = err.to_string();
                        error!("Task panic during sending message to Redis: {:?}", err);
                        return Err(MessengerError::SendError { msg });
                    }
                    Err(err) => {
                        let msg = err.to_string();
                        return Err(MessengerError::SendError { msg });
                    }
                }
            }

            debug!("Data Sent to {}", stream_key);
            stream.local_buffer.clear();
            stream.local_buffer_total = 0;
            stream.local_buffer_last_flush = Instant::now();
        }
        Ok(())
    }
}
