use crate::{error::MessengerError, Messenger, MessengerConfig, MessengerType, RecvData};
use async_trait::async_trait;

use log::*;
use redis::{
    aio::ConnectionManager,
    cmd,
    streams::{
        StreamId, StreamKey, StreamMaxlen, StreamPendingCountReply, StreamReadOptions,
        StreamReadReply,
    },
    AsyncCommands, RedisResult, Value,
};

use redis::streams::StreamRangeReply;
use std::{
    collections::{HashMap, LinkedList},
    fmt::{Debug, Formatter},
    time::{Duration, Instant},
};

// Redis stream values.
pub const GROUP_NAME: &str = "plerkle";
pub const DATA_KEY: &str = "data";
pub const DEFAULT_RETRIES: usize = 3;
pub const DEFAULT_MSG_BATCH_SIZE: usize = 10;
pub const MESSAGE_WAIT_TIMEOUT: usize = 10;
pub const IDLE_TIMEOUT: usize = 5000;
pub const REDIS_MAX_BYTES_COMMAND: usize = 536870912;
pub const PIPELINE_SIZE_BYTES: usize = REDIS_MAX_BYTES_COMMAND / 100;
pub const PIPELINE_MAX_TIME: u64 = 1000;

pub struct RedisMessenger {
    connection: ConnectionManager,
    streams: HashMap<&'static str, RedisMessengerStream>,
    consumer_id: String,
    retries: usize,
    batch_size: usize,
    idle_timeout: usize,
    message_wait_timeout: usize,
    consumer_group_name: String,
    pipeline_size: usize,
    pipeline_max_time: u64,
}

pub struct RedisMessengerStream {
    max_len: Option<StreamMaxlen>,
    local_buffer: LinkedList<Vec<u8>>,
    local_buffer_total: usize,
    local_buffer_last_flush: Instant,
}

const REDIS_CON_STR: &str = "redis_connection_str";

impl RedisMessenger {
    async fn xautoclaim(
        &mut self,
        stream_key: &'static str,
    ) -> Result<Vec<RecvData>, MessengerError> {
        let mut id = "0-0".to_owned();
        // We need to call `XAUTOCLAIM` repeatedly because it will (according to the docs)
        // only look at up to 10 * `count` PEL entries each time, and `id` is used to
        // know where we left off to continue from next call.

        // The `redis` crate doesn't appear to support this command so we have
        // to call it via the lower level primitives it provides.
        let mut xauto = cmd("XAUTOCLAIM");
        xauto
            .arg(stream_key)
            .arg(self.consumer_group_name.clone())
            .arg(self.consumer_id.as_str())
            // We only reclaim items that have been idle for at least 2 sec.
            .arg(self.idle_timeout)
            .arg(id.as_str())
            .arg("COUNT")
            .arg(self.batch_size);

        // Before Redis 7 (we're using 6.2.x presently), `XAUTOCLAIM` returns an array of
        // two items: an id to be used for the next call to continue scanning the PEL,
        // and a list of successfully claimed messages in the same format as `XRANGE`.
        let result: (String, StreamRangeReply) = xauto
            .query_async(&mut self.connection)
            .await
            .map_err(|e| MessengerError::AutoclaimError { msg: e.to_string() })?;

        id = result.0;
        let range_reply = result.1;

        if id == "0-0" || range_reply.ids.is_empty() {
            // We've reached the end of the PEL.
            return Ok(Vec::new());
        }

        let mut retained_ids = Vec::new();

        // We need to use `xpending_count` to get a `StreamPendingCountReply` which
        // contains information about the number of times a message has been
        // delivered.

        for sid in range_reply.ids {
            let pending_result: RedisResult<StreamPendingCountReply> = self
                .connection
                .xpending_count(
                    stream_key,
                    self.consumer_group_name.clone(),
                    &sid.id,
                    &sid.id,
                    1,
                )
                .await;

            match pending_result {
                Ok(reply) => {
                    if reply.ids.is_empty() {
                        error!("Missing pending message information for id {}", id);
                    } else {
                        let info = reply.ids.first().unwrap();
                        let StreamId { id, map } = sid;
                        let data = if let Some(data) = map.get(DATA_KEY) {
                            data
                        } else {
                            println!("No Data was stored in Redis for ID {id}");
                            continue;
                        };
                        // Get data from map.

                        let bytes = match data {
                            Value::Data(bytes) => bytes,
                            _ => {
                                println!("Redis data for ID {id} in wrong format");
                                continue;
                            }
                        };

                        if info.times_delivered > self.retries {
                            self.ack_msg(stream_key, &[id.clone()]).await?;
                            error!("Message has reached maximum retries {} for id", id);
                            continue;
                        }
                        retained_ids.push(RecvData::new_retry(
                            id,
                            bytes.to_vec(),
                            info.times_delivered,
                        ));
                    }
                }
                Err(e) => error!("Redis xpending_count error {} for id {}", e, id),
            }
        }

        Ok(retained_ids)
    }
}

#[async_trait]
impl Messenger for RedisMessenger {
    //pub async fn new(stream_key: &'static str) -> Result<Self> {
    async fn new(config: MessengerConfig) -> Result<Self, MessengerError> {
        let uri = config
            .get(REDIS_CON_STR)
            .and_then(|u| u.clone().into_string())
            .ok_or(MessengerError::ConfigurationError {
                msg: format!("Connection String Missing: {}", REDIS_CON_STR),
            })?;
        // Setup Redis client.
        let client = redis::Client::open(uri).unwrap();

        // Get connection.
        let connection = client.get_tokio_connection_manager().await.map_err(|e| {
            error!("{}", e.to_string());
            MessengerError::ConnectionError { msg: e.to_string() }
        })?;

        let _cluster_mode = config
            .get("cluster_mode")
            .and_then(|r| r.clone().to_bool())
            .unwrap_or(false);

        let consumer_id = config
            .get("consumer_id")
            .and_then(|id| id.clone().into_string())
            // Using the previous default name when the configuration does not
            // specify any particular consumer_id.
            .unwrap_or(String::from("ingester"));

        let retries = config
            .get("retries")
            .and_then(|r| r.clone().to_u128().map(|n| n as usize))
            .unwrap_or(DEFAULT_RETRIES);

        let batch_size = config
            .get("batch_size")
            .and_then(|r| r.clone().to_u128().map(|n| n as usize))
            .unwrap_or(DEFAULT_MSG_BATCH_SIZE);

        let idle_timeout = config
            .get("idle_timeout")
            .and_then(|r| r.clone().to_u128().map(|n| n as usize))
            .unwrap_or(IDLE_TIMEOUT);
        let message_wait_timeout = config
            .get("message_wait_timeout")
            .and_then(|r| r.clone().to_u128().map(|n| n as usize))
            .unwrap_or(MESSAGE_WAIT_TIMEOUT);
        let consumer_group_name = config
            .get("consumer_group_name")
            .and_then(|r| r.clone().into_string())
            .unwrap_or(GROUP_NAME.to_string());

        let pipeline_size = config
            .get("pipeline_size_bytes")
            .and_then(|r| {
                r.clone()
                    .to_u128()
                    .map(|n| n as usize)
                    .min(Some(PIPELINE_SIZE_BYTES))
            })
            .unwrap_or(PIPELINE_SIZE_BYTES);

        let pipeline_max_time = config
            .get("local_buffer_max_window")
            .and_then(|r| r.clone().to_u128().map(|n| n as u64))
            .unwrap_or(PIPELINE_MAX_TIME);

        Ok(Self {
            connection,
            streams: HashMap::<&'static str, RedisMessengerStream>::default(),
            consumer_id,
            retries,
            batch_size,
            idle_timeout,
            message_wait_timeout,
            consumer_group_name,
            pipeline_size,
            pipeline_max_time,
        })
    }

    fn messenger_type(&self) -> MessengerType {
        MessengerType::Redis
    }

    async fn stream_size(&mut self, stream_key: &'static str) -> Result<u64, MessengerError> {
        let result: RedisResult<u64> = self.connection.xlen(stream_key).await;
        match result {
            Ok(reply) => Ok(reply),
            Err(e) => Err(MessengerError::ConnectionError { msg: e.to_string() }),
        }
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

        // Add stream to Redis.
        let result: RedisResult<()> = self
            .connection
            .xgroup_create_mkstream(stream_key, self.consumer_group_name.as_str(), "$")
            .await;

        if let Err(e) = result {
            info!("Group already exists: {:?}", e)
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
                "Redis local buffer bytes {} and message pipeline size {} ",
                stream.local_buffer_total,
                stream.local_buffer.len()
            );
            return Ok(());
        } else {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for bytes in stream.local_buffer.iter() {
                pipe.xadd_maxlen(stream_key, maxlen, "*", &[(DATA_KEY, &bytes)]);
            }
            let result: Result<Vec<String>, redis::RedisError> =
                pipe.query_async(&mut self.connection).await;
            if let Err(e) = result {
                error!("Redis send error: {e}");
                return Err(MessengerError::SendError { msg: e.to_string() });
            } else {
                info!("Data Sent to {}", stream_key);
                stream.local_buffer.clear();
                stream.local_buffer_total = 0;
                stream.local_buffer_last_flush = Instant::now();
            }
        }
        Ok(())
    }

    async fn recv(&mut self, stream_key: &'static str) -> Result<Vec<RecvData>, MessengerError> {
        let xauto_reply = self.xautoclaim(stream_key).await?;
        let mut pending_messages = xauto_reply;
        let opts = StreamReadOptions::default()
            .block(self.message_wait_timeout)
            .count(self.batch_size)
            .group(self.consumer_group_name.as_str(), self.consumer_id.as_str());

        // Read on stream key and save the reply. Log but do not return errors.
        let reply: StreamReadReply = self
            .connection
            .xread_options(&[stream_key], &[">"], &opts)
            .await
            .map_err(|e| {
                error!("Redis receive error: {e}");
                MessengerError::ReceiveError { msg: e.to_string() }
            })?;

        let mut data_vec = Vec::new();
        data_vec.append(&mut pending_messages);
        // Parse data in stream read reply and store in Vec to return to caller.
        for StreamKey { key: _, ids } in reply.keys.into_iter() {
            for StreamId { id, map } in ids {
                // Get data from map.
                let data = if let Some(data) = map.get(DATA_KEY) {
                    data
                } else {
                    println!("No Data was stored in Redis for ID {id}");
                    continue;
                };
                let bytes = match data {
                    Value::Data(bytes) => bytes,
                    _ => {
                        println!("Redis data for ID {id} in wrong format");
                        continue;
                    }
                };

                data_vec.push(RecvData::new(id.clone(), bytes.to_vec()));
            }
        }
        Ok(data_vec)
    }

    async fn ack_msg(
        &mut self,
        stream_key: &'static str,
        ids: &[String],
    ) -> Result<(), MessengerError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        pipe.xack(stream_key, self.consumer_group_name.as_str(), ids);
        pipe.xdel(stream_key, ids);

        pipe.query_async(&mut self.connection)
            .await
            .map_err(|e| MessengerError::AckError { msg: e.to_string() })
    }
}

impl Debug for RedisMessenger {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
