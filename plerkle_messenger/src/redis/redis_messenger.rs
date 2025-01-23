use crate::{
    error::MessengerError, metric, ConsumptionType, MessageStreamer, Messenger, MessengerConfig,
    MessengerType, RecvData,
};
use async_trait::async_trait;

use cadence_macros::statsd_count;
use log::*;
use redis::{
    cmd,
    streams::{
        StreamId, StreamKey, StreamMaxlen, StreamPendingCountReply, StreamReadOptions,
        StreamReadReply,
    },
    Commands, Connection, RedisResult, Value,
};

use redis::streams::StreamRangeReply;
use std::{
    collections::{HashMap, LinkedList},
    fmt::{Debug, Formatter},
    time::{Duration, Instant},
};

use super::{
    DATA_KEY, DEFAULT_MSG_BATCH_SIZE, DEFAULT_RETRIES, GROUP_NAME, IDLE_TIMEOUT,
    MESSAGE_WAIT_TIMEOUT, PIPELINE_MAX_TIME, PIPELINE_SIZE_BYTES, REDIS_CON_STR,
};

pub struct RedisMessenger {
    connection: Connection,
    streams: HashMap<&'static str, RedisMessengerStream>,
    consumer_id: String,
    retries: usize,
    batch_size: usize,
    idle_timeout: usize,
    _message_wait_timeout: usize,
    consumer_group_name: String,
    pipeline_size: usize,
    pipeline_max_time: u64,
}

pub struct RedisMessengerStream {
    pub max_len: Option<StreamMaxlen>,
    pub local_buffer: LinkedList<Vec<u8>>,
    pub local_buffer_total: usize,
    pub local_buffer_last_flush: Instant,
}

impl RedisMessenger {
    pub async fn new(config: MessengerConfig) -> Result<Self, MessengerError> {
        let uri = config
            .get(REDIS_CON_STR)
            .and_then(|u| u.clone().into_string())
            .ok_or(MessengerError::ConfigurationError {
                msg: format!("Connection String Missing: {}", REDIS_CON_STR),
            })?;
        // Setup Redis client.
        let client = redis::Client::open(uri).unwrap();

        // Get connection.
        let connection = client.get_connection().map_err(|e| {
            error!("{}", e.to_string());
            MessengerError::ConnectionError { msg: e.to_string() }
        })?;

        let consumer_id = config
            .get("consumer_id")
            .and_then(|id| id.clone().into_string())
            // Using the previous default name when the configuration does not
            // specify any particular consumer_id.
            .unwrap_or_else(|| String::from("ingester"));

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
            connection,
            streams: HashMap::<&'static str, RedisMessengerStream>::default(),
            consumer_id,
            retries,
            batch_size,
            idle_timeout,
            _message_wait_timeout: message_wait_timeout,
            consumer_group_name,
            pipeline_size,
            pipeline_max_time,
        })
    }

    async fn xautoclaim(
        &mut self,
        stream_key: &'static str,
    ) -> Result<Vec<RecvData>, MessengerError> {
        let id = "0-0".to_owned();
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

        let result: (String, StreamRangeReply, Vec<String>) = xauto
            .query(&mut self.connection)
            .map_err(|e| MessengerError::AutoclaimError { msg: e.to_string() })?;

        let range_reply = result.1;
        if range_reply.ids.is_empty() {
            // We've reached the end of the PEL.
            return Ok(Vec::new());
        }

        let mut retained_ids = Vec::new();
        let f = range_reply.ids.first().unwrap();
        let l = range_reply.ids.last().unwrap();

        // We need to use `xpending_count` to get a `StreamPendingCountReply` which
        // // contains information about the number of times a message has been
        // // delivered.
        let pending_result: StreamPendingCountReply = self
            .connection
            .xpending_count(
                stream_key,
                self.consumer_group_name.clone(),
                &f.id.clone(),
                &l.id.clone(),
                range_reply.ids.len(),
            )
            .map_err(|e| {
                error!("Redis receive error: {e}");
                MessengerError::ReceiveError { msg: e.to_string() }
            })?;
        let mut pending = HashMap::new();
        let mut ack_list = Vec::new();
        let prs = pending_result.ids.into_iter();
        for pr in prs {
            pending.insert(pr.id.clone(), pr);
        }
        for sid in range_reply.ids {
            let StreamId { id, map } = sid;
            let info = if let Some(info) = pending.get(&id) {
                info
            } else {
                warn!("No pending info for ID {id}");
                continue;
            };
            let data = if let Some(data) = map.get(DATA_KEY) {
                data
            } else {
                info!("No Data was stored in Redis for ID {id}");
                continue;
            };
            // Get data from map.

            let bytes = match data {
                Value::BulkString(bytes) => bytes,
                _ => {
                    error!("Redis data for ID {id} in wrong format");
                    continue;
                }
            };

            if info.times_delivered > self.retries {
                metric! {
                    statsd_count!("plerkle.messenger.retries.exceeded", 1);
                }
                error!("Message has reached maximum retries {} for id", id);
                ack_list.push(id.clone());
                continue;
            }
            retained_ids.push(RecvData::new_retry(
                id,
                bytes.to_vec(),
                info.times_delivered,
            ));
        }
        if let Err(e) = self.ack_msg(stream_key, &ack_list).await {
            error!("Error acking pending messages: {}", e);
        }
        Ok(retained_ids)
    }

    pub fn force_flush(mut self) -> Result<(), MessengerError> {
        for (stream_key, mut stream) in self.streams.into_iter() {
            // Get max length for the stream.
            let maxlen = if let Some(maxlen) = stream.max_len {
                maxlen
            } else {
                error!("Cannot send data for stream key {stream_key}, buffer size not set.");
                return Ok(());
            };
            let mut pipe = redis::pipe();
            for bytes in stream.local_buffer.iter() {
                pipe.xadd_maxlen(stream_key, maxlen, "*", &[(DATA_KEY, &bytes)]);
            }
            let result: Result<Vec<String>, redis::RedisError> = pipe.query(&mut self.connection);
            if let Err(e) = result {
                error!("Redis send error: {e}");
                return Err(MessengerError::SendError { msg: e.to_string() });
            } else {
                debug!("Data Sent to {}", stream_key);
                stream.local_buffer.clear();
                stream.local_buffer_total = 0;
                stream.local_buffer_last_flush = Instant::now();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Messenger for RedisMessenger {
    fn messenger_type(&self) -> MessengerType {
        MessengerType::Redis
    }

    async fn stream_size(&mut self, stream_key: &'static str) -> Result<u64, MessengerError> {
        let result: RedisResult<u64> = self.connection.xlen(stream_key);
        match result {
            Ok(reply) => Ok(reply),
            Err(e) => Err(MessengerError::ConnectionError { msg: e.to_string() }),
        }
    }

    // is used only on client side
    // Geyser does not call this method
    async fn recv(
        &mut self,
        stream_key: &'static str,
        consumption_type: ConsumptionType,
    ) -> Result<Vec<RecvData>, MessengerError> {
        let mut data_vec = Vec::with_capacity(self.batch_size * 2);
        if consumption_type == ConsumptionType::New || consumption_type == ConsumptionType::All {
            let opts = StreamReadOptions::default()
                //.block(self.message_wait_timeout)
                .count(self.batch_size)
                .group(self.consumer_group_name.as_str(), self.consumer_id.as_str());

            // Read on stream key and save the reply. Log but do not return errors.
            let reply: StreamReadReply = self
                .connection
                .xread_options(&[stream_key], &[">"], &opts)
                .map_err(|e| {
                    error!("Redis receive error: {e}");
                    MessengerError::ReceiveError { msg: e.to_string() }
                })?;
            // Parse data in stream read reply and store in Vec to return to caller.
            for StreamKey { key: _, ids } in reply.keys.into_iter() {
                for StreamId { id, map } in ids {
                    // Get data from map.
                    let data = if let Some(data) = map.get(DATA_KEY) {
                        data
                    } else {
                        error!("No Data was stored in Redis for ID {id}");
                        continue;
                    };
                    let bytes = match data {
                        Value::BulkString(bytes) => bytes,
                        _ => {
                            error!("Redis data for ID {id} in wrong format");
                            continue;
                        }
                    };

                    data_vec.push(RecvData::new(id, bytes.to_vec()));
                }
            }
        }
        if consumption_type == ConsumptionType::Redeliver
            || consumption_type == ConsumptionType::All
        {
            let xauto_reply = self.xautoclaim(stream_key).await;
            match xauto_reply {
                Ok(reply) => {
                    let mut pending_messages = reply;
                    data_vec.append(&mut pending_messages);
                }
                Err(e) => {
                    error!("XPENDING ERROR {e}");
                }
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

        pipe.query(&mut self.connection)
            .map_err(|e| MessengerError::AckError { msg: e.to_string() })
    }
}

#[async_trait]
impl MessageStreamer for RedisMessenger {
    fn messenger_type(&self) -> MessengerType {
        MessengerType::Redis
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
        let result: RedisResult<()> = self.connection.xgroup_create_mkstream(
            stream_key,
            self.consumer_group_name.as_str(),
            "$",
        );

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

    fn send(&mut self, stream_key: &'static str, bytes: &[u8]) -> Result<(), MessengerError> {
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
            for bytes in stream.local_buffer.iter() {
                pipe.xadd_maxlen(stream_key, maxlen, "*", &[(DATA_KEY, &bytes)]);
            }
            let result: Result<Vec<String>, redis::RedisError> = pipe.query(&mut self.connection);
            if let Err(e) = result {
                error!("Redis send error: {e}");
                return Err(MessengerError::SendError { msg: e.to_string() });
            } else {
                debug!("Data Sent to {}", stream_key);
                stream.local_buffer.clear();
                stream.local_buffer_total = 0;
                stream.local_buffer_last_flush = Instant::now();
            }
        }
        Ok(())
    }
}

impl Debug for RedisMessenger {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
