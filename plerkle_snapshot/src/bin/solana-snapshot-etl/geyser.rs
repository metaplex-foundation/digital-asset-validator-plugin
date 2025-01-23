// TODO add multi-threading

use figment::value::{Map, Tag};
use indicatif::{ProgressBar, ProgressStyle};
use plerkle_messenger::redis_messenger::RedisMessenger;
use plerkle_messenger::{MessageStreamer, MessengerConfig};
use plerkle_serialization::serializer::serialize_account;
use plerkle_snapshot::append_vec::StoredMeta;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use solana_sdk::account::{Account, AccountSharedData};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;

const ACCOUNT_STREAM_KEY: &str = "ACC";

#[derive(Clone)]
pub(crate) struct GeyserDumper {
    messenger: Arc<Mutex<RedisMessenger>>,
    throttle_nanos: u64,
    pub accounts_spinner: ProgressBar,
    pub accounts_count: Arc<AtomicU64>,
}

impl GeyserDumper {
    pub(crate) async fn new(throttle_nanos: u64) -> Self {
        // TODO dedup spinner definitions
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");

        let mut connection_config = Map::new();
        connection_config.insert(
            "redis_connection_str".to_owned(),
            figment::value::Value::String(
                Tag::default(),
                std::env::var("SNAPSHOT_REDIS_CONNECTION_STR").expect(
                    "SNAPSHOT_REDIS_CONNECTION_STR must be present for snapshot processing",
                ),
            ),
        );
        let mut messenger = RedisMessenger::new(MessengerConfig {
            messenger_type: plerkle_messenger::MessengerType::Redis,
            connection_config,
        })
        .await
        .expect("create redis messenger");
        messenger
            .add_stream(ACCOUNT_STREAM_KEY)
            .await
            .expect("configure accounts stream");
        messenger
            .set_buffer_size(ACCOUNT_STREAM_KEY, 100_000_000)
            .await;

        Self {
            messenger: Arc::new(Mutex::new(messenger)),
            accounts_spinner,
            accounts_count: Arc::new(AtomicU64::new(0)),
            throttle_nanos,
        }
    }

    pub async fn dump_account(
        &mut self,
        (meta, account): (StoredMeta, AccountSharedData),
        slot: u64,
    ) -> Result<(), Box<dyn Error>> {
        let account: Account = account.into();
        // Get runtime and sender channel.
        // Serialize data.
        let ai = &ReplicaAccountInfo {
            pubkey: meta.pubkey.as_ref(),
            lamports: account.lamports,
            owner: account.owner.as_ref(),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: &account.data,
            write_version: meta.write_version,
        };
        let account =
            plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaAccountInfoV2 {
                pubkey: ai.pubkey,
                lamports: ai.lamports,
                owner: ai.owner,
                executable: ai.executable,
                rent_epoch: ai.rent_epoch,
                data: ai.data,
                write_version: ai.write_version,
                txn_signature: None,
            };
        let builder = flatbuffers::FlatBufferBuilder::new();
        let builder = serialize_account(builder, &account, slot, false);
        let data = builder.finished_data();

        self.messenger
            .lock()
            .await
            .send(ACCOUNT_STREAM_KEY, data)
            .await?;
        let prev = self
            .accounts_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.accounts_spinner.set_position(prev + 1);

        if self.throttle_nanos > 0 {
            tokio::time::sleep(std::time::Duration::from_nanos(self.throttle_nanos)).await;
        }
        Ok(())
    }

    pub async fn force_flush(self) {
        self.accounts_spinner.set_position(
            self.accounts_count
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        self.accounts_spinner
            .finish_with_message("Finished processing snapshot!");
        let messenger_mutex = Arc::into_inner(self.messenger)
            .expect("reference count to messenger to be 0 when forcing flush at the end");

        messenger_mutex
            .into_inner()
            .force_flush()
            .await
            .expect("force flush to succeed");
    }
}
