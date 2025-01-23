// TODO add multi-threading

use async_trait::async_trait;
use figment::value::Dict;
use indicatif::{ProgressBar, ProgressStyle};
use plerkle_messenger::redis_messenger::RedisMessenger;
use plerkle_messenger::{MessageStreamer, MessengerConfig};
use plerkle_serialization::serializer::serialize_account;
use plerkle_snapshot::append_vec::{AppendVec, StoredAccountMeta};
use plerkle_snapshot::append_vec_iter;
use plerkle_snapshot::parallel::{AppendVecConsumer, GenericResult};
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use std::error::Error;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

const ACCOUNT_STREAM_KEY: &str = "ACC";
const NUM_WORKERS: usize = 2000;

#[derive(Clone)]
pub(crate) struct GeyserDumper {
    worker_semaphore: Arc<tokio::sync::Semaphore>,
    messenger: Arc<tokio::sync::Mutex<RedisMessenger>>,
    throttle_nanos: u64,
    accounts_spinner: ProgressBar,
    accounts_count: Arc<AtomicU64>,
}

#[async_trait]
impl AppendVecConsumer for GeyserDumper {
    async fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        let slot = append_vec.get_slot();

        for account in append_vec_iter(Arc::new(append_vec)) {
            let permit = self
                .worker_semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("acquire semaphore");
            let account = account.access().unwrap();
            self.dump_account(account, slot, permit)
                .await
                .expect("failed to dump account");
        }
        Ok(())
    }
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

        let worker_semaphore = Arc::new(Semaphore::new(NUM_WORKERS));

        let mut connection_config = Dict::new();
        connection_config.insert(
            "redis_connection_str".to_owned(),
            figment::value::Value::from("redis://localhost:6379"),
        );
        let config = MessengerConfig {
            messenger_type: plerkle_messenger::MessengerType::Redis,
            connection_config,
        };
        let mut messenger = RedisMessenger::new(config)
            .await
            .expect("create redis messenger");
        messenger
            .add_stream(ACCOUNT_STREAM_KEY)
            .await
            .expect("configure accounts stream");
        messenger
            .set_buffer_size(ACCOUNT_STREAM_KEY, 100_000_000)
            .await;
        let messenger = Arc::new(Mutex::new(messenger));

        Self {
            worker_semaphore,
            messenger,
            accounts_spinner,
            accounts_count: Arc::new(AtomicU64::new(0)),
            throttle_nanos,
        }
    }

    pub(crate) async fn dump_account(
        &self,
        account: StoredAccountMeta<'_>,
        slot: u64,
        _permit: OwnedSemaphorePermit,
    ) -> Result<(), Box<dyn Error>> {
        // Get runtime and sender channel.
        // Serialize data.
        let ai = &ReplicaAccountInfo {
            pubkey: account.meta.pubkey.as_ref(),
            lamports: account.account_meta.lamports,
            owner: account.account_meta.owner.as_ref(),
            executable: account.account_meta.executable,
            rent_epoch: account.account_meta.rent_epoch,
            data: account.data,
            write_version: account.meta.write_version,
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
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if prev % 1024 == 0 {
            self.accounts_spinner.set_position(prev);
        }

        if self.throttle_nanos > 0 {
            std::thread::sleep(std::time::Duration::from_nanos(self.throttle_nanos));
        }
        Ok(())
    }
}
