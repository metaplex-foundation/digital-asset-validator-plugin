// TODO add multi-threading

use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use figment::value::{Map, Tag};
use indicatif::{ProgressBar, ProgressStyle};
use plerkle_messenger::redis_messenger::RedisMessenger;
use plerkle_messenger::{MessageStreamer, MessengerConfig};
use plerkle_serialization::serializer::serialize_account;
use plerkle_snapshot::append_vec::StoredMeta;
use solana_sdk::account::{Account, AccountSharedData, ReadableAccount};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::accounts_selector::AccountsSelector;

const ACCOUNT_STREAM_KEY: &str = "ACC";
// the upper limit of accounts stream length for when the snapshot is in progress
const MAX_INTERMEDIATE_STREAM_LEN: u64 = 50_000_000;
// every PROCESSED_CHECKPOINT we check the stream length and reset the local stream_counter
const PROCESSED_CHECKPOINT: u64 = 20_000_000;

#[derive(Clone)]
pub(crate) struct GeyserDumper {
    messenger: Arc<Mutex<RedisMessenger>>,
    throttle_nanos: u64,
    accounts_selector: AccountsSelector,
    pub accounts_spinner: ProgressBar,
    /// how many accounts were processed in total during the snapshot run.
    pub accounts_count: u64,
    /// intermediate counter of accounts sent to regulate XLEN checks.
    /// the reason for a separate field is that we initialize it as the current
    /// stream length, which might be non-zero.
    pub stream_counter: u64,
}

impl GeyserDumper {
    pub(crate) async fn new(throttle_nanos: u64, accounts_selector: AccountsSelector) -> Self {
        // TODO dedup spinner definitions
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec} total={human_pos}",
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
        let initial_stream_len = messenger
            .stream_len(&ACCOUNT_STREAM_KEY)
            .await
            .expect("get initial stream len of accounts");

        Self {
            messenger: Arc::new(Mutex::new(messenger)),
            accounts_spinner,
            accounts_selector,
            accounts_count: 0,
            throttle_nanos,
            stream_counter: initial_stream_len,
        }
    }

    pub async fn dump_account(
        &mut self,
        (meta, account): (StoredMeta, AccountSharedData),
        slot: u64,
    ) -> Result<(), Box<dyn Error>> {
        if self.stream_counter >= PROCESSED_CHECKPOINT {
            loop {
                let stream_len = self
                    .messenger
                    .lock()
                    .await
                    .stream_len(ACCOUNT_STREAM_KEY)
                    .await?;
                if stream_len < MAX_INTERMEDIATE_STREAM_LEN {
                    self.stream_counter = 0;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
        if self
            .accounts_selector
            .is_account_selected(meta.pubkey.as_ref(), account.owner().as_ref())
        {
            let account: Account = account.into();
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
            self.stream_counter += 1;
        } else {
            tracing::trace!(?account, ?meta, "Account filtered out by accounts selector");
            return Ok(());
        }

        self.accounts_count += 1;
        self.accounts_spinner.set_position(self.accounts_count);

        if self.throttle_nanos > 0 {
            tokio::time::sleep(std::time::Duration::from_nanos(self.throttle_nanos)).await;
        }

        Ok(())
    }

    pub async fn force_flush(self) {
        self.accounts_spinner.set_position(self.accounts_count);
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
