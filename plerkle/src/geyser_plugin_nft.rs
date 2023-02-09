use crate::{
    accounts_selector::AccountsSelector, error::PlerkleError, metrics::safe_metric,
    transaction_selector::TransactionSelector,
};
use cadence::{BufferedUdpMetricSink, QueuingMetricSink, StatsdClient};
use cadence_macros::*;
use crossbeam::channel::{unbounded, Sender};
use dashmap::DashMap;
use figment::{providers::Env, Figment};
use flatbuffers::FlatBufferBuilder;
use log::*;
use plerkle_messenger::{
    select_messenger, MessengerConfig, ACCOUNT_STREAM, BLOCK_STREAM, SLOT_STREAM,
    TRANSACTION_STREAM,
};
use plerkle_serialization::serializer::{
    serialize_account, serialize_block, serialize_transaction,
};
use serde::Deserialize;

use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaTransactionInfoVersions, Result, SlotStatus,
};
use solana_sdk::{message::AccountKeys, pubkey::Pubkey};
use std::{
    collections::BTreeSet,
    fmt::{Debug, Formatter},
    fs::File,
    io::Read,
    net::UdpSocket,
    ops::Bound::Included,
    ops::RangeBounds,
    sync::Arc,
};
use tokio::{
    self as tokio,
    runtime::{Builder, Runtime},
    time::Instant,
};

struct SerializedData<'a> {
    stream: &'static str,
    builder: FlatBufferBuilder<'a>,
    seen_at: Instant,
}

#[derive(Default)]
pub struct SlotStore {
    parents: BTreeSet<u64>,
}
const SLOT_EXPIRY: u64 = 600 * 2;
impl SlotStore {
    pub fn new() -> Self {
        SlotStore {
            parents: BTreeSet::new(),
        }
    }

    pub fn has_children(&self, slot: u64) -> bool {
        self.parents.contains(&slot)
    }

    pub fn needs_purge(&self, current_slot: u64) -> Option<Vec<u64>> {
        if current_slot <= SLOT_EXPIRY {
            //just in case we do some testing
            return None;
        }

        let rng = self
            .parents
            .range((Included(0), Included(current_slot - SLOT_EXPIRY)))
            .cloned()
            .collect();
        Some(rng)
    }

    pub fn insert(&mut self, parent: u64) {
        self.parents.insert(parent);
    }

    pub fn remove(&mut self, slot: u64) {
        self.parents.remove(&slot);
    }

    pub fn remove_range(&mut self, range: impl RangeBounds<u64>) {
        self.parents.retain(|slot| range.contains(slot));
    }
}

#[derive(Default)]
pub(crate) struct Plerkle<'a> {
    runtime: Option<Runtime>,
    accounts_selector: Option<AccountsSelector>,
    transaction_selector: Option<TransactionSelector>,
    sender: Option<Sender<SerializedData<'a>>>,
    started_at: Option<Instant>,
    handle_startup: bool,
    slots_seen: SlotStore,
    account_event_cache: Arc<DashMap<u64, DashMap<Pubkey, (u64, SerializedData<'a>)>>>,
}

#[derive(Deserialize, PartialEq, Debug)]
pub struct PluginConfig {
    pub messenger_config: MessengerConfig,
    pub num_workers: Option<usize>,
    pub config_reload_ttl: Option<i64>,
}

const MSG_BUFFER_SIZE: usize = 10_000_000;
const NUM_WORKERS: usize = 3;

impl<'a> Plerkle<'a> {
    pub fn new() -> Self {
        Plerkle {
            runtime: None,
            accounts_selector: None,
            transaction_selector: None,
            sender: None,
            started_at: None,
            handle_startup: false,
            slots_seen: SlotStore::new(),
            account_event_cache: Arc::new(DashMap::new()),
        }
    }

    fn send(
        sender: Sender<SerializedData<'static>>,
        runtime: &tokio::runtime::Runtime,
        data: SerializedData<'static>,
    ) -> Result<()> {
        // Send account info over channel.
        runtime.spawn(async move {
            let _ = sender.send(data);
        });
        Ok(())
    }

    fn create_accounts_selector_from_config(config: &serde_json::Value) -> AccountsSelector {
        let accounts_selector = &config["accounts_selector"];

        if accounts_selector.is_null() {
            AccountsSelector::default()
        } else {
            let accounts = &accounts_selector["accounts"];
            let accounts: Vec<String> = if accounts.is_array() {
                accounts
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            let owners = &accounts_selector["owners"];
            let owners: Vec<String> = if owners.is_array() {
                owners
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            AccountsSelector::new(&accounts, &owners)
        }
    }

    fn create_transaction_selector_from_config(config: &serde_json::Value) -> TransactionSelector {
        let transaction_selector = &config["transaction_selector"];

        if transaction_selector.is_null() {
            info!("TRANSACTION SELECTOR IS BROKEN");
            TransactionSelector::default()
        } else {
            let accounts = &transaction_selector["mentions"];
            let accounts: Vec<String> = if accounts.is_array() {
                accounts
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            TransactionSelector::new(&accounts)
        }
    }

    fn get_runtime(&self) -> Result<&tokio::runtime::Runtime> {
        if let Some(runtime) = &self.runtime {
            Ok(runtime)
        } else {
            Err(GeyserPluginError::Custom(Box::new(
                PlerkleError::GeneralPluginConfigError {
                    msg: "No runtime contained in struct".to_string(),
                },
            )))
        }
    }

    fn get_sender_clone(&self) -> Result<Sender<SerializedData<'a>>> {
        if let Some(sender) = &self.sender {
            Ok(sender.clone())
        } else {
            Err(GeyserPluginError::Custom(Box::new(
                PlerkleError::GeneralPluginConfigError {
                    msg: "No Sender channel contained in struct".to_string(),
                },
            )))
        }
    }

    // Currently not used but may want later.
    pub fn _txn_contains_program<'b>(keys: AccountKeys, program: &Pubkey) -> bool {
        keys.iter()
            .find(|p| {
                let d = *p;
                d.eq(program)
            })
            .is_some()
    }
}

impl<'a> Debug for Plerkle<'a> {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for Plerkle<'static> {
    fn name(&self) -> &'static str {
        "Plerkle"
    }

    fn on_load(&mut self, config_file: &str) -> Result<()> {
        solana_logger::setup_with_default("info");

        // Read in config file.
        info!(
            "Loading plugin {:?} from config_file {:?}",
            self.name(),
            config_file
        );
        let mut file = File::open(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        self.started_at = Some(Instant::now());
        // Setup accounts and transaction selectors based on config file JSON.
        let result = serde_json::from_str(&contents);
        match result {
            Ok(config) => {
                self.accounts_selector = Some(Self::create_accounts_selector_from_config(&config));
                self.transaction_selector =
                    Some(Self::create_transaction_selector_from_config(&config));
                let env = config["env"].as_str().unwrap_or("dev");
                if config["enable_metrics"].as_bool().unwrap_or(false) {
                    let uri = config["metrics_uri"].as_str().unwrap().to_string();
                    let port = config["metrics_port"].as_u64().unwrap() as u16;
                    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                    socket.set_nonblocking(true).unwrap();

                    let host = (uri, port);
                    let udp_sink = BufferedUdpMetricSink::from(host, socket).unwrap();
                    let queuing_sink = QueuingMetricSink::from(udp_sink);
                    let builder = StatsdClient::builder("plerkle", queuing_sink);
                    let client = builder.with_tag("env", env).build();
                    set_global_default(client);
                    statsd_count!("plugin.startup", 1);
                }
                self.handle_startup = config["handle_startup"].as_bool().unwrap_or(false);
            }
            Err(err) => {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: format!("Could not read config file JSON: {:?}", err),
                });
            }
        }

        let runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("plerkle-runtime-worker")
            .build()
            .map_err(|err| GeyserPluginError::ConfigFileReadError {
                msg: format!("Could not create tokio runtime: {:?}", err),
            })?;

        let (sender, mut receiver) = unbounded::<SerializedData>();

        let config: PluginConfig = Figment::new()
            .join(Env::prefixed("PLUGIN_"))
            .extract()
            .map_err(|config_error| GeyserPluginError::ConfigFileReadError {
                msg: format!("Could not read messenger config: {:?}", config_error),
            })?;

        let workers_num = config.num_workers.unwrap_or(NUM_WORKERS);
        runtime.spawn(async move {
            let mut messenger_workers = Vec::new();
            for _ in 0..workers_num {
                let mut msg = select_messenger(config.messenger_config.clone())
                    .await
                    .unwrap(); // We want to fail if the messenger is not configured correctly.
                msg.add_stream(ACCOUNT_STREAM).await;
                msg.add_stream(SLOT_STREAM).await;
                msg.add_stream(TRANSACTION_STREAM).await;
                msg.add_stream(BLOCK_STREAM).await;
                msg.set_buffer_size(ACCOUNT_STREAM, 50_000_000).await;
                msg.set_buffer_size(SLOT_STREAM, 100_000).await;
                msg.set_buffer_size(TRANSACTION_STREAM, 10_000_000).await;
                msg.set_buffer_size(BLOCK_STREAM, 100_000).await;
                // Idempotent call to add streams.

                messenger_workers.push(msg);
            }

            for mut worker in messenger_workers.into_iter() {
                let receiver = receiver.clone();
                tokio::spawn(async move {
                    while let Ok(data) = receiver.recv() {
                        let start = Instant::now();
                        let bytes = data.builder.finished_data();
                        safe_metric(|| {
                            statsd_time!(
                                "message_send_queue_time",
                                data.seen_at.elapsed().as_millis() as u64,
                                "stream" => data.stream
                            );
                        });
                        let _ = worker.send(data.stream, bytes).await;
                        safe_metric(|| {
                            statsd_time!(
                                "message_send_latency",
                                start.elapsed().as_millis() as u64,
                                "stream" => data.stream
                            );
                        })
                    }
                });
            }
        });
        self.sender = Some(sender);
        self.runtime = Some(runtime);
        Ok(())
    }

    fn on_unload(&mut self) {
        info!("Unloading plugin: {:?}", self.name());
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> solana_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        if !self.handle_startup && is_startup {
            return Ok(());
        }
        let rep: plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaAccountInfoV2;
        let account = match account {
            ReplicaAccountInfoVersions::V0_0_2(ai) => {
                rep = plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaAccountInfoV2 {
                    pubkey: ai.pubkey,
                    lamports: ai.lamports,
                    owner: ai.owner,
                    executable: ai.executable,
                    rent_epoch: ai.rent_epoch,
                    data: ai.data,
                    write_version: ai.write_version,
                    txn_signature: ai.txn_signature,
                };
                &rep
            }
            ReplicaAccountInfoVersions::V0_0_1(ai) => {
                rep = plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaAccountInfoV2 {
                    pubkey: ai.pubkey,
                    lamports: ai.lamports,
                    owner: ai.owner,
                    executable: ai.executable,
                    rent_epoch: ai.rent_epoch,
                    data: ai.data,
                    write_version: ai.write_version,
                    txn_signature: None,
                };
                &rep
            }
        };
        if let Some(accounts_selector) = &self.accounts_selector {
            if !accounts_selector.is_account_selected(account.pubkey, account.owner) {
                return Ok(());
            }
        } else {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "Accounts selector not initialized".to_string(),
            });
        }
        let seen = Instant::now();
        // Get runtime and sender channel.
        // Serialize data.
        let builder = FlatBufferBuilder::new();
        let builder = serialize_account(builder, account, slot, is_startup);
        let owner = bs58::encode(account.owner).into_string();
        safe_metric(|| {
            let s = is_startup.to_string();
            statsd_count!("account_seen_event", 1, "owner" => &owner, "is_startup" => &s);
        });
        let data = SerializedData {
            stream: ACCOUNT_STREAM,
            builder,
            seen_at: seen,
        };
        let runtime = self.get_runtime()?;
        let sender = self.get_sender_clone()?;

        if is_startup {
            Plerkle::send(sender, runtime, data)?;
        } else {
            let account_key = Pubkey::new(account.pubkey);
            let cache = self.account_event_cache.get_mut(&slot);
            if let Some(cache) = cache {
                if cache.contains_key(&account_key) {
                    cache.alter(&account_key, |_, v| {
                        if account.write_version > v.0 {
                            return (account.write_version, data);
                        } else {
                            v
                        }
                    });
                } else {
                    cache.insert(account_key, (account.write_version, data));
                }
            } else {
                let pubkey_cache = DashMap::new();
                pubkey_cache.insert(account_key, (account.write_version, data));
                self.account_event_cache.insert(slot, pubkey_cache);
            }
        }

        Ok(())
    }

    fn notify_end_of_startup(
        &mut self,
    ) -> solana_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        safe_metric(|| {
            statsd_time!("startup.timer", self.started_at.unwrap().elapsed());
        });
        info!("END OF STARTUP");
        Ok(())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> solana_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        info!("Slot status update: {:?} {:?}", slot, status);
        if status == SlotStatus::Processed && parent.is_some() {
            self.slots_seen.insert(parent.unwrap());
        }
        if status == SlotStatus::Confirmed {
            let slot_map = self.account_event_cache.remove(&slot);
            if let Some((_, events)) = slot_map {
                info!("Sending Account events for SLOT: {:?}", slot);
                for (_, event) in events.into_iter() {
                    info!("Sending Account event for stream: {:?}", event.1.stream);
                    let sender = self.get_sender_clone()?;
                    let runtime = self.get_runtime()?;
                    Plerkle::send(sender, runtime, event.1)?;
                }
            }
            let seen = &mut self.slots_seen;
            let slots_to_purge = seen.needs_purge(slot);
            if let Some(purgable) = slots_to_purge {
                debug!("Purging slots: {:?}", purgable);
                for slot in &purgable {
                    seen.remove(*slot);
                }
                let cl = self.account_event_cache.clone();
                self.get_runtime()?.spawn(async move {
                    for s in purgable {
                        cl.remove(&s);
                    }
                });
            }
        }
        Ok(())
    }

    fn notify_transaction(
        &mut self,
        transaction_info: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> solana_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        let seen = Instant::now();
        let rep: plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaTransactionInfoV2;
        let transaction_info = match transaction_info {
            ReplicaTransactionInfoVersions::V0_0_2(ti) => {
                rep = plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaTransactionInfoV2 {
                    signature: ti.signature,
                    is_vote: ti.is_vote,
                    transaction: ti.transaction,
                    transaction_status_meta: ti.transaction_status_meta,
                    index: ti.index,
                };
                &rep
            }
            ReplicaTransactionInfoVersions::V0_0_1(ti) => {
                rep = plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaTransactionInfoV2 {
                    signature: ti.signature,
                    is_vote: ti.is_vote,
                    transaction: ti.transaction,
                    transaction_status_meta: ti.transaction_status_meta,
                    index: 0,
                };
                &rep
            }
        };

        if transaction_info.is_vote || transaction_info.transaction_status_meta.status.is_err() {
            return Ok(());
        }

        // Check if transaction was selected in config.
        if let Some(transaction_selector) = &self.transaction_selector {
            if !transaction_selector.is_transaction_selected(
                transaction_info.is_vote,
                Box::new(transaction_info.transaction.message().account_keys().iter()),
            ) {
                return Ok(());
            }
        } else {
            return Ok(());
        }
        // Get runtime and sender channel.
        let runtime = self.get_runtime()?;
        let sender = self.get_sender_clone()?;

        // Serialize data.
        let builder = FlatBufferBuilder::new();
        let builder = serialize_transaction(builder, transaction_info, slot);
        let slt_idx = format!("{}-{}", slot, transaction_info.index);
        // Send transaction info over channel.
        runtime.spawn(async move {
            let data = SerializedData {
                stream: TRANSACTION_STREAM,
                builder,
                seen_at: seen.clone(),
            };
            let _ = sender.send(data);
        });
        safe_metric(|| {
            statsd_count!("transaction_seen_event", 1, "slot-idx" => &slt_idx);
        });
        Ok(())
    }

    fn notify_block_metadata(
        &mut self,
        blockinfo: ReplicaBlockInfoVersions,
    ) -> solana_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        let seen = Instant::now();
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(block_info) => {
                // Get runtime and sender channel.
                let runtime = self.get_runtime()?;
                let sender = self.get_sender_clone()?;

                // Serialize data.
                let builder = FlatBufferBuilder::new();
                // Hope to remove this when coupling is not an issue.
                let block_info = plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaBlockInfoV2 {
                     parent_slot: 0,
                     parent_blockhash: "",
                     slot: block_info.slot,
                     blockhash: block_info.blockhash,
                     block_time: block_info.block_time,
                     block_height: block_info.block_height,
                     executed_transaction_count: 0,
                };

                let builder = serialize_block(builder, &block_info);

                // Send block info over channel.
                runtime.spawn(async move {
                    let data = SerializedData {
                        stream: BLOCK_STREAM,
                        builder,
                        seen_at: seen.clone(),
                    };
                    let _ = sender.send(data);
                });
            }
        }

        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        self.accounts_selector
            .as_ref()
            .map_or_else(|| false, |selector| selector.is_enabled())
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the GeyserPluginPostgres pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plerkle::new();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
