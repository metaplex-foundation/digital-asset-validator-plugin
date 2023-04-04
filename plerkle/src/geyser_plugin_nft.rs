use crate::{
    accounts_selector::AccountsSelector, config::init_logger, error::PlerkleError, metric,
    transaction_selector::TransactionSelector,
};
use cadence::{BufferedUdpMetricSink, QueuingMetricSink, StatsdClient};
use cadence_macros::*;
use dashmap::DashMap;
use figment::{providers::Env, Figment};
use flatbuffers::FlatBufferBuilder;
use plerkle_messenger::{
    select_messenger, MessengerConfig, ACCOUNT_STREAM, BLOCK_STREAM, SLOT_STREAM,
    TRANSACTION_STREAM,
};
use plerkle_serialization::serializer::{
    serialize_account, serialize_block, serialize_transaction,
};
use serde::Deserialize;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaTransactionInfoVersions, Result, SlotStatus,
};
use solana_sdk::{message::AccountKeys, pubkey::Pubkey, signature::Signature};
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
use tracing::{debug, error, info, trace, warn};

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
    sender: Option<UnboundedSender<SerializedData<'a>>>,
    started_at: Option<Instant>,
    handle_startup: bool,
    slots_seen: SlotStore,
    account_event_cache: Arc<DashMap<u64, DashMap<Pubkey, (u64, SerializedData<'a>)>>>,
    transaction_event_cache: Arc<DashMap<u64, DashMap<Signature, (u64, SerializedData<'a>)>>>,
    conf_level: Option<SlotStatus>,
    cache_accounts_by_slot: Option<bool>
}

#[derive(Deserialize, PartialEq, Debug)]
pub enum ConfirmationLevel {
    Processed,
    Rooted,
    Confirmed,
}

impl Into<SlotStatus> for ConfirmationLevel {
    fn into(self) -> SlotStatus {
        match self {
            ConfirmationLevel::Processed => SlotStatus::Processed,
            ConfirmationLevel::Rooted => SlotStatus::Rooted,
            ConfirmationLevel::Confirmed => SlotStatus::Confirmed,
        }
    }
}

#[derive(Deserialize, PartialEq, Debug)]
pub struct PluginConfig {
    pub messenger_config: MessengerConfig,
    pub num_workers: Option<usize>,
    pub config_reload_ttl: Option<i64>,
    pub confirmation_level: Option<ConfirmationLevel>,
    pub account_stream_size: Option<usize>,
    pub slot_stream_size: Option<usize>,
    pub transaction_stream_size: Option<usize>,
    pub block_stream_size: Option<usize>,
    pub cache_accounts_by_slot: Option<bool>
}

const NUM_WORKERS: usize = 5;

impl<'a> Plerkle<'a> {
    pub fn new() -> Self {
        init_logger();
        Plerkle {
            runtime: None,
            accounts_selector: None,
            transaction_selector: None,
            sender: None,
            started_at: None,
            handle_startup: false,
            slots_seen: SlotStore::new(),
            account_event_cache: Arc::new(DashMap::new()),
            transaction_event_cache: Arc::new(DashMap::new()),
            conf_level: None,
            cache_accounts_by_slot: None,
        }
    }

    fn send(
        sender: UnboundedSender<SerializedData<'static>>,
        runtime: &tokio::runtime::Runtime,
        data: SerializedData<'static>,
    ) -> Result<()> {
        // Send account info over channel.
        runtime.spawn(async move {
            let s = sender.send(data);
            match s {
                Ok(_) => {}
                Err(e) => {
                    metric! {
                        statsd_count!("plerkle.send_error", 1, "error" => "main_send_error");
                    }
                    error!("Error sending data: {}", e);
                }
            }
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

    fn get_sender_clone(&self) -> Result<UnboundedSender<SerializedData<'a>>> {
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

    fn get_confirmation_level(&self) -> SlotStatus {
        self.conf_level.unwrap_or(SlotStatus::Processed)
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

        let (sender, mut main_receiver) = unbounded_channel::<SerializedData>();
        let config: PluginConfig = Figment::new()
            .join(Env::prefixed("PLUGIN_"))
            .extract()
            .map_err(|config_error| GeyserPluginError::ConfigFileReadError {
                msg: format!("Could not read messenger config: {:?}", config_error),
            })?;
        self.conf_level = config.confirmation_level.map(|c| c.into());
        let workers_num = config.num_workers.unwrap_or(NUM_WORKERS);

        runtime.spawn(async move {
            let mut messenger_workers = Vec::with_capacity(workers_num);
            let mut worker_senders = Vec::with_capacity(workers_num);
            for _ in 0..workers_num {
                let (send, recv) = unbounded_channel::<SerializedData>();
                let mut msg = select_messenger(config.messenger_config.clone())
                    .await
                    .unwrap(); // We want to fail if the messenger is not configured correctly.

                msg.add_stream(ACCOUNT_STREAM).await;
                msg.add_stream(SLOT_STREAM).await;
                msg.add_stream(TRANSACTION_STREAM).await;
                msg.add_stream(BLOCK_STREAM).await;
                msg.set_buffer_size(ACCOUNT_STREAM, config.account_stream_size.unwrap_or(100_000_000)).await;
                msg.set_buffer_size(SLOT_STREAM, config.slot_stream_size.unwrap_or(100_000)).await;
                msg.set_buffer_size(TRANSACTION_STREAM, config.transaction_stream_size.unwrap_or(10_000_000)).await;
                msg.set_buffer_size(BLOCK_STREAM, config.block_stream_size.unwrap_or(100_000)).await;
                let chan_msg = (recv, msg);
                // Idempotent call to add streams.
                messenger_workers.push(chan_msg);
                worker_senders.push(send);
            }
            let mut tasks = Vec::new();
            for worker in messenger_workers.into_iter() {
                tasks.push(tokio::spawn(async move {
                    let (mut reciever, mut messenger) = worker;
                    while let Some(data) = reciever.recv().await {
                        let start = Instant::now();
                        let bytes = data.builder.finished_data();
                        metric! {
                            statsd_time!(
                                "message_send_queue_time",
                                data.seen_at.elapsed().as_millis() as u64,
                                "stream" => data.stream
                            );
                        };
                        let _ = messenger.send(data.stream, bytes).await;
                        metric! {
                            statsd_time!(
                                "message_send_latency",
                                start.elapsed().as_millis() as u64,
                                "stream" => data.stream
                            );
                        };
                    }
                }));
            }

            tasks.push(tokio::spawn(async move { 
                let mut last_idx = 0;
                while let Some(data) = main_receiver.recv().await {
                    let seen = data.seen_at.elapsed().as_millis() as u64;
                    let str = data.stream;
                    let s = worker_senders[last_idx].send(data);
                    match s {
                        Ok(_) => {
                            metric! {
                                statsd_time!(
                                    "message_worker_hop_time",
                                    seen,
                                    "stream" => str
                                );
                            }
                        }
                        Err(e) => {
                            metric! {
                                statsd_count!("plerkle.send_error", 1, "error" => "broadcast_send_error");
                            }
                            error!("Error sending data: {}", e);
                        }
                    }
                    last_idx = (last_idx + 1) % worker_senders.len();

                } 
            }));

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
            trace!(
                "account selected: pubkey: {:?}, owner: {:?}",
                account.pubkey,
                account.owner
            );
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
        metric! {
            let s = is_startup.to_string();
            statsd_count!("account_seen_event", 1, "owner" => &owner, "is_startup" => &s);
        };
        let data = SerializedData {
            stream: ACCOUNT_STREAM,
            builder,
            seen_at: seen,
        };
        let runtime = self.get_runtime()?;
        let sender = self.get_sender_clone()?;

        if is_startup || self.cache_accounts_by_slot.unwrap_or(false) {
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
        metric! {
            statsd_time!("startup.timer", self.started_at.unwrap().elapsed());
        }
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
        if status == self.get_confirmation_level() {
            // playing with this value here
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

            let tx_slot_map = self.transaction_event_cache.remove(&slot);
            if let Some((_, events)) = tx_slot_map {
                info!("Sending Transaction events for SLOT: {:?}", slot);
                for (_, event) in events.into_iter() {
                    info!("Sending Transction event for stream: {:?}", event.1.stream);
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
                let tl = self.transaction_event_cache.clone();
                self.get_runtime()?.spawn(async move {
                    for s in purgable {
                        cl.remove(&s);
                        tl.remove(&s);
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
        trace!(
            signature = transaction_info.signature.to_string(),
            "matched transaction"
        );
        // Get runtime and sender channel.
        let runtime = self.get_runtime()?;
        let sender = self.get_sender_clone()?;

        // Serialize data.
        let builder = FlatBufferBuilder::new();
        let builder = serialize_transaction(builder, transaction_info, slot);

        // Push transaction events to queue
        let signature = transaction_info.signature.clone();
        let cache = self.transaction_event_cache.get_mut(&slot);

        let index = transaction_info.index.try_into().unwrap_or(0);
        let data = SerializedData {
            stream: TRANSACTION_STREAM,
            builder,
            seen_at: seen.clone(),
        };
        if let Some(cache) = cache {
            if cache.contains_key(&signature) {
                // Don't really know if this makes sense, I don't actually think we can get a notification for the same transaction twice in the same slot
                cache.alter(&signature, |_, _| (index, data));
            } else {
                cache.insert(signature, (index, data));
            }
        } else {
            let pubkey_cache = DashMap::new();
            pubkey_cache.insert(signature, (index, data));
            self.transaction_event_cache.insert(slot, pubkey_cache);
        }

        metric! {
            let slt_idx = format!("{}-{}", slot, transaction_info.index);
            statsd_count!("transaction_seen_event", 1, "slot-idx" => &slt_idx);
        }
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
