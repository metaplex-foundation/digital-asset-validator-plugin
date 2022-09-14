use crate::{
    accounts_selector::AccountsSelector,
    error::PlerkleError,
    metrics::safe_metric,
    serializer::{
        serialize_account, serialize_block, serialize_slot_status, serialize_transaction,
    },
    transaction_selector::TransactionSelector,
};
use cadence::{BufferedUdpMetricSink, QueuingMetricSink, StatsdClient};
use cadence_macros::*;
use figment::{providers::Env, Figment};
use flatbuffers::FlatBufferBuilder;
use log::*;
use plerkle_messenger::{
    select_messenger, Messenger, MessengerConfig, ACCOUNT_STREAM, BLOCK_STREAM, SLOT_STREAM,
    TRANSACTION_STREAM,
};
use serde::Deserialize;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoV2, ReplicaAccountInfoVersions,
    ReplicaBlockInfoVersions, ReplicaTransactionInfoV2,
    ReplicaTransactionInfoVersions, Result, SlotStatus,
};
use solana_sdk::{message::AccountKeys, pubkey::Pubkey};
use std::{
    fmt::{Debug, Formatter},
    fs::File,
    io::Read,
    net::UdpSocket,
};
use tokio::{
    self as tokio,
    runtime::{Builder, Runtime},
    sync::mpsc::{self as mpsc, Sender},
    time::Instant,
};

struct SerializedData<'a> {
    stream: &'static str,
    builder: FlatBufferBuilder<'a>,
}

#[derive(Default)]
pub(crate) struct Plerkle<'a> {
    runtime: Option<Runtime>,
    accounts_selector: Option<AccountsSelector>,
    transaction_selector: Option<TransactionSelector>,
    sender: Option<Sender<SerializedData<'a>>>,
    started_at: Option<Instant>,
}

#[derive(Deserialize, PartialEq, Debug)]
pub struct PluginConfig {
    pub messenger_config: MessengerConfig,
    pub config_reload_ttl: Option<i64>,
}

impl<'a> Plerkle<'a> {
    pub fn new() -> Self {
        Self::default()
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

                if config["enable_metrics"].as_bool().unwrap_or(false) {
                    let uri = config["metrics_uri"].as_str().unwrap().to_string();
                    let port = config["metrics_port"].as_u64().unwrap() as u16;
                    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                    socket.set_nonblocking(true).unwrap();

                    let host = (uri, port);
                    let udp_sink = BufferedUdpMetricSink::from(host, socket).unwrap();
                    let queuing_sink = QueuingMetricSink::from(udp_sink);
                    let client = StatsdClient::from_sink("plerkle", queuing_sink);
                    set_global_default(client);
                    statsd_count!("plugin.startup", 1);
                }
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

        let (sender, mut receiver) = mpsc::channel::<SerializedData>(32);
        self.sender = Some(sender);
        let config: PluginConfig = Figment::new()
            .join(Env::prefixed("PLUGIN_"))
            .extract()
            .map_err(|config_error| GeyserPluginError::ConfigFileReadError {
                msg: format!("Could not read messenger config: {:?}", config_error),
            })?;
        runtime.spawn(async move {
            // Create new Messenger connection.
            if let Ok(mut messenger) = select_messenger(config.messenger_config).await {
                messenger.add_stream(ACCOUNT_STREAM).await;
                messenger.add_stream(SLOT_STREAM).await;
                messenger.add_stream(TRANSACTION_STREAM).await;
                messenger.add_stream(BLOCK_STREAM).await;
                messenger.set_buffer_size(ACCOUNT_STREAM, 5000).await;
                messenger.set_buffer_size(SLOT_STREAM, 5000).await;
                messenger.set_buffer_size(TRANSACTION_STREAM, 500000).await;
                messenger.set_buffer_size(BLOCK_STREAM, 5000).await;

                // Receive messages in a loop as long as at least one Sender is in scope.
                while let Some(data) = receiver.recv().await {
                    let bytes = data.builder.finished_data();
                    let _ = messenger.send(data.stream, bytes).await;
                }
            }
        });

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
        let acct: ReplicaAccountInfoV2;
        let account = match account {
            ReplicaAccountInfoVersions::V0_0_2(a) => a,
            ReplicaAccountInfoVersions::V0_0_1(a) => {
                acct = ReplicaAccountInfoV2 {
                    pubkey: a.pubkey,
                    lamports: a.lamports,
                    owner: a.owner,
                    executable: a.executable,
                    rent_epoch: a.rent_epoch,
                    data: a.data,
                    write_version: a.write_version,
                    txn_signature: None,
                };
                &acct
            }
        };

        if let Some(accounts_selector) = &self.accounts_selector {
            if !accounts_selector.is_account_selected(account.pubkey, account.owner) {
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
        let builder = serialize_account(builder, &account, slot, is_startup);
        let owner = bs58::encode(account.owner).into_string();
        // Send account info over channel.
        runtime.spawn(async move {
            let data = SerializedData {
                stream: ACCOUNT_STREAM,
                builder,
            };
            let _ = sender.send(data).await;
        });
        safe_metric(|| {
            statsd_count!("account_seen_event", 1, "owner" => &owner);
        });

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
        // Get runtime and sender channel.
        let runtime = self.get_runtime()?;
        let sender = self.get_sender_clone()?;

        // Serialize data.
        let builder = FlatBufferBuilder::new();
        let builder = serialize_slot_status(builder, slot, parent, status);

        // Send slot status over channel.
        runtime.spawn(async move {
            let data = SerializedData {
                stream: SLOT_STREAM,
                builder,
            };
            let _ = sender.send(data).await;
        });

        Ok(())
    }

    fn notify_transaction(
        &mut self,
        transaction_info: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> solana_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        let rep: ReplicaTransactionInfoV2;
        let transaction_info = match transaction_info {
            ReplicaTransactionInfoVersions::V0_0_2(ti) => ti,
            ReplicaTransactionInfoVersions::V0_0_1(ti) => {
                rep = ReplicaTransactionInfoV2 {
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
            };
            let _ = sender.send(data).await;
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
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(block_info) => {
                // Get runtime and sender channel.
                let runtime = self.get_runtime()?;
                let sender = self.get_sender_clone()?;

                // Serialize data.
                let builder = FlatBufferBuilder::new();
                let builder = serialize_block(builder, block_info);

                // Send block info over channel.
                runtime.spawn(async move {
                    let data = SerializedData {
                        stream: BLOCK_STREAM,
                        builder,
                    };
                    let _ = sender.send(data).await;
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
