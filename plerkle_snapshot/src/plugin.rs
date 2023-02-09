
// Copyright 2022 Solana Foundation.
// Copyright 2022 Richard Patel (terorie).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Lifted from https://github.com/terorie/solana-snapshot-etl
use indicatif::{ProgressBar, ProgressStyle};
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaAccountInfoV2, ReplicaAccountInfoVersions,
};
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::parallel::{AppendVecConsumer, GenericResult};
use std::error::Error;
use std::rc::Rc;

use libloading::{Library, Symbol};
use std::path::{Path, PathBuf};

/// # Safety
///
/// This function loads the dynamically linked library specified in the config file.
///
/// Causes memory corruption/UB on mismatching rustc or Solana versions, or if you look at the wrong way.
pub unsafe fn load_plugin(
    config_file: &str,
) -> Result<Box<dyn GeyserPlugin>, Box<dyn std::error::Error>> {
    let config_path = PathBuf::from(config_file);

    let config_content = std::fs::read_to_string(config_file)?;
    let config: serde_json::Value = json5::from_str(&config_content)?;

    let libpath = config["libpath"]
        .as_str()
        .ok_or("Missing libpath param in Geyser config")?;
    let mut libpath = PathBuf::from(libpath);
    if libpath.is_relative() {
        let config_dir = config_path
            .parent()
            .expect("failed to resolve parent of Geyser config file");
        libpath = config_dir.join(libpath);
    }

    load_plugin_inner(&libpath, &config_path.to_string_lossy())
}

unsafe fn load_plugin_inner(
    libpath: &Path,
    config_file: &str,
) -> Result<Box<dyn GeyserPlugin>, Box<dyn std::error::Error>> {
    type PluginConstructor = unsafe fn() -> *mut dyn GeyserPlugin;
    // Load library and leak, as we never want to unload it.
    let lib = Box::leak(Box::new(Library::new(libpath)?));
    let constructor: Symbol<PluginConstructor> = lib.get(b"_create_plugin")?;
    // Unsafe call down to library.
    let plugin_raw = constructor();
    let mut plugin = Box::from_raw(plugin_raw);
    plugin.on_load(config_file)?;
    Ok(plugin)
}

pub(crate) struct GeyserDumper {
    accounts_spinner: ProgressBar,
    plugin: Box<dyn GeyserPlugin>,
    accounts_count: u64,
}

impl AppendVecConsumer for GeyserDumper {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            self.dump_account(account)?;
        }
        Ok(())
    }
}

impl GeyserDumper {
    pub(crate) fn new(plugin: Box<dyn GeyserPlugin>) -> Self {
        // TODO dedup spinner definitions
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");

        Self {
            accounts_spinner,
            plugin,
            accounts_count: 0,
        }
    }

    pub(crate) fn dump_account(
        &mut self,
        account: StoredAccountMeta,
    ) -> Result<(), Box<dyn Error>> {
        let slot = 0u64; // TODO fix slot number
        self.plugin.update_account(
            ReplicaAccountInfoVersions::V0_0_2(&ReplicaAccountInfoV2 {
                pubkey: account.meta.pubkey.as_ref(),
                lamports: account.account_meta.lamports,
                owner: account.account_meta.owner.as_ref(),
                executable: account.account_meta.executable,
                rent_epoch: account.account_meta.rent_epoch,
                data: account.data,
                write_version: account.meta.write_version,
                txn_signature: None,
            }),
            slot,
            /* is_startup */ false,
        )?;
        self.accounts_count += 1;
        if self.accounts_count % 1024 == 0 {
            self.accounts_spinner.set_position(self.accounts_count);
        }
        Ok(())
    }
}

impl Drop for GeyserDumper {
    fn drop(&mut self) {
        self.accounts_spinner.finish();
    }
}