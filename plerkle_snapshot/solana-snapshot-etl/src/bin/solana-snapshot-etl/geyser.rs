// TODO add multi-threading

use indicatif::{ProgressBar, ProgressStyle};
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaAccountInfo, ReplicaAccountInfoVersions,
};
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::parallel::{AppendVecConsumer, GenericResult};
use std::error::Error;
use std::rc::Rc;

pub(crate) struct GeyserDumper {
    throttle_nanos: u64,
    accounts_spinner: ProgressBar,
    plugin: Box<dyn GeyserPlugin>,
    accounts_count: u64,
}

impl AppendVecConsumer for GeyserDumper {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        let slot = append_vec.get_slot();

        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            self.dump_account(account, slot)?;
        }
        Ok(())
    }
}

impl GeyserDumper {
    pub(crate) fn new(plugin: Box<dyn GeyserPlugin>, throttle_nanos: u64) -> Self {
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
            throttle_nanos,
        }
    }

    pub(crate) fn dump_account(
        &mut self,
        account: StoredAccountMeta,
        slot: u64,
    ) -> Result<(), Box<dyn Error>> {
        self.plugin.update_account(
            ReplicaAccountInfoVersions::V0_0_1(&ReplicaAccountInfo {
                pubkey: account.meta.pubkey.as_ref(),
                lamports: account.account_meta.lamports,
                owner: account.account_meta.owner.as_ref(),
                executable: account.account_meta.executable,
                rent_epoch: account.account_meta.rent_epoch,
                data: account.data,
                write_version: account.meta.write_version,
            }),
            slot,
            /* is_startup */ false,
        )?;
        self.accounts_count += 1;
        if self.accounts_count % 1024 == 0 {
            self.accounts_spinner.set_position(self.accounts_count);
        }

        if self.throttle_nanos > 0 {
            std::thread::sleep(std::time::Duration::from_nanos(self.throttle_nanos));
        }
        Ok(())
    }
}

impl Drop for GeyserDumper {
    fn drop(&mut self) {
        self.accounts_spinner.finish();
    }
}
