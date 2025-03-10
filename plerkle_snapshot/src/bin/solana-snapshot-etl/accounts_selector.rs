//! Copied from plerkle/src/accounts_selector.rs with some API-changing improvements.

use std::collections::HashSet;

use tracing::*;

const fn select_all_accounts_by_default() -> bool {
    true
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct AccountsSelectorConfig {
    #[serde(default)]
    pub accounts: Vec<String>,
    #[serde(default)]
    pub owners: Vec<String>,
    #[serde(default = "select_all_accounts_by_default")]
    pub select_all_accounts: bool,
}

impl Default for AccountsSelectorConfig {
    fn default() -> Self {
        Self {
            accounts: vec![],
            owners: vec![],
            select_all_accounts: select_all_accounts_by_default(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct AccountsSelector {
    pub accounts: HashSet<Vec<u8>>,
    pub owners: HashSet<Vec<u8>>,
    pub select_all_accounts: bool,
}

impl AccountsSelector {
    pub fn new(config: AccountsSelectorConfig) -> Self {
        let AccountsSelectorConfig { accounts, owners, select_all_accounts } = config;
        info!("Creating AccountsSelector from accounts: {:?}, owners: {:?}", accounts, owners);

        let accounts = accounts.iter().map(|key| bs58::decode(key).into_vec().unwrap()).collect();
        let owners = owners.iter().map(|key| bs58::decode(key).into_vec().unwrap()).collect();
        AccountsSelector { accounts, owners, select_all_accounts }
    }

    pub fn is_account_selected(&self, account: &[u8], owner: &[u8]) -> bool {
        self.select_all_accounts || self.accounts.contains(account) || self.owners.contains(owner)
    }
}
