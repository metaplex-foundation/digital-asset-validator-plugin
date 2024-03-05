#[allow(unused_imports)]
mod account_info_generated;
#[allow(unused_imports)]
mod block_info_generated;
#[allow(unused_imports)]
mod common_generated;
#[allow(unused_imports)]
mod compiled_instruction_generated;
#[allow(unused_imports)]
mod slot_status_info_generated;
#[allow(unused_imports)]
mod transaction_info_generated;

pub mod deserializer;
pub mod error;
pub mod serializer;
pub use account_info_generated::*;
pub use block_info_generated::*;
pub use common_generated::*;
pub use compiled_instruction_generated::*;
pub use slot_status_info_generated::*;
pub use transaction_info_generated::*;

// ---- SHIMS
#[allow(unused_imports)]
pub mod solana_geyser_plugin_interface_shims;
