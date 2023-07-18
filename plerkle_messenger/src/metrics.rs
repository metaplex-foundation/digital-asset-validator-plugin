#[macro_export]
macro_rules! metric {
    {$($block:stmt;)*} => {
            if cadence_macros::is_global_default_set() {
                $(
                    $block
                )*
            }
    };
}
