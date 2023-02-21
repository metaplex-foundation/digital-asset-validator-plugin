use cadence_macros::is_global_default_set;

pub fn safe_metric<F: Fn()>(f: F) {
    if is_global_default_set() {
        f()
    }
}

#[macro_export]
macro_rules! metric {
    {$($block:stmt;)*} => {

            if is_global_default_set() {
                $(
                    $block
                )*
            }

    };
}
