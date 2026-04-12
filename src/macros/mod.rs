#[macro_export]
macro_rules! impl_from_variant {
    // Matches the enum name, followed by a comma-separated list of variants
    ($enum_name:ident, $($variant:ident),* $(,)?) => {
        $(
            impl From<$variant> for $enum_name {
                fn from(val: $variant) -> Self {
                    $enum_name::$variant(val)
                }
            }
        )*
    }
}
