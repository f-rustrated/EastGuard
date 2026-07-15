#[macro_export]
macro_rules! impl_from_variant {
    // Internal: variant name = type name
    (@single $enum_name:ident, $variant:ident) => {
        impl From<$variant> for $enum_name {
            fn from(val: $variant) -> Self {
                $enum_name::$variant(val)
            }
        }
    };
    // Internal: variant name differs from type (e.g., Timer(TimerCommand<T>))
    (@single $enum_name:ident, $variant:ident, $type:ty) => {
        impl From<$type> for $enum_name {
            fn from(val: $type) -> Self {
                $enum_name::$variant(val)
            }
        }
    };
    // Entry: comma-separated list of VariantName or VariantName(Type)
    ($enum_name:ident, $($variant:ident $(($type:ty))?),* $(,)?) => {
        $(
            impl_from_variant!(@single $enum_name, $variant $(, $type)?);
        )*
    };
}

#[macro_export]
macro_rules! impl_new_struct_wrapper {
    ($wrapper_name:ident,$type:ty) => {
        impl From<$type> for $wrapper_name {
            fn from(val: $type) -> Self {
                $wrapper_name(val)
            }
        }

        $crate::smart_pointer!($wrapper_name, $type);
    };
}

/// Generates transitive `From` impls: `InnerType → MiddleEnum → OuterEnum`.
/// Requires `From<InnerType> for MiddleEnum` (via `impl_from_variant!`)
/// and `From<MiddleEnum> for OuterEnum` to already exist.
#[macro_export]
macro_rules! impl_from_variant_via {
    ($outer:ty, $middle:ty, $($inner:ty),* $(,)?) => {
        $(
            impl From<$inner> for $outer {
                fn from(val: $inner) -> Self {
                    let mid: $middle = val.into();
                    mid.into()
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! smart_pointer {
    // Arm 1: For generic tuple structs like `struct Wrapper<T>(T);`
    ($name:ident) => {
        impl<T> std::ops::Deref for $name<T> {
            type Target = T;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<T> std::ops::DerefMut for $name<T> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };

    // Arm 2: For non-generic tuple structs like `struct Wrapper(String);`
    ($name:ident, $target:ty) => {
        impl std::ops::Deref for $name {
            type Target = $target;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}
