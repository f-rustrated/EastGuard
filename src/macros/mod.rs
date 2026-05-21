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
