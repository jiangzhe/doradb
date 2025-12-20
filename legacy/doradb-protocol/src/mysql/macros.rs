//! This module defines macros used by mysql protocol.
macro_rules! impl_from_ref {
    ($name:ident : $src:ident: $($f1:ident),* ; $($f2:ident),*; $($f3:ident = $expr:expr),*) => {
        impl $name<'static> {
            #[inline]
            pub fn from_ref($src: &$name) -> Self {
                $name{
                    $(
                        $f1: $src.$f1,
                    )*
                    $(
                        $f2: std::borrow::Cow::Owned((&*$src.$f2).to_owned()),
                    )*
                    $(
                        $f3: $expr,
                    )*
                }
            }
        }
    };
    ($name:ident : $($f1:ident),* ; $($f2:ident),*) => {
        impl $name<'static> {
            #[inline]
            pub fn from_ref(src: &$name) -> Self {
                $name{
                    $(
                        $f1: src.$f1,
                    )*
                    $(
                        $f2: std::borrow::Cow::Owned((&*src.$f2).to_owned()),
                    )*
                }
            }
        }
    }
}
