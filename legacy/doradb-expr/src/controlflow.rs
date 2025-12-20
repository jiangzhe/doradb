pub use std::ops::ControlFlow;

/// Convert to ControlFlow
pub trait Branch<B, C> {
    fn branch(self) -> ControlFlow<B, C>;
}

impl<B, C> Branch<B, C> for Result<C, B> {
    #[inline]
    fn branch(self) -> ControlFlow<B, C> {
        match self {
            Ok(c) => ControlFlow::Continue(c),
            Err(b) => ControlFlow::Break(b),
        }
    }
}

/// Convert from ControlFlow
pub trait Unbranch {
    type R;
    fn unbranch(self) -> Self::R;
}

impl<B, C> Unbranch for ControlFlow<B, C> {
    type R = Result<C, B>;
    #[inline]
    fn unbranch(self) -> Self::R {
        match self {
            ControlFlow::Continue(c) => Ok(c),
            ControlFlow::Break(b) => Err(b),
        }
    }
}
