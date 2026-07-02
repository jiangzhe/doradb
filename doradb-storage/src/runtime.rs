use futures::executor;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// One-shot cooperative yield future.
pub(crate) struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Block the current thread until `future` completes.
#[inline]
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    executor::block_on(future)
}

/// Return a future that yields back to the current executor once.
#[inline]
pub(crate) fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

#[cfg(test)]
mod tests {
    use super::{block_on, yield_now};
    use futures::task::noop_waker;
    use std::future::Future;
    use std::task::{Context, Poll};

    #[test]
    fn yield_now_yields_once() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(yield_now());

        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn block_on_drives_yield_now() {
        block_on(yield_now());
    }
}
