use futures::executor;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(crate) mod mandatory;
pub(crate) mod thread_pool;

/// Logical work items completed between cooperative executor yields.
pub(crate) const POLL_BUDGET: usize = 128;

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
    use futures::task::{ArcWake, waker_ref};
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    /// Purpose: Protect the cooperative yield future's polling boundary.
    /// Expected: Yielding schedules a wake and the next poll completes without another wake.
    #[test]
    fn yield_now_yields_once() {
        struct WakeCounter(AtomicUsize);

        impl ArcWake for WakeCounter {
            fn wake_by_ref(arc_self: &Arc<Self>) {
                arc_self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = waker_ref(&wakes);
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(yield_now());

        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    }

    /// Purpose: Exercise cooperative yielding through the blocking executor.
    /// Expected: A yielded future is resumed to completion.
    #[test]
    fn block_on_drives_yield_now() {
        block_on(yield_now());
    }
}
