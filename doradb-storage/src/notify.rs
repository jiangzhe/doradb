use event_listener::{Event, Listener};
use smol::future::FutureExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct Signal {
    inner: Arc<SignalImpl>,
    notified: bool,
}

impl Signal {
    /// Create a new notify object which can be used to wait for
    /// notification.
    #[inline]
    pub fn new_notify(&self) -> Notify {
        let ver = self.inner.version.load(Ordering::Acquire);
        Notify {
            inner: Arc::clone(&self.inner),
            ver,
        }
    }

    /// Notify addtional number of waiters.
    #[inline]
    pub fn notify(&self, count: usize) {
        self.inner.version.fetch_add(1, Ordering::SeqCst);
        self.inner.event.notify(count);
    }

    /// Consume the signal and notify addtional number of waiters.
    #[inline]
    pub fn done(mut self, count: usize) {
        self.inner.version.fetch_add(1, Ordering::SeqCst);
        self.inner.event.notify(count);
        self.notified = true;
    }
}

impl Drop for Signal {
    #[inline]
    fn drop(&mut self) {
        if !self.notified {
            self.inner.version.fetch_add(1, Ordering::SeqCst);
            self.inner.event.notify(usize::MAX);
        }
    }
}

pub struct Notify {
    inner: Arc<SignalImpl>,
    ver: u64,
}

impl Notify {
    #[inline]
    pub fn wait(self) {
        loop {
            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                break;
            }
            let listener = self.inner.event.listen();

            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                break;
            }
            listener.wait();
        }
    }

    #[inline]
    pub async fn wait_async(self) {
        loop {
            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                break;
            }
            let listener = self.inner.event.listen();

            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                break;
            }
            listener.await;
        }
    }

    /// Wait for notification until timeout.
    /// Returns true if notification is received, false if timeout.
    #[inline]
    pub fn wait_timeout(self, dur: Duration) -> bool {
        let start = Instant::now();
        let deadline = start.checked_add(dur).unwrap();
        loop {
            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                return true;
            }
            let listener = self.inner.event.listen();

            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                return true;
            }
            if start.elapsed() >= dur {
                return false;
            }
            listener.wait_deadline(deadline);
        }
    }

    #[inline]
    pub async fn wait_timeout_async(self, dur: Duration) -> bool {
        let start = Instant::now();
        let deadline = start.checked_add(dur).unwrap();
        loop {
            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                return true;
            }
            let listener = self.inner.event.listen();

            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                return true;
            }
            if start.elapsed() >= dur {
                return false;
            }
            let timeout = async {
                smol::Timer::at(deadline).await;
            };
            listener.or(timeout).await;
        }
    }
}

#[derive(Default)]
struct SignalImpl {
    version: AtomicU64,
    event: Event,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_notify_and_wait() {
        let signal = Signal::default();
        let handle = {
            let notify = signal.new_notify();
            thread::spawn(move || {
                notify.wait();
            })
        };
        signal.notify(1);
        handle.join().unwrap();
    }

    #[test]
    fn test_notify_and_wait_async() {
        let signal = Signal::default();
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let notify = signal.new_notify();
                thread::spawn(move || {
                    smol::block_on(async {
                        notify.wait_async().await;
                    });
                })
            })
            .collect();
        signal.notify(1);
        signal.done(2);
        // Signal::set_and_notify(&signal, 2);
        for h in handles {
            h.join().unwrap();
        }
    }
}
