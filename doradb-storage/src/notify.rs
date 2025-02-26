use event_listener::{Event, Listener};
use smol::future::FutureExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct Signal(Arc<SignalImpl>);

impl Signal {
    /// Create a new notify object which can be used to wait for
    /// notification.
    #[inline]
    pub fn new_notify(&self, from_last_notify: bool) -> Notify {
        let ver = if from_last_notify {
            self.0.notified.load(Ordering::Acquire)
        } else {
            self.0.version.load(Ordering::Acquire)
        };
        Notify {
            inner: Arc::clone(&self.0),
            ver,
        }
    }

    /// Increase version and notify waiters of given number.
    #[inline]
    pub fn set_and_notify(this: &Self, count: usize) {
        this.0.version.fetch_add(1, Ordering::SeqCst);
        this.0.event.notify(count);
    }

    /// Notify additional waiters.
    /// This method will not increase version.
    #[inline]
    pub fn notify(&self, count: usize) {
        self.0.event.notify(count);
    }
}

impl Drop for Signal {
    #[inline]
    fn drop(&mut self) {
        Signal::set_and_notify(self, usize::MAX);
    }
}

pub struct Notify {
    inner: Arc<SignalImpl>,
    ver: u64,
}

impl Notify {
    #[inline]
    pub fn wait(self, update: bool) {
        loop {
            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                if update {
                    self.inner.notified.store(ver, Ordering::Release);
                }
                break;
            }
            let listener = self.inner.event.listen();

            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                if update {
                    self.inner.notified.store(ver, Ordering::Release);
                }
                break;
            }
            listener.wait();
        }
    }

    #[inline]
    pub async fn wait_async(self, update: bool) {
        loop {
            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                if update {
                    self.inner.notified.store(self.ver, Ordering::Release);
                }
                break;
            }
            let listener = self.inner.event.listen();

            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                if update {
                    self.inner.notified.store(self.ver, Ordering::Release);
                }
                break;
            }
            listener.await;
        }
    }

    /// Wait for notification until timeout.
    /// Returns true if notification is received, false if timeout.
    #[inline]
    pub fn wait_timeout(self, update: bool, dur: Duration) -> bool {
        let start = Instant::now();
        let deadline = start.checked_add(dur).unwrap();
        loop {
            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                if update {
                    self.inner.notified.store(ver, Ordering::Release);
                }
                return true;
            }
            let listener = self.inner.event.listen();

            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                if update {
                    self.inner.notified.store(ver, Ordering::Release);
                }
                return true;
            }
            if start.elapsed() >= dur {
                return false;
            }
            listener.wait_deadline(deadline);
        }
    }

    #[inline]
    pub async fn wait_timeout_async(self, update: bool, dur: Duration) -> bool {
        let start = Instant::now();
        let deadline = start.checked_add(dur).unwrap();
        loop {
            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                if update {
                    self.inner.notified.store(ver, Ordering::Release);
                }
                return true;
            }
            let listener = self.inner.event.listen();

            let ver = self.inner.version.load(Ordering::Acquire);
            if ver > self.ver {
                if update {
                    self.inner.notified.store(ver, Ordering::Release);
                }
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
    // single thread notified can update this field to remember the previous
    // version it gets notified, so next time, it can pre-check it to know
    // whether there are signals during the interval.
    notified: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_notify_and_wait() {
        let signal = Signal::default();
        let handle = {
            let notify = signal.new_notify(false);
            thread::spawn(move || {
                notify.wait(false);
            })
        };
        Signal::set_and_notify(&signal, 1);
        handle.join().unwrap();
    }

    #[test]
    fn test_notify_and_wait_async() {
        let signal = Signal::default();
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let notify = signal.new_notify(false);
                thread::spawn(move || {
                    smol::block_on(async {
                        notify.wait_async(false).await;
                    });
                })
            })
            .collect();
        Signal::set_and_notify(&signal, 1);
        signal.notify(2);
        for h in handles {
            h.join().unwrap();
        }
    }
}
