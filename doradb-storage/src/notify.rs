use event_listener::{Event, Listener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct Signal(Arc<Inner>);

impl Signal {
    #[inline]
    pub fn new_notify(&self) -> Notify {
        Notify(Arc::clone(&self.0))
    }

    #[inline]
    pub fn done(self) {
        // destructor will mark it as done.
    }
}

impl Drop for Signal {
    #[inline]
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::SeqCst);
        self.0.event.notify(usize::MAX);
    }
}

pub struct Notify(Arc<Inner>);

impl Notify {
    #[inline]
    pub fn wait(self) {
        loop {
            if self.0.done.load(Ordering::SeqCst) {
                break;
            }
            let listener = self.0.event.listen();

            if self.0.done.load(Ordering::SeqCst) {
                break;
            }
            listener.wait();
        }
    }

    #[inline]
    pub async fn wait_async(self) {
        loop {
            if self.0.done.load(Ordering::SeqCst) {
                break;
            }
            let listener = self.0.event.listen();

            if self.0.done.load(Ordering::SeqCst) {
                break;
            }
            listener.await;
        }
    }
}

#[derive(Default)]
struct Inner {
    done: AtomicBool,
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
        signal.done();
        handle.join().unwrap();
    }

    #[test]
    fn test_notify_and_wait_async() {
        let signal = Signal::default();
        let handle = {
            let notify = signal.new_notify();
            thread::spawn(move || {
                smol::block_on(async {
                    notify.wait_async().await;
                });
            })
        };
        signal.done();
        handle.join().unwrap();
    }
}
