use std::thread::{self, JoinHandle};

#[cfg(test)]
pub(crate) use self::tests::join_worker;

#[inline]
pub fn spawn_named<S, F>(name: S, f: F) -> JoinHandle<()>
where
    String: From<S>,
    F: FnOnce() + Send + 'static,
{
    let thread_name = String::from(name);
    thread::Builder::new()
        .name(thread_name)
        .spawn(|| {
            let thd = thread::current();
            eprintln!(
                "thread[{:?}:{}] started",
                thd.id(),
                thd.name().unwrap_or("unknown")
            );
            f();
            eprintln!(
                "thread[{:?}:{}] finished",
                thd.id(),
                thd.name().unwrap_or("unknown")
            );
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;
    use std::thread::JoinHandle;

    #[inline]
    pub(crate) fn join_worker(handle: &Mutex<Option<JoinHandle<()>>>) {
        if let Some(handle) = handle.lock().take() {
            handle.join().unwrap();
        }
    }
}
