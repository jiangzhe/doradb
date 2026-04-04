use std::thread::{self, JoinHandle};

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
