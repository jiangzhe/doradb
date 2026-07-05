use crate::obs;
use std::thread::{self, Builder, JoinHandle};

/// Spawns a named thread and logs its start and finish lifecycle.
#[inline]
pub(crate) fn spawn_named<S, F>(name: S, f: F) -> JoinHandle<()>
where
    String: From<S>,
    F: FnOnce() + Send + 'static,
{
    let thread_name = String::from(name);
    Builder::new()
        .name(thread_name)
        .spawn(|| {
            let thd = thread::current();
            let worker = thd.name().unwrap_or("unknown");
            obs::info!(
                "event=worker_lifecycle component=runtime worker={} thread_id={:?} action=start result=ok",
                worker,
                thd.id()
            );
            f();
            obs::info!(
                "event=worker_lifecycle component=runtime worker={} thread_id={:?} action=finish result=ok",
                worker,
                thd.id()
            );
        })
        .unwrap()
}
