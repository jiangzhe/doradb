use std::panic::{catch_unwind, UnwindSafe};
use std::thread::{self, JoinHandle};

#[inline]
pub fn spawn<F: FnOnce() -> R + UnwindSafe + Send + 'static, R>(f: F) -> JoinHandle<()> {
    thread::spawn(|| {
        if let Err(_) = catch_unwind(f) {
            let thd = thread::current();
            println!("thread[{:?}:{:?}] panic", thd.id(), thd.name());
        }
    })
}

#[inline]
pub fn spawn_named<S, F>(name: S, f: F) -> JoinHandle<()>
where
    String: From<S>,
    F: FnOnce() -> () + UnwindSafe + Send + 'static,
{
    let thread_name = String::from(name);
    thread::Builder::new()
        .name(thread_name)
        .spawn(|| {
            let thd = thread::current();
            if let Err(_) = catch_unwind(f) {
                eprintln!(
                    "thread[{:?}:{}] panic",
                    thd.id(),
                    thd.name().unwrap_or("unknown")
                );
                return;
            }
            eprintln!(
                "thread[{:?}:{}] exit",
                thd.id(),
                thd.name().unwrap_or("unknown")
            );
        })
        .unwrap()
}
