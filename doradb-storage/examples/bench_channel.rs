use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    channel_sync();
    channel_async();
    mutex_deque();
}

fn channel_sync() {
    let count = 10_000_000;
    let (tx, rx) = flume::unbounded::<Vec<u8>>();
    let handle = std::thread::spawn(move || {
        let mut sum = 0;
        while let Ok(data) = rx.recv() {
            sum += data.len();
        }
        sum
    });

    let start = Instant::now();
    for _ in 0..count {
        let data = vec![0; 1];
        tx.send(data).unwrap();
    }
    drop(tx);
    handle.join().unwrap();
    let dur = start.elapsed();
    println!(
        "channel_sync dur={}us,count={},ops={:.3}",
        dur.as_micros(),
        count,
        count as f64 * 1000000f64 / dur.as_micros() as f64
    );
}

fn channel_async() {
    smol::block_on(async {
        let count = 10_000_000;
        let (tx, rx) = flume::unbounded::<Vec<u8>>();
        let (stop_tx, stop_rx) = flume::unbounded::<()>();
        smol::spawn(async move {
            let mut sum = 0;
            while let Ok(data) = rx.recv_async().await {
                sum += data.len();
            }
            drop(stop_tx);
            println!("received sum={}", sum);
        })
        .detach();

        let start = Instant::now();
        for _ in 0..count {
            let data = vec![0; 1];
            tx.send_async(data).await.unwrap();
        }
        drop(tx);
        let _ = stop_rx.recv_async().await;
        let dur = start.elapsed();
        println!(
            "channel_async dur={}us,count={},ops={:.3}",
            dur.as_micros(),
            count,
            count as f64 * 1000000f64 / dur.as_micros() as f64
        );
    })
}

fn mutex_deque() {
    let count = 10_000_000;
    let (tx, rx) = mutex_deque_unbounded::<Vec<u8>>();
    let handle = std::thread::spawn(move || {
        let mut sum = 0;
        while let Ok(data) = rx.recv() {
            sum += data.len();
        }
        sum
    });

    let start = Instant::now();
    for _ in 0..count {
        let data = vec![0; 1];
        tx.send(data).unwrap();
    }
    drop(tx);
    handle.join().unwrap();
    let dur = start.elapsed();
    println!(
        "mutex_deque dur={}us,count={},ops={:.3}",
        dur.as_micros(),
        count,
        count as f64 * 1000000f64 / dur.as_micros() as f64
    );
}

#[inline]
fn mutex_deque_unbounded<T>() -> (MutexDequeSender<T>, MutexDequeReceiver<T>) {
    let inner = Arc::new(Inner::new());
    (MutexDequeSender(inner.clone()), MutexDequeReceiver(inner))
}

struct MutexDequeSender<T>(Arc<Inner<T>>);

impl<T> MutexDequeSender<T> {
    #[inline]
    pub fn send(&self, data: T) -> Result<(), Error> {
        self.0.send(data)
    }
}

impl<T> Drop for MutexDequeSender<T> {
    fn drop(&mut self) {
        self.0.close();
    }
}

struct MutexDequeReceiver<T>(Arc<Inner<T>>);

impl<T> MutexDequeReceiver<T> {
    #[inline]
    pub fn recv(&self) -> Result<T, Error> {
        self.0.recv()
    }
}

impl<T> Drop for MutexDequeReceiver<T> {
    fn drop(&mut self) {
        self.0.close();
    }
}

struct Inner<T> {
    deque: Mutex<(VecDeque<T>, bool)>,
    empty: Condvar,
    // unbounded does not need full condition variable.
    // full: Condvar,
}

impl<T> Inner<T> {
    #[inline]
    pub fn new() -> Self {
        Inner {
            deque: Mutex::new((VecDeque::new(), false)),
            empty: Condvar::new(),
        }
    }

    #[inline]
    fn recv(&self) -> Result<T, Error> {
        let mut g = self.deque.lock();
        loop {
            if let Some(data) = g.0.pop_front() {
                return Ok(data);
            }
            if g.1 {
                return Err(Error::Disconnected);
            }
            self.empty.wait(&mut g);
        }
    }

    #[inline]
    pub fn send(&self, data: T) -> Result<(), Error> {
        let mut g = self.deque.lock();
        if g.1 {
            return Err(Error::Disconnected);
        }
        g.0.push_back(data);
        if g.0.len() == 1 {
            self.empty.notify_one();
        }
        Ok(())
    }

    #[inline]
    pub fn close(&self) {
        let mut g = self.deque.lock();
        g.1 = true;
    }
}

#[derive(Debug)]
enum Error {
    Disconnected,
}
