use crate::io::STORAGE_SECTOR_SIZE;
use parking_lot::{Condvar, Mutex};
use std::alloc::{alloc, Layout};
use std::cell::UnsafeCell;
use std::cmp;

/// RingBuffer is multi-producer single-consumer ring buffer,
/// which supports continuous range operations and is used
/// for log buffer assignment and passing.
/// It supports arbitrary producers.
///
/// -----------------------------------------------------------------------------------------------------------
/// done                              ready                              next                                 LEN
/// |<- avail. for consumer to read ->|<- avail. for producer to write ->|<- avail. for producer to acquire ->|
/// -----------------------------------------------------------------------------------------------------------
///
/// Invariant:
/// 1) LEN is power of 2
/// 2) 0 <= done < LEN
/// 3) 0 <= ready < LEN * 2
/// 4) 0 <= next < LEN * 2
/// 5) done + LEN >= ready, if done < ready
/// 6) done + LEN >= next, if done < ready
pub struct RingBuffer<const LEN: usize> {
    /// buffer is the byte array allocated for message passing.
    /// its size should be power of 2.
    buffer: UnsafeCell<Box<[u8; LEN]>>,
    /// Producers observe and save `next`, then advance it
    /// to request N bytes for write operation.
    /// Consumer updates `done` once read is done.
    status: Mutex<Status>,
    status_notify: Condvar,
    /// Once producer finishes writting, it tries to update this
    /// field. The update order should be same as the request
    /// order. Otherwise, it waits for previous producer.
    ready: Mutex<u64>,
    /// notify waiting producer to update ready field.
    ready_p: Condvar,
    /// notify consumer to read.
    ready_c: Condvar,
}

impl<const LEN: usize> RingBuffer<LEN> {
    /// Create a new ring buffer.
    #[inline]
    pub fn new() -> Self {
        assert!(LEN % STORAGE_SECTOR_SIZE == 0);
        assert!(LEN < i32::MAX as usize);
        let buffer = unsafe {
            let layout = Layout::from_size_align_unchecked(LEN, STORAGE_SECTOR_SIZE);
            let ptr = alloc(layout);
            Box::<[u8; LEN]>::from_raw(ptr as *mut _)
        };
        RingBuffer {
            buffer: UnsafeCell::new(buffer),
            status: Mutex::new(Status { done: 0, next: 0 }),
            status_notify: Condvar::new(),
            ready: Mutex::new(0),
            ready_p: Condvar::new(),
            ready_c: Condvar::new(),
        }
    }

    /// Request space of given length to write in this buffer.
    /// Will block if not enough space.
    #[inline]
    pub fn produce(&self, len: usize) -> ProducerGuard<'_, LEN> {
        debug_assert!(len <= LEN);
        debug_assert!(len > 0);
        let (next, target) = {
            let mut status = self.status.lock();
            debug_assert!(status.done < LEN as u64);
            debug_assert!(status.next < LEN as u64 * 2);
            loop {
                let target = status.next + len as u64;
                match status.next.cmp(&status.done) {
                    cmp::Ordering::Less => {
                        // next < done <= ready
                        // or ready <= next < done
                        if target > status.done {
                            self.status_notify.wait(&mut status);
                        } else {
                            // update next.
                            let next = status.next;
                            status.next = target;
                            break (next, target);
                        }
                    }
                    _ => {
                        // done <= ready <= next < target
                        if target - status.done > LEN as u64 {
                            self.status_notify.wait(&mut status);
                        } else {
                            // update next. note: done won't be LEN.
                            let next = status.next;
                            status.next = if target >= status.done + LEN as u64 {
                                target - LEN as u64
                            } else {
                                target
                            };
                            break (next, target);
                        }
                    }
                }
            }
        };
        unsafe {
            let ptr = self.buffer.get();
            let area = if next < target {
                AreaMut::Single(&mut (*ptr)[next as usize..len])
            } else {
                AreaMut::Double(&mut (*ptr)[next as usize..], &mut (*ptr)[..target as usize])
            };
            ProducerGuard {
                rb: self,
                next,
                target,
                area,
            }
        }
    }

    /// Consume all ready data in ring buffer.
    #[inline]
    pub fn consume(&self) -> ConsumerGuard<'_, LEN> {
        let done = {
            // should be only read once, because only the consumer can update it.
            self.status.lock().done
        };
        let ready = {
            let mut ready = self.ready.lock();
            self.ready_c.wait_while(&mut ready, |g| *g == done);
            *ready
        };
        unsafe {
            let ptr = self.buffer.get();
            let area = if done < ready {
                Area::Single(&(*ptr)[done as usize..ready as usize])
            } else {
                Area::Double(&(*ptr)[done as usize..], &mut (*ptr)[..ready as usize])
            };
            ConsumerGuard {
                rb: self,
                done: ready,
                area,
            }
        }
    }
}

struct Status {
    done: u64,
    next: u64,
}

#[must_use]
pub struct ProducerGuard<'a, const LEN: usize> {
    rb: &'a RingBuffer<LEN>,
    next: u64,
    target: u64,
    area: AreaMut<'a>,
}

impl<'a, const LEN: usize> ProducerGuard<'a, LEN> {
    #[inline]
    pub fn write(mut self, data: &[u8]) {
        debug_assert!(data.len() == self.area_len());
        match &mut self.area {
            AreaMut::Single(target) => {
                target.copy_from_slice(data);
            }
            AreaMut::Double(t1, t2) => {
                t1.copy_from_slice(&data[..t1.len()]);
                t2.copy_from_slice(&data[t1.len()..]);
            }
        }
    }

    #[inline]
    pub fn area_len(&self) -> usize {
        match &self.area {
            AreaMut::Single(area) => area.len(),
            AreaMut::Double(a1, a2) => a1.len() + a2.len(),
        }
    }
}

impl<'a, const LEN: usize> Drop for ProducerGuard<'a, LEN> {
    #[inline]
    fn drop(&mut self) {
        let mut ready = self.rb.ready.lock();
        self.rb.ready_p.wait_while(&mut ready, |g| *g != self.next);
        *ready = self.target;
    }
}

pub struct ConsumerGuard<'a, const LEN: usize> {
    rb: &'a RingBuffer<LEN>,
    done: u64,
    area: Area<'a>,
}

impl<'a, const LEN: usize> ConsumerGuard<'a, LEN> {
    #[inline]
    pub fn read(mut self, data: &mut [u8]) {
        debug_assert!(data.len() == self.area_len());
        match &mut self.area {
            Area::Single(src) => {
                data.copy_from_slice(src);
            }
            Area::Double(s1, s2) => {
                data[..s1.len()].copy_from_slice(s1);
                data[s1.len()..].copy_from_slice(s2);
            }
        }
    }

    #[inline]
    pub fn area_len(&self) -> usize {
        match &self.area {
            Area::Single(area) => area.len(),
            Area::Double(a1, a2) => a1.len() + a2.len(),
        }
    }
}

impl<'a, const LEN: usize> Drop for ConsumerGuard<'a, LEN> {
    #[inline]
    fn drop(&mut self) {
        let mut status = self.rb.status.lock();
        status.done = self.done;
    }
}

pub enum AreaMut<'a> {
    Single(&'a mut [u8]),
    Double(&'a mut [u8], &'a mut [u8]),
}

pub enum Area<'a> {
    Single(&'a [u8]),
    Double(&'a [u8], &'a [u8]),
}
