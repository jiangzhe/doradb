use crate::error::{Error, Result};
use crate::io::{
    AIOError, AIOManager, Buf, DirectBuf, FreeListWithFactory, IocbRawPtr, PageBuf, AIO,
};
use crate::notify::{Notify, Signal};
use crate::session::{IntoSession, Session};
use crate::trx::group::{Commit, CommitGroup, GroupCommit};
use crate::trx::purge::{GCBucket, GC};
use crate::trx::sys::TrxSysConfig;
use crate::trx::{CommittedTrx, PrecommitTrx, PreparedTrx, TrxID, MAX_COMMIT_TS, MAX_SNAPSHOT_TS};
use crossbeam_utils::CachePadded;
use flume::Sender;
// use parking_lot::{Condvar, Mutex, MutexGuard};
use crate::latch::{Mutex, MutexGuard};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::mem;
use std::os::fd::AsRawFd;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(super) struct LogPartition {
    /// Group commit of this partition.
    pub(super) group_commit: CachePadded<(Mutex<GroupCommit>, Signal)>,
    /// Maximum persisted CTS of this partition.
    pub(super) persisted_cts: CachePadded<AtomicU64>,
    /// Stats of transaction system.
    pub(super) stats: CachePadded<LogPartitionStats>,
    /// GC channel to send committed transactions to GC threads.
    pub(super) gc_chan: Sender<GC>,
    /// Each GC bucket contains active sts list, committed transaction list and
    /// old transaction list.
    /// Split into multiple buckets in order to avoid bottleneck of global synchronization.
    pub(super) gc_buckets: Box<[GCBucket]>,
    /// AIO manager to handle async IO with libaio.
    pub(super) aio_mgr: AIOManager,
    // Index of log partition in total partitions, starts from 0.
    pub(super) log_no: usize,
    // Maximum IO size of each group.
    pub(super) max_io_size: usize,
    // Log file prefix, including partition number.
    pub(super) file_prefix: String,
    // Free list of page buffer, which is used by commit group to concat
    // redo logs.
    pub(super) buf_free_list: FreeListWithFactory<PageBuf>,
    // Standalone thread to handle transaction commit.
    // Including submit IO requests, wait IO responses
    // and do fsync().
    pub(super) sync_thread: Mutex<Option<JoinHandle<()>>>,
    /// Standalone thread for GC info analysis.
    pub(super) gc_thread: Mutex<Option<JoinHandle<()>>>,
}

impl LogPartition {
    #[inline]
    fn buf(&self, data: &[u8]) -> Buf {
        if data.len() > self.max_io_size {
            let buf = DirectBuf::uninit(data.len());
            Buf::Direct(buf)
        } else {
            let mut buf = self.buf_free_list.pop_elem_or_new();
            buf.set_len(data.len());
            buf.clone_from_slice(0, data);
            Buf::Reuse(buf)
        }
    }

    #[inline]
    pub(super) fn min_active_sts(&self) -> TrxID {
        let mut min = MAX_SNAPSHOT_TS;
        for gc_bucket in &self.gc_buckets {
            let ts = gc_bucket.min_active_sts.load(Ordering::Relaxed);
            if ts < min {
                min = ts;
            }
        }
        min
    }

    #[inline]
    fn create_new_group(
        &self,
        mut trx: PrecommitTrx,
        mut group_commit_g: MutexGuard<'_, GroupCommit>,
    ) -> (Session, Notify) {
        let cts = trx.cts;
        let redo_bin = trx.redo_bin.take().unwrap();
        // inside the lock, we only need to determine which range of the log file this transaction
        // should write to.
        let log_file = group_commit_g.log_file.as_ref().unwrap();
        let offset = match log_file.alloc(redo_bin.len()) {
            Ok((offset, _)) => offset,
            Err(AIOError::OutOfRange) => {
                // todo: rotate if log file is full.
                todo!();
            }
            Err(_) => unreachable!(),
        };
        let fd = log_file.as_raw_fd();
        let log_buf = self.buf(&redo_bin);
        let session = trx.split_session();
        let sync_signal = Signal::default();
        let sync_notify = sync_signal.new_notify(false);
        let new_group = CommitGroup {
            trx_list: vec![trx],
            max_cts: cts,
            fd,
            offset,
            log_buf,
            sync_signal,
        };
        group_commit_g.queue.push_back(Commit::Group(new_group));
        drop(group_commit_g);

        (session, sync_notify)
    }

    #[inline]
    pub(super) async fn commit(&self, trx: PreparedTrx, ts: &AtomicU64) -> Result<Session> {
        let mut group_commit_g = self.group_commit.0.lock_async().await;
        let cts = ts.fetch_add(1, Ordering::SeqCst);
        debug_assert!(cts < MAX_COMMIT_TS);
        let precommit_trx = trx.fill_cts(cts);
        if group_commit_g.queue.is_empty() {
            let (session, sync_notify) = self.create_new_group(precommit_trx, group_commit_g);
            Signal::set_and_notify(&self.group_commit.1, 1); // notify sync thread to work.

            let _ = sync_notify.wait_async(false).await; // wait for fsync
            assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
            return Ok(session);
        }
        let last_group = match group_commit_g.queue.back_mut().unwrap() {
            Commit::Shutdown => return Err(Error::TransactionSystemShutdown),
            Commit::Group(group) => group,
        };
        if last_group.can_join(&precommit_trx) {
            let (session, sync_notify) = last_group.join(precommit_trx);
            drop(group_commit_g); // unlock to let other transactions to enter commit phase.

            let _ = sync_notify.wait_async(false).await; // wait for fsync
            assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
            return Ok(session);
        }

        let (session, sync_notify) = self.create_new_group(precommit_trx, group_commit_g);

        let _ = sync_notify.wait_async(false).await; // wait for fsync
        assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
        Ok(session)
    }

    #[inline]
    fn update_stats(
        &self,
        trx_count: usize,
        commit_count: usize,
        log_bytes: usize,
        sync_count: usize,
        sync_nanos: usize,
    ) {
        self.stats.trx_count.fetch_add(trx_count, Ordering::Relaxed);
        self.stats
            .commit_count
            .fetch_add(commit_count, Ordering::Relaxed);
        self.stats.log_bytes.fetch_add(log_bytes, Ordering::Relaxed);
        self.stats
            .sync_count
            .fetch_add(sync_count, Ordering::Relaxed);
        self.stats
            .sync_nanos
            .fetch_add(sync_nanos, Ordering::Relaxed);
    }

    #[inline]
    fn try_fetch_io_reqs(
        &self,
        io_reqs: &mut Vec<IocbRawPtr>,
        sync_groups: &mut VecDeque<SyncGroup>,
    ) -> bool {
        // Single thread perform IO so here we only need to use sync mutex.
        let mut group_commit_g = self.group_commit.0.lock();
        loop {
            match group_commit_g.queue.pop_front() {
                None => return false,
                Some(Commit::Shutdown) => return true,
                Some(Commit::Group(cg)) => {
                    let (iocb_ptr, sg) = cg.split();
                    io_reqs.push(iocb_ptr);
                    sync_groups.push_back(sg);
                }
            }
        }
    }

    #[inline]
    fn fetch_io_reqs(
        &self,
        io_reqs: &mut Vec<IocbRawPtr>,
        sync_groups: &mut VecDeque<SyncGroup>,
    ) -> bool {
        loop {
            let mut group_commit_g = self.group_commit.0.lock();
            loop {
                match group_commit_g.queue.pop_front() {
                    None => break,
                    Some(Commit::Shutdown) => {
                        return true;
                    }
                    Some(Commit::Group(cg)) => {
                        let (iocb_ptr, sg) = cg.split();
                        io_reqs.push(iocb_ptr);
                        sync_groups.push_back(sg);
                    }
                }
            }
            if !io_reqs.is_empty() {
                return false;
            }
            let notify = self.group_commit.1.new_notify(true);
            drop(group_commit_g);
            notify.wait_timeout(true, Duration::from_secs(1));
        }
    }

    #[inline]
    pub(super) fn io_loop_noop(&self, config: &TrxSysConfig) {
        let io_depth = config.io_depth_per_log;
        let mut io_reqs = Vec::with_capacity(io_depth * 2);
        let mut sync_groups = VecDeque::with_capacity(io_depth * 2);
        let mut shutdown = false;
        loop {
            if !shutdown {
                shutdown |= self.fetch_io_reqs(&mut io_reqs, &mut sync_groups);
            }
            if !io_reqs.is_empty() {
                let mut trx_count = 0;
                let mut commit_count = 0;
                let mut log_bytes = 0;
                for sg in &sync_groups {
                    trx_count += sg.trx_list.len();
                    commit_count += 1;
                    log_bytes += sg.log_bytes;
                }

                let max_cts = sync_groups.back().as_ref().unwrap().max_cts;

                self.persisted_cts.store(max_cts, Ordering::SeqCst);

                io_reqs.clear();

                for mut sync_group in sync_groups.drain(..) {
                    let mut committed_trx_list: HashMap<usize, Vec<CommittedTrx>> = HashMap::new();
                    for trx in mem::take(&mut sync_group.trx_list) {
                        let trx = trx.commit();
                        committed_trx_list.entry(trx.gc_no).or_default().push(trx);
                    }
                    let _ = self.gc_chan.send(GC::Commit(committed_trx_list));
                }

                self.update_stats(trx_count, commit_count, log_bytes, 1, 0);
            }
            if shutdown {
                return;
            }
        }
    }

    #[inline]
    pub(super) fn io_loop(&self, config: &TrxSysConfig) {
        let syncer = {
            self.group_commit
                .0
                .lock()
                .log_file
                .as_ref()
                .unwrap()
                .syncer()
        };
        let io_depth = config.io_depth_per_log;
        let mut inflight = BTreeMap::new();
        let mut in_progress = 0;
        let mut io_reqs = Vec::with_capacity(io_depth * 2);
        let mut sync_groups = VecDeque::with_capacity(io_depth * 2);
        let mut events = self.aio_mgr.events();
        let mut written = vec![];
        let mut shutdown = false;
        loop {
            debug_assert!(
                io_reqs.len() == sync_groups.len(),
                "pending IO number equals to pending group number"
            );
            if !shutdown {
                if in_progress == 0 {
                    // there is no processing AIO, so we can block on waiting for next request.
                    shutdown |= self.fetch_io_reqs(&mut io_reqs, &mut sync_groups);
                } else {
                    // only try non-blocking way to fetch incoming requests, because we also
                    // need to finish previous IO.
                    shutdown |= self.try_fetch_io_reqs(&mut io_reqs, &mut sync_groups);
                }
            }
            let (io_submit_count, io_submit_nanos) = if !io_reqs.is_empty() {
                let start = Instant::now();
                // try to submit as many IO requests as possible
                let limit = io_depth - in_progress;
                let submit_count = self.aio_mgr.submit_limit(&mut io_reqs, limit);
                // add sync groups to inflight tree.
                for sync_group in sync_groups.drain(..submit_count) {
                    let res = inflight.insert(sync_group.max_cts, sync_group);
                    debug_assert!(res.is_none());
                }
                in_progress += submit_count;
                debug_assert!(in_progress <= io_depth);
                (1, start.elapsed().as_nanos() as usize)
            } else {
                (0, 0)
            };

            // wait for any request to be done.
            let (io_wait_count, io_wait_nanos) = if in_progress != 0 {
                let start = Instant::now();
                let finish_count =
                    self.aio_mgr
                        .wait_at_least(&mut events, 1, |cts, res| match res {
                            Ok(len) => {
                                let sg = inflight.get_mut(&cts).expect("finish inflight IO");
                                debug_assert!(sg.aio.buf().unwrap().aligned_len() == len);
                                sg.finished = true;
                            }
                            Err(err) => {
                                let sg = inflight.remove(&cts).unwrap();
                                unimplemented!(
                                    "AIO error: task.cts={}, task.log_bytes={}, {}",
                                    sg.max_cts,
                                    sg.log_bytes,
                                    err
                                )
                            }
                        });
                in_progress -= finish_count;
                (1, start.elapsed().as_nanos() as usize)
            } else {
                (0, 0)
            };

            // after logs are written to disk, we need to do fsync to make sure its durablity.
            // Also, we need to check if any previous transaction is also done and notify them.
            written.clear();
            let (trx_count, commit_count, log_bytes) = shrink_inflight(&mut inflight, &mut written);
            if !written.is_empty() {
                let max_cts = written.last().unwrap().max_cts;

                let start = Instant::now();
                match config.log_sync {
                    LogSync::Fsync => syncer.fsync(),
                    LogSync::Fdatasync => syncer.fdatasync(),
                    LogSync::None => (),
                }
                let sync_dur = start.elapsed();

                self.persisted_cts.store(max_cts, Ordering::SeqCst);

                // put IO buffer back into free list.
                for mut sync_group in written.drain(..) {
                    if let Some(Buf::Reuse(elem)) = sync_group.aio.take_buf() {
                        self.buf_free_list.push_elem(elem); // return buf to free list for future reuse
                    }
                    // commit transactions to let waiting read operations to continue
                    let mut committed_trx_list: HashMap<usize, Vec<CommittedTrx>> = HashMap::new();
                    for trx in mem::take(&mut sync_group.trx_list) {
                        let trx = trx.commit();
                        committed_trx_list.entry(trx.gc_no).or_default().push(trx);
                    }
                    // send committed transaction list to GC thread
                    let _ = self.gc_chan.send(GC::Commit(committed_trx_list));
                    drop(sync_group); // notify transaction thread to continue.
                }

                self.update_stats(
                    trx_count,
                    commit_count,
                    log_bytes,
                    1,
                    sync_dur.as_nanos() as usize,
                );
            }
            if io_submit_count != 0 {
                self.stats
                    .io_submit_count
                    .fetch_add(io_submit_count, Ordering::Relaxed);
                self.stats
                    .io_submit_nanos
                    .fetch_add(io_submit_nanos, Ordering::Relaxed);
            }
            if io_wait_count != 0 {
                self.stats
                    .io_wait_count
                    .fetch_add(io_wait_count, Ordering::Relaxed);
                self.stats
                    .io_wait_nanos
                    .fetch_add(io_wait_nanos, Ordering::Relaxed);
            }

            if shutdown && inflight.is_empty() {
                return;
            }
        }
    }
}

impl UnwindSafe for LogPartition {}
impl RefUnwindSafe for LogPartition {}

pub(super) struct SyncGroup {
    pub(super) trx_list: Vec<PrecommitTrx>,
    pub(super) max_cts: TrxID,
    pub(super) log_bytes: usize,
    pub(super) aio: AIO,
    // Signal to notify transaction threads.
    // This field won't be used, until the group is dropped.
    #[allow(dead_code)]
    pub(super) sync_signal: Signal,
    pub(super) finished: bool,
}

#[derive(Default)]
pub struct LogPartitionStats {
    pub commit_count: AtomicUsize,
    pub trx_count: AtomicUsize,
    pub log_bytes: AtomicUsize,
    pub sync_count: AtomicUsize,
    pub sync_nanos: AtomicUsize,
    pub io_submit_count: AtomicUsize,
    pub io_submit_nanos: AtomicUsize,
    pub io_wait_count: AtomicUsize,
    pub io_wait_nanos: AtomicUsize,
    pub purge_trx_count: AtomicUsize,
    pub purge_row_count: AtomicUsize,
    pub purge_index_count: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSync {
    None,
    Fsync,
    Fdatasync,
}

impl FromStr for LogSync {
    type Err = Error;

    #[inline]
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("fsync") {
            Ok(LogSync::Fsync)
        } else if s.eq_ignore_ascii_case("fdatasync") {
            Ok(LogSync::Fdatasync)
        } else if s.eq_ignore_ascii_case("none") {
            Ok(LogSync::None)
        } else {
            Err(Error::InvalidArgument)
        }
    }
}

#[inline]
fn shrink_inflight(
    tree: &mut BTreeMap<TrxID, SyncGroup>,
    buffer: &mut Vec<SyncGroup>,
) -> (usize, usize, usize) {
    let mut trx_count = 0;
    let mut commit_count = 0;
    let mut log_bytes = 0;
    while let Some(entry) = tree.first_entry() {
        let task = entry.get();
        if task.finished {
            trx_count += task.trx_list.len();
            commit_count += 1;
            log_bytes += task.log_bytes;
            buffer.push(entry.remove());
        } else {
            break; // stop at the transaction which is not persisted.
        }
    }
    (trx_count, commit_count, log_bytes)
}
