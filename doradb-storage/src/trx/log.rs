use crate::error::{Error, Result};
use crate::file::{FileSyncer, SparseFile};
use crate::free_list::FreeList;
use crate::io::{AIO, AIOContext, AIOError, AIOKind, DirectBuf, IocbRawPtr, io_event};
use crate::notify::EventNotifyOnDrop;
use crate::serde::Ser;
use crate::session::SessionState;
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::group::{Commit, CommitGroup, GroupCommit, MutexGroupCommit};
use crate::trx::log_replay::{LogBuf, LogPartitionStream, MmapLogReader, TrxLog};
use crate::trx::purge::{GC, GCBucket};
use crate::trx::sys::GC_BUCKETS;
use crate::trx::sys_conf::TrxSysConfig;
use crate::trx::{CommittedTrx, MAX_COMMIT_TS, MAX_SNAPSHOT_TS, PrecommitTrx, PreparedTrx, TrxID};
use crossbeam_utils::CachePadded;
use event_listener::EventListener;
use flume::{Receiver, Sender};
use glob::glob;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::mem;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub const LOG_HEADER_PAGES: usize = 2;

pub struct LogPartitionInitializer {
    pub(super) ctx: AIOContext,
    pub(super) mode: LogPartitionMode,
    pub(super) file_prefix: String,
    pub(super) file_max_size: usize,
    pub(super) file_header_size: usize,
    pub(super) page_size: usize,
    pub(super) io_depth_per_log: usize,
    pub(super) log_no: usize,
    // sequence of last log file.
    pub(crate) file_seq: Option<u32>,
}

impl LogPartitionInitializer {
    #[inline]
    pub fn stream(self) -> LogPartitionStream {
        LogPartitionStream {
            initializer: self,
            reader: None,
            buffer: VecDeque::new(),
        }
    }

    #[inline]
    pub fn next_reader(&mut self) -> Result<Option<MmapLogReader>> {
        match &mut self.mode {
            LogPartitionMode::Done => Ok(None),
            LogPartitionMode::Recovery(logs) => {
                let log = logs.pop_front().unwrap();
                self.file_seq.replace(parse_file_seq(log.as_path())?);
                let reader = MmapLogReader::new(
                    &log,
                    self.page_size,
                    self.file_max_size,
                    self.file_header_size,
                )?;
                if logs.is_empty() {
                    // add file seq so we always open a new log file.
                    *self.file_seq.as_mut().unwrap() += 1;
                    self.mode = LogPartitionMode::Done;
                }
                Ok(Some(reader))
            }
        }
    }

    #[inline]
    pub(super) fn finish(self) -> Result<(LogPartition, Receiver<GC>)> {
        let mut file_seq = self.file_seq.unwrap_or(0);
        let log_file = create_log_file(
            &self.file_prefix,
            self.log_no,
            file_seq,
            self.file_max_size,
            self.file_header_size,
        )?;
        file_seq += 1;

        let group_commit = GroupCommit {
            queue: VecDeque::new(),
            log_file: Some(log_file),
        };
        let gc_info: Vec<_> = (0..GC_BUCKETS).map(|_| GCBucket::new()).collect();
        let (gc_chan, gc_rx) = flume::unbounded();
        Ok((
            LogPartition {
                group_commit: CachePadded::new(MutexGroupCommit::new(group_commit)),
                persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
                rr_gc_no: CachePadded::new(AtomicUsize::new(0)),
                stats: CachePadded::new(LogPartitionStats::default()),
                gc_chan,
                gc_buckets: gc_info.into_boxed_slice(),
                aio_ctx: self.ctx,
                log_no: self.log_no,
                max_io_size: self.page_size,
                file_prefix: self.file_prefix,
                file_seq: AtomicU32::new(file_seq),
                file_max_size: self.file_max_size,
                buf_free_list: FreeList::new(
                    self.io_depth_per_log,
                    self.io_depth_per_log * 2,
                    move || DirectBuf::zeroed(self.page_size),
                ),
                io_thread: Mutex::new(None),
                gc_thread: Mutex::new(None),
            },
            gc_rx,
        ))
    }
}

pub enum LogPartitionMode {
    /// Previous log should be analyzed and replayed
    /// for data recovery.
    Recovery(VecDeque<PathBuf>),
    /// Recovery is done or there is no existing log files.
    Done,
}

pub(super) struct LogPartition {
    /// Group commit of this partition.
    pub(super) group_commit: CachePadded<MutexGroupCommit>,
    /// Maximum persisted CTS of this partition.
    pub(super) persisted_cts: CachePadded<AtomicU64>,
    /// Round-robin GC number generator.
    rr_gc_no: CachePadded<AtomicUsize>,
    /// Stats of transaction system.
    pub(super) stats: CachePadded<LogPartitionStats>,
    /// GC channel to send committed transactions to GC threads.
    pub(super) gc_chan: Sender<GC>,
    /// Each GC bucket contains active sts list, committed transaction list and
    /// old transaction list.
    /// Split into multiple buckets in order to avoid bottleneck of global synchronization.
    pub(super) gc_buckets: Box<[GCBucket]>,
    /// AIO manager to handle async IO with libaio.
    pub(super) aio_ctx: AIOContext,
    /// Index of log partition in total partitions, starts from 0.
    pub(super) log_no: usize,
    /// Maximum IO size of each group.
    pub(super) max_io_size: usize,
    /// Log file prefix, including partition number.
    pub(super) file_prefix: String,
    /// sequence of current file in this partition, starts from 0.
    pub(super) file_seq: AtomicU32,
    /// Maximum size of single log file.
    pub(super) file_max_size: usize,
    /// Free list of page buffer, which is used by commit group to concat
    /// redo logs.
    pub(super) buf_free_list: FreeList<DirectBuf>,
    /// Standalone thread to handle transaction commit.
    /// Including submit IO requests, wait IO responses
    /// and do fsync().
    pub(super) io_thread: Mutex<Option<JoinHandle<()>>>,
    /// Standalone thread for GC info analysis.
    pub(super) gc_thread: Mutex<Option<JoinHandle<()>>>,
}

impl LogPartition {
    /// Returns next GC number.
    /// It is used to evenly dispatch transactions to all GC buckets.
    pub fn next_gc_no(&self) -> usize {
        self.rr_gc_no.fetch_add(1, Ordering::Relaxed) % GC_BUCKETS
    }

    /// Create a new log buffer to hold one transaction's redo log.
    #[inline]
    fn new_buf(&self, data: TrxLog) -> LogBuf {
        let ser_len = data.ser_len();
        if ser_len > self.max_io_size {
            // data is longer than single page, we need to
            let mut buf = LogBuf::new(ser_len);
            buf.ser(&data);
            return buf;
        }
        if let Some(mut buf) = self.try_buf_in_free_list(ser_len) {
            buf.ser(&data);
            return buf;
        }
        let mut buf = LogBuf::new(ser_len);
        buf.ser(&data);
        buf
    }

    #[inline]
    fn try_buf_in_free_list(&self, ser_len: usize) -> Option<LogBuf> {
        if LogBuf::actual_len(ser_len) <= self.max_io_size {
            // fit buffer size in free list.
            return self.buf_free_list.try_pop(true).map(LogBuf::with_buffer);
        }
        None
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
    fn rotate_log_file(&self, group_commit_g: &mut MutexGuard<'_, GroupCommit>) -> Result<()> {
        let new_log_file = self.create_log_file()?;
        let old_log_file = group_commit_g.log_file.replace(new_log_file).unwrap();
        group_commit_g.queue.push_back(Commit::Switch(old_log_file));
        Ok(())
    }

    #[inline]
    fn create_log_file(&self) -> Result<SparseFile> {
        let file_seq = self.file_seq.fetch_add(1, Ordering::SeqCst);
        create_log_file(
            &self.file_prefix,
            self.log_no,
            file_seq,
            self.file_max_size,
            // we use two pages as file header to maintain metadata of log file.
            self.max_io_size * LOG_HEADER_PAGES,
        )
    }

    #[inline]
    fn create_new_group(
        &self,
        mut trx: PrecommitTrx,
        group_commit_g: &mut MutexGuard<'_, GroupCommit>,
        wait_sync: bool,
    ) -> (Option<Arc<SessionState>>, Option<EventListener>) {
        let cts = trx.cts;
        let redo_bin = trx.redo_bin.take().unwrap();
        // Serialize redo log to buffer.
        let log_buf = self.new_buf(redo_bin);
        let log_file = group_commit_g.log_file.as_ref().unwrap();
        // Allocate space of log file.
        let (fd, offset) = match log_file.alloc(log_buf.capacity()) {
            Ok((offset, _)) => (log_file.as_raw_fd(), offset),
            Err(AIOError::OutOfRange) => {
                // rotate log file and try again.
                self.rotate_log_file(group_commit_g)
                    .expect("rotate log file");

                let new_log_file = group_commit_g.log_file.as_ref().unwrap();
                let (offset, _) = new_log_file
                    .alloc(log_buf.capacity())
                    .expect("alloc on new log file");
                (new_log_file.as_raw_fd(), offset)
            }
            Err(_) => unreachable!(),
        };
        let session = trx.take_session();
        let sync_ev = EventNotifyOnDrop::new();
        let listener = wait_sync.then(|| sync_ev.listen());
        let new_group = CommitGroup {
            trx_list: vec![trx],
            max_cts: cts,
            fd,
            offset,
            log_buf,
            sync_ev,
        };
        group_commit_g.queue.push_back(Commit::Group(new_group));
        (session, listener)
    }

    #[inline]
    fn enqueue_commit(
        &self,
        trx: PreparedTrx,
        global_ts: &AtomicU64,
        wait_sync: bool,
    ) -> Result<(TrxID, Option<Arc<SessionState>>, Option<EventListener>)> {
        let mut group_commit_g = self.group_commit.lock();
        let cts = global_ts.fetch_add(1, Ordering::SeqCst);
        debug_assert!(cts < MAX_COMMIT_TS);
        let precommit_trx = trx.fill_cts(cts);
        if group_commit_g.queue.is_empty() {
            let (session, listener) =
                self.create_new_group(precommit_trx, &mut group_commit_g, wait_sync);
            self.group_commit.notify_one(); // notify sync thread to work.
            drop(group_commit_g);
            return Ok((cts, session, listener));
        }
        let last_group = match group_commit_g.queue.back_mut().unwrap() {
            Commit::Shutdown => return Err(Error::TransactionSystemShutdown),
            Commit::Group(group) => group,
            Commit::Switch(_) => {
                // Impossible, switch always has one group followed.
                unreachable!()
            }
        };
        if last_group.can_join(&precommit_trx) {
            let (session, listener) = last_group.join(precommit_trx, wait_sync);
            drop(group_commit_g); // unlock to let other transactions to enter commit phase.
            return Ok((cts, session, listener));
        }
        let (session, listener) =
            self.create_new_group(precommit_trx, &mut group_commit_g, wait_sync);
        self.group_commit.notify_one(); // notify sync thread to work.
        drop(group_commit_g);
        Ok((cts, session, listener))
    }

    #[inline]
    pub(super) fn commit_no_wait(&self, trx: PreparedTrx, global_ts: &AtomicU64) -> Result<TrxID> {
        let (cts, session, listener) = self.enqueue_commit(trx, global_ts, false)?;
        debug_assert!(session.is_none());
        debug_assert!(listener.is_none());
        Ok(cts)
    }

    // disable clippy lint due to false positive.
    #[allow(clippy::await_holding_lock)]
    #[inline]
    pub(super) async fn commit(
        &self,
        trx: PreparedTrx,
        global_ts: &AtomicU64,
        wait_sync: bool,
    ) -> Result<TrxID> {
        let (cts, mut session, listener) = self.enqueue_commit(trx, global_ts, wait_sync)?;
        if !wait_sync {
            debug_assert!(session.is_none());
            return Ok(cts);
        }
        let listener = listener.expect("listener should exist when wait_sync");
        listener.await; // wait for fsync
        assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
        if let Some(s) = session.as_mut() {
            s.commit(cts);
        }
        Ok(cts)
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
    fn file_processor(&self, config: &TrxSysConfig) -> FileProcessor<'_> {
        let syncer = { self.group_commit.lock().log_file.as_ref().unwrap().syncer() };

        FileProcessor::new(self, config, syncer)
    }

    #[inline]
    pub(super) fn io_loop(&self, config: &TrxSysConfig) {
        loop {
            let mut fp = self.file_processor(config);
            if let Some(ended_log_file) = fp.process_single_file() {
                fp.finish_pending_io();

                debug_assert!(fp.in_progress == 0);
                debug_assert!(fp.inflight.is_empty());
                debug_assert!(fp.io_reqs.is_empty());
                debug_assert!(fp.sync_groups.is_empty());

                // todo: update file header.
                // this may include information which can speed up recovery.
                // e.g. DDL start/end pages, min/max transaction CTS.

                // Close file explicitly.
                drop(ended_log_file);
            }
            if fp.shutdown {
                return;
            }
        }
    }

    #[inline]
    pub fn logs(&self, desc: bool) -> Result<Vec<PathBuf>> {
        list_log_files(&self.file_prefix, self.log_no, desc)
    }
}

pub(super) struct SyncGroup {
    pub(super) trx_list: Vec<PrecommitTrx>,
    pub(super) max_cts: TrxID,
    pub(super) log_bytes: usize,
    pub(super) aio: AIO<DirectBuf>,
    // Signal to notify transaction threads.
    // This field won't be used, until the group is dropped.
    #[allow(dead_code)]
    pub(super) sync_ev: EventNotifyOnDrop,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LogSync {
    #[default]
    #[serde(rename = "none")]
    None,
    #[serde(rename = "fsync")]
    Fsync,
    #[serde(rename = "fdatasync")]
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

struct FileProcessor<'a> {
    partition: &'a LogPartition,
    io_depth: usize,
    max_io_size: usize,
    inflight: BTreeMap<TrxID, SyncGroup>,
    in_progress: usize,
    io_reqs: Vec<IocbRawPtr>,
    sync_groups: VecDeque<SyncGroup>,
    events: Box<[io_event]>,
    written: Vec<SyncGroup>,
    syncer: FileSyncer,
    log_sync: LogSync,
    shutdown: bool,
    io_submit_count: usize,
    io_submit_nanos: usize,
    io_wait_count: usize,
    io_wait_nanos: usize,
}

impl<'a> FileProcessor<'a> {
    #[inline]
    fn new(partition: &'a LogPartition, config: &TrxSysConfig, syncer: FileSyncer) -> Self {
        FileProcessor {
            partition,
            io_depth: config.io_depth_per_log,
            max_io_size: config.max_io_size.as_u64() as usize,
            inflight: BTreeMap::new(),
            in_progress: 0,
            io_reqs: vec![],
            sync_groups: VecDeque::new(),
            events: partition.aio_ctx.events(),
            written: vec![],
            syncer,
            log_sync: config.log_sync,
            shutdown: false,
            io_submit_count: 0,
            io_submit_nanos: 0,
            io_wait_count: 0,
            io_wait_nanos: 0,
        }
    }

    /// Process single log file until it is full or shutdown.
    #[inline]
    fn process_single_file(&mut self) -> Option<SparseFile> {
        loop {
            debug_assert!(
                self.io_reqs.len() == self.sync_groups.len(),
                "pending IO number equals to pending group number"
            );
            // If shutdown flag is set, we still submit and finish all pending IOs,
            // but do not accept any new IO requests.
            if !self.shutdown {
                let ended_log_file = self.fetch_io_reqs();
                // End this loop if log file is full.
                if ended_log_file.is_some() {
                    return ended_log_file;
                }
            }
            self.reset_stats();

            // submit IO requests if any.
            self.submit_io();

            // wait for any request to be done.
            self.wait_io(false);

            // Sync IO.
            self.sync_io();

            // update stats.
            self.update_stats();

            if self.shutdown && self.inflight.is_empty() {
                return None;
            }
        }
    }

    /// Finish pending IOs.
    #[inline]
    fn finish_pending_io(&mut self) {
        self.submit_io();

        self.wait_io(true);

        self.sync_io();

        self.update_stats();
    }

    /// Fetch IO requests, returns ended log file if any.
    #[inline]
    fn fetch_io_reqs(&mut self) -> Option<SparseFile> {
        if self.in_progress == 0 {
            // there is no processing AIO, so we can block on waiting for next request.
            self.fetch_io_reqs_internal()
        } else {
            // only try non-blocking way to fetch incoming requests, because we also
            // need to finish previous IO.
            self.try_fetch_io_reqs_internal()
        }
    }

    #[inline]
    fn fetch_io_reqs_internal(&mut self) -> Option<SparseFile> {
        loop {
            let mut group_commit_g = self.partition.group_commit.lock();
            loop {
                match group_commit_g.queue.pop_front() {
                    None => break,
                    Some(Commit::Shutdown) => {
                        self.shutdown = true;
                        return None;
                    }
                    Some(Commit::Switch(log_file)) => {
                        return Some(log_file);
                    }
                    Some(Commit::Group(cg)) => {
                        let (iocb_ptr, sg) = cg.split();
                        self.io_reqs.push(iocb_ptr);
                        self.sync_groups.push_back(sg);
                    }
                }
            }
            if !self.io_reqs.is_empty() {
                return None;
            }
            self.partition
                .group_commit
                .wait_for(&mut group_commit_g, Duration::from_secs(1));
        }
    }

    #[inline]
    fn try_fetch_io_reqs_internal(&mut self) -> Option<SparseFile> {
        // Single thread perform IO so here we only need to use sync mutex.
        let mut group_commit_g = self.partition.group_commit.lock();
        loop {
            match group_commit_g.queue.pop_front() {
                None => {
                    return None;
                }
                Some(Commit::Shutdown) => {
                    self.shutdown = true;
                    return None;
                }
                Some(Commit::Switch(log_file)) => {
                    return Some(log_file);
                }
                Some(Commit::Group(cg)) => {
                    let (iocb_ptr, sg) = cg.split();
                    self.io_reqs.push(iocb_ptr);
                    self.sync_groups.push_back(sg);
                }
            }
        }
    }

    /// After logs are written to disk, we need to do fsync to make sure its durability.
    /// Also, we need to check if any previous transaction is also done and notify them.
    #[inline]
    fn sync_io(&mut self) {
        self.written.clear();
        let (trx_count, commit_count, log_bytes) =
            shrink_inflight(&mut self.inflight, &mut self.written);
        if !self.written.is_empty() {
            let max_cts = self.written.last().unwrap().max_cts;

            let start = Instant::now();
            match self.log_sync {
                LogSync::Fsync => self.syncer.fsync(),
                LogSync::Fdatasync => self.syncer.fdatasync(),
                LogSync::None => (),
            }
            let sync_dur = start.elapsed();

            self.partition
                .persisted_cts
                .store(max_cts, Ordering::SeqCst);

            // Put IO buffer back into free list.
            for mut sync_group in self.written.drain(..) {
                if let Some(mut buf) = sync_group.aio.take_buf()
                    && buf.capacity() == self.max_io_size
                {
                    buf.reset(); // reset buffer to zero
                    self.partition.buf_free_list.push(buf); // return buf to free list for future reuse
                }
                // commit transactions to let waiting read operations to continue
                let mut committed_trx_list: HashMap<usize, Vec<CommittedTrx>> = HashMap::new();
                for trx in mem::take(&mut sync_group.trx_list) {
                    let trx = trx.commit();
                    if let Some(gc_no) = trx.gc_no() {
                        // Only user transaction is involved in GC process.
                        committed_trx_list.entry(gc_no).or_default().push(trx);
                    }
                }
                // send committed transaction list to GC thread
                let _ = self.partition.gc_chan.send(GC::Commit(committed_trx_list));
                drop(sync_group); // notify transaction thread to continue.
            }

            self.partition.update_stats(
                trx_count,
                commit_count,
                log_bytes,
                1,
                sync_dur.as_nanos() as usize,
            );
        }
    }

    #[inline]
    fn reset_stats(&mut self) {
        self.io_submit_count = 0;
        self.io_submit_nanos = 0;
        self.io_wait_count = 0;
        self.io_wait_nanos = 0;
    }

    #[inline]
    fn update_stats(&mut self) {
        if self.io_submit_count != 0 {
            self.partition
                .stats
                .io_submit_count
                .fetch_add(self.io_submit_count, Ordering::Relaxed);
            self.partition
                .stats
                .io_submit_nanos
                .fetch_add(self.io_submit_nanos, Ordering::Relaxed);
        }
        if self.io_wait_count != 0 {
            self.partition
                .stats
                .io_wait_count
                .fetch_add(self.io_wait_count, Ordering::Relaxed);
            self.partition
                .stats
                .io_wait_nanos
                .fetch_add(self.io_wait_nanos, Ordering::Relaxed);
        }
    }

    /// Submit IO requests, returns submitted count and elapsed time.
    #[inline]
    fn submit_io(&mut self) {
        if !self.io_reqs.is_empty() {
            let start = Instant::now();
            // try to submit as many IO requests as possible
            let limit = self.io_depth - self.in_progress;
            let submit_count = self.partition.aio_ctx.submit_limit(&self.io_reqs, limit);
            // remove io requests after submission.
            self.io_reqs.drain(0..submit_count);
            // add sync groups to inflight tree.
            for sync_group in self.sync_groups.drain(..submit_count) {
                let res = self.inflight.insert(sync_group.max_cts, sync_group);
                debug_assert!(res.is_none());
            }
            self.in_progress += submit_count;
            debug_assert!(self.in_progress <= self.io_depth);
            self.io_submit_count = 1;
            self.io_submit_nanos = start.elapsed().as_nanos() as usize;
        }
    }

    /// Wait for any IO request to be done, returns finished count and elapsed time.
    #[inline]
    fn wait_io(&mut self, all: bool) {
        if self.in_progress != 0 {
            let start = Instant::now();
            let wait_count = if all { self.in_progress } else { 1 };
            let (read_count, write_count) =
                self.partition
                    .aio_ctx
                    .wait_at_least(&mut self.events, wait_count, |cts, res| match res {
                        Ok(len) => {
                            let sg = self.inflight.get_mut(&cts).expect("finish inflight IO");
                            debug_assert!(sg.aio.buf().unwrap().capacity() == len);
                            sg.finished = true;
                            // only write IO
                            AIOKind::Write
                        }
                        Err(err) => {
                            let sg = self.inflight.remove(&cts).unwrap();
                            unimplemented!(
                                "AIO error: task.cts={}, task.log_bytes={}, {}",
                                sg.max_cts,
                                sg.log_bytes,
                                err
                            )
                        }
                    });
            self.in_progress -= read_count + write_count;
            self.io_wait_count = 1;
            self.io_wait_nanos = start.elapsed().as_nanos() as usize;
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

#[inline]
fn log_file_name(file_prefix: &str, log_no: usize, file_seq: u32) -> String {
    format!("{file_prefix}.{log_no}.{file_seq:08x}")
}

/// Create a new log file.
#[inline]
pub(super) fn create_log_file(
    file_prefix: &str,
    log_no: usize,
    file_seq: u32,
    file_max_size: usize,
    file_header_size: usize,
) -> Result<SparseFile> {
    let file_name = log_file_name(file_prefix, log_no, file_seq);
    let log_file = SparseFile::create_or_fail(&file_name, file_max_size)?;
    // todo: Add two pages as file header.
    let _ = log_file.alloc(file_header_size)?;
    Ok(log_file)
}

#[inline]
pub fn list_log_files(file_prefix: &str, log_no: usize, desc: bool) -> Result<Vec<PathBuf>> {
    let pattern = format!("{file_prefix}.{log_no}.*");
    let mut res = vec![];
    for entry in glob(&pattern).unwrap() {
        res.push(entry?);
    }
    if desc {
        // reverse sort.
        res.sort_by(|a, b| b.cmp(a));
    } else {
        res.sort();
    }
    Ok(res)
}

#[inline]
pub fn parse_file_seq(file_path: &Path) -> Result<u32> {
    let file_name = file_path
        .file_name()
        .ok_or(Error::InvalidFormat)?
        .to_str()
        .ok_or(Error::InvalidFormat)?;
    if file_name.len() < 9 {
        return Err(Error::InvalidFormat);
    }
    // last 8 bytes are hex encoded.
    let suffix = std::str::from_utf8(&file_name.as_bytes()[file_name.len() - 8..])?;
    let file_seq = u32::from_str_radix(suffix, 16)?;
    Ok(file_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::EvictableBufferPoolConfig;
    use crate::catalog::tests::table2;
    use crate::engine::EngineConfig;
    use crate::trx::log_replay::{LogMerger, ReadLog};
    use crate::trx::redo::RedoLogs;
    use crate::trx::sys_conf::TrxSysConfig;
    use crate::trx::sys_trx::SysTrx;
    use crate::value::Val;
    use std::sync::atomic::AtomicU64;
    use tempfile::TempDir;

    #[test]
    fn test_mmap_log_reader() {
        smol::block_on(async {
            const SIZE: i32 = 100;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix(String::from("mmap_log_reader_redo.log"))
                        .skip_recovery(true),
                )
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64u64 * 1024 * 1024)
                        .max_file_size(128u64 * 1024 * 1024),
                )
                .build()
                .await
                .unwrap();
            let table_id = table2(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();

            let mut session = engine.new_session();
            {
                for i in 0..SIZE {
                    let mut trx = session.begin_trx().unwrap();
                    let mut stmt = trx.start_stmt();
                    let s = format!("{}", i);
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    let res = stmt.insert_row(&table, insert).await;
                    debug_assert!(res.is_ok());
                    trx = stmt.succeed();
                    trx.commit().await.unwrap();
                }
            }
            drop(session);

            let mut log_recs = 0usize;
            let logs = engine.trx_sys.log_partitions[0].logs(false).unwrap();
            for log in logs {
                println!("log file {:?}", log.file_name());
                let mut reader = engine.trx_sys.log_reader(&log).unwrap();
                loop {
                    match reader.read() {
                        ReadLog::SizeLimit => unreachable!(),
                        ReadLog::DataCorrupted => unreachable!(),
                        ReadLog::DataEnd => break,
                        ReadLog::Some(mut group) => {
                            let data_len = group.data().len();
                            while let Some(pod) = group.try_next().unwrap() {
                                println!(
                                    "log {}, len={}, header={:?}, payload={:?}",
                                    log_recs, data_len, pod.header, pod.payload
                                );
                                log_recs += 1;
                            }
                        }
                    }
                }
            }
            println!("total log records {}", log_recs);

            drop(engine);
        });
    }

    #[test]
    fn test_log_merger() {
        smol::block_on(async {
            const SIZE: i32 = 1000;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let log_prefix = temp_dir.path().join("redo_merger");
            let log_prefix = log_prefix.to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix(log_prefix.clone())
                        .log_partitions(2)
                        .skip_recovery(true),
                )
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64u64 * 1024 * 1024)
                        .max_file_size(128u64 * 1024 * 1024),
                )
                .build()
                .await
                .unwrap();
            let table_id = table2(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();

            let mut session = engine.new_session();
            {
                for i in 0..SIZE {
                    let mut trx = session.begin_trx().unwrap();
                    let mut stmt = trx.start_stmt();
                    let s = format!("{}", i);
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    let res = stmt.insert_row(&table, insert).await;
                    debug_assert!(res.is_ok());
                    trx = stmt.succeed();
                    trx.commit().await.unwrap();
                }
            }
            drop(session);

            let mut log_recs = 0usize;
            let logs = engine.trx_sys.log_partitions[0].logs(false).unwrap();
            for log in logs {
                println!("log file {:?}", log.file_name());
                let mut reader = engine.trx_sys.log_reader(&log).unwrap();
                loop {
                    match reader.read() {
                        ReadLog::SizeLimit => unreachable!(),
                        ReadLog::DataCorrupted => unreachable!(),
                        ReadLog::DataEnd => break,
                        ReadLog::Some(mut group) => {
                            while let Some(pod) = group.try_next().unwrap() {
                                println!("header={:?}, payload={:?}", pod.header, pod.payload);
                                log_recs += 1;
                            }
                        }
                    }
                }
            }
            println!("total log records {}", log_recs);

            drop(engine);

            // after the first engine is done, we reopen log files to test log merger.
            let trx_sys_config = TrxSysConfig::default()
                .log_file_prefix(log_prefix)
                .log_partitions(2)
                .skip_recovery(true);

            let mut log_merger = LogMerger::default();
            for i in 0..trx_sys_config.log_partitions {
                let initializer = trx_sys_config.log_partition_initializer(i).unwrap();
                let stream = initializer.stream();
                log_merger.add_stream(stream).unwrap();
            }

            while let Some(log) = log_merger.try_next().unwrap() {
                println!("header={:?}, payload={:?}", log.header, log.payload);
            }
        });
    }

    #[test]
    fn test_commit_no_wait_returns_cts() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let log_prefix = temp_dir
                .path()
                .join("redo_no_wait")
                .to_string_lossy()
                .to_string();
            let config = TrxSysConfig::default()
                .log_file_prefix(log_prefix)
                .skip_recovery(true);
            let initializer = config.log_partition_initializer(0).unwrap();
            let (partition, _gc_rx) = initializer.finish().unwrap();

            let mut sys_trx = SysTrx {
                redo: RedoLogs::default(),
            };
            sys_trx.create_row_page(1, 1, 0, 1);
            let prepared = sys_trx.prepare();
            let global_ts = AtomicU64::new(MIN_SNAPSHOT_TS);
            let cts = partition.commit(prepared, &global_ts, false).await.unwrap();
            assert!(cts >= MIN_SNAPSHOT_TS);
        });
    }
}
