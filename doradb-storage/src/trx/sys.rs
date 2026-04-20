use crate::DiskPool;
use crate::catalog::{Catalog, CatalogCheckpointScanConfig, TableCache};
use crate::component::{Component, ComponentRegistry, IndexPool, MemPool, MetaPool, ShelfScope};
use crate::conf::TrxSysConfig;
use crate::error::{Error, Result, StoragePoisonSource};
use crate::file::fs::FileSystem;
use crate::quiescent::{QuiescentBox, QuiescentGuard, SyncQuiescentGuard};
use crate::session::SessionState;
use crate::thread;
use crate::trx::group::Commit;
use crate::trx::log::{LOG_HEADER_PAGES, LogPartition};
use crate::trx::log_replay::MmapLogReader;
use crate::trx::purge::{GC, Purge};
use crate::trx::redo::RedoLogs;
use crate::trx::sys_trx::SysTrx;
use crate::trx::{
    ActiveTrx, MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS, PreparedTrx, TrxID,
};
use crossbeam_utils::CachePadded;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;

pub use crate::catalog::{
    CatalogCheckpointBatch, CatalogCheckpointBlockingDDL, CatalogCheckpointScanStopReason,
    CatalogRedoEntry,
};
pub const GC_BUCKETS: usize = 64;

/// TransactionSystem controls lifecycle of all transactions.
///
/// 1. Transaction begin:
///    a) Generate STS and TrxID.
///    b) Put it into active transaction list.
///
/// 2. Transaction pre-commmit:
///    a) Generate CTS.
///    b) Put it into precommit transaction list.
///
/// Note 1: Before pre-commit, the transaction should serialize its redo log to binary
/// because group commit is single-threaded and the serialization may require
/// much CPU and slow down the log writer. So each transaction
///
/// Note 2: In this phase, transaction is still in active transaction list.
/// Only when redo log is persisted, we can move it from active list to committed list.
/// One optimization is Early Lock Release, which unlock all row-locks(backfill CTS to undo)
/// and move it to committed list. This can improve performance because it does not wait
/// log writer to fsync. But final-commit step must wait for additional transaction dependencies,
/// to ensure any previous dependent transaction's log are already persisted.
/// Currently, we do NOT apply this optimization.  
///
/// 3. Transaction group-commit:
///
/// A single-threaded log writer is responsible for persisting redo logs.
/// It also notify all transactions in group commit to check if log has been persisted.
///
/// 4. Transaction final-commit:
///
/// TrxID in all undo log entries of current transaction should be updated to CTS after the
/// transaction reaches its ordered commit barrier. Durability-required transactions reach that
/// barrier after redo persistence; no-log runtime-effect transactions reach it through the same
/// group ordering without publishing a recovery-visible timestamp carrier.
/// As undo logs are maintained purely in memory, we can use shared pointer with atomic variable
/// to perform very fast CTS backfill.
pub struct TransactionSystem {
    /// A sequence to generate snapshot timestamp(abbr. sts) and commit timestamp(abbr. cts).
    /// They share the same sequence and start from 1.
    /// The two timestamps are used to identify which version of data a transaction should see.
    /// Transaction id is derived from snapshot timestamp with highest bit set to 1.
    ///
    /// trx_id range: (1<<63)+1 to uint::MAX-1
    /// sts range: 1 to 1<<63
    /// cts range: 1 to 1<<63
    pub(super) ts: CachePadded<AtomicU64>,
    /// Global visible snapshot timestamp.
    /// It's updated by query/GC thread after cleaning out-of-date version chains.
    ///
    /// Data associated with smaller timestamp will be always visible to all transactions.
    global_visible_sts: CachePadded<AtomicU64>,
    /// Round-robin partition id generator.
    rr_partition_id: CachePadded<AtomicUsize>,
    /// Multiple log partitions.
    pub(super) log_partitions: CachePadded<Box<[CachePadded<LogPartition>]>>,
    /// Transaction system configuration.
    pub(super) config: CachePadded<TrxSysConfig>,
    /// Catalog of the database.
    pub(crate) catalog: CachePadded<QuiescentGuard<Catalog>>,
    /// Storage-runtime poison flag for fatal storage background or durability failures.
    storage_poisoned: CachePadded<AtomicBool>,
    /// First fatal storage error that poisoned runtime admission.
    storage_poison_err: CachePadded<Mutex<Option<Error>>>,
}

pub(crate) struct TransactionSystemWorkers;

pub(crate) struct TransactionSystemWorkersOwned {
    pub(crate) trx_sys: SyncQuiescentGuard<TransactionSystem>,
    pub(crate) purge_tx: Sender<Purge>,
    pub(crate) io_threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) gc_threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) purge_threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) shutdown_started: AtomicBool,
}

impl TransactionSystem {
    #[inline]
    pub(crate) fn new(
        config: TrxSysConfig,
        catalog: QuiescentGuard<Catalog>,
        log_partitions: Vec<CachePadded<LogPartition>>,
        initial_ts: TrxID,
    ) -> Self {
        debug_assert!((MIN_SNAPSHOT_TS..MAX_SNAPSHOT_TS).contains(&initial_ts));
        TransactionSystem {
            ts: CachePadded::new(AtomicU64::new(initial_ts)),
            global_visible_sts: CachePadded::new(AtomicU64::new(initial_ts)),
            rr_partition_id: CachePadded::new(AtomicUsize::new(0)),
            log_partitions: CachePadded::new(log_partitions.into_boxed_slice()),
            config: CachePadded::new(config),
            catalog: CachePadded::new(catalog),
            storage_poisoned: CachePadded::new(AtomicBool::new(false)),
            storage_poison_err: CachePadded::new(Mutex::new(None)),
        }
    }

    /// Returns the first fatal storage poison error, if runtime admission has been poisoned.
    #[inline]
    pub fn storage_poison_error(&self) -> Option<Error> {
        if !self.storage_poisoned.load(Ordering::Acquire) {
            return None;
        }
        let guard = self.storage_poison_err.lock();
        debug_assert!(
            guard.is_some(),
            "storage poison flag published before poison error was recorded"
        );
        guard.as_ref().cloned()
    }

    /// Returns `Err` once a fatal storage failure poisoned runtime admission.
    #[inline]
    pub fn ensure_runtime_healthy(&self) -> Result<()> {
        match self.storage_poison_error() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Records the first fatal storage poison source and returns the shared poison error.
    #[inline]
    pub fn poison_storage(&self, source: StoragePoisonSource) -> Error {
        let err = Error::StorageEnginePoisoned(source);
        {
            let mut guard = self.storage_poison_err.lock();
            if guard.is_none() {
                *guard = Some(err.clone());
            }
        }
        self.storage_poisoned.swap(true, Ordering::AcqRel);
        self.storage_poison_error().unwrap_or(err)
    }

    /// Create a new transaction.
    #[inline]
    pub fn begin_trx(&self, session_state: Arc<SessionState>) -> ActiveTrx {
        // Assign log partition index so current transaction will stick
        // to certain log partition for commit.
        let log_no = self.next_log_no();
        let partition = &*self.log_partitions[log_no];
        let gc_no = partition.next_gc_no();
        let gc_bucket = &partition.gc_buckets[gc_no];
        // Add to active sts list.
        let mut g = gc_bucket.active_sts_list.lock();
        // With bucket lock, we can make sure all transactions are ordered by STS.
        let sts = self.ts.fetch_add(1, Ordering::SeqCst);
        let trx_id = sts | (1 << 63);
        debug_assert!(sts < MAX_SNAPSHOT_TS);
        debug_assert!(trx_id >= MIN_ACTIVE_TRX_ID);
        g.insert(sts);
        if g.len() == 1 {
            // Only when the previous list is empty, we should update min_active_sts
            // as STS of current transaction.
            // In this case, current value of min_active_sts should be MAX.
            debug_assert!(gc_bucket.min_active_sts.load(Ordering::Relaxed) == MAX_SNAPSHOT_TS);
            gc_bucket.min_active_sts.store(sts, Ordering::Relaxed);
        }
        drop(g); // release bucket lock.
        ActiveTrx::new(session_state, trx_id, sts, log_no, gc_no)
    }

    /// Returns next log(partition) number.
    #[inline]
    fn next_log_no(&self) -> usize {
        if self.config.log_partitions == 1 {
            0
        } else {
            self.rr_partition_id.fetch_add(1, Ordering::Relaxed) % self.config.log_partitions
        }
    }

    #[inline]
    pub fn begin_sys_trx(&self) -> SysTrx {
        SysTrx {
            redo: RedoLogs::default(),
        }
    }

    /// Commit an active transaction.
    /// The commit process is implemented as group commit.
    /// If multiple transactions are being committed at the same time, one of them
    /// will become leader of the commit group. Others become followers waiting for
    /// leader to persist log and backfill CTS.
    /// This strategy can largely reduce logging IO, therefore improve throughput.
    #[inline]
    pub async fn commit(&self, trx: ActiveTrx) -> Result<TrxID> {
        if let Err(err) = self.ensure_runtime_healthy() {
            self.rollback(trx).await?;
            return Err(err);
        }
        // Prepare redo log first, this may take some time,
        // so keep it out of lock scope, and we can fill cts after the lock is held.
        let partition = &*self.log_partitions[trx.log_no()];
        let prepared_trx = trx.prepare();
        if !prepared_trx.require_ordered_commit() {
            // No runtime effects means there is no CTS to publish and no commit
            // order to preserve. Effects without redo still enter group commit
            // because readers, sessions, and GC depend on ordered CTS backfill.
            self.discard_unordered_prepared(prepared_trx);
            return Ok(0);
        }
        // start group commit
        partition.commit(prepared_trx, &self.ts, true).await
    }

    #[inline]
    pub fn commit_sys(&self, trx: SysTrx) -> Result<TrxID> {
        self.ensure_runtime_healthy()?;
        if trx.redo.is_empty() {
            // System transaction does not hold any active start timestamp
            // so we can just drop it if there is no change to replay.
            return Ok(0);
        }
        // System transactions are always submitted to first log partition and
        // use the no-wait piggyback flow intentionally. They publish internal
        // state that becomes globally visible once the redo group commits, and
        // they are not modeled as session-bound transactions that can later be
        // rolled back through the normal user transaction API.
        const LOG_NO: usize = 0;
        let partition = &*self.log_partitions[LOG_NO];
        let prepared_trx = trx.prepare();
        partition.commit_no_wait(prepared_trx, &self.ts)
    }

    /// Rollback active transaction.
    #[inline]
    pub async fn rollback(&self, mut trx: ActiveTrx) -> Result<()> {
        let sts = trx.sts();
        let log_no = trx.log_no();
        let gc_no = trx.gc_no();
        let pool_guards = trx
            .pool_guards()
            .expect("transaction rollback requires session pool guards")
            .clone();
        let mut table_cache = TableCache::new(&self.catalog);
        if trx
            .index_undo_mut()
            .rollback(&mut table_cache, &pool_guards, sts)
            .await
            .is_err()
        {
            trx.discard_after_fatal_rollback();
            return Err(self.poison_storage(StoragePoisonSource::RollbackAccess));
        }
        if trx
            .row_undo_mut()
            .rollback(&mut table_cache, &pool_guards, Some(sts))
            .await
            .is_err()
        {
            trx.discard_after_fatal_rollback();
            return Err(self.poison_storage(StoragePoisonSource::RollbackAccess));
        }
        trx.effects_mut().clear_for_rollback();
        self.log_partitions[log_no].gc_buckets[gc_no].gc_analyze_rollback(sts);
        if let Some(s) = trx.take_session() {
            s.rollback();
        }
        Ok(())
    }

    /// Discard a prepared transaction that has no runtime effects.
    ///
    /// This path does not assign a CTS. Transactions with volatile effects but
    /// no log still require ordered commit and must not route here.
    #[inline]
    fn discard_unordered_prepared(&self, mut trx: PreparedTrx) {
        debug_assert!(!trx.require_durability());
        debug_assert!(!trx.require_ordered_commit());
        let payload = trx
            .payload
            .take()
            .expect("prepared no-op cleanup requires user transaction payload");
        self.log_partitions[payload.log_no].gc_buckets[payload.gc_no]
            .gc_analyze_rollback(payload.sts);
        if let Some(s) = trx.session.take() {
            s.rollback();
        }
    }

    /// Returns statistics of group commit.
    #[inline]
    pub fn trx_sys_stats(&self) -> TrxSysStats {
        let mut stats = TrxSysStats::default();
        for partition in &*self.log_partitions {
            stats.trx_count += partition.stats.trx_count.load(Ordering::Relaxed);
            stats.commit_count += partition.stats.commit_count.load(Ordering::Relaxed);
            stats.log_bytes += partition.stats.log_bytes.load(Ordering::Relaxed);
            stats.sync_count += partition.stats.sync_count.load(Ordering::Relaxed);
            stats.sync_nanos += partition.stats.sync_nanos.load(Ordering::Relaxed);
            let io_stats = partition.io_backend_stats();
            stats.io_submit_and_wait_count += io_stats.submit_and_wait_calls;
            stats.io_submit_and_wait_nanos += io_stats.submit_and_wait_nanos;
            stats.purge_trx_count += partition.stats.purge_trx_count.load(Ordering::Relaxed);
            stats.purge_row_count += partition.stats.purge_row_count.load(Ordering::Relaxed);
            stats.purge_index_count += partition.stats.purge_index_count.load(Ordering::Relaxed);
        }
        stats
    }

    /// Returns global visible snapshot timestamp.
    #[inline]
    pub fn global_visible_sts(&self) -> TrxID {
        self.global_visible_sts.load(Ordering::Relaxed)
    }

    /// Update global visible snapshot timestamp.
    #[inline]
    pub fn update_global_visible_sts(&self, sts: TrxID) {
        debug_assert!({
            let curr_sts = self.global_visible_sts.load(Ordering::Relaxed);
            sts >= curr_sts
        });
        self.global_visible_sts.store(sts, Ordering::SeqCst)
    }

    /// Start background GC threads.
    #[inline]
    pub(crate) fn start_gc_threads(
        trx_sys: QuiescentGuard<Self>,
        gc_rxs: Vec<Receiver<GC>>,
        purge_chan: Sender<Purge>,
    ) -> Vec<JoinHandle<()>> {
        let trx_sys = trx_sys.into_sync();
        let mut handles = Vec::with_capacity(gc_rxs.len());
        for (idx, gc_rx) in gc_rxs.into_iter().enumerate() {
            let thread_name = format!("GC-Thread-{idx}");
            let task_trx_sys = trx_sys.clone();
            let task_purge_chan = purge_chan.clone();
            let handle = thread::spawn_named(thread_name, move || {
                let partition = &*task_trx_sys.log_partitions[idx];
                partition.gc_loop(gc_rx, task_purge_chan);
            });
            handles.push(handle);
        }
        handles
    }

    /// Start background IO threads.
    #[inline]
    pub(crate) fn start_io_threads(trx_sys: QuiescentGuard<Self>) -> Vec<JoinHandle<()>> {
        let trx_sys = trx_sys.into_sync();
        let mut handles = Vec::with_capacity(trx_sys.log_partitions.len());
        for idx in 0..trx_sys.log_partitions.len() {
            let thread_name = format!("IO-Thread-{idx}");
            let task_trx_sys = trx_sys.clone();
            let handle = thread::spawn_named(thread_name, move || {
                let partition = &*task_trx_sys.log_partitions[idx];
                partition.io_loop(&task_trx_sys, &task_trx_sys.config);
            });
            handles.push(handle);
        }
        handles
    }

    #[inline]
    pub fn log_reader(&self, log_file_path: impl AsRef<Path>) -> Result<MmapLogReader> {
        MmapLogReader::new(
            log_file_path,
            self.config.max_io_size.as_u64() as usize,
            self.config.log_file_max_size.as_u64() as usize,
            self.config.max_io_size.as_u64() as usize * LOG_HEADER_PAGES,
        )
    }

    /// Returns the global ordered-completion watermark `W` from all partitions.
    ///
    /// This is used as the upper bound for scanning durable redo. No-log
    /// commit barriers can advance this runtime watermark, but recovery still
    /// seeds future timestamps only from checkpoint metadata, table roots, and
    /// real redo headers.
    #[inline]
    pub fn persisted_watermark_cts(&self) -> TrxID {
        self.log_partitions
            .iter()
            .map(|partition| partition.persisted_cts.load(Ordering::Acquire))
            .min()
            .unwrap_or(MIN_SNAPSHOT_TS)
    }

    #[inline]
    pub(crate) fn catalog_checkpoint_scan_config(&self) -> Result<CatalogCheckpointScanConfig> {
        Ok(CatalogCheckpointScanConfig {
            file_prefix: self.config.file_prefix()?,
            log_partitions: self.config.log_partitions,
            io_depth_per_log: self.config.io_depth_per_log,
            log_file_max_size: self.config.log_file_max_size.as_u64() as usize,
            max_io_size: self.config.max_io_size.as_u64() as usize,
        })
    }
}

#[derive(Default)]
pub struct TrxSysStats {
    pub commit_count: usize,
    pub trx_count: usize,
    pub log_bytes: usize,
    pub sync_count: usize,
    pub sync_nanos: usize,
    /// Number of backend submit-or-wait calls observed across redo workers.
    ///
    /// On `libaio`, one logical IO commonly contributes separate submit and
    /// wait syscalls, so this count can be roughly doubled compared with
    /// `io_uring` for serialized workloads.
    pub io_submit_and_wait_count: usize,
    /// Total non-overlapping nanoseconds spent in backend submit-or-wait calls.
    pub io_submit_and_wait_nanos: usize,
    pub purge_trx_count: usize,
    pub purge_row_count: usize,
    pub purge_index_count: usize,
}

impl Component for TransactionSystem {
    type Config = TrxSysConfig;
    type Owned = Self;
    type Access = QuiescentGuard<Self>;

    const NAME: &'static str = "trx_sys";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let meta_pool = registry.dependency::<MetaPool>()?;
        let index_pool = registry.dependency::<IndexPool>()?;
        let mem_pool = registry.dependency::<MemPool>()?;
        let table_fs = registry.dependency::<FileSystem>()?;
        let disk_pool = registry.dependency::<DiskPool>()?;
        let catalog = registry.dependency::<Catalog>()?;

        let (trx_sys, startup) = config
            .prepare(
                meta_pool.clone_inner(),
                index_pool.clone_inner(),
                mem_pool.clone_inner(),
                table_fs,
                disk_pool.clone_inner(),
                catalog,
            )
            .await?;
        registry.register::<Self>(trx_sys)?;
        shelf.put::<TransactionSystemWorkers>(startup)?;
        TransactionSystemWorkers::build((), registry, shelf.scope::<TransactionSystemWorkers>())
            .await
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

impl Component for TransactionSystemWorkers {
    type Config = ();
    type Owned = TransactionSystemWorkersOwned;
    type Access = ();

    const NAME: &'static str = "trx_sys_workers";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let trx_sys = registry.dependency::<TransactionSystem>()?;
        let startup = shelf
            .take::<TransactionSystem>()
            .ok_or(Error::InvalidState)?;
        registry.register::<Self>(startup.start(trx_sys))
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    #[inline]
    fn shutdown(component: &Self::Owned) {
        if component.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }

        let log_partitions = &*component.trx_sys.log_partitions;
        for partition in log_partitions {
            {
                let mut group_commit_g = partition.group_commit.lock();
                group_commit_g.queue.push_back(Commit::Shutdown);
                if group_commit_g.queue.len() == 1 {
                    partition.group_commit.notify_one();
                }
            }
            let _ = partition.gc_chan.send(GC::Stop);
        }

        let io_threads = { mem::take(&mut *component.io_threads.lock()) };
        for handle in io_threads {
            handle.join().unwrap();
        }
        let gc_threads = { mem::take(&mut *component.gc_threads.lock()) };
        for handle in gc_threads {
            handle.join().unwrap();
        }

        let _ = component.purge_tx.send(Purge::Stop);
        let purge_threads = { mem::take(&mut *component.purge_threads.lock()) };
        for handle in purge_threads {
            handle.join().unwrap();
        }

        for partition in log_partitions {
            let mut group_commit_g = partition.group_commit.lock();
            let Some(log_file) = group_commit_g.log_file.take() else {
                continue;
            };
            drop(log_file);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::tests::table2;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig};
    use crate::table::TableAccess;
    use crate::value::Val;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_transaction_system() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(128usize * 1024 * 1024)
                        .max_file_size(256usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_trx"))
                .build()
                .await
                .unwrap();
            let mut session = engine.try_new_session().unwrap();
            {
                let trx = session.try_begin_trx().unwrap().unwrap();
                let _ = smol::block_on(trx.commit());
            }
            {
                let engine = engine.new_ref().unwrap();
                std::thread::spawn(move || {
                    let mut session = engine.try_new_session().unwrap();
                    let trx = session.try_begin_trx().unwrap().unwrap();
                    let _ = smol::block_on(trx.commit());
                })
                .join()
                .unwrap();
            }

            drop(session);
            drop(engine);
        })
    }

    #[test]
    fn test_poison_storage_records_error_before_publishing_flag() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(128usize * 1024 * 1024)
                        .max_file_size(256usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_poison_lock_order"))
                .build()
                .await
                .unwrap();

            let trx_sys = engine.trx_sys.clone();
            let blocked = trx_sys.storage_poison_err.lock();
            let started = Arc::new(AtomicBool::new(false));
            let finished = Arc::new(AtomicBool::new(false));

            let worker_started = Arc::clone(&started);
            let worker_finished = Arc::clone(&finished);
            let worker_trx_sys = trx_sys.clone();
            let handle = std::thread::spawn(move || {
                worker_started.store(true, Ordering::Release);
                let err = worker_trx_sys.poison_storage(StoragePoisonSource::RedoSubmit);
                worker_finished.store(true, Ordering::Release);
                err
            });

            while !started.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            for _ in 0..20 {
                assert!(
                    !trx_sys.storage_poisoned.load(Ordering::Acquire),
                    "poison flag must not publish before poison error is recorded"
                );
                assert!(
                    !finished.load(Ordering::Acquire),
                    "poison call should remain blocked while poison error lock is held"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(trx_sys.storage_poison_error().is_none());

            drop(blocked);

            let err = handle.join().unwrap();
            assert!(matches!(
                err,
                Error::StorageEnginePoisoned(StoragePoisonSource::RedoSubmit)
            ));
            assert!(trx_sys.storage_poisoned.load(Ordering::Acquire));
            assert!(matches!(
                trx_sys.storage_poison_error(),
                Some(Error::StorageEnginePoisoned(
                    StoragePoisonSource::RedoSubmit
                ))
            ));
            assert!(matches!(
                trx_sys.ensure_runtime_healthy(),
                Err(Error::StorageEnginePoisoned(
                    StoragePoisonSource::RedoSubmit
                ))
            ));

            drop(trx_sys);
            drop(engine);
        });
    }

    #[test]
    fn test_poison_storage_concurrent_callers_share_first_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(128usize * 1024 * 1024)
                        .max_file_size(256usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_poison_concurrent"))
                .build()
                .await
                .unwrap();

            let trx_sys = engine.trx_sys.clone();
            let barrier = Arc::new(Barrier::new(3));

            let worker_a_barrier = Arc::clone(&barrier);
            let worker_a_trx_sys = trx_sys.clone();
            let worker_a = std::thread::spawn(move || {
                worker_a_barrier.wait();
                worker_a_trx_sys.poison_storage(StoragePoisonSource::RedoSubmit)
            });

            let worker_b_barrier = Arc::clone(&barrier);
            let worker_b_trx_sys = trx_sys.clone();
            let worker_b = std::thread::spawn(move || {
                worker_b_barrier.wait();
                worker_b_trx_sys.poison_storage(StoragePoisonSource::RedoSync)
            });

            barrier.wait();

            let err_a = worker_a.join().unwrap();
            let err_b = worker_b.join().unwrap();
            let stored = trx_sys.storage_poison_error().unwrap();
            let err_a_source = match err_a {
                Error::StorageEnginePoisoned(source) => source,
                other => panic!("expected poison error, got {other:?}"),
            };
            let err_b_source = match err_b {
                Error::StorageEnginePoisoned(source) => source,
                other => panic!("expected poison error, got {other:?}"),
            };
            let stored_source = match stored {
                Error::StorageEnginePoisoned(source) => source,
                other => panic!("expected poison error, got {other:?}"),
            };

            assert!(trx_sys.storage_poisoned.load(Ordering::Acquire));
            assert_eq!(err_a_source, err_b_source);
            assert_eq!(stored_source, err_a_source);
            assert!(matches!(
                trx_sys.ensure_runtime_healthy(),
                Err(Error::StorageEnginePoisoned(source)) if source == stored_source
            ));
            assert!(matches!(
                stored_source,
                StoragePoisonSource::RedoSubmit | StoragePoisonSource::RedoSync
            ));

            drop(trx_sys);
            drop(engine);
        });
    }

    #[test]
    fn test_log_rotate() {
        // 2000 rows, 200 bytes each row, 4M log file size.
        // log file is 1MB, so it will rotate at least 4 times.
        // Due to alignment of direct IO, the write amplification might
        // be higher and produce more files.
        const COUNT: usize = 2000;
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(128usize * 1024 * 1024)
                        .max_file_size(256usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_partitions(1)
                        .log_file_stem("redo_rotate")
                        .log_file_max_size(1024u64 * 1024),
                )
                .build()
                .await
                .unwrap();
            let table_id = table2(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();

            let mut session = engine.try_new_session().unwrap();
            let s = [1u8; 196];
            for i in 0..COUNT {
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                let insert = vec![Val::from(i as i32), Val::from(&s[..])];
                let (ctx, effects) = stmt.ctx_and_effects_mut();
                let res = table.accessor().insert_mvcc(ctx, effects, insert).await;
                assert!(res.is_ok());
                let trx = stmt.succeed();
                trx.commit().await.unwrap();
            }
            drop(session);
            drop(table);
            drop(engine);
        });
    }
}
