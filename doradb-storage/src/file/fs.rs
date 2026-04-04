use crate::buffer::guard::PageExclusiveGuard;
use crate::buffer::page::Page;
use crate::buffer::{
    BlockKey, EvictReadSubmission, EvictSubmission, EvictableBufferPool, EvictablePoolStateMachine,
    GlobalReadonlyBufferPool, PageID, PoolRequest, PoolRole, ReadSubmission, ReadonlyBufferPool,
};
use crate::catalog::table::TableMetadata;
use crate::catalog::{TableID, USER_OBJ_ID_START};
use crate::component::{Component, ComponentRegistry, ShelfScope, Supplier};
use crate::conf::FileSystemConfig;
use crate::conf::path::path_to_utf8;
use crate::error::{Error, FileKind, Result};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::file::multi_table_file::{CATALOG_MTB_PERSISTED_FILE_ID, MultiTableFile};
use crate::file::table_file::{ActiveRoot, TABLE_FILE_INITIAL_SIZE};
use crate::file::table_file::{MutableTableFile, TableFile};
use crate::file::{SparseFile, TableFsStateMachine, TableFsSubmission, WriteSubmission};
use crate::io::{
    AIOClient, AIOKind, AIOMessage, BackendToken, IOBackend, IOBackendStats, IOBackendStatsHandle,
    IOQueue, IOStateMachine, IOSubmission, Operation, StorageBackend,
};
use crate::notify::EventNotifyOnDrop;
use crate::quiescent::{QuiescentBox, QuiescentGuard, SyncQuiescentGuard};
use crate::thread;
use crate::{IndexPool, MemPool};
use flume::{Receiver, RecvError, Selector, SendError, TryRecvError};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::thread::JoinHandle;

const STORAGE_SERVICE_BACKLOG: usize = 10;
const STORAGE_BACKGROUND_WRITE_BURST_LIMIT: usize = 1;
const INVALID_SLOT: u32 = u32::MAX;

/// Pool-read lane payload for one evictable-pool page-in request.
pub(crate) enum PoolReadRequest {
    Mem(EvictReadSubmission),
    Index(EvictReadSubmission),
}

/// Pool background-write lane payload for one eviction writeback batch.
pub(crate) struct PoolBatchWriteRequest {
    page_guards: Vec<PageExclusiveGuard<Page>>,
    done_ev: Arc<EventNotifyOnDrop>,
}

impl PoolBatchWriteRequest {
    #[inline]
    fn new(page_guards: Vec<PageExclusiveGuard<Page>>, done_ev: Arc<EventNotifyOnDrop>) -> Self {
        Self {
            page_guards,
            done_ev,
        }
    }

    #[inline]
    fn into_pool_request(self) -> PoolRequest {
        PoolRequest::BatchWrite(self.page_guards, self.done_ev)
    }
}

/// Background-write lane payload shared by table-file writes and pool writeback.
///
/// Table writes and pool batch writes share one lane so the scheduler can keep
/// write-heavy traffic throttled independently from read traffic.
pub(crate) enum BackgroundWriteRequest {
    Table(WriteSubmission),
    MemPool(Box<PoolBatchWriteRequest>),
    IndexPool(Box<PoolBatchWriteRequest>),
}

/// Unified completion key space for the shared storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageKey {
    Table(BlockKey),
    Pool(PageID),
}

/// Domain-specific submission owner for one inflight backend operation.
enum StorageSubmissionKind {
    Table(TableFsSubmission),
    MemPool(EvictSubmission),
    IndexPool(EvictSubmission),
}

/// Backend-facing submission stored in the shared storage inflight table.
struct StorageSubmission {
    key: StorageKey,
    inner: StorageSubmissionKind,
}

impl StorageSubmission {
    #[inline]
    fn table(sub: TableFsSubmission) -> Self {
        Self {
            key: StorageKey::Table(*sub.key()),
            inner: StorageSubmissionKind::Table(sub),
        }
    }

    #[inline]
    fn mem_pool(sub: EvictSubmission) -> Self {
        Self {
            key: StorageKey::Pool(*sub.key()),
            inner: StorageSubmissionKind::MemPool(sub),
        }
    }

    #[inline]
    fn index_pool(sub: EvictSubmission) -> Self {
        Self {
            key: StorageKey::Pool(*sub.key()),
            inner: StorageSubmissionKind::IndexPool(sub),
        }
    }
}

impl IOSubmission for StorageSubmission {
    type Key = StorageKey;

    #[inline]
    fn key(&self) -> &Self::Key {
        &self.key
    }

    #[inline]
    fn operation(&mut self) -> &mut Operation {
        match &mut self.inner {
            StorageSubmissionKind::Table(sub) => sub.operation(),
            StorageSubmissionKind::MemPool(sub) => sub.operation(),
            StorageSubmissionKind::IndexPool(sub) => sub.operation(),
        }
    }
}

/// Shared storage state machine bound by `fs_workers`.
///
/// This adapter keeps one backend submission stream while delegating actual
/// request preparation and completion handling to the table-file and pool
/// state machines.
struct StorageStateMachine {
    table_fs: TableFsStateMachine,
    mem_pool: EvictablePoolStateMachine,
    index_pool: EvictablePoolStateMachine,
}

impl StorageStateMachine {
    /// Build the shared storage state machine once both pools are registered.
    #[inline]
    fn new(
        mem_pool: SyncQuiescentGuard<EvictableBufferPool>,
        mem_pool_file: SparseFile,
        index_pool: SyncQuiescentGuard<EvictableBufferPool>,
        index_pool_file: SparseFile,
    ) -> Self {
        Self {
            table_fs: TableFsStateMachine::new(),
            mem_pool: EvictablePoolStateMachine::new(mem_pool, mem_pool_file),
            index_pool: EvictablePoolStateMachine::new(index_pool, index_pool_file),
        }
    }

    /// Admit one table-read request into the backend-facing queue.
    #[inline]
    fn prepare_table_read_request(
        &mut self,
        req: ReadSubmission,
        max_new: usize,
        queue: &mut IOQueue<StorageSubmission>,
    ) -> Option<ReadSubmission> {
        let mut table_queue = IOQueue::with_capacity(max_new);
        let remainder = self
            .table_fs
            .prepare_read_request(req, max_new, &mut table_queue);
        for sub in table_queue.drain_to(table_queue.len()) {
            queue.push(StorageSubmission::table(sub));
        }
        remainder
    }

    /// Admit one pool page-in read into the backend-facing queue.
    #[inline]
    fn prepare_pool_read_request(
        &mut self,
        req: PoolReadRequest,
        max_new: usize,
        queue: &mut IOQueue<StorageSubmission>,
    ) -> Option<PoolReadRequest> {
        let mut pool_queue = IOQueue::with_capacity(max_new);
        let (role, req) = match req {
            PoolReadRequest::Mem(req) => (PoolRole::Mem, PoolRequest::Read(req)),
            PoolReadRequest::Index(req) => (PoolRole::Index, PoolRequest::Read(req)),
        };
        let remainder = match role {
            PoolRole::Mem => self.mem_pool.prepare_request(req, max_new, &mut pool_queue),
            PoolRole::Index => self
                .index_pool
                .prepare_request(req, max_new, &mut pool_queue),
            other => panic!("unsupported shared storage pool role: {other:?}"),
        }
        .map(|req| match req {
            PoolRequest::Read(req) => match role {
                PoolRole::Mem => PoolReadRequest::Mem(req),
                PoolRole::Index => PoolReadRequest::Index(req),
                other => panic!("unsupported shared storage pool role: {other:?}"),
            },
            PoolRequest::BatchWrite(_, _) => {
                unreachable!("pool read lane returned a batch write remainder")
            }
        });
        for sub in pool_queue.drain_to(pool_queue.len()) {
            match role {
                PoolRole::Mem => queue.push(StorageSubmission::mem_pool(sub)),
                PoolRole::Index => queue.push(StorageSubmission::index_pool(sub)),
                other => panic!("unsupported shared storage pool role: {other:?}"),
            }
        }
        remainder
    }

    /// Admit one background write into the backend-facing queue.
    #[inline]
    fn prepare_background_write_request(
        &mut self,
        req: BackgroundWriteRequest,
        max_new: usize,
        queue: &mut IOQueue<StorageSubmission>,
    ) -> Option<BackgroundWriteRequest> {
        match req {
            BackgroundWriteRequest::Table(req) => {
                let mut table_queue = IOQueue::with_capacity(max_new);
                let remainder = self
                    .table_fs
                    .prepare_write_request(req, max_new, &mut table_queue)
                    .map(BackgroundWriteRequest::Table);
                for sub in table_queue.drain_to(table_queue.len()) {
                    queue.push(StorageSubmission::table(sub));
                }
                remainder
            }
            BackgroundWriteRequest::MemPool(req) => {
                self.prepare_pool_batch_write_request(PoolRole::Mem, *req, max_new, queue)
            }
            BackgroundWriteRequest::IndexPool(req) => {
                self.prepare_pool_batch_write_request(PoolRole::Index, *req, max_new, queue)
            }
        }
    }

    /// Expand one pool writeback batch through the pool state machine.
    #[inline]
    fn prepare_pool_batch_write_request(
        &mut self,
        role: PoolRole,
        req: PoolBatchWriteRequest,
        max_new: usize,
        queue: &mut IOQueue<StorageSubmission>,
    ) -> Option<BackgroundWriteRequest> {
        let mut pool_queue = IOQueue::with_capacity(max_new);
        let remainder = match role {
            PoolRole::Mem => {
                self.mem_pool
                    .prepare_request(req.into_pool_request(), max_new, &mut pool_queue)
            }
            PoolRole::Index => {
                self.index_pool
                    .prepare_request(req.into_pool_request(), max_new, &mut pool_queue)
            }
            other => panic!("unsupported shared storage pool role: {other:?}"),
        }
        .map(|req| match req {
            PoolRequest::BatchWrite(page_guards, done_ev) => match role {
                PoolRole::Mem => BackgroundWriteRequest::MemPool(Box::new(
                    PoolBatchWriteRequest::new(page_guards, done_ev),
                )),
                PoolRole::Index => BackgroundWriteRequest::IndexPool(Box::new(
                    PoolBatchWriteRequest::new(page_guards, done_ev),
                )),
                other => panic!("unsupported shared storage pool role: {other:?}"),
            },
            PoolRequest::Read(_) => {
                unreachable!("background write lane returned a pool read remainder")
            }
        });
        for sub in pool_queue.drain_to(pool_queue.len()) {
            match role {
                PoolRole::Mem => queue.push(StorageSubmission::mem_pool(sub)),
                PoolRole::Index => queue.push(StorageSubmission::index_pool(sub)),
                other => panic!("unsupported shared storage pool role: {other:?}"),
            }
        }
        remainder
    }

    /// Forward submit-side bookkeeping to the owning domain state machine.
    #[inline]
    fn on_submit(&mut self, sub: &StorageSubmission) {
        match &sub.inner {
            StorageSubmissionKind::Table(sub) => self.table_fs.on_submit(sub),
            StorageSubmissionKind::MemPool(sub) => self.mem_pool.on_submit(sub),
            StorageSubmissionKind::IndexPool(sub) => self.index_pool.on_submit(sub),
        }
    }

    /// Route one backend completion back to the owning domain state machine.
    #[inline]
    fn on_complete(&mut self, sub: StorageSubmission, res: std::io::Result<usize>) -> AIOKind {
        match sub.inner {
            StorageSubmissionKind::Table(sub) => self.table_fs.on_complete(sub, res),
            StorageSubmissionKind::MemPool(sub) => self.mem_pool.on_complete(sub, res),
            StorageSubmissionKind::IndexPool(sub) => self.index_pool.on_complete(sub, res),
        }
    }
}

/// Scheduler state for the table-read ingress lane.
struct TableReadLane {
    rx: Receiver<AIOMessage<ReadSubmission>>,
    deferred_req: Option<ReadSubmission>,
    shutdown: bool,
}

/// Scheduler state for the pool-read ingress lane.
struct PoolReadLane {
    rx: Receiver<AIOMessage<PoolReadRequest>>,
    deferred_req: Option<PoolReadRequest>,
    shutdown: bool,
}

/// Scheduler state for the shared background-write ingress lane.
struct BackgroundWriteLane {
    rx: Receiver<AIOMessage<BackgroundWriteRequest>>,
    deferred_req: Option<BackgroundWriteRequest>,
    shutdown: bool,
}

/// Round-robin lane identity used by the shared storage scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageLaneId {
    TableReads,
    PoolReads,
    BackgroundWrites,
}

impl StorageLaneId {
    /// Return one full scheduler turn order starting from the current lane.
    #[inline]
    fn ordered_from(self) -> [StorageLaneId; 3] {
        match self {
            StorageLaneId::TableReads => [
                StorageLaneId::TableReads,
                StorageLaneId::PoolReads,
                StorageLaneId::BackgroundWrites,
            ],
            StorageLaneId::PoolReads => [
                StorageLaneId::PoolReads,
                StorageLaneId::BackgroundWrites,
                StorageLaneId::TableReads,
            ],
            StorageLaneId::BackgroundWrites => [
                StorageLaneId::BackgroundWrites,
                StorageLaneId::TableReads,
                StorageLaneId::PoolReads,
            ],
        }
    }

    /// Advance to the next lane in the fixed round-robin order.
    #[inline]
    fn next(self) -> Self {
        match self {
            StorageLaneId::TableReads => StorageLaneId::PoolReads,
            StorageLaneId::PoolReads => StorageLaneId::BackgroundWrites,
            StorageLaneId::BackgroundWrites => StorageLaneId::TableReads,
        }
    }
}

/// First ready item for one scheduler turn.
enum StorageLaneTurnStart {
    TableReadDeferred,
    TableReadMessage(AIOMessage<ReadSubmission>),
    TableReadDisconnected,
    PoolReadDeferred,
    PoolReadMessage(AIOMessage<PoolReadRequest>),
    PoolReadDisconnected,
    BackgroundWriteDeferred,
    BackgroundWriteMessage(AIOMessage<BackgroundWriteRequest>),
    BackgroundWriteDisconnected,
}

impl StorageLaneTurnStart {
    /// Identify which lane owns this turn start.
    #[inline]
    fn lane_id(&self) -> StorageLaneId {
        match self {
            StorageLaneTurnStart::TableReadDeferred
            | StorageLaneTurnStart::TableReadMessage(_)
            | StorageLaneTurnStart::TableReadDisconnected => StorageLaneId::TableReads,
            StorageLaneTurnStart::PoolReadDeferred
            | StorageLaneTurnStart::PoolReadMessage(_)
            | StorageLaneTurnStart::PoolReadDisconnected => StorageLaneId::PoolReads,
            StorageLaneTurnStart::BackgroundWriteDeferred
            | StorageLaneTurnStart::BackgroundWriteMessage(_)
            | StorageLaneTurnStart::BackgroundWriteDisconnected => StorageLaneId::BackgroundWrites,
        }
    }
}

/// Fixed three-lane scheduler for the shared storage worker.
///
/// Each lane keeps one deferred remainder so partially admitted work is
/// retried before the lane consumes newer messages.
struct StorageRequestScheduler {
    table_reads: TableReadLane,
    pool_reads: PoolReadLane,
    background_writes: BackgroundWriteLane,
    cursor: StorageLaneId,
}

impl StorageRequestScheduler {
    #[inline]
    fn all_shutdown(&self) -> bool {
        self.table_reads.shutdown && self.pool_reads.shutdown && self.background_writes.shutdown
    }

    #[inline]
    fn has_deferred(&self) -> bool {
        self.table_reads.deferred_req.is_some()
            || self.pool_reads.deferred_req.is_some()
            || self.background_writes.deferred_req.is_some()
    }

    #[inline]
    fn local_depth<S>(
        &self,
        queue: &IOQueue<S>,
        staged_slots_len: usize,
        submitted: usize,
    ) -> usize {
        queue.len() + staged_slots_len + submitted
    }

    /// Return the next ready scheduler turn without blocking.
    fn next_ready_turn_start(&mut self) -> Option<StorageLaneTurnStart> {
        for lane_id in self.cursor.ordered_from() {
            match lane_id {
                StorageLaneId::TableReads => {
                    if self.table_reads.deferred_req.is_some() {
                        return Some(StorageLaneTurnStart::TableReadDeferred);
                    }
                    if self.table_reads.shutdown {
                        continue;
                    }
                    match self.table_reads.rx.try_recv() {
                        Ok(msg) => return Some(StorageLaneTurnStart::TableReadMessage(msg)),
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => {
                            return Some(StorageLaneTurnStart::TableReadDisconnected);
                        }
                    }
                }
                StorageLaneId::PoolReads => {
                    if self.pool_reads.deferred_req.is_some() {
                        return Some(StorageLaneTurnStart::PoolReadDeferred);
                    }
                    if self.pool_reads.shutdown {
                        continue;
                    }
                    match self.pool_reads.rx.try_recv() {
                        Ok(msg) => return Some(StorageLaneTurnStart::PoolReadMessage(msg)),
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => {
                            return Some(StorageLaneTurnStart::PoolReadDisconnected);
                        }
                    }
                }
                StorageLaneId::BackgroundWrites => {
                    if self.background_writes.deferred_req.is_some() {
                        return Some(StorageLaneTurnStart::BackgroundWriteDeferred);
                    }
                    if self.background_writes.shutdown {
                        continue;
                    }
                    match self.background_writes.rx.try_recv() {
                        Ok(msg) => {
                            return Some(StorageLaneTurnStart::BackgroundWriteMessage(msg));
                        }
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => {
                            return Some(StorageLaneTurnStart::BackgroundWriteDisconnected);
                        }
                    }
                }
            }
        }
        None
    }

    /// Wait for the next lane to produce work or disconnect.
    fn wait_turn_start(&mut self) -> Option<StorageLaneTurnStart> {
        if self.all_shutdown() {
            return None;
        }
        let mut selector = Selector::new();
        let mut active_lanes = 0usize;
        for lane_id in self.cursor.ordered_from() {
            match lane_id {
                StorageLaneId::TableReads if !self.table_reads.shutdown => {
                    active_lanes += 1;
                    selector = selector.recv(&self.table_reads.rx, |res| match res {
                        Ok(msg) => StorageLaneTurnStart::TableReadMessage(msg),
                        Err(RecvError::Disconnected) => StorageLaneTurnStart::TableReadDisconnected,
                    });
                }
                StorageLaneId::PoolReads if !self.pool_reads.shutdown => {
                    active_lanes += 1;
                    selector = selector.recv(&self.pool_reads.rx, |res| match res {
                        Ok(msg) => StorageLaneTurnStart::PoolReadMessage(msg),
                        Err(RecvError::Disconnected) => StorageLaneTurnStart::PoolReadDisconnected,
                    });
                }
                StorageLaneId::BackgroundWrites if !self.background_writes.shutdown => {
                    active_lanes += 1;
                    selector = selector.recv(&self.background_writes.rx, |res| match res {
                        Ok(msg) => StorageLaneTurnStart::BackgroundWriteMessage(msg),
                        Err(RecvError::Disconnected) => {
                            StorageLaneTurnStart::BackgroundWriteDisconnected
                        }
                    });
                }
                _ => {}
            }
        }
        if active_lanes == 0 {
            None
        } else {
            Some(selector.wait())
        }
    }

    /// Fill the backend-facing queue while respecting lane rotation and IO depth.
    fn fetch_reqs(
        &mut self,
        state_machine: &mut StorageStateMachine,
        queue: &mut IOQueue<StorageSubmission>,
        staged_slots_len: usize,
        submitted: usize,
        io_depth: usize,
        mut min_turns: usize,
    ) {
        loop {
            if self.local_depth(queue, staged_slots_len, submitted) >= io_depth {
                return;
            }

            let Some(turn_start) = self.next_ready_turn_start().or_else(|| {
                if min_turns == 0 {
                    None
                } else {
                    self.wait_turn_start()
                }
            }) else {
                return;
            };
            min_turns = min_turns.saturating_sub(1);
            self.process_turn(
                turn_start,
                state_machine,
                queue,
                staged_slots_len,
                submitted,
                io_depth,
            );
        }
    }

    /// Consume one scheduler turn for the selected lane.
    fn process_turn(
        &mut self,
        turn_start: StorageLaneTurnStart,
        state_machine: &mut StorageStateMachine,
        queue: &mut IOQueue<StorageSubmission>,
        staged_slots_len: usize,
        submitted: usize,
        io_depth: usize,
    ) {
        let lane_id = turn_start.lane_id();
        let headroom = io_depth - self.local_depth(queue, staged_slots_len, submitted);
        debug_assert!(headroom != 0);
        let mut budget = match lane_id {
            StorageLaneId::BackgroundWrites => headroom.min(STORAGE_BACKGROUND_WRITE_BURST_LIMIT),
            StorageLaneId::TableReads | StorageLaneId::PoolReads => headroom,
        };

        match turn_start {
            StorageLaneTurnStart::TableReadDeferred => {
                self.process_table_read_turn(None, state_machine, queue, &mut budget)
            }
            StorageLaneTurnStart::TableReadMessage(msg) => {
                self.process_table_read_turn(Some(msg), state_machine, queue, &mut budget)
            }
            StorageLaneTurnStart::TableReadDisconnected => {
                self.table_reads.shutdown = true;
            }
            StorageLaneTurnStart::PoolReadDeferred => {
                self.process_pool_read_turn(None, state_machine, queue, &mut budget)
            }
            StorageLaneTurnStart::PoolReadMessage(msg) => {
                self.process_pool_read_turn(Some(msg), state_machine, queue, &mut budget)
            }
            StorageLaneTurnStart::PoolReadDisconnected => {
                self.pool_reads.shutdown = true;
            }
            StorageLaneTurnStart::BackgroundWriteDeferred => {
                self.process_background_write_turn(None, state_machine, queue, &mut budget)
            }
            StorageLaneTurnStart::BackgroundWriteMessage(msg) => {
                self.process_background_write_turn(Some(msg), state_machine, queue, &mut budget)
            }
            StorageLaneTurnStart::BackgroundWriteDisconnected => {
                self.background_writes.shutdown = true;
            }
        }

        self.cursor = lane_id.next();
    }

    fn process_table_read_turn(
        &mut self,
        mut first_msg: Option<AIOMessage<ReadSubmission>>,
        state_machine: &mut StorageStateMachine,
        queue: &mut IOQueue<StorageSubmission>,
        budget: &mut usize,
    ) {
        while *budget != 0 {
            // Retry the deferred remainder before taking newer messages so each
            // lane preserves its own FIFO ordering.
            if let Some(req) = self.table_reads.deferred_req.take() {
                let queue_len = queue.len();
                self.table_reads.deferred_req =
                    state_machine.prepare_table_read_request(req, *budget, queue);
                let admitted = queue.len() - queue_len;
                debug_assert!(admitted <= *budget);
                *budget -= admitted;
                if self.table_reads.deferred_req.is_some() {
                    break;
                }
                continue;
            }

            let next_msg = match first_msg.take() {
                Some(msg) => Some(msg),
                None if self.table_reads.shutdown => None,
                None => match self.table_reads.rx.try_recv() {
                    Ok(msg) => Some(msg),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        self.table_reads.shutdown = true;
                        None
                    }
                },
            };
            let Some(msg) = next_msg else {
                break;
            };
            match msg {
                AIOMessage::Shutdown => {
                    self.table_reads.shutdown = true;
                    break;
                }
                AIOMessage::Req(req) => {
                    let queue_len = queue.len();
                    self.table_reads.deferred_req =
                        state_machine.prepare_table_read_request(req, *budget, queue);
                    let admitted = queue.len() - queue_len;
                    debug_assert!(admitted <= *budget);
                    *budget -= admitted;
                    if self.table_reads.deferred_req.is_some() {
                        break;
                    }
                }
            }
        }
    }

    fn process_pool_read_turn(
        &mut self,
        mut first_msg: Option<AIOMessage<PoolReadRequest>>,
        state_machine: &mut StorageStateMachine,
        queue: &mut IOQueue<StorageSubmission>,
        budget: &mut usize,
    ) {
        while *budget != 0 {
            // Retry the deferred remainder before taking newer messages so each
            // lane preserves its own FIFO ordering.
            if let Some(req) = self.pool_reads.deferred_req.take() {
                let queue_len = queue.len();
                self.pool_reads.deferred_req =
                    state_machine.prepare_pool_read_request(req, *budget, queue);
                let admitted = queue.len() - queue_len;
                debug_assert!(admitted <= *budget);
                *budget -= admitted;
                if self.pool_reads.deferred_req.is_some() {
                    break;
                }
                continue;
            }

            let next_msg = match first_msg.take() {
                Some(msg) => Some(msg),
                None if self.pool_reads.shutdown => None,
                None => match self.pool_reads.rx.try_recv() {
                    Ok(msg) => Some(msg),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        self.pool_reads.shutdown = true;
                        None
                    }
                },
            };
            let Some(msg) = next_msg else {
                break;
            };
            match msg {
                AIOMessage::Shutdown => {
                    self.pool_reads.shutdown = true;
                    break;
                }
                AIOMessage::Req(req) => {
                    let queue_len = queue.len();
                    self.pool_reads.deferred_req =
                        state_machine.prepare_pool_read_request(req, *budget, queue);
                    let admitted = queue.len() - queue_len;
                    debug_assert!(admitted <= *budget);
                    *budget -= admitted;
                    if self.pool_reads.deferred_req.is_some() {
                        break;
                    }
                }
            }
        }
    }

    fn process_background_write_turn(
        &mut self,
        mut first_msg: Option<AIOMessage<BackgroundWriteRequest>>,
        state_machine: &mut StorageStateMachine,
        queue: &mut IOQueue<StorageSubmission>,
        budget: &mut usize,
    ) {
        while *budget != 0 {
            // Retry the deferred remainder before taking newer messages so each
            // lane preserves its own FIFO ordering.
            if let Some(req) = self.background_writes.deferred_req.take() {
                let queue_len = queue.len();
                self.background_writes.deferred_req =
                    state_machine.prepare_background_write_request(req, *budget, queue);
                let admitted = queue.len() - queue_len;
                debug_assert!(admitted <= *budget);
                *budget -= admitted;
                if self.background_writes.deferred_req.is_some() {
                    break;
                }
                continue;
            }

            let next_msg = match first_msg.take() {
                Some(msg) => Some(msg),
                None if self.background_writes.shutdown => None,
                None => match self.background_writes.rx.try_recv() {
                    Ok(msg) => Some(msg),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        self.background_writes.shutdown = true;
                        None
                    }
                },
            };
            let Some(msg) = next_msg else {
                break;
            };
            match msg {
                AIOMessage::Shutdown => {
                    self.background_writes.shutdown = true;
                    break;
                }
                AIOMessage::Req(req) => {
                    let queue_len = queue.len();
                    self.background_writes.deferred_req =
                        state_machine.prepare_background_write_request(req, *budget, queue);
                    let admitted = queue.len() - queue_len;
                    debug_assert!(admitted <= *budget);
                    *budget -= admitted;
                    if self.background_writes.deferred_req.is_some() {
                        break;
                    }
                }
            }
        }
    }
}

/// One generation-tracked inflight slot entry.
enum StorageSlotEntry<T> {
    Occupied(T),
    Vacant(u32),
}

/// Slot metadata used to validate backend completion tokens.
struct StorageSlot<T> {
    generation: u32,
    entry: StorageSlotEntry<T>,
}

/// Token-indexed inflight table for the shared storage worker.
struct StorageInflightSlots<T> {
    slots: Vec<StorageSlot<T>>,
    free_head: u32,
}

impl<T> StorageInflightSlots<T> {
    /// Create one free-list-backed inflight slot table with fixed capacity.
    #[inline]
    fn new(capacity: usize) -> Self {
        assert!(capacity <= u32::MAX as usize);
        let mut slots = Vec::with_capacity(capacity);
        for idx in 0..capacity {
            let next = if idx + 1 < capacity {
                (idx + 1) as u32
            } else {
                INVALID_SLOT
            };
            slots.push(StorageSlot {
                generation: 0,
                entry: StorageSlotEntry::Vacant(next),
            });
        }
        Self {
            slots,
            free_head: if capacity == 0 { INVALID_SLOT } else { 0 },
        }
    }

    #[inline]
    fn has_vacant(&self) -> bool {
        self.free_head != INVALID_SLOT
    }

    /// Reserve one slot and return the backend token that names it.
    #[inline]
    fn reserve(&mut self) -> Option<(BackendToken, u32)> {
        let slot = self.free_head;
        if slot == INVALID_SLOT {
            return None;
        }
        let slot_ref = &self.slots[slot as usize];
        let StorageSlotEntry::Vacant(next) = slot_ref.entry else {
            unreachable!("free list head must be vacant");
        };
        self.free_head = next;
        Some((BackendToken::new(slot_ref.generation, slot), slot))
    }

    /// Fill a previously reserved slot with one inflight value.
    #[inline]
    fn occupy_reserved(&mut self, slot: u32, value: T) {
        let slot_ref = &mut self.slots[slot as usize];
        debug_assert!(matches!(slot_ref.entry, StorageSlotEntry::Vacant(_)));
        slot_ref.entry = StorageSlotEntry::Occupied(value);
    }

    #[inline]
    fn get_mut(&mut self, slot: u32) -> &mut T {
        match &mut self.slots[slot as usize].entry {
            StorageSlotEntry::Occupied(value) => value,
            StorageSlotEntry::Vacant(_) => panic!("slot {slot} is not occupied"),
        }
    }

    /// Take the inflight value referenced by one backend completion token.
    #[inline]
    fn take(&mut self, token: BackendToken) -> T {
        let slot = token.slot_index();
        let generation = token.generation();
        let slot_ref = self.slots.get_mut(slot as usize).unwrap_or_else(|| {
            panic!(
                "completion references invalid inflight slot: token={}",
                token.raw()
            )
        });
        assert!(
            slot_ref.generation == generation,
            "completion references stale inflight token: token={} slot_generation={}",
            token.raw(),
            slot_ref.generation
        );
        let prev_head = self.free_head;
        let entry = std::mem::replace(&mut slot_ref.entry, StorageSlotEntry::Vacant(prev_head));
        slot_ref.generation = slot_ref.generation.wrapping_add(1);
        self.free_head = slot;
        match entry {
            StorageSlotEntry::Occupied(value) => value,
            StorageSlotEntry::Vacant(_) => panic!(
                "completion references vacant inflight slot: token={}",
                token.raw()
            ),
        }
    }
}

/// Prepared backend state retained until the submission completes.
struct StorageInflightEntry<S, P> {
    submission: S,
    _prepared: P,
    submitted: bool,
}

/// Late-bound builder for the shared storage worker.
///
/// The filesystem creates lane clients immediately, but the worker is bound only
/// after `fs_workers` can see the registered mem/index pools.
pub(crate) struct StorageIOWorkerBuilder<B = StorageBackend> {
    backend: B,
    table_reads_rx: Receiver<AIOMessage<ReadSubmission>>,
    pool_reads_rx: Receiver<AIOMessage<PoolReadRequest>>,
    background_writes_rx: Receiver<AIOMessage<BackgroundWriteRequest>>,
}

impl<B> StorageIOWorkerBuilder<B>
where
    B: IOBackend,
{
    /// Create the worker builder together with the three ingress lane clients.
    #[inline]
    fn new(
        backend: B,
    ) -> (
        Self,
        AIOClient<ReadSubmission>,
        AIOClient<PoolReadRequest>,
        AIOClient<BackgroundWriteRequest>,
    ) {
        let (table_reads_rx, table_reads) = AIOClient::bounded(STORAGE_SERVICE_BACKLOG);
        let (pool_reads_rx, pool_reads) = AIOClient::bounded(STORAGE_SERVICE_BACKLOG);
        let (background_writes_rx, background_writes) = AIOClient::bounded(STORAGE_SERVICE_BACKLOG);
        (
            Self {
                backend,
                table_reads_rx,
                pool_reads_rx,
                background_writes_rx,
            },
            table_reads,
            pool_reads,
            background_writes,
        )
    }

    /// Bind the late-resolved state machine and finalize the worker.
    #[inline]
    fn bind(self, state_machine: StorageStateMachine) -> StorageIOWorker<B> {
        let io_depth = self.backend.max_events();
        let submit_batch = self.backend.new_submit_batch();
        StorageIOWorker {
            backend: self.backend,
            scheduler: StorageRequestScheduler {
                table_reads: TableReadLane {
                    rx: self.table_reads_rx,
                    deferred_req: None,
                    shutdown: false,
                },
                pool_reads: PoolReadLane {
                    rx: self.pool_reads_rx,
                    deferred_req: None,
                    shutdown: false,
                },
                background_writes: BackgroundWriteLane {
                    rx: self.background_writes_rx,
                    deferred_req: None,
                    shutdown: false,
                },
                cursor: StorageLaneId::TableReads,
            },
            submitted: 0,
            staged_slots: VecDeque::new(),
            submit_batch,
            slots: StorageInflightSlots::new(io_depth),
            state_machine,
        }
    }
}

/// Backend-owning event loop for the shared storage service.
struct StorageIOWorker<B: IOBackend = StorageBackend> {
    backend: B,
    scheduler: StorageRequestScheduler,
    submitted: usize,
    staged_slots: VecDeque<u32>,
    submit_batch: B::SubmitBatch,
    slots: StorageInflightSlots<StorageInflightEntry<StorageSubmission, B::Prepared>>,
    state_machine: StorageStateMachine,
}

impl<B> StorageIOWorker<B>
where
    B: IOBackend,
    B::Prepared: Send + 'static,
    B::SubmitBatch: Send + 'static,
{
    /// Spawn the shared storage event loop on its dedicated thread.
    fn start_thread(self) -> JoinHandle<()>
    where
        B: Send + 'static,
    {
        thread::spawn_named("StorageIOWorker", move || self.run())
    }

    #[inline]
    fn io_depth(&self) -> usize {
        self.backend.max_events()
    }

    #[inline]
    fn local_depth(&self, queue: &IOQueue<StorageSubmission>) -> usize {
        queue.len() + self.staged_slots.len() + self.submitted
    }

    #[inline]
    fn fetch_reqs(&mut self, queue: &mut IOQueue<StorageSubmission>, min_reqs: usize) {
        let staged_slots_len = self.staged_slots.len();
        let submitted = self.submitted;
        let io_depth = self.io_depth();
        self.scheduler.fetch_reqs(
            &mut self.state_machine,
            queue,
            staged_slots_len,
            submitted,
            io_depth,
            min_reqs,
        );
    }

    /// Move queued submissions into backend-prepared staged slots.
    #[inline]
    fn prepare_staged(&mut self, queue: &mut IOQueue<StorageSubmission>) {
        while self.slots.has_vacant() {
            let Some(mut sub) = queue.pop_front() else {
                break;
            };
            let (token, slot) = self
                .slots
                .reserve()
                .expect("slot reservation should succeed while vacant slots exist");
            let mut prepared = self.backend.prepare(token, sub.operation());
            self.backend
                .push_prepared(&mut self.submit_batch, &mut prepared);
            self.slots.occupy_reserved(
                slot,
                StorageInflightEntry {
                    submission: sub,
                    _prepared: prepared,
                    submitted: false,
                },
            );
            self.staged_slots.push_back(slot);
        }
    }

    fn run(mut self) {
        let mut results = self.backend.new_events();
        let mut queue: IOQueue<StorageSubmission> = IOQueue::with_capacity(self.io_depth());
        loop {
            debug_assert!(queue.consistent());
            if self.scheduler.has_deferred() {
                self.fetch_reqs(&mut queue, 0);
            } else if !self.scheduler.all_shutdown() {
                if self.local_depth(&queue) == 0 {
                    self.fetch_reqs(&mut queue, 1);
                } else if self.local_depth(&queue) < self.io_depth() {
                    self.fetch_reqs(&mut queue, 0);
                }
            }
            self.prepare_staged(&mut queue);
            if !self.staged_slots.is_empty() {
                let limit = self.io_depth() - self.submitted;
                let submit_count = self.backend.submit_batch(&mut self.submit_batch, limit);
                #[cfg(test)]
                let hook = crate::io::current_storage_backend_test_hook();
                for _ in 0..submit_count {
                    let slot = self
                        .staged_slots
                        .pop_front()
                        .expect("submitted slots must have queued staging order");
                    let entry = self.slots.get_mut(slot);
                    debug_assert!(!entry.submitted);
                    #[cfg(test)]
                    if let Some(hook) = &hook {
                        let op = entry.submission.operation();
                        hook.on_submit(crate::io::StorageBackendOp::new(
                            op.kind(),
                            op.fd(),
                            op.offset(),
                        ));
                    }
                    // Submit-side bookkeeping runs only after the backend has
                    // accepted the prepared operation into this batch.
                    self.state_machine.on_submit(&entry.submission);
                    entry.submitted = true;
                }
                self.submitted += submit_count;
                debug_assert!(self.submitted <= self.io_depth());
            }

            if self.submitted != 0 {
                let completions = self.backend.wait_at_least(&mut results, 1);
                let completed_count = completions.len();
                for (token, res) in completions {
                    let entry = self.slots.take(token);
                    debug_assert!(entry.submitted);
                    #[cfg(test)]
                    let (entry, res) = {
                        let mut entry = entry;
                        let mut res = res;
                        if let Some(hook) = crate::io::current_storage_backend_test_hook() {
                            let op = entry.submission.operation();
                            hook.on_complete(
                                crate::io::StorageBackendOp::new(op.kind(), op.fd(), op.offset()),
                                &mut res,
                            );
                        }
                        (entry, res)
                    };
                    let _ = self.state_machine.on_complete(entry.submission, res);
                }
                self.submitted -= completed_count;
            }

            // Exit only after every lane is shut down and all deferred, queued,
            // staged, and inflight work has drained.
            if self.scheduler.all_shutdown()
                && self.submitted == 0
                && !self.scheduler.has_deferred()
                && self.staged_slots.is_empty()
                && queue.is_empty()
            {
                break;
            }
        }
    }
}

/// Build the filesystem facade together with the deferred shared worker state.
#[inline]
pub(crate) fn build_file_system(
    io_depth: usize,
    data_dir: PathBuf,
    catalog_file_name: String,
) -> Result<(FileSystem, StorageIOWorkerBuilder)> {
    let backend = StorageBackend::new(io_depth)?;
    let stats = backend.stats_handle();
    let (builder, table_reads, pool_reads, background_writes) =
        StorageIOWorkerBuilder::new(backend);
    Ok((
        FileSystem::new(
            table_reads,
            pool_reads,
            background_writes,
            stats,
            data_dir,
            catalog_file_name,
        ),
        builder,
    ))
}

/// Registered shutdown state for the running shared storage worker.
pub(crate) struct FileSystemWorkersOwned {
    fs: QuiescentGuard<FileSystem>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// Shared storage worker that serves table files plus both evictable pools.
pub(crate) struct FileSystemWorkers;

impl Component for FileSystemWorkers {
    type Config = ();
    type Owned = FileSystemWorkersOwned;
    type Access = ();

    const NAME: &'static str = "fs_workers";

    /// Bind and start the shared storage worker after all dependent pools exist.
    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let builder = shelf.take::<FileSystem>().ok_or(Error::InvalidState)?;
        let mem_pool_file = shelf.take::<MemPool>().ok_or(Error::InvalidState)?;
        let index_pool_file = shelf.take::<IndexPool>().ok_or(Error::InvalidState)?;

        let fs = registry.dependency::<FileSystem>()?;
        let mem_pool = registry.dependency::<MemPool>()?;
        let index_pool = registry.dependency::<IndexPool>()?;
        let handle = builder
            .bind(StorageStateMachine::new(
                mem_pool.clone_inner().into_sync(),
                mem_pool_file,
                index_pool.clone_inner().into_sync(),
                index_pool_file,
            ))
            .start_thread();
        registry.register::<Self>(FileSystemWorkersOwned {
            fs,
            handle: Mutex::new(Some(handle)),
        })
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    /// Stop ingress, then join the worker thread after all queued work drains.
    #[inline]
    fn shutdown(component: &Self::Owned) {
        component.fs.shutdown_io_clients();
        if let Some(handle) = component.handle.lock().take() {
            handle.join().unwrap();
        }
    }
}

/// Filesystem facade for all table-file and shared storage IO entrypoints.
///
/// `FileSystem` owns the three ingress lane clients plus the shared backend
/// stats handle. The backend itself remains owned by `fs_workers`.
pub struct FileSystem {
    table_reads: AIOClient<ReadSubmission>,
    pool_reads: AIOClient<PoolReadRequest>,
    background_writes: AIOClient<BackgroundWriteRequest>,
    io_backend_stats: IOBackendStatsHandle,
    data_dir: PathBuf,
    // Catalog multi-table file name.
    catalog_file_name: String,
}

impl FileSystem {
    /// Create the filesystem facade from prebuilt shared ingress clients.
    #[inline]
    pub(crate) fn new(
        table_reads: AIOClient<ReadSubmission>,
        pool_reads: AIOClient<PoolReadRequest>,
        background_writes: AIOClient<BackgroundWriteRequest>,
        io_backend_stats: IOBackendStatsHandle,
        data_dir: PathBuf,
        catalog_file_name: String,
    ) -> Self {
        FileSystem {
            table_reads,
            pool_reads,
            background_writes,
            io_backend_stats,
            data_dir,
            catalog_file_name,
        }
    }

    /// Clone the file-facing read and write clients for a table-file handle.
    #[inline]
    pub(crate) fn table_io_clients(
        &self,
    ) -> (AIOClient<ReadSubmission>, AIOClient<BackgroundWriteRequest>) {
        (self.table_reads.clone(), self.background_writes.clone())
    }

    #[inline]
    fn assert_pool_role(role: PoolRole, context: &'static str) {
        match role {
            PoolRole::Mem | PoolRole::Index => {}
            other => panic!("unsupported pool role {other:?} in {context}"),
        }
    }

    /// Send one pool page-in read onto the shared pool-read lane.
    #[inline]
    pub(crate) async fn send_pool_read_async(
        &self,
        role: PoolRole,
        req: EvictReadSubmission,
    ) -> StdResult<(), SendError<EvictReadSubmission>> {
        Self::assert_pool_role(role, "shared pool read dispatch");
        self.pool_reads
            .send_async(match role {
                PoolRole::Mem => PoolReadRequest::Mem(req),
                PoolRole::Index => PoolReadRequest::Index(req),
                other => panic!("unsupported pool role {other:?} in shared pool read dispatch"),
            })
            .await
            .map_err(|err| {
                let failed_req = match err.into_inner() {
                    PoolReadRequest::Mem(req) | PoolReadRequest::Index(req) => req,
                };
                SendError(failed_req)
            })
    }

    /// Send one pool writeback batch onto the shared background-write lane.
    #[inline]
    pub(crate) fn send_pool_batch_write(
        &self,
        role: PoolRole,
        page_guards: Vec<PageExclusiveGuard<Page>>,
        done_ev: Arc<EventNotifyOnDrop>,
    ) -> StdResult<(), (Vec<PageExclusiveGuard<Page>>, Arc<EventNotifyOnDrop>)> {
        Self::assert_pool_role(role, "shared pool write dispatch");
        self.background_writes
            .send(match role {
                PoolRole::Mem => BackgroundWriteRequest::MemPool(Box::new(
                    PoolBatchWriteRequest::new(page_guards, done_ev),
                )),
                PoolRole::Index => BackgroundWriteRequest::IndexPool(Box::new(
                    PoolBatchWriteRequest::new(page_guards, done_ev),
                )),
                other => panic!("unsupported pool role {other:?} in shared pool write dispatch"),
            })
            .map_err(|err| {
                let req = match err.into_inner() {
                    BackgroundWriteRequest::MemPool(req)
                    | BackgroundWriteRequest::IndexPool(req) => req,
                    BackgroundWriteRequest::Table(_) => {
                        unreachable!("shared pool write lane returned a table write request");
                    }
                };
                let req = *req;
                (req.page_guards, req.done_ev)
            })
    }

    /// Signal shutdown to all shared ingress lanes.
    #[inline]
    fn shutdown_io_clients(&self) {
        self.table_reads.shutdown();
        self.pool_reads.shutdown();
        self.background_writes.shutdown();
    }

    /// Create a new table file.
    /// If trunc is set to true, old file will be overwritten.
    /// Otherwise, an error will be returned if file already exists.
    #[inline]
    pub fn create_table_file(
        &self,
        table_id: TableID,
        metadata: Arc<TableMetadata>,
        trunc: bool,
    ) -> Result<MutableTableFile> {
        let file_path = self.table_file_path(table_id);
        let table_file = TableFile::create(
            &file_path,
            TABLE_FILE_INITIAL_SIZE,
            table_id,
            self.table_reads.clone(),
            self.background_writes.clone(),
            trunc,
        )?;
        let initial_pages = TABLE_FILE_INITIAL_SIZE / COW_FILE_PAGE_SIZE;
        let active_root = ActiveRoot::new(0, initial_pages, metadata);
        Ok(MutableTableFile::new(Arc::new(table_file), active_root))
    }

    /// Open an existing table file.
    #[inline]
    pub async fn open_table_file(
        &self,
        table_id: TableID,
        global_disk_pool: QuiescentGuard<GlobalReadonlyBufferPool>,
    ) -> Result<(Arc<TableFile>, ReadonlyBufferPool)> {
        let file_path = self.table_file_path(table_id);
        let (table_reads, background_writes) = self.table_io_clients();
        let table_file = Arc::new(TableFile::open(
            &file_path,
            table_id,
            table_reads,
            background_writes,
        )?);
        let disk_pool = ReadonlyBufferPool::new(
            table_id,
            FileKind::TableFile,
            Arc::clone(&table_file),
            global_disk_pool,
        );
        let active_root = table_file.load_active_root_from_pool(&disk_pool).await?;
        let old_root = table_file.swap_active_root(active_root);
        debug_assert!(old_root.is_none());
        Ok((table_file, disk_pool))
    }

    /// Build file path for a logical table id.
    ///
    /// User table ids use fixed-width hex naming (`<016x>.tbl`), while
    /// reserved/catalog ids keep compact decimal names.
    #[inline]
    pub fn table_file_path(&self, table_id: TableID) -> String {
        let file_name = if table_id >= USER_OBJ_ID_START {
            format!("{table_id:016x}.tbl")
        } else {
            format!("{table_id}.tbl")
        };
        path_to_string(&self.data_dir.join(file_name), "table file path")
    }

    /// Build absolute path for the unified catalog file (`*.mtb`).
    #[inline]
    pub fn catalog_mtb_file_path(&self) -> String {
        path_to_string(
            &self.data_dir.join(&self.catalog_file_name),
            "catalog multi-table file path",
        )
    }

    /// Returns one snapshot of backend-owned submit/wait activity.
    #[inline]
    pub fn io_backend_stats(&self) -> IOBackendStats {
        self.io_backend_stats.snapshot()
    }

    /// Open existing catalog multi-table file or create a new one.
    #[inline]
    pub async fn open_or_create_multi_table_file(
        &self,
        global_disk_pool: QuiescentGuard<GlobalReadonlyBufferPool>,
    ) -> Result<(Arc<MultiTableFile>, ReadonlyBufferPool)> {
        let (table_reads, background_writes) = self.table_io_clients();
        let mtb = MultiTableFile::open_or_create(
            self.catalog_mtb_file_path(),
            table_reads,
            background_writes,
        )
        .await?;
        let disk_pool = ReadonlyBufferPool::new(
            CATALOG_MTB_PERSISTED_FILE_ID,
            FileKind::CatalogMultiTableFile,
            Arc::clone(&mtb),
            global_disk_pool,
        );
        if mtb
            .active_root_ptr()
            .load(std::sync::atomic::Ordering::Acquire)
            .is_null()
        {
            let active_root = mtb.load_active_root_from_pool(&disk_pool).await?;
            let old_root = mtb.swap_active_root(active_root);
            debug_assert!(old_root.is_none());
        }
        Ok((mtb, disk_pool))
    }
}

impl Supplier<FileSystemWorkers> for FileSystem {
    type Provision = StorageIOWorkerBuilder;
}

impl Component for FileSystem {
    type Config = FileSystemConfig;
    type Owned = Self;
    type Access = QuiescentGuard<Self>;

    const NAME: &'static str = "fs";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let (fs, builder) = config.build_engine_parts()?;
        registry.register::<Self>(fs)?;
        shelf.put::<FileSystemWorkers>(builder)
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

#[inline]
fn path_to_string(path: &Path, field: &str) -> String {
    path_to_utf8(path, field)
        .expect("table file system paths are validated during construction")
        .to_owned()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::{IndexPoolWorkers, MemPoolWorkers, PoolRole};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, USER_OBJ_ID_START,
    };
    use crate::component::{DiskPoolConfig, IndexPoolConfig, MetaPoolConfig, RegistryBuilder};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::Error;
    use crate::value::ValKind;
    use crate::{DiskPool, IndexPool, MemPool, MetaPool};
    use std::ops::Deref;
    use tempfile::TempDir;

    const TEST_META_POOL_BYTES: usize = 32 * 1024 * 1024;
    const TEST_INDEX_POOL_BYTES: usize = 64 * 1024 * 1024;
    const TEST_INDEX_MAX_FILE_BYTES: usize = 128 * 1024 * 1024;
    const TEST_DATA_POOL_BYTES: usize = 64 * 1024 * 1024;
    const TEST_DATA_MAX_FILE_BYTES: usize = 128 * 1024 * 1024;
    const TEST_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;

    pub(crate) struct TestFileSystem {
        fs: Option<QuiescentGuard<FileSystem>>,
        registry: ComponentRegistry,
    }

    impl TestFileSystem {
        #[inline]
        pub(crate) fn shutdown(&self) {
            self.registry.shutdown_all();
        }
    }

    impl Deref for TestFileSystem {
        type Target = FileSystem;

        #[inline]
        fn deref(&self) -> &Self::Target {
            self.fs.as_ref().unwrap()
        }
    }

    impl Drop for TestFileSystem {
        #[inline]
        fn drop(&mut self) {
            self.fs.take();
            self.registry.shutdown_all();
        }
    }

    #[inline]
    pub(crate) fn io_backend_stats_handle_identity(fs: &FileSystem) -> usize {
        fs.io_backend_stats.identity()
    }

    #[inline]
    pub(crate) fn build_test_fs_owner_in(data_dir: &Path) -> Result<QuiescentBox<FileSystem>> {
        let (fs, _workers) = FileSystemConfig::default()
            .data_dir(data_dir)
            .readonly_buffer_size(TEST_READONLY_BUFFER_BYTES)
            .build_engine_parts()?;
        Ok(QuiescentBox::new(fs))
    }

    fn build_test_engine(storage_root: &Path, file: FileSystemConfig) -> Result<Engine> {
        smol::block_on(async {
            EngineConfig::default()
                .storage_root(storage_root)
                .meta_buffer(TEST_META_POOL_BYTES)
                .index_buffer(TEST_INDEX_POOL_BYTES)
                .index_max_file_size(TEST_INDEX_MAX_FILE_BYTES)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_DATA_POOL_BYTES)
                        .max_file_size(TEST_DATA_MAX_FILE_BYTES),
                )
                .file(file)
                .trx(TrxSysConfig::default().skip_recovery(true))
                .build()
                .await
        })
    }

    fn build_test_fs_with_config_in(
        data_dir: &Path,
        file: FileSystemConfig,
    ) -> Result<TestFileSystem> {
        smol::block_on(async {
            let file = file
                .data_dir(data_dir)
                .readonly_buffer_size(TEST_READONLY_BUFFER_BYTES);
            let mut builder = RegistryBuilder::new();
            builder
                .build::<DiskPool>(DiskPoolConfig::new(file.readonly_buffer_size))
                .await?;
            builder.build::<FileSystem>(file).await?;
            builder
                .build::<MetaPool>(MetaPoolConfig::new(TEST_META_POOL_BYTES))
                .await?;
            builder
                .build::<IndexPool>(IndexPoolConfig::new(
                    TEST_INDEX_POOL_BYTES,
                    data_dir.join("index.swp"),
                    TEST_INDEX_MAX_FILE_BYTES,
                ))
                .await?;
            builder
                .build::<MemPool>(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_DATA_POOL_BYTES)
                        .max_file_size(TEST_DATA_MAX_FILE_BYTES)
                        .data_swap_file(data_dir.join("data.swp")),
                )
                .await?;
            builder.build::<FileSystemWorkers>(()).await?;
            builder.build::<IndexPoolWorkers>(()).await?;
            builder.build::<MemPoolWorkers>(()).await?;
            let registry = builder.finish()?;
            let fs = registry.dependency::<FileSystem>()?;
            Ok(TestFileSystem {
                fs: Some(fs),
                registry,
            })
        })
    }

    #[inline]
    pub(crate) fn build_test_fs() -> (TempDir, TestFileSystem) {
        let temp_dir = TempDir::new().unwrap();
        let fs = build_test_fs_in(temp_dir.path());
        (temp_dir, fs)
    }

    #[inline]
    pub(crate) fn build_test_fs_in(data_dir: &Path) -> TestFileSystem {
        build_test_fs_with_config_in(
            data_dir,
            FileSystemConfig::default().readonly_buffer_size(TEST_READONLY_BUFFER_BYTES),
        )
        .unwrap()
    }

    #[test]
    fn test_table_file_system_shutdown_is_idempotent() {
        let (_temp_dir, fs) = build_test_fs();

        fs.shutdown();
        fs.shutdown();
    }

    #[test]
    fn test_user_table_file_uses_hex_name() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();

            let metadata = Arc::new(TableMetadata::new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U32,
                    ColumnAttributes::empty(),
                )],
                vec![IndexSpec::new(
                    "idx_pk",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            ));
            let mutable = fs
                .create_table_file(USER_OBJ_ID_START, Arc::clone(&metadata), false)
                .unwrap();
            let (table_file, old_root) = mutable.commit(1, false).await.unwrap();
            drop(old_root);

            let path = fs.table_file_path(USER_OBJ_ID_START);
            assert!(
                path.ends_with("0001000000000000.tbl"),
                "unexpected user table file path: {path}"
            );
            assert!(Path::new(&path).exists());

            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_catalog_file_name_default_and_custom_path() {
        let (temp_dir, fs) = build_test_fs();
        assert!(fs.catalog_mtb_file_path().ends_with("catalog.mtb"));
        drop(fs);

        let custom_dir = TempDir::new().unwrap();
        let fs = build_test_fs_with_config_in(
            custom_dir.path(),
            FileSystemConfig::default().catalog_file_name("cat_meta.mtb"),
        )
        .unwrap();
        assert!(fs.catalog_mtb_file_path().ends_with("cat_meta.mtb"));
        drop(fs);
        drop(temp_dir);
    }

    #[test]
    fn test_catalog_file_name_validation() {
        let temp_dir = TempDir::new().unwrap();
        let res = build_test_engine(
            temp_dir.path(),
            FileSystemConfig::default().catalog_file_name("catalog.bin"),
        );
        assert!(res.is_err());

        let res = build_test_engine(
            temp_dir.path(),
            FileSystemConfig::default().catalog_file_name("dir/catalog.mtb"),
        );
        assert!(res.is_err());

        let res = build_test_engine(
            temp_dir.path(),
            FileSystemConfig::default().catalog_file_name("../catalog.mtb"),
        );
        assert!(res.is_err());
    }

    #[test]
    fn test_data_dir_validation() {
        let temp_dir = TempDir::new().unwrap();
        let engine = build_test_engine(
            temp_dir.path(),
            FileSystemConfig::default().data_dir(PathBuf::new()),
        )
        .unwrap();
        assert!(
            engine
                .table_fs
                .catalog_mtb_file_path()
                .starts_with(temp_dir.path().to_str().unwrap())
        );
        drop(engine);

        let err = match build_test_engine(
            temp_dir.path(),
            FileSystemConfig::default().data_dir("../data"),
        ) {
            Ok(_) => panic!("expected invalid storage path"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }

    #[cfg(unix)]
    #[test]
    fn test_data_dir_rejects_non_utf8_path() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let path = PathBuf::from(OsString::from_vec(vec![b'd', b'a', b't', b'a', 0xff]));
        let temp_dir = TempDir::new().unwrap();
        let err =
            match build_test_engine(temp_dir.path(), FileSystemConfig::default().data_dir(path)) {
                Ok(_) => panic!("expected invalid storage path"),
                Err(err) => err,
            };
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }
}
