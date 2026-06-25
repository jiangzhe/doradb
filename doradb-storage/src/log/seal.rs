use super::{
    LogRequestId, LogRequestKind, LogRequestOwner, LogSync, LogWriteCompletion, LogWriteDriver,
    LogWriteKind, LogWriteSubmission, RedoGroupWriteMeta, RedoLogFile,
};
use crate::conf::TrxSysConfig;
use crate::error::{CompletionErrorKind, FatalError};
use crate::file::FileSyncer;
use crate::id::TrxID;
use crate::io::{DirectBuf, IOBackend, IOBuf};
use crate::log::format::{
    REDO_DEFAULT_DATA_START_OFFSET, REDO_SUPER_BLOCK_SLOT_SIZE, RedoSuperBlock,
    serialize_redo_super_block, slot_offset,
};
use crate::trx::sys::TransactionSystem;
use error_stack::Report;
use std::collections::VecDeque;
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::result::Result as StdResult;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogFileSealTarget {
    fd: RawFd,
    open_super_block: RedoSuperBlock,
}

impl RedoLogFile {
    /// Return the target information required to seal this file.
    #[inline]
    fn seal_target(&self) -> LogFileSealTarget {
        LogFileSealTarget {
            fd: self.as_raw_fd(),
            open_super_block: self.super_block(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LogFileSealAccumulator {
    file_seq: Option<u32>,
    durable_end_offset: usize,
    min_redo_cts: Option<TrxID>,
    max_redo_cts: Option<TrxID>,
}

impl LogFileSealAccumulator {
    #[inline]
    fn new() -> Self {
        Self {
            file_seq: None,
            durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET,
            min_redo_cts: None,
            max_redo_cts: None,
        }
    }

    #[inline]
    fn record_group(&mut self, write_meta: RedoGroupWriteMeta) {
        if let Some(file_seq) = self.file_seq {
            debug_assert_eq!(file_seq, write_meta.file_seq);
        } else {
            self.file_seq = Some(write_meta.file_seq);
        }
        self.durable_end_offset = write_meta.end_offset;
        self.min_redo_cts = Some(
            self.min_redo_cts
                .map_or(write_meta.min_redo_cts, |current| {
                    current.min(write_meta.min_redo_cts)
                }),
        );
        self.max_redo_cts = Some(
            self.max_redo_cts
                .map_or(write_meta.max_redo_cts, |current| {
                    current.max(write_meta.max_redo_cts)
                }),
        );
    }

    #[inline]
    fn redo_range(&self) -> Option<(TrxID, TrxID)> {
        self.min_redo_cts.zip(self.max_redo_cts)
    }
}

struct PendingSeal {
    request_id: LogRequestId,
}

/// Tracks sealed redo metadata and pending seal writes for rotated files.
pub(crate) struct LogFileSealer {
    accumulator: LogFileSealAccumulator,
    seal_writes: VecDeque<LogWriteSubmission>,
    inflight_seals: Vec<PendingSeal>,
    log_sync: LogSync,
}

impl LogFileSealer {
    /// Create a redo file sealer using the configured log-sync mode.
    #[inline]
    pub(crate) fn new(config: &TrxSysConfig) -> Self {
        LogFileSealer {
            accumulator: LogFileSealAccumulator::new(),
            seal_writes: VecDeque::new(),
            inflight_seals: Vec::new(),
            log_sync: config.log_sync,
        }
    }

    /// Record one durable redo-bearing group for the current file seal.
    #[inline]
    pub(crate) fn record_group(&mut self, write_meta: RedoGroupWriteMeta) {
        self.accumulator.record_group(write_meta);
    }

    #[inline]
    pub(super) fn has_pending(&self) -> bool {
        !self.seal_writes.is_empty() || !self.inflight_seals.is_empty()
    }

    #[inline]
    pub(super) fn driver_owned_len(&self) -> usize {
        self.inflight_seals.len()
    }

    #[inline]
    pub(super) fn fail_unsubmitted(&mut self) {
        let drained: Vec<_> = self.seal_writes.drain(..).collect();
        for submission in drained {
            submission.fail_unsubmitted_seal();
        }
    }

    /// Queue a rotated log file for inactive-slot sealing.
    #[inline]
    pub(crate) fn enqueue_rotated_file(
        &mut self,
        log_file: RedoLogFile,
        request_id: LogRequestId,
    ) -> StdResult<(), FatalError> {
        let accumulator = mem::replace(&mut self.accumulator, LogFileSealAccumulator::new());
        let mut submission = Self::prepare_seal_submission(log_file, accumulator)?;
        submission.attach_owner(LogRequestOwner::seal(request_id));
        self.seal_writes.push_back(submission);
        Ok(())
    }

    #[inline]
    fn prepare_seal_submission(
        log_file: RedoLogFile,
        accumulator: LogFileSealAccumulator,
    ) -> StdResult<LogWriteSubmission, FatalError> {
        if let Some(file_seq) = accumulator.file_seq {
            debug_assert_eq!(file_seq, log_file.file_seq());
        }
        let target = log_file.seal_target();
        let (fd, offset, buf) = build_sealed_header_write(target, accumulator)?;
        debug_assert_eq!(fd, log_file.as_raw_fd());
        Ok(LogWriteSubmission::seal(log_file, offset, buf))
    }

    #[inline]
    pub(super) fn stage_ready<B>(&mut self, write_driver: &mut LogWriteDriver<B>)
    where
        B: IOBackend,
    {
        while let Some(submission) = self.seal_writes.pop_front() {
            if write_driver.available_capacity() == 0 {
                self.seal_writes.push_front(submission);
                break;
            }
            let request_id = submission
                .owner()
                .expect("redo seal submission must carry a request owner")
                .request_id;
            if let Err(submission) = write_driver.push_write(submission) {
                self.seal_writes.push_front(submission);
                break;
            }
            self.inflight_seals.push(PendingSeal { request_id });
        }
    }

    #[inline]
    pub(super) fn handle_completion(
        &mut self,
        trx_sys: &TransactionSystem,
        request_id: LogRequestId,
        log_file: Box<RedoLogFile>,
        poison: Option<FatalError>,
    ) -> Option<Report<FatalError>> {
        let pos = self
            .inflight_seals
            .iter()
            .position(|seal| seal.request_id == request_id)
            .expect("redo seal completion must match inflight seal request id");
        self.inflight_seals.swap_remove(pos);
        if let Some(reason) = poison {
            return Some(trx_sys.poison_storage(reason));
        }
        if let Err(reason) = sync_sealed_header(self.log_sync, log_file.as_raw_fd()) {
            return Some(trx_sys.poison_storage(reason));
        }
        drop(log_file);
        None
    }

    /// Drain all pending seal writes and return the first fatal seal error.
    #[inline]
    pub(crate) fn finish_pending(
        &mut self,
        trx_sys: &TransactionSystem,
        write_driver: &mut LogWriteDriver<impl IOBackend>,
    ) -> Option<Report<FatalError>> {
        let mut first_err = None;
        while self.has_pending() {
            self.stage_ready(write_driver);
            write_driver.submit_ready();
            if write_driver.submitted_len() == 0 {
                if first_err.is_none() {
                    first_err = Some(trx_sys.poison_storage(FatalError::RedoWrite));
                }
                self.fail_unsubmitted();
                break;
            }
            let LogWriteCompletion {
                owner,
                kind,
                buf,
                poison,
            } = write_driver.wait_at_least_one();
            match kind {
                LogWriteKind::Seal { log_file } => {
                    drop(buf);
                    let owner = owner.expect("redo seal completion must carry request owner");
                    debug_assert_eq!(owner.kind, LogRequestKind::Seal);
                    if let Some(err) =
                        self.handle_completion(trx_sys, owner.request_id, log_file, poison)
                        && first_err.is_none()
                    {
                        first_err = Some(err);
                    }
                }
                LogWriteKind::Header { .. } | LogWriteKind::Group { .. } => {
                    unreachable!("redo file sealer must only drain seal completions");
                }
            }
        }
        first_err
    }

    /// Best-effort seal of the active file during clean shutdown.
    #[inline]
    pub(crate) fn seal_active_file_best_effort(
        &mut self,
        trx_sys: &TransactionSystem,
        write_driver: &mut LogWriteDriver<impl IOBackend>,
    ) {
        let target = {
            let group_commit_g = trx_sys.redo_log.group_commit.lock();
            group_commit_g
                .log_file
                .as_ref()
                .map(RedoLogFile::seal_target)
        };
        let Some(target) = target else {
            return;
        };
        if self
            .seal_file_target_best_effort(target, write_driver)
            .is_err()
        {
            trx_sys
                .redo_log
                .stats
                .seal_failure_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn seal_file_target_best_effort(
        &self,
        target: LogFileSealTarget,
        write_driver: &mut LogWriteDriver<impl IOBackend>,
    ) -> StdResult<(), FatalError> {
        debug_assert_eq!(write_driver.pending_len(), 0);
        debug_assert_eq!(write_driver.submitted_len(), 0);
        let (fd, offset, buf) = build_sealed_header_write(target, self.accumulator)?;
        let (submission, _completion) = LogWriteSubmission::header(fd, offset, buf);
        if let Err(submission) = write_driver.push_write(submission) {
            submission.fail_unsubmitted_header(FatalError::RedoWrite);
            return Err(FatalError::RedoWrite);
        }
        if write_driver.submit_ready() == 0 && write_driver.submitted_len() == 0 {
            return Err(FatalError::RedoWrite);
        }
        let LogWriteCompletion {
            owner: _,
            kind,
            buf,
            poison,
        } = write_driver.wait_at_least_one();
        // This best-effort shutdown path starts from an empty driver and
        // submits exactly one header write above. Any other completion kind
        // means the caller violated that isolation invariant.
        let LogWriteKind::Header { completion } = kind else {
            unreachable!("active file seal submits exactly one header write");
        };
        drop(buf);
        if let Some(reason) = poison {
            completion.complete(Err(CompletionErrorKind::report_fatal(
                reason,
                "redo sealed super-block write failed",
            )));
            return Err(reason);
        }
        completion.complete(Ok(()));
        sync_sealed_header(self.log_sync, fd)
    }
}

#[inline]
fn build_sealed_header_write(
    target: LogFileSealTarget,
    accumulator: LogFileSealAccumulator,
) -> StdResult<(RawFd, usize, DirectBuf), FatalError> {
    let slot_no = inactive_slot_no(target.open_super_block.slot_no);
    let sealed = RedoSuperBlock::sealed_from_open(
        &target.open_super_block,
        slot_no,
        accumulator.durable_end_offset,
        accumulator.redo_range(),
    )
    .map_err(|_| FatalError::RedoWrite)?;
    let mut buf = DirectBuf::zeroed(REDO_SUPER_BLOCK_SLOT_SIZE);
    serialize_redo_super_block(buf.as_bytes_mut(), &sealed).map_err(|_| FatalError::RedoWrite)?;
    Ok((target.fd, slot_offset(slot_no), buf))
}

#[inline]
fn sync_sealed_header(log_sync: LogSync, fd: RawFd) -> StdResult<(), FatalError> {
    let syncer = FileSyncer::from_borrowed_fd(fd);
    match log_sync {
        LogSync::Fsync => syncer.fsync().map_err(|_| FatalError::RedoSync),
        LogSync::Fdatasync => syncer.fdatasync().map_err(|_| FatalError::RedoSync),
        LogSync::None => Ok(()),
    }
}

#[inline]
fn inactive_slot_no(slot_no: u32) -> u32 {
    if slot_no == 0 { 1 } else { 0 }
}
