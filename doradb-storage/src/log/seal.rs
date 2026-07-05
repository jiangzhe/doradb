use super::{
    LogSync, LogWriteCompletion, LogWriteDriver, LogWriteKind, LogWriteSubmission,
    RedoGroupWriteMeta, RedoLogFile, RedoLogSealMetadata,
};
use crate::conf::TrxSysConfig;
use crate::error::{CompletionErrorKind, FatalError};
use crate::id::TrxID;
use crate::io::{DirectBuf, IOBackend, IOBuf, SubmitAttempt};
use crate::log::format::{
    REDO_DEFAULT_DATA_START_OFFSET, REDO_SUPER_BLOCK_SLOT_SIZE, RedoSuperBlock,
    serialize_redo_super_block, slot_offset,
};
use crate::trx::sys::TransactionSystem;
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
    fn from_metadata(metadata: RedoLogSealMetadata) -> Self {
        let (min_redo_cts, max_redo_cts) = match metadata.redo_range {
            Some((min_redo_cts, max_redo_cts)) => (Some(min_redo_cts), Some(max_redo_cts)),
            None => (None, None),
        };
        Self {
            file_seq: Some(metadata.file_seq),
            durable_end_offset: metadata.durable_end_offset,
            min_redo_cts,
            max_redo_cts,
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

/// Tracks sealed redo metadata and builds seal writes for rotated files.
pub(crate) struct LogFileSealer {
    accumulator: LogFileSealAccumulator,
    log_sync: LogSync,
}

impl LogFileSealer {
    /// Create a redo file sealer using the configured log-sync mode.
    #[inline]
    pub(crate) fn new(config: &TrxSysConfig) -> Self {
        LogFileSealer {
            accumulator: LogFileSealAccumulator::new(),
            log_sync: config.log_sync,
        }
    }

    /// Record one durable redo-bearing group for the current file seal.
    #[inline]
    pub(crate) fn record_group(&mut self, write_meta: RedoGroupWriteMeta) {
        self.accumulator.record_group(write_meta);
    }

    /// Prepare a prefix-owned rotated-file seal write.
    #[inline]
    pub(super) fn prepare_prefix_seal(
        &mut self,
        mut log_file: RedoLogFile,
    ) -> StdResult<LogWriteSubmission, FatalError> {
        let accumulator = match log_file.seal_metadata.take() {
            // Recovery attaches accepted-prefix metadata to the file so startup
            // sealing uses the same ordered prefix barrier as normal rotation.
            Some(metadata) => LogFileSealAccumulator::from_metadata(*metadata),
            None => mem::replace(&mut self.accumulator, LogFileSealAccumulator::new()),
        };
        prepare_seal_submission(log_file, accumulator)
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
        match write_driver
            .submit_ready()
            .map_err(|_| FatalError::RedoWrite)?
        {
            SubmitAttempt::Noop | SubmitAttempt::Retry(_) if write_driver.submitted_len() == 0 => {
                return Err(FatalError::RedoWrite);
            }
            SubmitAttempt::Submitted(_) | SubmitAttempt::Retry(_) | SubmitAttempt::Noop => {}
        }
        let LogWriteCompletion {
            owner: _,
            kind,
            buf,
            poison,
        } = write_driver
            .wait_at_least_one()
            .map_err(|_| FatalError::RedoWrite)?;
        // This best-effort shutdown path starts from an empty driver and
        // submits exactly one header write above. Any other completion kind
        // means the caller violated that isolation invariant.
        let LogWriteKind::Header { completion } = kind else {
            unreachable!("active file seal submits exactly one header write");
        };
        drop(buf.expect("active file seal header write must return a buffer"));
        if let Some(reason) = poison {
            completion.complete(Err(CompletionErrorKind::report_fatal(
                reason,
                "redo sealed super-block write failed",
            )));
            return Err(reason);
        }
        completion.complete(Ok(()));
        self.sync_file_target_best_effort(fd, write_driver)
    }

    #[inline]
    fn sync_file_target_best_effort(
        &self,
        fd: RawFd,
        write_driver: &mut LogWriteDriver<impl IOBackend>,
    ) -> StdResult<(), FatalError> {
        let Some(submission) = LogWriteSubmission::standalone_sync(fd, self.log_sync) else {
            return Ok(());
        };
        if write_driver.push_write(submission).is_err() {
            return Err(FatalError::RedoSync);
        }
        match write_driver
            .submit_ready()
            .map_err(|_| FatalError::RedoSync)?
        {
            SubmitAttempt::Noop | SubmitAttempt::Retry(_) if write_driver.submitted_len() == 0 => {
                return Err(FatalError::RedoSync);
            }
            SubmitAttempt::Submitted(_) | SubmitAttempt::Retry(_) | SubmitAttempt::Noop => {}
        }
        let LogWriteCompletion {
            owner: _,
            kind,
            buf,
            poison,
        } = write_driver
            .wait_at_least_one()
            .map_err(|_| FatalError::RedoSync)?;
        let LogWriteKind::StandaloneSync = kind else {
            unreachable!("active file seal submits exactly one standalone sync");
        };
        debug_assert!(buf.is_none());
        poison.map_or(Ok(()), Err)
    }
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
fn inactive_slot_no(slot_no: u32) -> u32 {
    if slot_no == 0 { 1 } else { 0 }
}
