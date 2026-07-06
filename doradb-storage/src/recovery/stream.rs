use crate::error::{DataIntegrityError, Error, InternalError, IoError, Result};
use crate::file::{SparseFile, UNTRACKED_FILE_ID};
use crate::id::TrxID;
use crate::io::{
    CompletedSubmission, DirectBuf, IOBackend, IOBuf, IOSubmission, Operation, STORAGE_SECTOR_SIZE,
    StorageBackend, SubmissionDriver, SubmitAttempt,
};
use crate::log::block_group::{TrxLog, block_count_for_payload};
use crate::log::format::{
    REDO_DEFAULT_DATA_START_OFFSET, RedoBlockHeader, RedoGroupStartExtension, RedoSuperBlock,
    is_zero_redo_block, select_redo_super_block,
};
use crate::log::{RedoLogFileDescriptor, next_redo_file_seq};
use crate::obs;
use crate::serde::Deser;
use crate::thread as doradb_thread;
use error_stack::Report;
use flume::{Receiver, SendTimeoutError, Sender, TryRecvError};
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{ErrorKind as IoErrorKind, Read, Result as IoResult};
use std::mem::take;
use std::os::fd::{AsRawFd, RawFd};
use std::panic::resume_unwind;
use std::path::PathBuf;
use std::thread::{JoinHandle, panicking};
use std::time::Duration;

const REDO_READ_AHEAD_SEND_TIMEOUT: Duration = Duration::from_millis(100);

/// Outcome of reading the next logical group from the redo stream.
enum ReadGroup {
    /// The current segment is exhausted; the next item should be another segment or stream end.
    SegmentExhausted,
    /// The replay stream reached logical EOF.
    ReplayEof(Option<UnsealedSegmentTerminal>),
    /// A transaction log iterator assembled from CRC32-validated redo blocks.
    Group(TrxLogIterator),
}

enum ContinuationRead {
    Appended,
    IncompleteTail,
}

enum GroupStartRead {
    Group(ValidatedGroupStart),
    UnsealedTail(UnsealedSegmentTerminalReason),
}

/// Terminal condition accepted for an unsealed redo segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnsealedSegmentTerminalReason {
    /// An all-zero block was found while looking for the next group start.
    ZeroTail,
    /// A malformed group-start block ended the accepted prefix.
    MalformedGroupStartTail,
    /// A multi-block group had an incomplete unsealed continuation tail.
    IncompleteContinuationTail,
    /// The scan reached the open file's configured end.
    ScanEndExhausted,
}

/// Accepted durable-prefix metadata for one unsealed segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnsealedSegmentTerminal {
    /// Full path to the redo log file.
    pub(crate) path: PathBuf,
    /// Sequence number parsed from the file name.
    pub(crate) file_seq: u32,
    /// Selected open super-block used as the repair base.
    pub(crate) super_block: RedoSuperBlock,
    /// Accepted durable end offset before the terminal tail.
    pub(crate) accepted_end_offset: usize,
    /// Real redo CTS range from structurally complete accepted groups.
    pub(crate) redo_range: Option<(TrxID, TrxID)>,
    /// Tail condition that ended the accepted prefix.
    pub(crate) terminal_reason: UnsealedSegmentTerminalReason,
}

/// Real CTS range advertised by one sealed redo segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RedoSegmentCtsRange {
    /// Lowest commit timestamp contained in the segment.
    pub(crate) min_cts: TrxID,
    /// Highest commit timestamp contained in the segment.
    pub(crate) max_cts: TrxID,
}

/// Catalog-checkpoint observation for one sealed redo segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogSafeRedoSegment {
    /// Redo file sequence number.
    pub(crate) file_seq: u32,
    /// Real redo CTS range, or `None` for a sealed empty file.
    pub(crate) redo_range: Option<RedoSegmentCtsRange>,
}

/// Metadata-only retained redo segment summary for truncation planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoRetentionSegment {
    /// Redo file sequence number.
    pub(crate) file_seq: u32,
    /// Seal state and optional real redo CTS range.
    pub(crate) state: RedoRetentionSegmentState,
}

/// Metadata-only retained redo segment state for truncation planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RedoRetentionSegmentState {
    /// File is sealed and contains no durable redo groups.
    SealedEmpty,
    /// File is sealed and contains durable redo groups in the advertised range.
    SealedNonEmpty(RedoSegmentCtsRange),
    /// File is not sealed and cannot be truncated.
    Unsealed,
}

/// Iterator over transaction redo records inside one validated redo group.
///
/// The group owns an assembled logical payload and enforces the commit
/// timestamp range advertised by the group-start block.
pub(crate) struct TrxLogIterator {
    /// Unconsumed logical body bytes.
    data: Vec<u8>,
    /// Current offset into `data`.
    offset: usize,
    /// Lowest commit timestamp accepted for body records.
    min_cts: TrxID,
    /// Highest commit timestamp accepted for body records.
    max_cts: TrxID,
}

impl TrxLogIterator {
    /// Create a group iterator from an assembled payload whose source blocks were validated.
    #[inline]
    fn new(data: Vec<u8>, min_cts: TrxID, max_cts: TrxID) -> Self {
        TrxLogIterator {
            data,
            offset: 0,
            min_cts,
            max_cts,
        }
    }

    /// Return the next transaction frame, or `None` once the body is exhausted.
    #[inline]
    pub(crate) fn try_next(&mut self) -> Result<Option<TrxLog>> {
        if self.offset == self.data.len() {
            return Ok(None);
        }
        let (offset, res) = TrxLog::deser(&self.data[..], self.offset)?;
        // Defensive progress check: a zero-byte parser result would make replay
        // loop forever on corrupt input.
        if offset == self.offset {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=redo-group, trx parser consumed zero bytes")
                .into());
        }
        let cts = res.header.cts;
        // The header range is a cheap cross-check that every transaction record
        // belongs to this group without requiring exact min/max recomputation.
        if cts < self.min_cts || cts > self.max_cts {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-group, cts={cts}, min_cts={}, max_cts={}",
                    self.min_cts, self.max_cts
                ))
                .into());
        }
        self.offset = offset;
        Ok(Some(res))
    }
}

/// Validated metadata for one redo log segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoLogSegment {
    /// Full path to the redo log file.
    pub(crate) path: PathBuf,
    /// Sequence number parsed from the file name.
    pub(crate) file_seq: u32,
    /// Newest checksum-valid super-block selected for this file.
    pub(crate) super_block: RedoSuperBlock,
}

impl RedoLogSegment {
    /// Build validated segment metadata by reading only the fixed super-block area.
    #[inline]
    pub(crate) fn from_descriptor(descriptor: RedoLogFileDescriptor) -> Result<Self> {
        Self::from_path(descriptor.path, descriptor.file_seq)
    }

    /// Build validated segment metadata for a path and expected sequence.
    #[inline]
    pub(crate) fn from_path(path: PathBuf, file_seq: u32) -> Result<Self> {
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len < REDO_DEFAULT_DATA_START_OFFSET as u64 {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "redo log file is shorter than fixed super-block area: path={}, len={}, data_start={}",
                    path.display(),
                    file_len,
                    REDO_DEFAULT_DATA_START_OFFSET
                ))
                .into());
        }
        let mut header = vec![0; REDO_DEFAULT_DATA_START_OFFSET];
        if let Err(err) = file.read_exact(&mut header) {
            if err.kind() == IoErrorKind::UnexpectedEof {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "redo log file ended while reading fixed super-block area: path={}, data_start={}",
                        path.display(),
                        REDO_DEFAULT_DATA_START_OFFSET
                    ))
                    .into());
            }
            return Err(err.into());
        }
        let super_block = select_redo_super_block(&header, file_seq)?;
        if super_block.file_max_size > file_len {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "redo super-block file_max_size exceeds file length: path={}, file_max_size={}, file_len={}",
                    path.display(),
                    super_block.file_max_size,
                    file_len
                ))
                .into());
        }
        Ok(Self {
            path,
            file_seq,
            super_block,
        })
    }

    /// Return true when this segment is sealed and contains no durable redo groups.
    #[inline]
    pub(crate) fn sealed_empty(&self) -> bool {
        self.super_block.sealed_empty()
    }

    /// Return the sealed redo range when this segment is sealed and non-empty.
    #[inline]
    pub(crate) fn sealed_redo_range(&self) -> Option<(TrxID, TrxID)> {
        self.super_block.sealed_redo_range()
    }
}

/// Planner that selects which discovered redo segments require normal replay.
pub(crate) struct RedoReplayPlanner {
    /// Discovered file names in ascending sequence order.
    discovered: Vec<RedoLogFileDescriptor>,
}

impl RedoReplayPlanner {
    /// Create a replay planner from discovered redo files.
    #[inline]
    pub(crate) fn new(discovered: Vec<RedoLogFileDescriptor>) -> Self {
        Self { discovered }
    }

    /// Build a startup recovery stream and post-replay repair policy.
    #[inline]
    pub(crate) fn plan_recovery(
        &self,
        floor: TrxID,
        read_depth: usize,
    ) -> Result<PlannedRedoRecovery> {
        let suffix = self.load_replay_suffix(floor)?;
        let planned = self.plan_replay_segments(&suffix, floor)?;

        Ok(PlannedRedoRecovery {
            skipped_max_recovered_cts: planned.skipped_max_recovered_cts,
            stream: RedoLogStream::from_planned_segments(planned.stream_segments, read_depth),
            repair_policy: planned.repair_policy,
        })
    }

    /// Build a catalog checkpoint scan stream and sealed-segment observations.
    #[inline]
    pub(crate) fn plan_catalog_scan(
        &self,
        floor: TrxID,
        read_depth: usize,
    ) -> Result<PlannedCatalogRedoScan> {
        let segments = self.load_all_segments()?;
        let suffix_start = replay_suffix_start(&segments, floor);
        let suffix = &segments[suffix_start..];
        let planned = self.plan_replay_segments(suffix, floor)?;

        let sealed_segments = segments
            .iter()
            .filter_map(sealed_catalog_segment_summary)
            .collect();

        Ok(PlannedCatalogRedoScan {
            stream: RedoLogStream::from_planned_segments(planned.stream_segments, read_depth),
            sealed_segments,
        })
    }

    #[inline]
    fn load_all_segments(&self) -> Result<Vec<RedoLogSegment>> {
        self.discovered
            .iter()
            .cloned()
            .map(RedoLogSegment::from_descriptor)
            .collect()
    }

    /// Build retained-segment summaries by validating only redo super-block metadata.
    #[inline]
    pub(crate) fn plan_retention_segments(&self) -> Result<Vec<RedoRetentionSegment>> {
        self.load_all_segments().map(|segments| {
            segments
                .iter()
                .map(retention_segment_summary)
                .collect::<Vec<_>>()
        })
    }

    #[inline]
    fn load_replay_suffix(&self, floor: TrxID) -> Result<Vec<RedoLogSegment>> {
        let mut suffix = Vec::new();
        for descriptor in self.discovered.iter().rev() {
            let segment = RedoLogSegment::from_descriptor(descriptor.clone())?;
            let stop = segment
                .sealed_redo_range()
                .is_some_and(|(min_redo_cts, _)| min_redo_cts < floor);
            suffix.push(segment);
            if stop {
                break;
            }
        }
        suffix.reverse();
        Ok(suffix)
    }

    #[inline]
    fn plan_replay_segments(
        &self,
        suffix: &[RedoLogSegment],
        floor: TrxID,
    ) -> Result<PlannedReplaySegments> {
        let repair_policy = self.repair_policy_for_segments(suffix)?;
        let replayable_segments = match repair_policy {
            RedoRecoveryRepairPolicy::FinalTwoUnsealed { .. } => {
                &suffix[..suffix.len().saturating_sub(1)]
            }
            RedoRecoveryRepairPolicy::CreateNext { .. }
            | RedoRecoveryRepairPolicy::SingleFinalUnsealed { .. } => suffix,
        };

        let mut skipped_max_recovered_cts = None::<TrxID>;
        let mut stream_segments = Vec::new();
        for segment in replayable_segments.iter().cloned() {
            if segment.sealed_empty() {
                continue;
            }
            if let Some((_, max_redo_cts)) = segment.sealed_redo_range()
                && max_redo_cts < floor
            {
                skipped_max_recovered_cts = Some(
                    skipped_max_recovered_cts
                        .map_or(max_redo_cts, |current| current.max(max_redo_cts)),
                );
                continue;
            }
            stream_segments.push(segment);
        }

        Ok(PlannedReplaySegments {
            repair_policy,
            stream_segments,
            skipped_max_recovered_cts,
        })
    }

    #[inline]
    fn repair_policy_for_segments(
        &self,
        segments: &[RedoLogSegment],
    ) -> Result<RedoRecoveryRepairPolicy> {
        let Some(last) = segments.last() else {
            return Ok(RedoRecoveryRepairPolicy::CreateNext { file_seq: 0 });
        };
        let unsealed_positions = segments
            .iter()
            .enumerate()
            .filter_map(|(idx, segment)| (!segment.super_block.is_sealed()).then_some(idx))
            .collect::<Vec<_>>();
        // Unsealed redo files are accepted only as the crash tail of the
        // selected replay suffix. A sealed file after an unsealed file proves
        // the older open header was stale or incomplete, so that shape is
        // corruption instead of a repair candidate.
        match unsealed_positions.as_slice() {
            [] => Ok(RedoRecoveryRepairPolicy::CreateNext {
                file_seq: next_redo_file_seq(last.file_seq)?,
            }),
            // Normal crash tail: the final file is open, so replay its accepted
            // prefix and then either seal it or recreate it after recovery.
            [idx] if *idx + 1 == segments.len() => {
                Ok(RedoRecoveryRepairPolicy::SingleFinalUnsealed {
                    file_seq: last.file_seq,
                })
            }
            // Rotation crash tail: the previous file may have durable redo that
            // was not sealed yet, while the newest file is the untrusted active
            // tail and must not be replayed.
            [older_idx, newest_idx]
                if *older_idx + 2 == segments.len() && *newest_idx + 1 == segments.len() =>
            {
                Ok(RedoRecoveryRepairPolicy::FinalTwoUnsealed {
                    older_file_seq: segments[*older_idx].file_seq,
                    newest_file_seq: segments[*newest_idx].file_seq,
                })
            }
            _ => Err(log_file_corrupted()),
        }
    }
}

struct PlannedReplaySegments {
    repair_policy: RedoRecoveryRepairPolicy,
    stream_segments: Vec<RedoLogSegment>,
    skipped_max_recovered_cts: Option<TrxID>,
}

/// Complete redo startup plan: stream plus post-replay repair policy.
pub(crate) struct PlannedRedoRecovery {
    /// Highest CTS from sealed skipped segments below the replay floor.
    pub(crate) skipped_max_recovered_cts: Option<TrxID>,
    /// Stream over the planned durable redo prefix.
    pub(crate) stream: RedoLogStream,
    /// Repair and writable-file policy to apply after stream replay.
    pub(crate) repair_policy: RedoRecoveryRepairPolicy,
}

/// Catalog checkpoint scan plan with sealed redo segment summaries.
pub(crate) struct PlannedCatalogRedoScan {
    /// Stream over the durable redo suffix needed by catalog scan.
    pub(crate) stream: RedoLogStream,
    /// Sealed retained redo segments observed from super-block metadata.
    pub(crate) sealed_segments: Vec<CatalogSafeRedoSegment>,
}

/// Post-replay recovery repair and runtime active-file policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RedoRecoveryRepairPolicy {
    /// No unsealed tail participates in startup; create this next file.
    CreateNext { file_seq: u32 },
    /// Exactly one final unsealed file was scanned.
    SingleFinalUnsealed { file_seq: u32 },
    /// A crash rotation left previous and newest files unsealed.
    FinalTwoUnsealed {
        older_file_seq: u32,
        newest_file_seq: u32,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RedoLogStreamState {
    Active,
    Ended,
    Failed,
}

/// Buffered stream of transaction redo records across a sequence of redo files.
pub(crate) struct RedoLogStream {
    /// Direct-IO read-ahead worker for the planned logical stream.
    reader: Option<RedoReadAheadHandle>,
    /// Parser state for the current redo segment.
    current_segment: Option<SegmentReadState>,
    /// Decoded records ready for recovery to consume.
    buffer: VecDeque<TrxLog>,
    /// Terminal state for the logical stream.
    state: RedoLogStreamState,
    /// Accepted-prefix metadata for scanned unsealed segments.
    unsealed_terminals: Vec<UnsealedSegmentTerminal>,
}

impl RedoLogStream {
    /// Create a stream over an already planned redo segment sequence.
    #[inline]
    fn from_planned_segments(segments: Vec<RedoLogSegment>, read_depth: usize) -> Self {
        let state = if segments.is_empty() {
            RedoLogStreamState::Ended
        } else {
            RedoLogStreamState::Active
        };
        let reader = if state == RedoLogStreamState::Ended {
            None
        } else {
            Some(RedoReadAheadWorker::spawn(segments, read_depth))
        };
        Self {
            reader,
            current_segment: None,
            buffer: VecDeque::new(),
            state,
            unsealed_terminals: Vec::new(),
        }
    }

    /// Refill the in-memory queue from the direct-IO stream.
    #[inline]
    async fn fill_buffer(&mut self) -> Result<()> {
        while self.state == RedoLogStreamState::Active {
            if let Some(mut iter) = self.read_next_group().await? {
                loop {
                    match iter.try_next() {
                        Ok(Some(res)) => self.buffer.push_back(res),
                        Ok(None) => return Ok(()),
                        Err(err) => return Err(self.fail_stream(err)),
                    }
                }
            }
        }
        Ok(())
    }

    /// Try to read the next transaction redo record, reading direct-IO blocks on demand.
    #[inline]
    pub(crate) async fn try_next(&mut self) -> Result<Option<TrxLog>> {
        if self.state == RedoLogStreamState::Failed {
            return Err(Self::read_after_failed_error());
        }
        match self.buffer.pop_front() {
            res @ Some(_) => Ok(res),
            None => {
                self.fill_buffer().await?;
                Ok(self.buffer.pop_front())
            }
        }
    }

    #[inline]
    async fn read_next_group(&mut self) -> Result<Option<TrxLogIterator>> {
        loop {
            match self.state {
                RedoLogStreamState::Active => {}
                RedoLogStreamState::Ended => return Ok(None),
                RedoLogStreamState::Failed => return Err(Self::read_after_failed_error()),
            }
            let mut state = match self.current_segment.take() {
                Some(state) => state,
                None => match self.receive_next_segment().await {
                    Ok(Some(state)) => state,
                    Ok(None) => return Ok(None),
                    Err(err) => return Err(self.fail_stream(err)),
                },
            };
            let read = match self.read_group_from_segment(&mut state).await {
                Ok(read) => read,
                Err(err) => return Err(self.fail_stream(err)),
            };
            match read {
                ReadGroup::Group(iter) => {
                    self.current_segment = Some(state);
                    return Ok(Some(iter));
                }
                ReadGroup::SegmentExhausted => {
                    self.current_segment = None;
                }
                ReadGroup::ReplayEof(terminal) => {
                    if let Some(terminal) = terminal {
                        self.unsealed_terminals.push(terminal);
                    }
                    self.end_stream();
                    return Ok(None);
                }
            }
        }
    }

    #[inline]
    async fn receive_next_segment(&mut self) -> Result<Option<SegmentReadState>> {
        match self.recv_item().await? {
            RedoReadItem::SegmentStart(segment) => {
                Ok(Some(SegmentReadState::from_segment(segment)?))
            }
            RedoReadItem::End => {
                self.end_stream();
                Ok(None)
            }
            RedoReadItem::Error(err) => Err(err),
            RedoReadItem::Block { buf, .. } => {
                self.recycle_buf(buf);
                Err(Report::new(InternalError::Generic)
                    .attach("redo read-ahead emitted block before segment start")
                    .into())
            }
            RedoReadItem::SegmentEnd { .. } => Err(Report::new(InternalError::Generic)
                .attach("redo read-ahead emitted segment end before segment start")
                .into()),
        }
    }

    #[inline]
    async fn read_group_from_segment(&mut self, state: &mut SegmentReadState) -> Result<ReadGroup> {
        if let Some(result) = state.read_exhausted_result()? {
            self.consume_segment_end(state).await?;
            return Ok(result);
        }

        let first_block = self
            .recv_expected_block(state.file_seq, state.offset)
            .await?;
        let group_start = match state.read_group_start(first_block.as_bytes()) {
            Ok(GroupStartRead::Group(group_start)) => group_start,
            Ok(GroupStartRead::UnsealedTail(reason)) => {
                let terminal = state.unsealed_terminal(reason);
                self.recycle_buf(first_block);
                return Ok(ReadGroup::ReplayEof(Some(terminal)));
            }
            Err(err) => {
                self.recycle_buf(first_block);
                return Err(err);
            }
        };

        let Some(group_end_offset) = group_start
            .block_count
            .checked_mul(state.log_block_size)
            .and_then(|group_len| state.offset.checked_add(group_len))
        else {
            self.recycle_buf(first_block);
            return Err(log_file_corrupted());
        };
        if group_end_offset > state.scan_end_offset {
            self.recycle_buf(first_block);
            if state.sealed.is_some() {
                return Err(log_file_corrupted());
            }
            return Ok(ReadGroup::ReplayEof(Some(state.unsealed_terminal(
                UnsealedSegmentTerminalReason::IncompleteContinuationTail,
            ))));
        }

        let mut payload = Vec::with_capacity(group_start.payload_len);
        if !append_block_payload(
            first_block.as_bytes(),
            state.log_block_size,
            group_start.header,
            RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE,
            group_start.block_count == 1,
            &mut payload,
        ) {
            self.recycle_buf(first_block);
            return Err(log_file_corrupted());
        }
        self.recycle_buf(first_block);

        for expected_idx in 1..group_start.block_count {
            let Some(block_offset) = expected_idx
                .checked_mul(state.log_block_size)
                .and_then(|delta| state.offset.checked_add(delta))
            else {
                return Err(log_file_corrupted());
            };
            if block_offset >= state.scan_end_offset {
                return Err(log_file_corrupted());
            }
            let block = self
                .recv_expected_block(state.file_seq, block_offset)
                .await?;
            let append_res = state.append_continuation_payload(
                block.as_bytes(),
                &mut payload,
                expected_idx,
                group_start.block_count,
            );
            self.recycle_buf(block);
            match append_res? {
                ContinuationRead::Appended => {}
                ContinuationRead::IncompleteTail => {
                    return Ok(ReadGroup::ReplayEof(Some(state.unsealed_terminal(
                        UnsealedSegmentTerminalReason::IncompleteContinuationTail,
                    ))));
                }
            }
        }

        if payload.len() != group_start.payload_len {
            return Err(log_file_corrupted());
        }
        state.finish_group_read(group_start, payload)
    }

    #[inline]
    async fn consume_segment_end(&mut self, state: &SegmentReadState) -> Result<()> {
        match self.recv_item().await? {
            RedoReadItem::SegmentEnd { file_seq } if file_seq == state.file_seq => Ok(()),
            RedoReadItem::SegmentEnd { .. } | RedoReadItem::SegmentStart(_) => {
                Err(log_file_corrupted())
            }
            RedoReadItem::Block { buf, .. } => {
                self.recycle_buf(buf);
                Err(log_file_corrupted())
            }
            RedoReadItem::End => Err(log_file_corrupted()),
            RedoReadItem::Error(err) => Err(err),
        }
    }

    #[inline]
    async fn recv_expected_block(
        &mut self,
        expected_file_seq: u32,
        expected_offset: usize,
    ) -> Result<DirectBuf> {
        match self.recv_item().await? {
            RedoReadItem::Block {
                file_seq,
                offset,
                buf,
            } if file_seq == expected_file_seq && offset == expected_offset => Ok(buf),
            RedoReadItem::Block { buf, .. } => {
                self.recycle_buf(buf);
                Err(log_file_corrupted())
            }
            RedoReadItem::SegmentEnd { .. } | RedoReadItem::SegmentStart(_) | RedoReadItem::End => {
                Err(log_file_corrupted())
            }
            RedoReadItem::Error(err) => Err(err),
        }
    }

    #[inline]
    async fn recv_item(&mut self) -> Result<RedoReadItem> {
        let reader = self.reader.as_ref().ok_or_else(|| {
            Error::from(
                Report::new(InternalError::Generic)
                    .attach("redo read-ahead receive before worker start"),
            )
        })?;
        reader.items.recv_async().await.map_err(|_| {
            Error::from(
                Report::new(InternalError::Generic).attach("redo read-ahead worker channel closed"),
            )
        })
    }

    #[inline]
    fn recycle_buf(&self, buf: DirectBuf) {
        if let Some(reader) = &self.reader {
            let _ = reader.recycle.try_send(buf);
        }
    }

    #[inline]
    fn stop_reader(&mut self) {
        if let Some(reader) = &self.reader {
            reader.stop();
        }
        self.reader.take();
    }

    #[inline]
    fn fail_stream(&mut self, err: Error) -> Error {
        self.buffer.clear();
        self.current_segment = None;
        self.stop_reader();
        self.state = RedoLogStreamState::Failed;
        err
    }

    #[inline]
    fn end_stream(&mut self) {
        self.current_segment = None;
        self.stop_reader();
        self.state = RedoLogStreamState::Ended;
    }

    #[inline]
    fn read_after_failed_error() -> Error {
        Report::new(InternalError::Generic)
            .attach("redo stream read after terminal error")
            .into()
    }

    /// Take accepted-prefix metadata for unsealed segments observed by this stream.
    #[inline]
    pub(crate) fn take_unsealed_terminals(&mut self) -> Vec<UnsealedSegmentTerminal> {
        take(&mut self.unsealed_terminals)
    }
}

/// Ordered handoff from the read-ahead worker to `RedoLogStream`.
///
/// The worker publishes one segment at a time as:
/// `SegmentStart`, zero or more contiguous `Block`s in ascending offset order,
/// then `SegmentEnd`. After all planned segments it publishes `End`. If the
/// worker hits an error, it publishes `Error` as the terminal item and exits;
/// the stream turns any read, protocol, or parse error into a failed terminal
/// state so later reads cannot advance to another segment or leak buffered
/// records.
enum RedoReadItem {
    /// Starts a new segment; no block or segment-end item for this segment may precede it.
    SegmentStart(RedoLogSegment),
    /// A direct-IO block for the active segment.
    ///
    /// Blocks are emitted with the same `file_seq` as the active segment and
    /// with contiguous offsets from `REDO_DEFAULT_DATA_START_OFFSET` up to the
    /// segment scan end. Out-of-order, duplicate, cross-segment, or unexpected
    /// blocks are protocol violations handled as stream errors by the consumer.
    Block {
        file_seq: u32,
        offset: usize,
        buf: DirectBuf,
    },
    /// Ends the active segment after its last emitted block.
    SegmentEnd { file_seq: u32 },
    /// Marks normal logical EOF after every planned segment has ended.
    End,
    /// Terminal worker failure; no later item is valid.
    Error(Error),
}

struct RedoReadAheadHandle {
    items: Receiver<RedoReadItem>,
    recycle: Sender<DirectBuf>,
    stop: Sender<()>,
    join: Option<JoinHandle<()>>,
}

impl RedoReadAheadHandle {
    #[inline]
    fn stop(&self) {
        let _ = self.stop.try_send(());
    }
}

impl Drop for RedoReadAheadHandle {
    #[inline]
    fn drop(&mut self) {
        let _ = self.stop.try_send(());
        if let Some(join) = self.join.take() {
            match join.join().inspect_err(|_| {
                obs::error!(
                    "event=worker_shutdown component=recovery worker=Redo-ReadAhead action=join result=error reason=panic"
                );
            }) {
                Ok(()) => {}
                Err(payload) => {
                    // Known read-ahead failures are emitted as
                    // RedoReadItem::Error. A join panic is an invariant
                    // failure. During unwinding we only log it to avoid
                    // aborting on a second panic from Drop.
                    if panicking() {
                        obs::error!(
                            "event=worker_shutdown component=recovery worker=Redo-ReadAhead action=join result=ignored reason=caller_panicking"
                        );
                    } else {
                        resume_unwind(payload);
                    }
                }
            }
        }
    }
}

struct RedoReadSubmission {
    file_seq: u32,
    offset: usize,
    operation: Operation,
}

impl RedoReadSubmission {
    #[inline]
    fn new(file_seq: u32, offset: usize, fd: RawFd, buf: DirectBuf) -> Self {
        Self {
            file_seq,
            offset,
            operation: Operation::pread_owned(fd, offset, buf),
        }
    }
}

impl IOSubmission for RedoReadSubmission {
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

struct RedoReadCompletion {
    file_seq: u32,
    offset: usize,
    expected_bytes: usize,
    actual_bytes: IoResult<usize>,
    buf: DirectBuf,
}

impl RedoReadCompletion {
    #[inline]
    fn validate(self) -> Result<(u32, usize, DirectBuf)> {
        let actual_bytes = self.actual_bytes.map_err(|err| {
            Error::from(IoError::report(err).attach(format!(
                "redo direct read failed: file_seq={:08x}, offset={}",
                self.file_seq, self.offset
            )))
        })?;
        if actual_bytes != self.expected_bytes {
            return Err(
                IoError::report_unexpected_eof(actual_bytes, self.expected_bytes)
                    .attach(format!(
                        "redo direct read short read: file_seq={:08x}, offset={}",
                        self.file_seq, self.offset
                    ))
                    .into(),
            );
        }
        Ok((self.file_seq, self.offset, self.buf))
    }
}

struct RedoReadAheadWorker {
    segments: Vec<RedoLogSegment>,
    read_depth: usize,
    items: Sender<RedoReadItem>,
    recycle: Receiver<DirectBuf>,
    stop: Receiver<()>,
    stop_requested: bool,
    free: Vec<DirectBuf>,
    free_block_size: Option<usize>,
}

impl RedoReadAheadWorker {
    #[inline]
    fn spawn(segments: Vec<RedoLogSegment>, read_depth: usize) -> RedoReadAheadHandle {
        let capacity = read_depth.max(1).saturating_mul(2).max(1);
        let (items_tx, items_rx) = flume::bounded(capacity);
        let (recycle_tx, recycle_rx) = flume::bounded(capacity);
        let (stop_tx, stop_rx) = flume::bounded(1);
        let join = doradb_thread::spawn_named("Redo-ReadAhead", move || {
            let mut worker = RedoReadAheadWorker {
                segments,
                read_depth,
                items: items_tx,
                recycle: recycle_rx,
                stop: stop_rx,
                stop_requested: false,
                free: Vec::with_capacity(capacity),
                free_block_size: None,
            };
            match worker.run() {
                Ok(true) => {
                    let _ = worker.send_item(RedoReadItem::End);
                }
                Ok(false) => {}
                Err(err) => {
                    obs::error!(
                        "event=worker_failure component=recovery worker=Redo-ReadAhead action=read_ahead result=error error={}",
                        err
                    );
                    let _ = worker.send_item(RedoReadItem::Error(err));
                }
            }
        });
        RedoReadAheadHandle {
            items: items_rx,
            recycle: recycle_tx,
            stop: stop_tx,
            join: Some(join),
        }
    }

    #[inline]
    fn run(&mut self) -> Result<bool> {
        let backend = StorageBackend::new(self.read_depth)?;
        let mut driver = SubmissionDriver::new(backend);
        for segment in take(&mut self.segments) {
            if self.stopped() {
                return Ok(false);
            }
            self.scan_segment(&mut driver, segment)?;
        }
        Ok(!self.stopped())
    }

    #[inline]
    fn scan_segment(
        &mut self,
        driver: &mut SubmissionDriver<RedoReadSubmission>,
        segment: RedoLogSegment,
    ) -> Result<()> {
        let file = open_direct_segment_file(&segment)?;
        let file_seq = segment.file_seq;
        let log_block_size = segment.super_block.log_block_size as usize;
        let scan_end_offset = segment_scan_end_offset(&segment)?;
        self.switch_block_size(log_block_size);
        if !self.send_item(RedoReadItem::SegmentStart(segment)) {
            return Ok(());
        }

        let mut next_submit_offset = REDO_DEFAULT_DATA_START_OFFSET;
        let mut next_emit_offset = REDO_DEFAULT_DATA_START_OFFSET;
        let mut completed = BTreeMap::<usize, DirectBuf>::new();
        while next_emit_offset < scan_end_offset || driver.pending_len() != 0 {
            self.drain_recycled();
            while next_submit_offset < scan_end_offset
                && !self.stopped()
                && driver.available_capacity() != 0
            {
                let buf = self.take_buf(log_block_size);
                let submission =
                    RedoReadSubmission::new(file_seq, next_submit_offset, file.as_raw_fd(), buf);
                if driver.push(submission).is_err() {
                    return Err(Report::new(InternalError::Generic)
                        .attach("redo read-ahead driver rejected submission despite capacity")
                        .into());
                }
                next_submit_offset =
                    next_submit_offset
                        .checked_add(log_block_size)
                        .ok_or_else(|| {
                            Error::from(
                                Report::new(DataIntegrityError::InvalidPayload)
                                    .attach("redo read-ahead offset overflow"),
                            )
                        })?;
            }
            let submit_attempt = match driver.submit_ready() {
                Ok(attempt) => attempt,
                Err(err) => return Err(self.handle_backend_progress_error(driver, err)),
            };
            let submit_retry_without_inflight = match submit_attempt {
                SubmitAttempt::Noop | SubmitAttempt::Submitted(_) => false,
                SubmitAttempt::Retry(reason) => {
                    if driver.submitted_len() == 0 {
                        obs::debug!(
                            "event=redo_readahead_backend_submit_retry component=recovery reason={} errno={} submitted={} pending={} action=backoff",
                            reason,
                            reason.raw_errno(),
                            driver.submitted_len(),
                            driver.pending_len()
                        );
                        if let Err(err) = driver.backoff_submit_retry_or_progress_error(reason) {
                            return Err(self.handle_backend_progress_error(driver, err));
                        }
                        true
                    } else {
                        obs::debug!(
                            "event=redo_readahead_backend_submit_retry component=recovery reason={} errno={} submitted={} pending={} action=wait",
                            reason,
                            reason.raw_errno(),
                            driver.submitted_len(),
                            driver.pending_len()
                        );
                        false
                    }
                }
            };
            if !self.emit_ready(
                file_seq,
                &mut next_emit_offset,
                log_block_size,
                &mut completed,
            ) {
                self.drain_driver(driver);
                return Ok(());
            }
            if self.stopped() {
                self.drain_driver(driver);
                return Ok(());
            }
            if driver.submitted_len() == 0 {
                if driver.pending_len() == 0 {
                    continue;
                }
                if !submit_retry_without_inflight {
                    driver.backoff_submit_retry();
                }
                continue;
            }
            let completed_submission = match driver.wait_at_least_one() {
                Ok(completed) => completed,
                Err(err) => return Err(self.handle_backend_progress_error(driver, err)),
            };
            let completion = take_read_completion(completed_submission)?;
            let (completed_file_seq, completed_offset, buf) = match completion.validate() {
                Ok(completed) => completed,
                Err(err) => {
                    self.drain_driver(driver);
                    return Err(err);
                }
            };
            if completed_file_seq != file_seq || completed.insert(completed_offset, buf).is_some() {
                self.drain_driver(driver);
                return Err(Report::new(InternalError::Generic)
                    .attach(format!(
                        "redo read-ahead duplicate or mismatched completion: file_seq={completed_file_seq:08x}, offset={completed_offset}"
                    ))
                    .into());
            }
        }
        if self.send_item(RedoReadItem::SegmentEnd { file_seq }) {
            self.drain_recycled();
        }
        Ok(())
    }

    #[inline]
    fn emit_ready(
        &mut self,
        file_seq: u32,
        next_emit_offset: &mut usize,
        log_block_size: usize,
        completed: &mut BTreeMap<usize, DirectBuf>,
    ) -> bool {
        while let Some(buf) = completed.remove(next_emit_offset) {
            let offset = *next_emit_offset;
            if !self.send_item(RedoReadItem::Block {
                file_seq,
                offset,
                buf,
            }) {
                return false;
            }
            *next_emit_offset += log_block_size;
        }
        true
    }

    #[inline]
    fn switch_block_size(&mut self, log_block_size: usize) {
        if self.free_block_size == Some(log_block_size) {
            return;
        }
        self.free.clear();
        self.free_block_size = Some(log_block_size);
        self.drain_recycled();
    }

    #[inline]
    fn take_buf(&mut self, log_block_size: usize) -> DirectBuf {
        self.drain_recycled();
        self.free
            .pop()
            .unwrap_or_else(|| DirectBuf::zeroed(log_block_size))
    }

    #[inline]
    fn drain_recycled(&mut self) {
        while let Ok(buf) = self.recycle.try_recv() {
            if Some(buf.capacity()) == self.free_block_size {
                self.free.push(buf);
            }
        }
    }

    #[inline]
    fn drain_driver<B>(&mut self, driver: &mut SubmissionDriver<RedoReadSubmission, B>) -> usize
    where
        B: IOBackend,
    {
        let mut quarantined = 0;
        while driver.pending_len() != 0 {
            match driver.submit_ready() {
                Ok(SubmitAttempt::Retry(reason)) if driver.submitted_len() == 0 => {
                    if driver
                        .backoff_submit_retry_or_progress_error(reason)
                        .is_err()
                    {
                        quarantined += self.quarantine_driver(driver);
                        break;
                    }
                    continue;
                }
                Ok(SubmitAttempt::Noop | SubmitAttempt::Submitted(_) | SubmitAttempt::Retry(_)) => {
                }
                Err(_) => {
                    quarantined += self.quarantine_driver(driver);
                    break;
                }
            }
            if driver.submitted_len() == 0 {
                driver.backoff_submit_retry();
                continue;
            }
            let Ok(completed) = driver.wait_at_least_one() else {
                quarantined += self.quarantine_driver(driver);
                break;
            };
            if let Ok(completion) = take_read_completion(completed) {
                drop(completion.buf);
            }
        }
        self.drain_recycled();
        quarantined
    }

    #[inline]
    fn handle_backend_progress_error<B>(
        &mut self,
        driver: &mut SubmissionDriver<RedoReadSubmission, B>,
        err: Report<IoError>,
    ) -> Error
    where
        B: IOBackend,
    {
        self.quarantine_driver(driver);
        backend_progress_error(err)
    }

    #[inline]
    fn quarantine_driver<B>(
        &mut self,
        driver: &mut SubmissionDriver<RedoReadSubmission, B>,
    ) -> usize
    where
        B: IOBackend,
    {
        let pending = driver.pending_len();
        let submitted = driver.submitted_len();
        let quarantined = driver.quarantine_owned_buffers();
        if quarantined != 0 {
            obs::error!(
                "event=redo_readahead_backend_quarantine component=recovery pending={} submitted={} quarantined_buffers={} action=quarantine",
                pending,
                submitted,
                quarantined
            );
        }
        quarantined
    }

    #[inline]
    fn send_item(&mut self, mut item: RedoReadItem) -> bool {
        loop {
            if self.stopped() {
                return false;
            }
            match self.items.send_timeout(item, REDO_READ_AHEAD_SEND_TIMEOUT) {
                Ok(()) => return true,
                Err(SendTimeoutError::Timeout(returned)) => {
                    item = returned;
                }
                Err(SendTimeoutError::Disconnected(_)) => return false,
            }
        }
    }

    #[inline]
    fn stopped(&mut self) -> bool {
        if self.stop_requested {
            return true;
        }
        match self.stop.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => {
                self.stop_requested = true;
                true
            }
            Err(TryRecvError::Empty) => false,
        }
    }
}

#[inline]
fn backend_progress_error(err: Report<IoError>) -> Error {
    err.attach("redo read-ahead backend progress failure")
        .into()
}

#[derive(Debug)]
struct SegmentReadState {
    path: PathBuf,
    file_seq: u32,
    super_block: RedoSuperBlock,
    log_block_size: usize,
    scan_end_offset: usize,
    sealed: Option<SealedReplayState>,
    offset: usize,
    /// Lowest CTS from structurally complete groups accepted in this segment.
    ///
    /// For unsealed files this becomes repair metadata: recovery writes the
    /// real accepted redo range into the sealed super-block after replay.
    accepted_min_cts: Option<TrxID>,
    /// Highest CTS from structurally complete groups accepted in this segment.
    ///
    /// Stays `None` with `accepted_min_cts` when the unsealed prefix contains
    /// no complete redo group, allowing recovery to recreate the empty tail.
    accepted_max_cts: Option<TrxID>,
}

impl SegmentReadState {
    #[inline]
    fn from_segment(segment: RedoLogSegment) -> Result<Self> {
        let scan_end_offset = segment_scan_end_offset(&segment)?;
        let sealed = segment
            .super_block
            .is_sealed()
            .then(|| SealedReplayState::new(segment.super_block.sealed_redo_range()));
        Ok(Self {
            path: segment.path,
            file_seq: segment.file_seq,
            super_block: segment.super_block.clone(),
            log_block_size: segment.super_block.log_block_size as usize,
            scan_end_offset,
            sealed,
            offset: REDO_DEFAULT_DATA_START_OFFSET,
            accepted_min_cts: None,
            accepted_max_cts: None,
        })
    }

    #[inline]
    fn validate_sealed_end(&self) -> bool {
        self.sealed.is_none_or(SealedReplayState::validate)
    }

    #[inline]
    fn read_exhausted_result(&self) -> Result<Option<ReadGroup>> {
        if self.offset < self.scan_end_offset {
            return Ok(None);
        }
        if !self.validate_sealed_end() {
            return Err(log_file_corrupted());
        }
        if self.sealed.is_none() {
            return Ok(Some(ReadGroup::ReplayEof(Some(self.unsealed_terminal(
                UnsealedSegmentTerminalReason::ScanEndExhausted,
            )))));
        }
        Ok(Some(ReadGroup::SegmentExhausted))
    }

    #[inline]
    fn read_group_start(&self, block: &[u8]) -> Result<GroupStartRead> {
        debug_assert!(self.offset.is_multiple_of(STORAGE_SECTOR_SIZE));
        if is_zero_redo_block(block) {
            return self.group_start_tail(UnsealedSegmentTerminalReason::ZeroTail);
        }
        let Ok((idx, header)) = RedoBlockHeader::deser(block, 0) else {
            return self.malformed_group_start_tail();
        };
        debug_assert_eq!(idx, RedoBlockHeader::SIZE);
        if header.verify_checksum(block).is_err()
            || header.validate(self.log_block_size).is_err()
            || !header.is_group_start()
        {
            return self.malformed_group_start_tail();
        }
        let Ok((extension_end, extension)) = RedoGroupStartExtension::deser(block, idx) else {
            return self.malformed_group_start_tail();
        };
        debug_assert_eq!(
            extension_end,
            RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE
        );
        if extension.validate().is_err() {
            return self.malformed_group_start_tail();
        }
        let Ok(payload_len) = extension.group_payload_len_usize() else {
            return self.malformed_group_start_tail();
        };
        let Ok(expected_block_count) = block_count_for_payload(self.log_block_size, payload_len)
        else {
            return self.malformed_group_start_tail();
        };
        let block_count = extension.group_block_count_usize();
        if expected_block_count != block_count {
            return self.malformed_group_start_tail();
        }
        if block_count == 1 && !header.is_group_end() {
            return self.malformed_group_start_tail();
        }
        if block_count > 1 && header.is_group_end() {
            return self.malformed_group_start_tail();
        }
        Ok(GroupStartRead::Group(ValidatedGroupStart {
            header,
            extension,
            block_count,
            payload_len,
        }))
    }

    #[inline]
    fn malformed_group_start_tail(&self) -> Result<GroupStartRead> {
        // Malformed group-start bytes can be the crash tail of an unsealed
        // file. Only sealed files report corruption here because their durable
        // end offset says another complete group start must exist.
        self.group_start_tail(UnsealedSegmentTerminalReason::MalformedGroupStartTail)
    }

    #[inline]
    fn group_start_tail(&self, reason: UnsealedSegmentTerminalReason) -> Result<GroupStartRead> {
        // At group-start boundaries, zero or malformed blocks terminate only an
        // unsealed file. Sealed files already published their durable byte
        // range, so encountering such a tail inside that range is corruption.
        if self.sealed.is_some() {
            Err(log_file_corrupted())
        } else {
            Ok(GroupStartRead::UnsealedTail(reason))
        }
    }

    #[inline]
    fn append_continuation_payload(
        &self,
        block: &[u8],
        payload: &mut Vec<u8>,
        expected_idx: usize,
        block_count: usize,
    ) -> Result<ContinuationRead> {
        if is_zero_redo_block(block) {
            return self.incomplete_continuation_result();
        }
        let Ok((idx, header)) = RedoBlockHeader::deser(block, 0) else {
            return self.incomplete_continuation_result();
        };
        debug_assert_eq!(idx, RedoBlockHeader::SIZE);
        if header.verify_checksum(block).is_err() {
            return self.incomplete_continuation_result();
        }
        if header.validate(self.log_block_size).is_err()
            || header.is_group_start()
            || header.group_block_idx as usize != expected_idx
        {
            return Err(log_file_corrupted());
        }
        let final_block = expected_idx + 1 == block_count;
        if header.is_group_end() != final_block {
            return Err(log_file_corrupted());
        }
        if !append_block_payload(
            block,
            self.log_block_size,
            header,
            RedoBlockHeader::SIZE,
            final_block,
            payload,
        ) {
            return Err(log_file_corrupted());
        }
        Ok(ContinuationRead::Appended)
    }

    #[inline]
    fn finish_group_read(
        &mut self,
        group_start: ValidatedGroupStart,
        payload: Vec<u8>,
    ) -> Result<ReadGroup> {
        let extension = group_start.extension;
        self.record_accepted_group(extension.min_redo_cts, extension.max_redo_cts);
        if let Some(sealed) = self.sealed.as_mut() {
            sealed.record_group(extension.min_redo_cts, extension.max_redo_cts);
        }
        self.offset += group_start.block_count * self.log_block_size;
        if self.offset == self.scan_end_offset && !self.validate_sealed_end() {
            return Err(log_file_corrupted());
        }
        Ok(ReadGroup::Group(TrxLogIterator::new(
            payload,
            extension.min_redo_cts,
            extension.max_redo_cts,
        )))
    }

    #[inline]
    fn incomplete_continuation_result(&self) -> Result<ContinuationRead> {
        if self.sealed.is_some() {
            Err(log_file_corrupted())
        } else {
            Ok(ContinuationRead::IncompleteTail)
        }
    }

    #[inline]
    fn record_accepted_group(&mut self, min_redo_cts: TrxID, max_redo_cts: TrxID) {
        self.accepted_min_cts = Some(
            self.accepted_min_cts
                .map_or(min_redo_cts, |current| current.min(min_redo_cts)),
        );
        self.accepted_max_cts = Some(
            self.accepted_max_cts
                .map_or(max_redo_cts, |current| current.max(max_redo_cts)),
        );
    }

    #[inline]
    fn accepted_redo_range(&self) -> Option<(TrxID, TrxID)> {
        self.accepted_min_cts.zip(self.accepted_max_cts)
    }

    #[inline]
    fn unsealed_terminal(
        &self,
        terminal_reason: UnsealedSegmentTerminalReason,
    ) -> UnsealedSegmentTerminal {
        debug_assert!(self.sealed.is_none());
        UnsealedSegmentTerminal {
            path: self.path.clone(),
            file_seq: self.file_seq,
            super_block: self.super_block.clone(),
            accepted_end_offset: self.offset,
            redo_range: self.accepted_redo_range(),
            terminal_reason,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SealedReplayState {
    expected_range: Option<(TrxID, TrxID)>,
    actual_min_cts: Option<TrxID>,
    actual_max_cts: Option<TrxID>,
}

impl SealedReplayState {
    #[inline]
    fn new(expected_range: Option<(TrxID, TrxID)>) -> Self {
        Self {
            expected_range,
            actual_min_cts: None,
            actual_max_cts: None,
        }
    }

    #[inline]
    fn record_group(&mut self, min_redo_cts: TrxID, max_redo_cts: TrxID) {
        self.actual_min_cts = Some(
            self.actual_min_cts
                .map_or(min_redo_cts, |current| current.min(min_redo_cts)),
        );
        self.actual_max_cts = Some(
            self.actual_max_cts
                .map_or(max_redo_cts, |current| current.max(max_redo_cts)),
        );
    }

    #[inline]
    fn actual_range(&self) -> Option<(TrxID, TrxID)> {
        self.actual_min_cts.zip(self.actual_max_cts)
    }

    #[inline]
    fn validate(self) -> bool {
        self.actual_range() == self.expected_range
    }
}

#[derive(Debug, Clone, Copy)]
struct ValidatedGroupStart {
    header: RedoBlockHeader,
    extension: RedoGroupStartExtension,
    block_count: usize,
    payload_len: usize,
}

#[inline]
fn replay_suffix_start(segments: &[RedoLogSegment], floor: TrxID) -> usize {
    let mut suffix_start = segments.len();
    for (idx, segment) in segments.iter().enumerate().rev() {
        suffix_start = idx;
        if segment
            .sealed_redo_range()
            .is_some_and(|(min_redo_cts, _)| min_redo_cts < floor)
        {
            break;
        }
    }
    suffix_start
}

#[inline]
fn sealed_catalog_segment_summary(segment: &RedoLogSegment) -> Option<CatalogSafeRedoSegment> {
    if segment.sealed_empty() {
        return Some(CatalogSafeRedoSegment {
            file_seq: segment.file_seq,
            redo_range: None,
        });
    }
    segment
        .sealed_redo_range()
        .map(|(min_cts, max_cts)| CatalogSafeRedoSegment {
            file_seq: segment.file_seq,
            redo_range: Some(RedoSegmentCtsRange { min_cts, max_cts }),
        })
}

#[inline]
fn retention_segment_summary(segment: &RedoLogSegment) -> RedoRetentionSegment {
    let state = if segment.sealed_empty() {
        RedoRetentionSegmentState::SealedEmpty
    } else if let Some((min_cts, max_cts)) = segment.sealed_redo_range() {
        RedoRetentionSegmentState::SealedNonEmpty(RedoSegmentCtsRange { min_cts, max_cts })
    } else {
        RedoRetentionSegmentState::Unsealed
    };
    RedoRetentionSegment {
        file_seq: segment.file_seq,
        state,
    }
}

#[inline]
fn log_file_corrupted() -> Error {
    Report::new(DataIntegrityError::LogFileCorrupted).into()
}

#[inline]
fn open_direct_segment_file(segment: &RedoLogSegment) -> Result<SparseFile> {
    let path = segment.path.to_str().ok_or_else(|| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "redo log path is not valid UTF-8: path={}",
                segment.path.display()
            )),
        )
    })?;
    SparseFile::open(path, UNTRACKED_FILE_ID)
}

#[inline]
fn segment_scan_end_offset(segment: &RedoLogSegment) -> Result<usize> {
    let raw = if segment.super_block.is_sealed() {
        segment.super_block.durable_end_offset
    } else {
        segment.super_block.file_max_size
    };
    usize::try_from(raw).map_err(|_| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "redo scan end offset exceeds usize: file_seq={:08x}, offset={raw}",
                segment.file_seq
            )),
        )
    })
}

#[inline]
fn take_read_completion(
    completed: CompletedSubmission<RedoReadSubmission>,
) -> Result<RedoReadCompletion> {
    let mut submission = completed.submission;
    let expected_bytes = submission.operation.len();
    let buf = submission.operation.take_buf().ok_or_else(|| {
        Error::from(
            Report::new(InternalError::Generic)
                .attach("redo direct read completion did not return owned buffer"),
        )
    })?;
    Ok(RedoReadCompletion {
        file_seq: submission.file_seq,
        offset: submission.offset,
        expected_bytes,
        actual_bytes: completed.result,
        buf,
    })
}

#[inline]
fn append_block_payload(
    block: &[u8],
    log_block_size: usize,
    header: RedoBlockHeader,
    payload_start: usize,
    final_block: bool,
    out: &mut Vec<u8>,
) -> bool {
    let payload_len = header.payload_len_usize();
    let payload_end = match payload_start.checked_add(payload_len) {
        Some(payload_end) if payload_end <= log_block_size => payload_end,
        _ => return false,
    };
    if !final_block {
        let capacity = log_block_size - payload_start;
        if payload_len != capacity {
            return false;
        }
    }
    if block[payload_end..log_block_size]
        .iter()
        .any(|&byte| byte != 0)
    {
        return false;
    }
    out.extend_from_slice(&block[payload_start..payload_end]);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::test_page_id;
    use crate::id::{RowID, TableID, TrxID};
    use crate::io::{
        BackendResult, BackendToken, DirectBuf, IOBackendErrorPhase, IOBackendFailure,
        IOBackendQueueState, IOBuf,
    };
    use crate::log::block_group::LogBlockGroup;
    use crate::log::format::{
        REDO_BLOCK_GROUP_END, REDO_BLOCK_GROUP_START, REDO_SUPER_BLOCK_SLOT_SIZE,
        patch_redo_block_checksum, redo_start_block_payload_capacity, serialize_redo_super_block,
    };
    use crate::log::redo::{
        DDLRedo, RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind, TableDML,
    };
    use crate::serde::Ser;
    use std::collections::{BTreeMap, VecDeque};
    use std::io::{Error as StdIoError, Write};
    use std::path::Path;
    use std::thread::spawn;

    const TEST_FILE_SEQ: u32 = 0;

    #[derive(Clone, Copy)]
    enum TestSegmentSeal {
        Open,
        Sealed {
            durable_end_offset: usize,
            redo_range: Option<(TrxID, TrxID)>,
        },
    }

    fn simple_trx_log(cts: TrxID) -> TrxLog {
        TrxLog::new(
            RedoHeader {
                cts,
                trx_kind: RedoTrxKind::System,
            },
            RedoLogs::default(),
        )
    }

    fn write_stream_log_file(
        path: &Path,
        log_block_size: usize,
        data_block_count: usize,
        blocks: &[DirectBuf],
        seal: TestSegmentSeal,
    ) -> RedoLogFileDescriptor {
        write_stream_log_file_with_seq(
            path,
            TEST_FILE_SEQ,
            log_block_size,
            data_block_count,
            blocks,
            seal,
        )
    }

    fn write_stream_log_file_with_seq(
        path: &Path,
        file_seq: u32,
        log_block_size: usize,
        data_block_count: usize,
        blocks: &[DirectBuf],
        seal: TestSegmentSeal,
    ) -> RedoLogFileDescriptor {
        assert!(data_block_count >= 1);
        assert!(blocks.len() <= data_block_count);
        let file_max_size = REDO_DEFAULT_DATA_START_OFFSET + data_block_count * log_block_size;
        let open = RedoSuperBlock::initial(file_seq, log_block_size, file_max_size);
        let mut bytes = vec![0; file_max_size];
        let mut slot = vec![0; REDO_SUPER_BLOCK_SLOT_SIZE];
        serialize_redo_super_block(&mut slot, &open).unwrap();
        bytes[..REDO_SUPER_BLOCK_SLOT_SIZE].copy_from_slice(&slot);
        if let TestSegmentSeal::Sealed {
            durable_end_offset,
            redo_range,
        } = seal
        {
            let sealed =
                RedoSuperBlock::sealed_from_open(&open, 1, durable_end_offset, redo_range).unwrap();
            serialize_redo_super_block(&mut slot, &sealed).unwrap();
            bytes[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET]
                .copy_from_slice(&slot);
        }
        for (idx, block) in blocks.iter().enumerate() {
            assert_eq!(block.as_bytes().len(), log_block_size);
            let offset = REDO_DEFAULT_DATA_START_OFFSET + idx * log_block_size;
            bytes[offset..offset + log_block_size].copy_from_slice(block.as_bytes());
        }
        let mut file = File::create(path).unwrap();
        file.write_all(&bytes).unwrap();
        file.flush().unwrap();
        RedoLogFileDescriptor {
            file_seq,
            path: path.to_path_buf(),
        }
    }

    fn stream_for_test_file(
        path: &Path,
        log_block_size: usize,
        data_block_count: usize,
        blocks: &[DirectBuf],
        seal: TestSegmentSeal,
    ) -> RedoLogStream {
        let descriptor =
            write_stream_log_file(path, log_block_size, data_block_count, blocks, seal);
        let planner = RedoReplayPlanner::new(vec![descriptor]);
        let planned = planner.plan_recovery(TrxID::new(0), 1).unwrap();
        assert_eq!(planned.skipped_max_recovered_cts, None);
        planned.stream
    }

    fn injected_segment(data_block_count: usize) -> RedoLogSegment {
        let file_max_size = REDO_DEFAULT_DATA_START_OFFSET + data_block_count * STORAGE_SECTOR_SIZE;
        RedoLogSegment {
            path: PathBuf::from("injected.log"),
            file_seq: TEST_FILE_SEQ,
            super_block: RedoSuperBlock::initial(TEST_FILE_SEQ, STORAGE_SECTOR_SIZE, file_max_size),
        }
    }

    fn injected_stream(items: Vec<RedoReadItem>) -> RedoLogStream {
        let capacity = items.len().max(1);
        let (items_tx, items_rx) = flume::bounded(capacity);
        for item in items {
            assert!(items_tx.send(item).is_ok());
        }
        drop(items_tx);
        let (recycle_tx, _recycle_rx) = flume::bounded(capacity);
        let (stop_tx, _stop_rx) = flume::bounded(1);
        RedoLogStream {
            reader: Some(RedoReadAheadHandle {
                items: items_rx,
                recycle: recycle_tx,
                stop: stop_tx,
                join: None,
            }),
            current_segment: None,
            buffer: VecDeque::new(),
            state: RedoLogStreamState::Active,
            unsealed_terminals: Vec::new(),
        }
    }

    struct WaitErrorBackend {
        max_events: usize,
        inflight: VecDeque<BackendToken>,
    }

    impl WaitErrorBackend {
        #[inline]
        fn new(max_events: usize) -> Self {
            Self {
                max_events,
                inflight: VecDeque::new(),
            }
        }
    }

    impl IOBackend for WaitErrorBackend {
        type Prepared = BackendToken;
        type SubmitBatch = VecDeque<BackendToken>;
        type Events = ();

        fn max_events(&self) -> usize {
            self.max_events
        }

        fn new_submit_batch(&self) -> Self::SubmitBatch {
            VecDeque::with_capacity(self.max_events)
        }

        fn new_events(&self) -> Self::Events {}

        fn prepare(&mut self, token: BackendToken, _operation: &mut Operation) -> Self::Prepared {
            token
        }

        fn push_prepared(&mut self, batch: &mut Self::SubmitBatch, prepared: &mut Self::Prepared) {
            batch.push_back(*prepared);
        }

        fn submit_batch(
            &mut self,
            batch: &mut Self::SubmitBatch,
            limit: usize,
        ) -> BackendResult<SubmitAttempt> {
            let submit_count = limit.min(batch.len());
            let Some(submitted) = std::num::NonZeroUsize::new(submit_count) else {
                return Ok(SubmitAttempt::Noop);
            };
            for _ in 0..submit_count {
                let token = batch
                    .pop_front()
                    .expect("test backend submit count must match staged tokens");
                self.inflight.push_back(token);
            }
            Ok(SubmitAttempt::Submitted(submitted))
        }

        fn wait_at_least(
            &mut self,
            _events: &mut Self::Events,
            _min_nr: usize,
        ) -> BackendResult<Vec<(BackendToken, IoResult<usize>)>> {
            Err(IOBackendFailure::report(
                "recovery_test",
                IOBackendErrorPhase::Wait,
                StdIoError::from_raw_os_error(libc::EIO),
                1,
                IOBackendQueueState::wait_with_completions(0),
            ))
        }
    }

    #[test]
    fn test_read_ahead_send_item_stops_while_item_queue_full() {
        let (items_tx, items_rx) = flume::bounded(1);
        items_tx.send(RedoReadItem::End).unwrap();
        let (_recycle_tx, recycle_rx) = flume::bounded(1);
        let (stop_tx, stop_rx) = flume::bounded(1);
        let (started_tx, started_rx) = flume::bounded(1);
        let join = spawn(move || {
            let mut worker = RedoReadAheadWorker {
                segments: Vec::new(),
                read_depth: 1,
                items: items_tx,
                recycle: recycle_rx,
                stop: stop_rx,
                stop_requested: false,
                free: Vec::new(),
                free_block_size: None,
            };
            started_tx.send(()).unwrap();
            worker.send_item(RedoReadItem::End)
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        stop_tx.send(()).unwrap();
        assert!(!join.join().unwrap());
        assert_eq!(items_rx.len(), 1);
    }

    #[test]
    fn test_read_ahead_drain_quarantines_pending_reads_after_wait_error() {
        let (items_tx, _items_rx) = flume::bounded(1);
        let (_recycle_tx, recycle_rx) = flume::bounded(1);
        let (_stop_tx, stop_rx) = flume::bounded(1);
        let mut worker = RedoReadAheadWorker {
            segments: Vec::new(),
            read_depth: 1,
            items: items_tx,
            recycle: recycle_rx,
            stop: stop_rx,
            stop_requested: false,
            free: Vec::new(),
            free_block_size: Some(STORAGE_SECTOR_SIZE),
        };
        let mut driver = SubmissionDriver::new(WaitErrorBackend::new(1));
        assert!(
            driver
                .push(RedoReadSubmission::new(
                    TEST_FILE_SEQ,
                    REDO_DEFAULT_DATA_START_OFFSET,
                    0,
                    DirectBuf::zeroed(STORAGE_SECTOR_SIZE),
                ))
                .is_ok()
        );

        assert_eq!(worker.drain_driver(&mut driver), 1);
        assert_eq!(driver.pending_len(), 1);
        assert_eq!(driver.quarantine_owned_buffers(), 0);
    }

    async fn assert_stream_corrupted(stream: &mut RedoLogStream) {
        let err = stream.try_next().await.unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::LogFileCorrupted),
            "{err:?}"
        );
    }

    async fn assert_unsealed_terminal(
        stream: &mut RedoLogStream,
        terminal_reason: UnsealedSegmentTerminalReason,
        accepted_end_offset: usize,
        redo_range: Option<(TrxID, TrxID)>,
    ) {
        assert!(stream.try_next().await.unwrap().is_none());
        let terminals = stream.take_unsealed_terminals();
        assert_eq!(terminals.len(), 1);
        assert_eq!(terminals[0].terminal_reason, terminal_reason);
        assert_eq!(terminals[0].accepted_end_offset, accepted_end_offset);
        assert_eq!(terminals[0].redo_range, redo_range);
    }

    fn assert_read_after_failed(err: Error) {
        assert_eq!(
            err.downcast_ref::<InternalError>().copied(),
            Some(InternalError::Generic),
            "{err:?}"
        );
        assert!(
            format!("{err:?}").contains("redo stream read after terminal error"),
            "{err:?}"
        );
    }

    fn block_group_with_range(log: TrxLog, min_cts: TrxID, max_cts: TrxID) -> DirectBuf {
        block_group_with_range_and_size(STORAGE_SECTOR_SIZE, log, min_cts, max_cts)
    }

    fn block_group_with_range_and_size(
        log_block_size: usize,
        log: TrxLog,
        min_cts: TrxID,
        max_cts: TrxID,
    ) -> DirectBuf {
        let payload_len = log.ser_len();
        let group = LogBlockGroup::new(log_block_size, log).unwrap();
        let mut blocks = group
            .finish_with(|count| {
                (0..count)
                    .map(|_| DirectBuf::zeroed(log_block_size))
                    .collect()
            })
            .unwrap();
        assert_eq!(blocks.len(), 1);
        let mut direct_buf = blocks.pop().unwrap();
        RedoGroupStartExtension::new(payload_len, 1, min_cts, max_cts)
            .unwrap()
            .ser(direct_buf.as_bytes_mut(), RedoBlockHeader::SIZE);
        patch_redo_block_checksum(direct_buf.as_bytes_mut()).unwrap();
        direct_buf
    }

    fn corrupt_padding_after_payload(block: &mut DirectBuf) {
        let (_, header) = RedoBlockHeader::deser(block.as_bytes(), 0).unwrap();
        let payload_start = if header.is_group_start() {
            RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE
        } else {
            RedoBlockHeader::SIZE
        };
        let payload_end = payload_start + header.payload_len_usize();
        assert!(payload_end < block.capacity());
        block.as_bytes_mut()[payload_end] = 1;
        patch_redo_block_checksum(block.as_bytes_mut()).unwrap();
    }

    fn bad_checksum_final_continuation() -> DirectBuf {
        let mut block = DirectBuf::zeroed(STORAGE_SECTOR_SIZE);
        let header = RedoBlockHeader::new(REDO_BLOCK_GROUP_END, 1, 1).unwrap();
        header.ser(block.as_bytes_mut(), 0);
        block.as_bytes_mut()[RedoBlockHeader::SIZE] = 1;
        block
    }

    fn two_block_start_only() -> DirectBuf {
        let start_capacity = redo_start_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap();
        let mut block = DirectBuf::zeroed(STORAGE_SECTOR_SIZE);
        let header = RedoBlockHeader::new(REDO_BLOCK_GROUP_START, start_capacity, 0).unwrap();
        header.ser(block.as_bytes_mut(), 0);
        RedoGroupStartExtension::new(start_capacity + 1, 2, TrxID::new(1), TrxID::new(1))
            .unwrap()
            .ser(block.as_bytes_mut(), RedoBlockHeader::SIZE);
        let payload_start = RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE;
        block.as_bytes_mut()[payload_start..payload_start + start_capacity].fill(1);
        patch_redo_block_checksum(block.as_bytes_mut()).unwrap();
        block
    }

    #[test]
    fn test_direct_stream_treats_unsealed_group_crossing_segment_end_as_incomplete_tail() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let block = two_block_start_only();
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[block],
                TestSegmentSeal::Open,
            );

            assert_unsealed_terminal(
                &mut stream,
                UnsealedSegmentTerminalReason::IncompleteContinuationTail,
                REDO_DEFAULT_DATA_START_OFFSET,
                None,
            )
            .await;
        });
    }

    #[test]
    fn test_direct_stream_treats_in_range_zero_continuation_as_incomplete_tail() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let block = two_block_start_only();
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                2,
                &[block],
                TestSegmentSeal::Open,
            );

            assert_unsealed_terminal(
                &mut stream,
                UnsealedSegmentTerminalReason::IncompleteContinuationTail,
                REDO_DEFAULT_DATA_START_OFFSET,
                None,
            )
            .await;
        });
    }

    #[test]
    fn test_direct_stream_treats_bad_checksum_continuation_as_unsealed_incomplete_tail() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let blocks = [two_block_start_only(), bad_checksum_final_continuation()];
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                2,
                &blocks,
                TestSegmentSeal::Open,
            );

            assert_unsealed_terminal(
                &mut stream,
                UnsealedSegmentTerminalReason::IncompleteContinuationTail,
                REDO_DEFAULT_DATA_START_OFFSET,
                None,
            )
            .await;
        });
    }

    #[test]
    fn test_direct_stream_treats_all_zero_group_header_as_eof() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[],
                TestSegmentSeal::Open,
            );

            assert_unsealed_terminal(
                &mut stream,
                UnsealedSegmentTerminalReason::ZeroTail,
                REDO_DEFAULT_DATA_START_OFFSET,
                None,
            )
            .await;
        });
    }

    #[test]
    fn test_direct_stream_treats_unsealed_nonzero_empty_group_header_as_tail() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut block = DirectBuf::zeroed(STORAGE_SECTOR_SIZE);
            block.as_bytes_mut()[0] = 1;
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[block],
                TestSegmentSeal::Open,
            );

            assert_unsealed_terminal(
                &mut stream,
                UnsealedSegmentTerminalReason::MalformedGroupStartTail,
                REDO_DEFAULT_DATA_START_OFFSET,
                None,
            )
            .await;
        });
    }

    #[test]
    fn test_direct_stream_reports_unsealed_accepted_prefix_metadata() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let valid =
                block_group_with_range(simple_trx_log(TrxID::new(5)), TrxID::new(4), TrxID::new(6));
            let mut malformed_tail = DirectBuf::zeroed(STORAGE_SECTOR_SIZE);
            malformed_tail.as_bytes_mut()[0] = 1;
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                2,
                &[valid, malformed_tail],
                TestSegmentSeal::Open,
            );

            let recovered = stream.try_next().await.unwrap().unwrap();
            assert_eq!(recovered.header.cts, TrxID::new(5));
            assert_unsealed_terminal(
                &mut stream,
                UnsealedSegmentTerminalReason::MalformedGroupStartTail,
                REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                Some((TrxID::new(4), TrxID::new(6))),
            )
            .await;
        });
    }

    #[test]
    fn test_replay_planner_rejects_three_unsealed_files() {
        let dir = tempfile::tempdir().unwrap();
        let descriptors = (0..3)
            .map(|seq| {
                write_stream_log_file_with_seq(
                    &dir.path().join(format!("redo.{seq:08x}")),
                    seq,
                    STORAGE_SECTOR_SIZE,
                    1,
                    &[],
                    TestSegmentSeal::Open,
                )
            })
            .collect::<Vec<_>>();
        let planner = RedoReplayPlanner::new(descriptors);

        let err = match planner.plan_recovery(TrxID::new(0), 1) {
            Ok(_) => panic!("three unsealed redo files must be rejected"),
            Err(err) => err,
        };
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::LogFileCorrupted)
        );
    }

    #[test]
    fn test_replay_planner_rejects_unsealed_before_sealed_newer_file() {
        let dir = tempfile::tempdir().unwrap();
        let open = write_stream_log_file_with_seq(
            &dir.path().join("redo.00000000"),
            0,
            STORAGE_SECTOR_SIZE,
            1,
            &[],
            TestSegmentSeal::Open,
        );
        let sealed = write_stream_log_file_with_seq(
            &dir.path().join("redo.00000001"),
            1,
            STORAGE_SECTOR_SIZE,
            1,
            &[],
            TestSegmentSeal::Sealed {
                durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET,
                redo_range: None,
            },
        );
        let planner = RedoReplayPlanner::new(vec![open, sealed]);

        let err = match planner.plan_recovery(TrxID::new(0), 1) {
            Ok(_) => panic!("unsealed redo file before sealed newer file must be rejected"),
            Err(err) => err,
        };
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::LogFileCorrupted)
        );
    }

    #[test]
    fn test_catalog_scan_planner_reports_sealed_segment_summaries() {
        let dir = tempfile::tempdir().unwrap();
        let sealed_empty = write_stream_log_file_with_seq(
            &dir.path().join("redo.00000000"),
            0,
            STORAGE_SECTOR_SIZE,
            1,
            &[],
            TestSegmentSeal::Sealed {
                durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET,
                redo_range: None,
            },
        );
        let sealed_non_empty = write_stream_log_file_with_seq(
            &dir.path().join("redo.00000001"),
            1,
            STORAGE_SECTOR_SIZE,
            1,
            &[],
            TestSegmentSeal::Sealed {
                durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                redo_range: Some((TrxID::new(2), TrxID::new(4))),
            },
        );
        let open = write_stream_log_file_with_seq(
            &dir.path().join("redo.00000002"),
            2,
            STORAGE_SECTOR_SIZE,
            1,
            &[],
            TestSegmentSeal::Open,
        );
        let planner = RedoReplayPlanner::new(vec![sealed_empty, sealed_non_empty, open]);

        let planned = planner.plan_catalog_scan(TrxID::new(5), 1).unwrap();

        assert_eq!(
            planned.sealed_segments,
            vec![
                CatalogSafeRedoSegment {
                    file_seq: 0,
                    redo_range: None,
                },
                CatalogSafeRedoSegment {
                    file_seq: 1,
                    redo_range: Some(RedoSegmentCtsRange {
                        min_cts: TrxID::new(2),
                        max_cts: TrxID::new(4),
                    }),
                },
            ]
        );
    }

    #[test]
    fn test_replay_planner_final_two_unsealed_skips_newest_tail() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let older_block =
                block_group_with_range(simple_trx_log(TrxID::new(5)), TrxID::new(5), TrxID::new(5));
            let older = write_stream_log_file_with_seq(
                &dir.path().join("redo.00000000"),
                0,
                STORAGE_SECTOR_SIZE,
                2,
                &[older_block],
                TestSegmentSeal::Open,
            );
            let newest_block =
                block_group_with_range(simple_trx_log(TrxID::new(9)), TrxID::new(9), TrxID::new(9));
            let newest = write_stream_log_file_with_seq(
                &dir.path().join("redo.00000001"),
                1,
                STORAGE_SECTOR_SIZE,
                2,
                &[newest_block],
                TestSegmentSeal::Open,
            );
            let planner = RedoReplayPlanner::new(vec![older, newest]);
            let planned = planner.plan_recovery(TrxID::new(0), 1).unwrap();
            assert_eq!(
                planned.repair_policy,
                RedoRecoveryRepairPolicy::FinalTwoUnsealed {
                    older_file_seq: 0,
                    newest_file_seq: 1
                }
            );
            let mut stream = planned.stream;

            let recovered = stream.try_next().await.unwrap().unwrap();
            assert_eq!(recovered.header.cts, TrxID::new(5));
            assert_unsealed_terminal(
                &mut stream,
                UnsealedSegmentTerminalReason::ZeroTail,
                REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                Some((TrxID::new(5), TrxID::new(5))),
            )
            .await;
        });
    }

    #[test]
    fn test_direct_stream_fails_closed_after_segment_error() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let corrupt_path = dir.path().join("corrupt.log");
            let valid_path = dir.path().join("valid.log");
            let mut corrupt_block = DirectBuf::zeroed(STORAGE_SECTOR_SIZE);
            corrupt_block.as_bytes_mut()[0] = 1;
            let corrupt = write_stream_log_file_with_seq(
                &corrupt_path,
                0,
                STORAGE_SECTOR_SIZE,
                1,
                &[corrupt_block],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                    redo_range: Some((TrxID::new(1), TrxID::new(1))),
                },
            );
            let valid_block =
                block_group_with_range(simple_trx_log(TrxID::new(9)), TrxID::new(9), TrxID::new(9));
            let valid = write_stream_log_file_with_seq(
                &valid_path,
                1,
                STORAGE_SECTOR_SIZE,
                1,
                &[valid_block],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                    redo_range: Some((TrxID::new(9), TrxID::new(9))),
                },
            );
            let planner = RedoReplayPlanner::new(vec![corrupt, valid]);
            let planned = planner.plan_recovery(TrxID::new(0), 1).unwrap();
            assert_eq!(planned.skipped_max_recovered_cts, None);
            let mut stream = planned.stream;

            assert_stream_corrupted(&mut stream).await;
            let err = stream.try_next().await.unwrap_err();
            assert_read_after_failed(err);
        });
    }

    #[test]
    fn test_direct_stream_rejects_early_segment_end_while_waiting_for_continuation() {
        smol::block_on(async {
            let block = two_block_start_only();
            let mut stream = injected_stream(vec![
                RedoReadItem::SegmentStart(injected_segment(2)),
                RedoReadItem::Block {
                    file_seq: TEST_FILE_SEQ,
                    offset: REDO_DEFAULT_DATA_START_OFFSET,
                    buf: block,
                },
                RedoReadItem::SegmentEnd {
                    file_seq: TEST_FILE_SEQ,
                },
            ]);

            assert_stream_corrupted(&mut stream).await;
            let err = stream.try_next().await.unwrap_err();
            assert_read_after_failed(err);
        });
    }

    #[test]
    fn test_direct_stream_rejects_early_end_while_waiting_for_continuation() {
        smol::block_on(async {
            let block = two_block_start_only();
            let mut stream = injected_stream(vec![
                RedoReadItem::SegmentStart(injected_segment(2)),
                RedoReadItem::Block {
                    file_seq: TEST_FILE_SEQ,
                    offset: REDO_DEFAULT_DATA_START_OFFSET,
                    buf: block,
                },
                RedoReadItem::End,
            ]);

            assert_stream_corrupted(&mut stream).await;
            let err = stream.try_next().await.unwrap_err();
            assert_read_after_failed(err);
        });
    }

    #[test]
    fn test_direct_stream_treats_unsealed_bad_group_start_checksum_as_tail() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut direct_buf =
                block_group_with_range(simple_trx_log(TrxID::new(1)), TrxID::new(1), TrxID::new(1));
            let payload_start = RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE;
            direct_buf.as_bytes_mut()[payload_start] ^= 0x80;
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[direct_buf],
                TestSegmentSeal::Open,
            );

            assert_unsealed_terminal(
                &mut stream,
                UnsealedSegmentTerminalReason::MalformedGroupStartTail,
                REDO_DEFAULT_DATA_START_OFFSET,
                None,
            )
            .await;
        });
    }

    #[test]
    fn test_direct_stream_rejects_sealed_bad_group_start_checksum() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut direct_buf =
                block_group_with_range(simple_trx_log(TrxID::new(1)), TrxID::new(1), TrxID::new(1));
            let payload_start = RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE;
            direct_buf.as_bytes_mut()[payload_start] ^= 0x80;
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[direct_buf],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                    redo_range: Some((TrxID::new(1), TrxID::new(1))),
                },
            );

            assert_stream_corrupted(&mut stream).await;
        });
    }

    #[test]
    fn test_direct_stream_rejects_nonzero_padding_after_payload() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut direct_buf =
                block_group_with_range(simple_trx_log(TrxID::new(1)), TrxID::new(1), TrxID::new(1));
            corrupt_padding_after_payload(&mut direct_buf);
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[direct_buf],
                TestSegmentSeal::Open,
            );

            assert_stream_corrupted(&mut stream).await;
        });
    }

    #[test]
    fn test_direct_stream_rejects_body_cts_below_header_range() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let log = simple_trx_log(TrxID::new(5));
            let direct_buf = block_group_with_range(log, TrxID::new(6), TrxID::new(8));
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[direct_buf],
                TestSegmentSeal::Open,
            );

            let err = stream.try_next().await.unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );
        });
    }

    #[test]
    fn test_direct_stream_clears_partial_buffer_after_group_decode_error() {
        smol::block_on(async {
            let log1 = simple_trx_log(TrxID::new(1));
            let log2 = simple_trx_log(TrxID::new(2));
            let payload_len = log1.ser_len() + log2.ser_len();
            let mut group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log1).unwrap();
            assert!(group.append_trx_log(log2).is_none());
            let mut blocks = group
                .finish_with(|count| {
                    (0..count)
                        .map(|_| DirectBuf::zeroed(STORAGE_SECTOR_SIZE))
                        .collect()
                })
                .unwrap();
            RedoGroupStartExtension::new(payload_len, blocks.len(), TrxID::new(1), TrxID::new(1))
                .unwrap()
                .ser(blocks[0].as_bytes_mut(), RedoBlockHeader::SIZE);
            patch_redo_block_checksum(blocks[0].as_bytes_mut()).unwrap();

            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                blocks.len(),
                &blocks,
                TestSegmentSeal::Open,
            );

            let err = stream.try_next().await.unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let err = stream.try_next().await.unwrap_err();
            assert_read_after_failed(err);
        });
    }

    #[test]
    fn test_direct_stream_rejects_body_cts_above_header_range() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let log = simple_trx_log(TrxID::new(5));
            let direct_buf = block_group_with_range(log, TrxID::new(1), TrxID::new(4));
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[direct_buf],
                TestSegmentSeal::Open,
            );

            let err = stream.try_next().await.unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );
        });
    }

    #[test]
    fn test_direct_stream_accepts_body_cts_within_loose_header_range() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let log = simple_trx_log(TrxID::new(5));
            let direct_buf = block_group_with_range(log, TrxID::new(4), TrxID::new(6));
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[direct_buf],
                TestSegmentSeal::Open,
            );

            let recovered = stream.try_next().await.unwrap().unwrap();
            assert_eq!(recovered.header.cts, TrxID::new(5));
            assert!(stream.try_next().await.unwrap().is_none());
        });
    }

    #[test]
    fn test_direct_stream_accepts_matching_sealed_group_range() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let log = simple_trx_log(TrxID::new(5));
            let direct_buf = block_group_with_range(log, TrxID::new(4), TrxID::new(6));
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[direct_buf],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                    redo_range: Some((TrxID::new(4), TrxID::new(6))),
                },
            );

            assert!(stream.try_next().await.unwrap().is_some());
            assert!(stream.try_next().await.unwrap().is_none());
        });
    }

    #[test]
    fn test_direct_stream_skips_empty_sealed_file_without_zero_eof() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET,
                    redo_range: None,
                },
            );

            assert!(stream.try_next().await.unwrap().is_none());
        });
    }

    #[test]
    fn test_direct_stream_rejects_zero_eof_before_sealed_durable_end() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                    redo_range: Some((TrxID::new(1), TrxID::new(1))),
                },
            );

            assert_stream_corrupted(&mut stream).await;
        });
    }

    #[test]
    fn test_direct_stream_rejects_group_crossing_sealed_durable_end() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let block = two_block_start_only();
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[block],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                    redo_range: Some((TrxID::new(1), TrxID::new(1))),
                },
            );

            assert_stream_corrupted(&mut stream).await;
        });
    }

    #[test]
    fn test_direct_stream_rejects_mismatched_sealed_group_range() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let log = simple_trx_log(TrxID::new(5));
            let direct_buf = block_group_with_range(log, TrxID::new(5), TrxID::new(5));
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                1,
                &[direct_buf],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                    redo_range: Some((TrxID::new(4), TrxID::new(6))),
                },
            );

            assert_stream_corrupted(&mut stream).await;
        });
    }

    #[test]
    fn test_direct_stream_reads_multi_trx_log_in_one_group() {
        smol::block_on(async {
            let log1 = TrxLog::new(
                RedoHeader {
                    cts: TrxID::new(1),
                    trx_kind: RedoTrxKind::System,
                },
                RedoLogs {
                    ddl: Some(Box::new(DDLRedo::CreateRowPage {
                        table_id: TableID::new(6),
                        page_id: test_page_id(5),
                        start_row_id: RowID::new(0),
                        end_row_id: RowID::new(574),
                    })),
                    dml: BTreeMap::new(),
                },
            );
            let mut group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log1).unwrap();

            let mut rows = BTreeMap::new();
            rows.insert(
                RowID::new(100),
                RowRedo {
                    page_id: test_page_id(5),
                    row_id: RowID::new(100),
                    kind: RowRedoKind::Delete,
                },
            );
            let mut dml = BTreeMap::new();
            dml.insert(TableID::new(6), TableDML { rows });

            let log2 = TrxLog::new(
                RedoHeader {
                    cts: TrxID::new(2),
                    trx_kind: RedoTrxKind::User,
                },
                RedoLogs { ddl: None, dml },
            );
            assert!(group.append_trx_log(log2).is_none());
            let blocks = group
                .finish_with(|count| {
                    (0..count)
                        .map(|_| DirectBuf::zeroed(STORAGE_SECTOR_SIZE))
                        .collect()
                })
                .unwrap();

            let dir = tempfile::tempdir().unwrap();
            let file_path = dir.path().join("test.log");
            let mut stream = stream_for_test_file(
                &file_path,
                STORAGE_SECTOR_SIZE,
                blocks.len(),
                &blocks,
                TestSegmentSeal::Open,
            );
            let log1 = stream.try_next().await.unwrap().unwrap();
            assert!(log1.header.trx_kind == RedoTrxKind::System);
            let log2 = stream.try_next().await.unwrap().unwrap();
            assert!(log2.header.trx_kind == RedoTrxKind::User);
            assert!(stream.try_next().await.unwrap().is_none());
        });
    }

    #[test]
    fn test_direct_stream_reads_segments_with_different_persisted_block_sizes() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let first_path = dir.path().join("first.log");
            let second_path = dir.path().join("second.log");
            let first = block_group_with_range_and_size(
                STORAGE_SECTOR_SIZE,
                simple_trx_log(TrxID::new(1)),
                TrxID::new(1),
                TrxID::new(1),
            );
            let second_block_size = STORAGE_SECTOR_SIZE * 2;
            let second = block_group_with_range_and_size(
                second_block_size,
                simple_trx_log(TrxID::new(2)),
                TrxID::new(2),
                TrxID::new(2),
            );
            let first_descriptor = write_stream_log_file_with_seq(
                &first_path,
                0,
                STORAGE_SECTOR_SIZE,
                1,
                &[first],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
                    redo_range: Some((TrxID::new(1), TrxID::new(1))),
                },
            );
            let second_descriptor = write_stream_log_file_with_seq(
                &second_path,
                1,
                second_block_size,
                1,
                &[second],
                TestSegmentSeal::Sealed {
                    durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET + second_block_size,
                    redo_range: Some((TrxID::new(2), TrxID::new(2))),
                },
            );
            let planner = RedoReplayPlanner::new(vec![first_descriptor, second_descriptor]);
            let planned = planner.plan_recovery(TrxID::new(0), 2).unwrap();
            assert_eq!(planned.skipped_max_recovered_cts, None);
            let mut stream = planned.stream;

            let first = stream.try_next().await.unwrap().unwrap();
            let second = stream.try_next().await.unwrap().unwrap();

            assert_eq!(first.header.cts, TrxID::new(1));
            assert_eq!(second.header.cts, TrxID::new(2));
            assert!(stream.try_next().await.unwrap().is_none());
        });
    }
}
