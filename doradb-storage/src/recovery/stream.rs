use crate::error::{DataIntegrityError, InternalError, Result};
use crate::id::TrxID;
use crate::io::STORAGE_SECTOR_SIZE;
use crate::log::block_group::{TrxLog, block_count_for_payload};
use crate::log::format::{
    REDO_DEFAULT_DATA_START_OFFSET, RedoBlockHeader, RedoGroupStartExtension, RedoSuperBlock,
    is_zero_redo_block, select_redo_super_block,
};
use crate::log::{RedoLogFileDescriptor, next_redo_file_seq};
use crate::serde::Deser;
use error_stack::Report;
use memmap2::Mmap;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{ErrorKind as IoErrorKind, Read};
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;

/// Result of reading one redo group from a log file.
pub(crate) enum ReadLog {
    /// The current group header is all zeroes, which marks logical EOF.
    DataEnd,
    /// The configured logical file size has been reached.
    SizeLimit,
    /// The current group or file header failed integrity validation.
    DataCorrupted,
    /// A transaction log iterator assembled from CRC32-validated redo blocks.
    Iterator(TrxLogIterator),
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
pub(crate) struct ReplayPlanner {
    /// Discovered file names in ascending sequence order.
    discovered: Vec<RedoLogFileDescriptor>,
    /// Planned segments that still require normal mmap replay.
    planned: VecDeque<RedoLogSegment>,
    /// Replay floor used to build the planned queue.
    replay_floor: Option<TrxID>,
    /// Highest CTS contributed by skipped sealed segments.
    skipped_max_recovered_cts: Option<TrxID>,
}

impl ReplayPlanner {
    /// Create a replay planner from discovered redo files.
    #[inline]
    pub(crate) fn new(discovered: Vec<RedoLogFileDescriptor>) -> Self {
        Self {
            discovered,
            planned: VecDeque::new(),
            replay_floor: None,
            skipped_max_recovered_cts: None,
        }
    }

    /// Return the sequence for the next writable redo file after this family.
    #[inline]
    pub(crate) fn next_file_seq(&self) -> Result<Option<u32>> {
        self.discovered
            .last()
            .map(|descriptor| next_redo_file_seq(descriptor.file_seq))
            .transpose()
    }

    /// Plan which discovered redo segments must be replayed for a replay floor.
    ///
    /// This is a one-time transition: the floor controls which sealed segments
    /// may be skipped, so a replan would make the already-built queue ambiguous.
    #[inline]
    pub(crate) fn plan(&mut self, floor: TrxID) -> Result<Option<TrxID>> {
        if let Some(existing_floor) = self.replay_floor {
            return Err(Report::new(InternalError::Generic)
                .attach(format!(
                    "redo replay already planned: existing_floor={existing_floor}, requested_floor={floor}"
                ))
                .into());
        }

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

        let mut skipped_max = None::<TrxID>;
        for segment in suffix {
            if segment.sealed_empty() {
                continue;
            }
            if let Some((_, max_redo_cts)) = segment.sealed_redo_range()
                && max_redo_cts < floor
            {
                skipped_max =
                    Some(skipped_max.map_or(max_redo_cts, |current| current.max(max_redo_cts)));
                continue;
            }
            self.planned.push_back(segment);
        }

        self.replay_floor = Some(floor);
        self.skipped_max_recovered_cts = skipped_max;
        Ok(skipped_max)
    }

    /// Return the next mmap reader that must be scanned.
    ///
    /// Callers must plan replay first so the checkpoint-derived floor is known
    /// before any segment is opened or skipped.
    #[inline]
    pub(crate) fn next_reader(&mut self) -> Result<Option<MmapLogReader>> {
        if self.replay_floor.is_none() {
            return Err(Report::new(InternalError::Generic)
                .attach("redo replay read before explicit plan")
                .into());
        }
        self.planned
            .pop_front()
            .map(MmapLogReader::from_segment)
            .transpose()
    }

    /// Return the planned segment sequence list for tests.
    #[inline]
    #[cfg(test)]
    pub(crate) fn planned_file_seqs(&self) -> Vec<u32> {
        self.planned
            .iter()
            .map(|segment| segment.file_seq)
            .collect()
    }
}

/// Buffered stream of transaction redo records across a sequence of redo files.
pub(crate) struct RedoLogStream {
    /// Source of readers for successive redo files.
    planner: ReplayPlanner,
    /// Reader for the current redo file, if one is open.
    reader: Option<MmapLogReader>,
    /// Decoded records ready for recovery to consume.
    buffer: VecDeque<TrxLog>,
}

impl RedoLogStream {
    /// Create a redo stream from discovered redo files.
    #[inline]
    pub(crate) fn new(discovered: Vec<RedoLogFileDescriptor>) -> Self {
        Self {
            planner: ReplayPlanner::new(discovered),
            reader: None,
            buffer: VecDeque::new(),
        }
    }

    /// Return the sequence for the next writable redo file after this family.
    #[inline]
    pub(crate) fn next_file_seq(&self) -> Result<Option<u32>> {
        self.planner.next_file_seq()
    }

    /// Plan replay using the checkpoint-derived floor and return skipped CTS seed.
    ///
    /// This must be called exactly once before `try_next`; repeated calls are
    /// rejected even if they pass the same floor.
    #[inline]
    pub(crate) fn plan_replay(&mut self, floor: TrxID) -> Result<Option<TrxID>> {
        self.planner.plan(floor)
    }

    /// Return the planned segment sequence list for tests.
    #[inline]
    #[cfg(test)]
    pub(crate) fn planned_file_seqs(&self) -> Vec<u32> {
        self.planner.planned_file_seqs()
    }

    /// Refill the in-memory queue from the current reader or the next redo file.
    ///
    /// Requires prior `plan_replay` because opening the next reader may depend
    /// on checkpoint-derived segment skipping decisions.
    #[inline]
    pub(crate) fn fill_buffer(&mut self) -> Result<()> {
        loop {
            // fill buffer by reading current log file.
            if let Some(reader) = self.reader.as_mut() {
                match reader.read() {
                    ReadLog::DataCorrupted => {
                        return Err(Report::new(DataIntegrityError::LogFileCorrupted).into());
                    }
                    ReadLog::Iterator(mut iter) => {
                        while let Some(res) = iter.try_next()? {
                            self.buffer.push_back(res);
                        }
                        return Ok(());
                    }
                    ReadLog::DataEnd | ReadLog::SizeLimit => {
                        // current file exhausted.
                        self.reader.take();
                    }
                }
            }
            debug_assert!(self.reader.is_none());
            let reader = self.planner.next_reader()?;
            if reader.is_none() {
                return Ok(());
            }
            self.reader = reader;
        }
    }

    /// Try to read the next transaction redo record, reading more files on demand.
    ///
    /// The stream is deliberately not self-planning: callers must provide the
    /// replay floor via `plan_replay` before consuming records.
    #[inline]
    pub(crate) fn try_next(&mut self) -> Result<Option<TrxLog>> {
        match self.buffer.pop_front() {
            res @ Some(_) => Ok(res),
            None => {
                self.fill_buffer()?;
                Ok(self.buffer.pop_front())
            }
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

/// Memory-mapped reader for checksum-protected redo groups in one file.
pub(crate) struct MmapLogReader {
    /// Memory map of the redo file.
    m: Mmap,
    /// Normal physical stride for redo groups in this file.
    log_block_size: usize,
    /// End offset for the current scan; sealed files stop at durable end.
    scan_end_offset: usize,
    /// Sealed range validation state, when the selected super-block is sealed.
    sealed: Option<SealedReplayState>,
    /// Next group offset to read.
    offset: usize,
}

impl MmapLogReader {
    /// Open a redo file, select its newest valid super-block slot, and mmap it.
    #[inline]
    pub(crate) fn new(log_file_path: impl AsRef<Path>, expected_file_seq: u32) -> Result<Self> {
        let segment =
            RedoLogSegment::from_path(PathBuf::from(log_file_path.as_ref()), expected_file_seq)?;
        Self::from_segment(segment)
    }

    /// Open and mmap a redo file using an already validated segment header.
    #[inline]
    pub(crate) fn from_segment(segment: RedoLogSegment) -> Result<Self> {
        let RedoLogSegment {
            path, super_block, ..
        } = segment;
        let file = File::open(&path)?;
        // SAFETY: the file handle stays alive for the duration of mapping
        // creation, and the returned `Mmap` owns the mapping afterward.
        let m = unsafe { Mmap::map(&file)? };
        if super_block.file_max_size as usize > m.len() {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "redo super-block file_max_size exceeds mapped length: path={}, file_max_size={}, mapped_len={}",
                    path.display(),
                    super_block.file_max_size,
                    m.len()
                ))
                .into());
        }
        let (scan_end_offset, sealed) = if super_block.is_sealed() {
            (
                super_block.durable_end_offset as usize,
                Some(SealedReplayState::new(super_block.sealed_redo_range())),
            )
        } else {
            (super_block.file_max_size as usize, None)
        };
        Ok(MmapLogReader {
            m,
            log_block_size: super_block.log_block_size as usize,
            scan_end_offset,
            sealed,
            offset: REDO_DEFAULT_DATA_START_OFFSET,
        })
    }

    #[inline]
    fn validate_sealed_end(&self) -> bool {
        self.sealed.is_none_or(SealedReplayState::validate)
    }

    /// Read and validate one logical fixed-block redo group.
    ///
    /// The payload is not exposed until the group-start metadata, every block
    /// header, each block-level CRC32 checksum, continuation order, end flags,
    /// and assembled payload length have been validated.
    #[inline]
    pub(crate) fn read(&mut self) -> ReadLog {
        if let Some(result) = self.read_exhausted_result() {
            return result;
        }
        let group_start = match self.read_group_start() {
            Ok(group_start) => group_start,
            Err(result) => return result,
        };
        let payload = match self.assemble_group_payload(&group_start) {
            Ok(payload) => payload,
            Err(result) => return result,
        };
        self.finish_group_read(group_start, payload)
    }

    #[inline]
    fn read_exhausted_result(&self) -> Option<ReadLog> {
        if self.offset < self.scan_end_offset {
            return None;
        }
        if !self.validate_sealed_end() {
            return Some(ReadLog::DataCorrupted);
        }
        if self.sealed.is_some() {
            return Some(ReadLog::DataEnd);
        }
        Some(ReadLog::SizeLimit)
    }

    #[inline]
    fn block_at(&self, offset: usize) -> Option<&[u8]> {
        let end = offset.checked_add(self.log_block_size)?;
        if end > self.scan_end_offset {
            return None;
        }
        self.m.get(offset..end)
    }

    #[inline]
    fn read_group_start(&self) -> StdResult<ValidatedGroupStart, ReadLog> {
        // Redo groups always start at fixed-block-aligned offsets.
        debug_assert!(self.offset.is_multiple_of(STORAGE_SECTOR_SIZE));
        let Some(block) = self.block_at(self.offset) else {
            return Err(ReadLog::DataCorrupted);
        };
        if is_zero_redo_block(block) {
            if self.sealed.is_some() {
                return Err(ReadLog::DataCorrupted);
            }
            return Err(ReadLog::DataEnd);
        }
        let Ok((idx, header)) = RedoBlockHeader::deser(block, 0) else {
            return Err(ReadLog::DataCorrupted);
        };
        debug_assert_eq!(idx, RedoBlockHeader::SIZE);
        if header.verify_checksum(block).is_err()
            || header.validate(self.log_block_size).is_err()
            || !header.is_group_start()
        {
            return Err(ReadLog::DataCorrupted);
        }
        let Ok((extension_end, extension)) = RedoGroupStartExtension::deser(block, idx) else {
            return Err(ReadLog::DataCorrupted);
        };
        debug_assert_eq!(
            extension_end,
            RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE
        );
        if extension.validate().is_err() {
            return Err(ReadLog::DataCorrupted);
        }
        let Ok(payload_len) = extension.group_payload_len_usize() else {
            return Err(ReadLog::DataCorrupted);
        };
        let Ok(expected_block_count) = block_count_for_payload(self.log_block_size, payload_len)
        else {
            return Err(ReadLog::DataCorrupted);
        };
        let block_count = extension.group_block_count_usize();
        if expected_block_count != block_count {
            return Err(ReadLog::DataCorrupted);
        }
        if block_count == 1 && !header.is_group_end() {
            return Err(ReadLog::DataCorrupted);
        }
        if block_count > 1 && header.is_group_end() {
            return Err(ReadLog::DataCorrupted);
        }
        Ok(ValidatedGroupStart {
            header,
            extension,
            block_count,
            payload_len,
        })
    }

    #[inline]
    fn assemble_group_payload(
        &self,
        group_start: &ValidatedGroupStart,
    ) -> StdResult<Vec<u8>, ReadLog> {
        let Some(block) = self.block_at(self.offset) else {
            return Err(ReadLog::DataCorrupted);
        };
        let mut payload = Vec::with_capacity(group_start.payload_len);
        if !append_block_payload(
            block,
            self.log_block_size,
            group_start.header,
            RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE,
            group_start.block_count == 1,
            &mut payload,
        ) {
            return Err(ReadLog::DataCorrupted);
        }
        for expected_idx in 1..group_start.block_count {
            self.append_continuation_payload(&mut payload, expected_idx, group_start.block_count)?;
        }
        if payload.len() != group_start.payload_len {
            return Err(ReadLog::DataCorrupted);
        }
        Ok(payload)
    }

    #[inline]
    fn append_continuation_payload(
        &self,
        payload: &mut Vec<u8>,
        expected_idx: usize,
        block_count: usize,
    ) -> StdResult<(), ReadLog> {
        let Some(block_offset) = expected_idx
            .checked_mul(self.log_block_size)
            .and_then(|delta| self.offset.checked_add(delta))
        else {
            return Err(self.incomplete_group_result());
        };
        let Some(block) = self.block_at(block_offset) else {
            return Err(self.incomplete_group_result());
        };
        if is_zero_redo_block(block) {
            return Err(self.incomplete_group_result());
        }
        let Ok((idx, header)) = RedoBlockHeader::deser(block, 0) else {
            return Err(self.incomplete_group_result());
        };
        debug_assert_eq!(idx, RedoBlockHeader::SIZE);
        if header.verify_checksum(block).is_err() {
            return Err(self.incomplete_group_result());
        }
        if header.validate(self.log_block_size).is_err()
            || header.is_group_start()
            || header.group_block_idx as usize != expected_idx
        {
            return Err(ReadLog::DataCorrupted);
        }
        let final_block = expected_idx + 1 == block_count;
        if header.is_group_end() != final_block {
            return Err(ReadLog::DataCorrupted);
        }
        if !append_block_payload(
            block,
            self.log_block_size,
            header,
            RedoBlockHeader::SIZE,
            final_block,
            payload,
        ) {
            return Err(ReadLog::DataCorrupted);
        }
        Ok(())
    }

    #[inline]
    fn finish_group_read(&mut self, group_start: ValidatedGroupStart, payload: Vec<u8>) -> ReadLog {
        let extension = group_start.extension;
        if let Some(sealed) = self.sealed.as_mut() {
            sealed.record_group(extension.min_redo_cts, extension.max_redo_cts);
        }
        self.offset += group_start.block_count * self.log_block_size;
        if self.offset == self.scan_end_offset && !self.validate_sealed_end() {
            return ReadLog::DataCorrupted;
        }
        ReadLog::Iterator(TrxLogIterator::new(
            payload,
            extension.min_redo_cts,
            extension.max_redo_cts,
        ))
    }

    #[inline]
    fn incomplete_group_result(&self) -> ReadLog {
        if self.sealed.is_some() {
            ReadLog::DataCorrupted
        } else {
            ReadLog::DataEnd
        }
    }
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
    use crate::io::{DirectBuf, IOBuf};
    use crate::log::block_group::LogBlockGroup;
    use crate::log::format::{
        REDO_BLOCK_GROUP_START, patch_redo_block_checksum, redo_start_block_payload_capacity,
    };
    use crate::log::redo::{
        DDLRedo, RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind, TableDML,
    };
    use crate::serde::Ser;
    use std::collections::BTreeMap;
    use std::io::Write;

    fn new_reader_at(
        log_file_path: impl AsRef<Path>,
        log_block_size: usize,
        max_file_size: usize,
        offset: usize,
    ) -> Result<MmapLogReader> {
        let file = File::open(log_file_path.as_ref())?;
        // SAFETY: the file handle stays alive for the duration of mapping
        // creation, and the returned `Mmap` owns the mapping afterward.
        let m = unsafe { Mmap::map(&file)? };
        Ok(MmapLogReader {
            m,
            log_block_size,
            scan_end_offset: max_file_size,
            sealed: None,
            offset,
        })
    }

    fn new_sealed_reader_at(
        log_file_path: impl AsRef<Path>,
        log_block_size: usize,
        scan_end_offset: usize,
        offset: usize,
        expected_range: Option<(TrxID, TrxID)>,
    ) -> Result<MmapLogReader> {
        let file = File::open(log_file_path.as_ref())?;
        // SAFETY: the file handle stays alive for the duration of mapping
        // creation, and the returned `Mmap` owns the mapping afterward.
        let m = unsafe { Mmap::map(&file)? };
        Ok(MmapLogReader {
            m,
            log_block_size,
            scan_end_offset,
            sealed: Some(SealedReplayState::new(expected_range)),
            offset,
        })
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

    fn write_log_file(path: impl AsRef<Path>, bytes: &[u8]) {
        let mut file = File::create(path).unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
    }

    fn block_group_with_range(log: TrxLog, min_cts: TrxID, max_cts: TrxID) -> DirectBuf {
        let payload_len = log.ser_len();
        let group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log).unwrap();
        let mut blocks = group
            .finish_with(|count| {
                (0..count)
                    .map(|_| DirectBuf::zeroed(STORAGE_SECTOR_SIZE))
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
    fn test_log_reader_stops_when_initial_block_crosses_logical_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        write_log_file(&file_path, &vec![0u8; STORAGE_SECTOR_SIZE * 2]);

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE * 2, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
    }

    #[test]
    fn test_log_reader_stops_when_expanded_group_crosses_logical_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let block = two_block_start_only();
        write_log_file(&file_path, block.as_bytes());

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataEnd));
    }

    #[test]
    fn test_log_reader_treats_all_zero_group_header_as_eof() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        write_log_file(&file_path, &vec![0u8; STORAGE_SECTOR_SIZE]);

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataEnd));
    }

    #[test]
    fn test_log_reader_rejects_nonzero_empty_group_header() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut bytes = vec![0u8; STORAGE_SECTOR_SIZE];
        bytes[0] = 1;
        write_log_file(&file_path, &bytes);

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
    }

    #[test]
    fn test_log_reader_rejects_bad_checksum_before_body_parsing() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut direct_buf =
            block_group_with_range(simple_trx_log(TrxID::new(1)), TrxID::new(1), TrxID::new(1));
        let payload_start = RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE;
        direct_buf.as_bytes_mut()[payload_start] ^= 0x80;
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
    }

    #[test]
    fn test_trx_log_iterator_rejects_body_cts_below_header_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = block_group_with_range(log, TrxID::new(6), TrxID::new(8));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE * 2, 0).unwrap();
        let ReadLog::Iterator(mut iter) = reader.read() else {
            panic!("expected valid group");
        };

        let err = iter.try_next().unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_trx_log_iterator_rejects_body_cts_above_header_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = block_group_with_range(log, TrxID::new(1), TrxID::new(4));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE * 2, 0).unwrap();
        let ReadLog::Iterator(mut iter) = reader.read() else {
            panic!("expected valid group");
        };

        let err = iter.try_next().unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_trx_log_iterator_accepts_body_cts_within_loose_header_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = block_group_with_range(log, TrxID::new(4), TrxID::new(6));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE * 2, 0).unwrap();
        let ReadLog::Iterator(mut iter) = reader.read() else {
            panic!("expected valid group");
        };

        assert!(iter.try_next().unwrap().is_some());
        assert!(iter.try_next().unwrap().is_none());
    }

    #[test]
    fn test_sealed_reader_accepts_matching_group_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = block_group_with_range(log, TrxID::new(4), TrxID::new(6));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader = new_sealed_reader_at(
            file_path,
            STORAGE_SECTOR_SIZE,
            STORAGE_SECTOR_SIZE,
            0,
            Some((TrxID::new(4), TrxID::new(6))),
        )
        .unwrap();

        let ReadLog::Iterator(mut iter) = reader.read() else {
            panic!("expected sealed group");
        };
        assert!(iter.try_next().unwrap().is_some());
        assert!(iter.try_next().unwrap().is_none());
        assert!(matches!(reader.read(), ReadLog::DataEnd));
    }

    #[test]
    fn test_sealed_reader_accepts_empty_file_without_zero_eof() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        write_log_file(&file_path, &vec![0u8; STORAGE_SECTOR_SIZE]);

        let mut reader = new_sealed_reader_at(file_path, STORAGE_SECTOR_SIZE, 0, 0, None).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataEnd));
    }

    #[test]
    fn test_sealed_reader_rejects_zero_eof_before_durable_end() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        write_log_file(&file_path, &vec![0u8; STORAGE_SECTOR_SIZE]);

        let mut reader = new_sealed_reader_at(
            file_path,
            STORAGE_SECTOR_SIZE,
            STORAGE_SECTOR_SIZE,
            0,
            Some((TrxID::new(1), TrxID::new(1))),
        )
        .unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
    }

    #[test]
    fn test_sealed_reader_rejects_group_crossing_durable_end() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let block = two_block_start_only();
        write_log_file(&file_path, block.as_bytes());

        let mut reader = new_sealed_reader_at(
            file_path,
            STORAGE_SECTOR_SIZE,
            STORAGE_SECTOR_SIZE,
            0,
            Some((TrxID::new(1), TrxID::new(1))),
        )
        .unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
    }

    #[test]
    fn test_sealed_reader_rejects_mismatched_group_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = block_group_with_range(log, TrxID::new(5), TrxID::new(5));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader = new_sealed_reader_at(
            file_path,
            STORAGE_SECTOR_SIZE,
            STORAGE_SECTOR_SIZE,
            0,
            Some((TrxID::new(4), TrxID::new(6))),
        )
        .unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
    }

    #[test]
    fn test_log_reader_read_multi_trx_log_in_one_group() {
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
        let mut file = File::create(&file_path).unwrap();
        for block in &blocks {
            file.write_all(block.as_bytes()).unwrap();
        }
        file.flush().unwrap();

        let mut reader = new_reader_at(file_path, 4096, 1024 * 1024, 0).unwrap();
        match reader.read() {
            ReadLog::DataCorrupted | ReadLog::DataEnd | ReadLog::SizeLimit => {
                panic!("invalid data")
            }
            ReadLog::Iterator(mut iter) => {
                let log1 = iter.try_next().unwrap().unwrap();
                assert!(log1.header.trx_kind == RedoTrxKind::System);
                let log2 = iter.try_next().unwrap().unwrap();
                assert!(log2.header.trx_kind == RedoTrxKind::User);
                assert!(iter.try_next().unwrap().is_none());
            }
        }
    }
}
