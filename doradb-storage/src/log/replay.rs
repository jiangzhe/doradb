use crate::error::{DataIntegrityError, InternalError, Result};
use crate::id::TrxID;
use crate::io::STORAGE_SECTOR_SIZE;
use crate::log::buf::TrxLog;
use crate::log::format::{
    REDO_DEFAULT_DATA_START_OFFSET, RedoGroupHeader, RedoSuperBlock, select_redo_super_block,
};
use crate::log::{RedoLogFileDescriptor, next_redo_file_seq};
use crate::serde::Deser;
use error_stack::Report;
use memmap2::Mmap;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{ErrorKind as IoErrorKind, Read};
use std::path::{Path, PathBuf};

/// Result of reading one redo group from a log file.
pub(crate) enum ReadLog<'a> {
    /// The current group header is all zeroes, which marks logical EOF.
    DataEnd,
    /// The configured logical file size has been reached.
    SizeLimit,
    /// The current group or file header failed integrity validation.
    DataCorrupted,
    /// A checksum-verified group body ready for transaction iteration.
    Some(LogGroup<'a>),
}

/// Iterator over transaction redo records inside one validated redo group.
///
/// The group owns no bytes; it advances through the mmap-backed body slice and
/// enforces the commit timestamp range advertised by the group header.
pub(crate) struct LogGroup<'a> {
    /// Unconsumed logical body bytes.
    data: &'a [u8],
    /// Lowest commit timestamp accepted for body records.
    min_cts: TrxID,
    /// Highest commit timestamp accepted for body records.
    max_cts: TrxID,
}

impl<'a> LogGroup<'a> {
    /// Create a group iterator from a checksum-verified body slice.
    #[inline]
    fn new(data: &'a [u8], min_cts: TrxID, max_cts: TrxID) -> Self {
        LogGroup {
            data,
            min_cts,
            max_cts,
        }
    }

    /// Return the next transaction frame, or `None` once the body is exhausted.
    #[inline]
    pub(crate) fn try_next(&mut self) -> Result<Option<TrxLog>> {
        if self.data.is_empty() {
            return Ok(None);
        }
        let (offset, res) = TrxLog::deser(self.data, 0)?;
        // Defensive progress check: a zero-byte parser result would make replay
        // loop forever on corrupt input.
        if offset == 0 {
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
        self.data = &self.data[offset..];
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

/// Buffered replayer of transaction redo records across a sequence of redo files.
pub(crate) struct RedoLogReplayer {
    /// Source of readers for successive redo files.
    planner: ReplayPlanner,
    /// Reader for the current redo file, if one is open.
    reader: Option<MmapLogReader>,
    /// Decoded records ready for recovery to consume.
    buffer: VecDeque<TrxLog>,
}

impl RedoLogReplayer {
    /// Create a redo replayer from discovered redo files.
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
    /// This must be called exactly once before `pop`; repeated calls are
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
                    ReadLog::Some(mut log_group) => {
                        while let Some(res) = log_group.try_next()? {
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

    /// Pop the next transaction redo record, reading more files on demand.
    ///
    /// The replayer is deliberately not self-planning: callers must provide the
    /// replay floor via `plan_replay` before consuming records.
    #[inline]
    pub(crate) fn pop(&mut self) -> Result<Option<TrxLog>> {
        match self.buffer.pop_front() {
            res @ Some(_) => Ok(res),
            None => {
                self.fill_buffer()?;
                Ok(self.buffer.pop_front())
            }
        }
    }
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
    fn record_group(&mut self, header: RedoGroupHeader) {
        self.actual_min_cts = Some(
            self.actual_min_cts
                .map_or(header.min_cts, |current| current.min(header.min_cts)),
        );
        self.actual_max_cts = Some(
            self.actual_max_cts
                .map_or(header.max_cts, |current| current.max(header.max_cts)),
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

    /// Return the end offset for a bounded read from the current file offset.
    #[inline]
    fn bounded_read_end(&self, len: usize) -> Option<usize> {
        let end = self.offset.checked_add(len)?;
        (end <= self.scan_end_offset).then_some(end)
    }

    #[inline]
    fn validate_sealed_end(&self) -> bool {
        self.sealed.is_none_or(SealedReplayState::validate)
    }

    /// Read and validate one physical redo group.
    ///
    /// The body is not exposed until the header fields and whole-group checksum
    /// have both been validated, so transaction parsing never sees bytes from a
    /// corrupt or partially written group.
    #[inline]
    pub(crate) fn read(&mut self) -> ReadLog<'_> {
        if self.offset >= self.scan_end_offset {
            if !self.validate_sealed_end() {
                return ReadLog::DataCorrupted;
            }
            if self.sealed.is_some() {
                return ReadLog::DataEnd;
            }
            return ReadLog::SizeLimit; // file is exhausted.
        }
        // Redo groups always start at sector-aligned offsets.
        debug_assert!(self.offset.is_multiple_of(STORAGE_SECTOR_SIZE));
        let Some(header_end) = self.bounded_read_end(RedoGroupHeader::SIZE) else {
            return ReadLog::DataCorrupted;
        };
        let Some(header_bytes) = self.m.get(self.offset..header_end) else {
            return ReadLog::DataCorrupted;
        };
        let Ok((idx, header)) = RedoGroupHeader::deser(header_bytes, 0) else {
            return ReadLog::DataCorrupted;
        };
        debug_assert_eq!(idx, RedoGroupHeader::SIZE);
        if header.is_zero_eof() {
            if self.sealed.is_some() {
                return ReadLog::DataCorrupted;
            }
            return ReadLog::DataEnd;
        }
        if header.validate().is_err() {
            return ReadLog::DataCorrupted;
        }
        // The header determines how many physical bytes must be present and
        // covered by the checksum, including sector padding for large groups.
        let Ok(physical_len) = header.physical_len(self.log_block_size) else {
            return ReadLog::DataCorrupted;
        };
        let Some(read_end) = self.bounded_read_end(physical_len) else {
            return ReadLog::DataCorrupted;
        };
        let Some(buf) = self.m.get(self.offset..read_end) else {
            return ReadLog::DataCorrupted;
        };
        if header.verify_checksum(buf).is_err() {
            return ReadLog::DataCorrupted;
        }
        // Only the logical body is handed to `LogGroup`; padding stays part of
        // the checksum domain but is not parsed as transaction data.
        let Ok(body_len) = header.body_len_usize() else {
            return ReadLog::DataCorrupted;
        };
        let Some(body_end) = RedoGroupHeader::SIZE.checked_add(body_len) else {
            return ReadLog::DataCorrupted;
        };
        let Some(body) = buf.get(RedoGroupHeader::SIZE..body_end) else {
            return ReadLog::DataCorrupted;
        };
        if let Some(sealed) = self.sealed.as_mut() {
            sealed.record_group(header);
        }
        self.offset += physical_len;
        if self.offset == self.scan_end_offset && !self.validate_sealed_end() {
            return ReadLog::DataCorrupted;
        }
        ReadLog::Some(LogGroup::new(body, header.min_cts, header.max_cts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::test_page_id;
    use crate::id::{RowID, TableID, TrxID};
    use crate::io::IOBuf;
    use crate::log::buf::LogBuf;
    use crate::log::redo::{
        DDLRedo, RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind, TableDML,
    };
    use crate::serde::{Ser, Serde};
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

    fn log_buf_with_header_range(
        log: &TrxLog,
        min_cts: TrxID,
        max_cts: TrxID,
    ) -> crate::io::DirectBuf {
        let body_len = log.ser_len();
        let mut buf = LogBuf::new(STORAGE_SECTOR_SIZE);
        buf.append_trx_log(log);
        let mut direct_buf = buf.finish();
        let header = RedoGroupHeader::new(body_len, min_cts, max_cts);
        header.ser(direct_buf.as_bytes_mut(), 0);
        RedoGroupHeader::patch_checksum(direct_buf.as_bytes_mut()).unwrap();
        direct_buf
    }

    #[test]
    fn test_log_reader_stops_when_initial_block_crosses_logical_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        write_log_file(&file_path, &vec![0u8; STORAGE_SECTOR_SIZE * 2]);

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE * 2, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataEnd));
    }

    #[test]
    fn test_log_reader_stops_when_expanded_group_crosses_logical_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut bytes = vec![0u8; STORAGE_SECTOR_SIZE * 2];
        let header = RedoGroupHeader::new(STORAGE_SECTOR_SIZE, TrxID::new(1), TrxID::new(1));
        header.ser(&mut bytes[..], 0);
        write_log_file(&file_path, &bytes);

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
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
        let header = RedoGroupHeader {
            checksum: 1,
            body_len: 0,
            min_cts: TrxID::new(1),
            max_cts: TrxID::new(1),
        };
        header.ser(&mut bytes[..], 0);
        write_log_file(&file_path, &bytes);

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
    }

    #[test]
    fn test_log_reader_rejects_bad_checksum_before_body_parsing() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut bytes = vec![0u8; STORAGE_SECTOR_SIZE];
        let body_start = RedoGroupHeader::SIZE;
        let mut idx = body_start;
        idx = bytes[..].ser_u64(idx, 18);
        idx = bytes[..].ser_u64(idx, 1);
        idx = bytes[..].ser_u8(idx, RedoTrxKind::System as u8);
        idx = bytes[..].ser_bool(idx, false);
        idx = bytes[..].ser_u64(idx, u64::MAX);
        let body_len = idx - body_start;
        let header = RedoGroupHeader::new(body_len, TrxID::new(1), TrxID::new(1));
        header.ser(&mut bytes[..], 0);
        write_log_file(&file_path, &bytes);

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE, 0).unwrap();

        assert!(matches!(reader.read(), ReadLog::DataCorrupted));
    }

    #[test]
    fn test_log_group_rejects_body_cts_below_header_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = log_buf_with_header_range(&log, TrxID::new(6), TrxID::new(8));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE * 2, 0).unwrap();
        let ReadLog::Some(mut group) = reader.read() else {
            panic!("expected valid group");
        };

        let err = group.try_next().unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_log_group_rejects_body_cts_above_header_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = log_buf_with_header_range(&log, TrxID::new(1), TrxID::new(4));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE * 2, 0).unwrap();
        let ReadLog::Some(mut group) = reader.read() else {
            panic!("expected valid group");
        };

        let err = group.try_next().unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_log_group_accepts_body_cts_within_loose_header_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = log_buf_with_header_range(&log, TrxID::new(4), TrxID::new(6));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader =
            new_reader_at(file_path, STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE * 2, 0).unwrap();
        let ReadLog::Some(mut group) = reader.read() else {
            panic!("expected valid group");
        };

        assert!(group.try_next().unwrap().is_some());
        assert!(group.try_next().unwrap().is_none());
    }

    #[test]
    fn test_sealed_reader_accepts_matching_group_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let log = simple_trx_log(TrxID::new(5));
        let direct_buf = log_buf_with_header_range(&log, TrxID::new(4), TrxID::new(6));
        write_log_file(&file_path, direct_buf.as_bytes());

        let mut reader = new_sealed_reader_at(
            file_path,
            STORAGE_SECTOR_SIZE,
            STORAGE_SECTOR_SIZE,
            0,
            Some((TrxID::new(4), TrxID::new(6))),
        )
        .unwrap();

        let ReadLog::Some(mut group) = reader.read() else {
            panic!("expected sealed group");
        };
        assert!(group.try_next().unwrap().is_some());
        assert!(group.try_next().unwrap().is_none());
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
        let mut bytes = vec![0u8; STORAGE_SECTOR_SIZE * 2];
        let header = RedoGroupHeader::new(STORAGE_SECTOR_SIZE, TrxID::new(1), TrxID::new(1));
        header.ser(&mut bytes[..], 0);
        RedoGroupHeader::patch_checksum(&mut bytes).unwrap();
        write_log_file(&file_path, &bytes);

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
        let direct_buf = log_buf_with_header_range(&log, TrxID::new(5), TrxID::new(5));
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
        let mut buf = LogBuf::new(STORAGE_SECTOR_SIZE);
        buf.append_trx_log(&log1);

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
        buf.append_trx_log(&log2);

        let direct_buf = buf.finish();

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(direct_buf.as_bytes()).unwrap();
        file.flush().unwrap();

        let mut reader = new_reader_at(file_path, 4096, 1024 * 1024, 0).unwrap();
        match reader.read() {
            ReadLog::DataCorrupted | ReadLog::DataEnd | ReadLog::SizeLimit => {
                panic!("invalid data")
            }
            ReadLog::Some(mut group) => {
                let log1 = group.try_next().unwrap().unwrap();
                assert!(log1.header.trx_kind == RedoTrxKind::System);
                let log2 = group.try_next().unwrap().unwrap();
                assert!(log2.header.trx_kind == RedoTrxKind::User);
                assert!(group.try_next().unwrap().is_none());
            }
        }
    }
}
