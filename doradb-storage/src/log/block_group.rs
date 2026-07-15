use crate::error::{DataIntegrityError, Result};
use crate::id::TrxID;
use crate::io::{DirectBuf, IOBuf};
use crate::log::format::{
    REDO_BLOCK_GROUP_END, REDO_BLOCK_GROUP_START, RedoBlockHeader, RedoGroupStartExtension,
    patch_redo_block_checksum, redo_continuation_block_payload_capacity,
    redo_start_block_payload_capacity,
};
use crate::log::redo::{RedoHeader, RedoLogs};
use crate::serde::{Deser, MinBytesHint, Ser, Serde, min_bytes_hint};
use error_stack::Report;
use std::mem;

/// Smallest valid transaction frame: redo header plus an empty redo payload.
const MIN_TRX_LOG_FRAME_LEN: usize =
    mem::size_of::<TrxID>() + mem::size_of::<u8>() + mem::size_of::<u8>() + mem::size_of::<u64>();

/// Fixed-block builder for one logical redo group.
///
/// The builder keeps transaction frames in logical order and materializes them
/// into one or more exact-`log_block_size` direct buffers only when the group is
/// handed to the redo writer.
pub(crate) struct LogBlockGroup {
    /// Fixed physical write size for every redo data block.
    log_block_size: usize,
    /// Transaction frames in this logical group.
    trx_logs: Vec<TrxLog>,
    /// Total logical payload length across all transaction frames.
    payload_len: usize,
    /// Inclusive commit timestamp range among appended transaction frames.
    cts_range: (TrxID, TrxID),
    /// Number of fixed data blocks needed for this logical group.
    block_count: usize,
}

impl LogBlockGroup {
    /// Start a logical redo group with one redo-bearing transaction.
    #[inline]
    pub(crate) fn new(log_block_size: usize, trx_log: TrxLog) -> Result<Self> {
        let payload_len = trx_log.ser_len();
        let block_count = block_count_for_payload(log_block_size, payload_len)?;
        let cts = trx_log.header.cts;
        Ok(Self {
            log_block_size,
            trx_logs: vec![trx_log],
            payload_len,
            cts_range: (cts, cts),
            block_count,
        })
    }

    /// Return the physical byte count written for this logical group.
    #[inline]
    pub(crate) fn physical_len(&self) -> usize {
        self.block_count * self.log_block_size
    }

    /// Return whether this group already spans multiple fixed data blocks.
    #[inline]
    pub(crate) fn is_multi_block(&self) -> bool {
        self.block_count > 1
    }

    /// Return the real serialized redo CTS range tracked for this group.
    #[inline]
    pub(crate) fn redo_cts_range(&self) -> (TrxID, TrxID) {
        self.cts_range
    }

    /// Return whether an additional serialized frame of `len` bytes can join.
    ///
    /// Redo-bearing multi-transaction groups are intentionally kept within one
    /// fixed block. A group that already spans blocks is a single oversized
    /// transaction group and cannot accept more redo-bearing transactions.
    #[inline]
    pub(crate) fn capable_for(&self, len: usize) -> bool {
        if self.is_multi_block() {
            return false;
        }
        let Ok(start_capacity) = redo_start_block_payload_capacity(self.log_block_size) else {
            return false;
        };
        self.payload_len
            .checked_add(len)
            .is_some_and(|payload_len| payload_len <= start_capacity)
    }

    /// Append one transaction frame to a one-block logical group.
    #[inline]
    pub(crate) fn append_trx_log(&mut self, trx_log: TrxLog) -> Option<TrxLog> {
        let ser_len = trx_log.ser_len();
        if !self.capable_for(ser_len) {
            return Some(trx_log);
        }
        let cts = trx_log.header.cts;
        self.payload_len += ser_len;
        self.cts_range = (self.cts_range.0.min(cts), self.cts_range.1.max(cts));
        self.block_count = block_count_for_payload(self.log_block_size, self.payload_len)
            .expect("existing redo group must have a valid block size");
        debug_assert_eq!(self.block_count, 1);
        self.trx_logs.push(trx_log);
        None
    }

    /// Materialize this logical group using caller-supplied write buffers.
    #[inline]
    pub(crate) fn finish_with<F>(self, take_blocks: F) -> Result<Vec<DirectBuf>>
    where
        F: FnOnce(usize) -> Vec<DirectBuf>,
    {
        let blocks = take_blocks(self.block_count);
        LogBlockGroupWriter::new(&self, blocks)?.finish()
    }

    #[inline]
    fn block_payload_start(&self, block_idx: usize) -> usize {
        if block_idx == 0 {
            RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE
        } else {
            RedoBlockHeader::SIZE
        }
    }

    #[inline]
    fn block_payload_capacity(&self, block_idx: usize) -> usize {
        self.log_block_size - self.block_payload_start(block_idx)
    }

    #[inline]
    fn block_payload_remaining(&self, block_idx: usize, payload_len: usize) -> usize {
        self.block_payload_capacity(block_idx) - payload_len
    }
}

struct LogBlockGroupWriter<'a> {
    group: &'a LogBlockGroup,
    blocks: Vec<DirectBuf>,
    payload_lens: Vec<usize>,
    block_idx: usize,
    scratch: Vec<u8>,
}

impl<'a> LogBlockGroupWriter<'a> {
    #[inline]
    fn new(group: &'a LogBlockGroup, blocks: Vec<DirectBuf>) -> Result<Self> {
        if blocks.len() != group.block_count {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-data, expected_block_count={}, actual_block_count={}",
                    group.block_count,
                    blocks.len()
                ))
                .into());
        }
        for (block_idx, block) in blocks.iter().enumerate() {
            if block.capacity() != group.log_block_size {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "block=redo-data, block_idx={block_idx}, expected_block_size={}, actual_block_size={}",
                        group.log_block_size,
                        block.capacity()
                    ))
                    .into());
            }
        }
        Ok(Self {
            group,
            blocks,
            payload_lens: vec![0; group.block_count],
            block_idx: 0,
            scratch: Vec::new(),
        })
    }

    #[inline]
    fn finish(mut self) -> Result<Vec<DirectBuf>> {
        self.write_payloads();
        self.finalize_blocks()?;
        Ok(self.blocks)
    }

    #[inline]
    fn write_payloads(&mut self) {
        for trx_log in &self.group.trx_logs {
            self.write_trx_log(trx_log);
        }
    }

    #[inline]
    fn write_trx_log(&mut self, trx_log: &TrxLog) {
        self.skip_full_blocks();
        let frame_len = trx_log.ser_len();
        let remaining = self
            .group
            .block_payload_remaining(self.block_idx, self.payload_lens[self.block_idx]);
        if frame_len <= remaining {
            self.write_trx_log_direct(trx_log, frame_len);
        } else {
            self.write_trx_log_via_scratch(trx_log, frame_len);
        }
    }

    #[inline]
    fn write_trx_log_direct(&mut self, trx_log: &TrxLog, frame_len: usize) {
        let block_idx = self.block_idx;
        let dst_start = self.group.block_payload_start(block_idx) + self.payload_lens[block_idx];
        let dst_end = trx_log.ser(self.blocks[block_idx].as_bytes_mut(), dst_start);
        debug_assert_eq!(dst_end, dst_start + frame_len);
        self.payload_lens[block_idx] += frame_len;
    }

    #[inline]
    fn write_trx_log_via_scratch(&mut self, trx_log: &TrxLog, frame_len: usize) {
        self.scratch.resize(frame_len, 0);
        let frame_end = trx_log.ser(&mut self.scratch[..], 0);
        debug_assert_eq!(frame_end, frame_len);
        let mut copied = 0usize;
        while copied < frame_len {
            self.skip_full_blocks();
            let remaining = self
                .group
                .block_payload_remaining(self.block_idx, self.payload_lens[self.block_idx]);
            let chunk_len = remaining.min(frame_len - copied);
            self.copy_scratch_chunk(copied, chunk_len);
            copied += chunk_len;
        }
    }

    #[inline]
    fn copy_scratch_chunk(&mut self, copied: usize, chunk_len: usize) {
        let block_idx = self.block_idx;
        let dst_start = self.group.block_payload_start(block_idx) + self.payload_lens[block_idx];
        let dst_end = dst_start + chunk_len;
        self.blocks[block_idx].as_bytes_mut()[dst_start..dst_end]
            .copy_from_slice(&self.scratch[copied..copied + chunk_len]);
        self.payload_lens[block_idx] += chunk_len;
    }

    #[inline]
    fn skip_full_blocks(&mut self) {
        while self.block_idx + 1 < self.group.block_count
            && self.payload_lens[self.block_idx]
                == self.group.block_payload_capacity(self.block_idx)
        {
            self.block_idx += 1;
        }
    }

    #[inline]
    fn finalize_blocks(&mut self) -> Result<()> {
        debug_assert_eq!(
            self.payload_lens.iter().sum::<usize>(),
            self.group.payload_len
        );
        for idx in 0..self.group.block_count {
            self.finalize_block(idx)?;
        }
        Ok(())
    }

    #[inline]
    fn finalize_block(&mut self, idx: usize) -> Result<()> {
        let payload_len = self.payload_lens[idx];
        let capacity = self.group.block_payload_capacity(idx);
        if idx + 1 < self.group.block_count {
            debug_assert_eq!(payload_len, capacity);
        } else {
            debug_assert!(payload_len <= capacity);
        }
        let header = RedoBlockHeader::new(self.block_flags(idx), payload_len, idx)?;
        let header_end = header.ser(self.blocks[idx].as_bytes_mut(), 0);
        debug_assert_eq!(header_end, RedoBlockHeader::SIZE);
        if idx == 0 {
            let extension = RedoGroupStartExtension::new(
                self.group.payload_len,
                self.group.block_count,
                self.group.cts_range.0,
                self.group.cts_range.1,
            )?;
            let extension_end = extension.ser(self.blocks[idx].as_bytes_mut(), header_end);
            debug_assert_eq!(
                extension_end,
                RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE
            );
        }
        patch_redo_block_checksum(self.blocks[idx].as_bytes_mut())?;
        Ok(())
    }

    #[inline]
    fn block_flags(&self, block_idx: usize) -> u8 {
        let mut flags = 0u8;
        if block_idx == 0 {
            flags |= REDO_BLOCK_GROUP_START;
        }
        if block_idx + 1 == self.group.block_count {
            flags |= REDO_BLOCK_GROUP_END;
        }
        flags
    }
}

/// Length-prefixed transaction redo record stored inside a redo group body.
///
/// `data_len` covers `header + payload` only. The encoded frame length prefix
/// lets replay skip exactly one transaction record and reject under-consumed or
/// over-consumed frame payloads.
#[derive(Debug)]
pub(crate) struct TrxLog {
    /// Serialized length after the u64 frame prefix.
    data_len: usize,
    /// Transaction redo metadata.
    pub(crate) header: RedoHeader,
    /// Transaction redo payload.
    pub(crate) payload: RedoLogs,
}

impl TrxLog {
    /// Build a transaction frame from redo header and payload values.
    #[inline]
    pub(crate) fn new(header: RedoHeader, payload: RedoLogs) -> Self {
        let data_len = header.ser_len() + payload.ser_len();
        TrxLog {
            data_len,
            header,
            payload,
        }
    }

    /// Split the frame into the redo header and payload.
    #[inline]
    pub(crate) fn into_inner(self) -> (RedoHeader, RedoLogs) {
        (self.header, self.payload)
    }
}

impl Deser for TrxLog {
    const MIN_BYTES_HINT: MinBytesHint =
        min_bytes_hint(mem::size_of::<u64>() + MIN_TRX_LOG_FRAME_LEN);

    #[inline]
    fn deser<S: Serde + ?Sized>(
        input: &S,
        start_idx: usize,
    ) -> crate::serde::DeserResult<(usize, Self)> {
        let (frame_start, data_len) = input.deser_u64(start_idx)?;
        let data_len = usize::try_from(data_len).map_err(|_| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=redo-trx, trx_data_len_exceeds_usize")
        })?;
        // Check the advertised frame boundary before slicing. The primitive
        // deserializers rely on debug assertions and the group checksum/length
        // checks, so this is the runtime frame boundary for transaction replay.
        let remaining = input.size().checked_sub(frame_start).ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("block=redo-trx, frame_start={frame_start}"))
        })?;
        if data_len > remaining {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "block=redo-trx, trx_data_len={data_len}, remaining_group_body={remaining}"
                )),
            );
        }
        // A frame too small to hold an empty transaction record is always
        // corrupt and would otherwise fail later with less useful context.
        if data_len < MIN_TRX_LOG_FRAME_LEN {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-trx, trx_data_len={data_len}, min_trx_frame_len={MIN_TRX_LOG_FRAME_LEN}"
                )));
        }
        let frame_end = frame_start + data_len;
        let (_, frame) = input.deser(frame_start, data_len)?;
        let (idx, header) = RedoHeader::deser(frame, 0)?;
        let (idx, payload) = RedoLogs::deser(frame, idx)?;
        // The length prefix is authoritative: nested payload parsers must
        // consume exactly the advertised frame, with no trailing garbage.
        if idx != frame.len() {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "block=redo-trx, trx_frame_len={}, consumed={idx}",
                    frame.len()
                )),
            );
        }
        Ok((
            frame_end,
            TrxLog {
                data_len,
                header,
                payload,
            },
        ))
    }
}

impl Ser<'_> for TrxLog {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>() + self.data_len
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        debug_assert!(self.data_len == self.header.ser_len() + self.payload.ser_len());
        let idx = out.ser_u64(start_idx, self.data_len as u64);
        let idx = self.header.ser(out, idx);
        self.payload.ser(out, idx)
    }
}

/// Return the fixed-block count for a logical payload length.
#[inline]
pub(crate) fn block_count_for_payload(log_block_size: usize, payload_len: usize) -> Result<usize> {
    let start_capacity = redo_start_block_payload_capacity(log_block_size)?;
    if payload_len <= start_capacity {
        return Ok(1);
    }
    let continuation_capacity = redo_continuation_block_payload_capacity(log_block_size)?;
    if continuation_capacity == 0 {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!("block=redo-data, log_block_size={log_block_size}"))
            .into());
    }
    Ok(1 + (payload_len - start_capacity).div_ceil(continuation_capacity))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::test_page_id;
    use crate::id::{RowID, TableID};
    use crate::io::STORAGE_SECTOR_SIZE;
    use crate::log::redo::{RedoTrxKind, RowRedo, RowRedoKind, TableDML};
    use crate::value::Val;
    use std::collections::BTreeMap;
    use std::iter::repeat_n;

    fn simple_trx_log(cts: TrxID) -> TrxLog {
        TrxLog::new(
            RedoHeader {
                cts,
                trx_kind: RedoTrxKind::System,
            },
            RedoLogs::default(),
        )
    }

    fn large_trx_log(cts: TrxID) -> TrxLog {
        let mut rows = BTreeMap::new();
        let s: String = repeat_n('a', 8000).collect();
        rows.insert(
            RowID::new(1u64),
            RowRedo {
                page_id: test_page_id(5),
                row_id: RowID::new(100),
                kind: RowRedoKind::Insert(vec![Val::from(1u32), Val::from(&s[..])]),
            },
        );
        let mut dml = BTreeMap::new();
        dml.insert(TableID::new(5u64), TableDML { rows });
        TrxLog::new(
            RedoHeader {
                cts,
                trx_kind: RedoTrxKind::User,
            },
            RedoLogs { ddl: None, dml },
        )
    }

    #[test]
    fn test_trx_log_rejects_frame_exceeding_group_body() {
        let mut bytes = vec![0u8; mem::size_of::<u64>()];
        bytes[..].ser_u64(0, 1);

        let err = TrxLog::deser(&bytes[..], 0).unwrap_err();

        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_trx_log_rejects_under_consumed_frame() {
        let log = simple_trx_log(TrxID::new(7));
        let mut bytes = vec![0u8; log.ser_len() + 1];
        let idx = log.ser(&mut bytes[..], 0);
        bytes[..].ser_u64(0, (log.data_len + 1) as u64);
        bytes[idx] = 99;

        let err = TrxLog::deser(&bytes[..], 0).unwrap_err();

        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_trx_log_rejects_over_consumed_frame() {
        let log = simple_trx_log(TrxID::new(7));
        let mut bytes = vec![0u8; log.ser_len()];
        log.ser(&mut bytes[..], 0);
        bytes[..].ser_u64(0, 1);

        let err = TrxLog::deser(&bytes[..], 0).unwrap_err();

        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_log_block_group_exposes_serialized_redo_cts_range() {
        let mut group =
            LogBlockGroup::new(STORAGE_SECTOR_SIZE, simple_trx_log(TrxID::new(7))).unwrap();
        assert_eq!(group.redo_cts_range(), (TrxID::new(7), TrxID::new(7)));

        assert!(
            group
                .append_trx_log(simple_trx_log(TrxID::new(9)))
                .is_none()
        );
        assert_eq!(group.redo_cts_range(), (TrxID::new(7), TrxID::new(9)));
    }

    #[test]
    fn test_log_block_group_append_rejects_over_capacity_without_mutation() {
        let first_log = simple_trx_log(TrxID::new(7));
        let initial_payload_len = first_log.ser_len();
        let mut group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, first_log).unwrap();
        let initial_block_count = group.block_count;
        let initial_cts_range = group.cts_range;
        let initial_trx_count = group.trx_logs.len();
        let second_log = large_trx_log(TrxID::new(9));
        let second_len = second_log.ser_len();
        assert!(!group.capable_for(second_len));

        let rejected = group
            .append_trx_log(second_log)
            .expect("oversized second trx log must be rejected");

        assert_eq!(rejected.ser_len(), second_len);
        assert_eq!(rejected.header.cts, TrxID::new(9));
        assert_eq!(group.payload_len, initial_payload_len);
        assert_eq!(group.cts_range, initial_cts_range);
        assert_eq!(group.block_count, initial_block_count);
        assert_eq!(group.trx_logs.len(), initial_trx_count);
    }

    #[test]
    fn test_log_block_group_materializes_fixed_block() {
        let log = simple_trx_log(TrxID::new(7));
        let payload_len = log.ser_len();
        let group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log).unwrap();

        let blocks = group
            .finish_with(|count| {
                (0..count)
                    .map(|_| DirectBuf::zeroed(STORAGE_SECTOR_SIZE))
                    .collect()
            })
            .unwrap();

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].capacity(), STORAGE_SECTOR_SIZE);
        let (_, header) = RedoBlockHeader::deser(blocks[0].as_bytes(), 0).unwrap();
        header.verify_checksum(blocks[0].as_bytes()).unwrap();
        assert!(header.is_group_start());
        assert!(header.is_group_end());
        assert_eq!(header.payload_len_usize(), payload_len);
        let (_, extension) =
            RedoGroupStartExtension::deser(blocks[0].as_bytes(), RedoBlockHeader::SIZE).unwrap();
        assert_eq!(extension.group_payload_len_usize().unwrap(), payload_len);
        assert_eq!(extension.group_block_count_usize(), 1);
        assert_eq!(extension.min_redo_cts, TrxID::new(7));
        assert_eq!(extension.max_redo_cts, TrxID::new(7));
    }

    #[test]
    fn test_log_block_group_finish_with_uses_supplied_block_batch() {
        let cts = TrxID::new(13);
        let log = large_trx_log(cts);
        let payload_len = log.ser_len();
        let block_count = block_count_for_payload(STORAGE_SECTOR_SIZE, payload_len).unwrap();
        let group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log).unwrap();
        let mut requested_count = 0usize;

        let blocks = group
            .finish_with(|count| {
                requested_count = count;
                (0..count)
                    .map(|_| DirectBuf::zeroed(STORAGE_SECTOR_SIZE))
                    .collect()
            })
            .unwrap();

        assert_eq!(requested_count, block_count);
        assert_eq!(blocks.len(), block_count);
        for block in blocks {
            assert_eq!(block.capacity(), STORAGE_SECTOR_SIZE);
        }
    }

    #[test]
    fn test_log_block_group_finish_with_rejects_wrong_block_count() {
        let cts = TrxID::new(13);
        let log = simple_trx_log(cts);
        let group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log).unwrap();

        let err = match group.finish_with(|_| Vec::new()) {
            Ok(_) => panic!("wrong block count must be rejected"),
            Err(err) => err,
        };

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_block_count_for_payload_uses_start_and_continuation_boundaries() {
        let start_capacity = redo_start_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap();
        let continuation_capacity =
            redo_continuation_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap();
        let cases = [
            (1, 1),
            (start_capacity, 1),
            (start_capacity + 1, 2),
            (start_capacity + continuation_capacity, 2),
            (start_capacity + continuation_capacity + 1, 3),
        ];

        for (payload_len, expected_block_count) in cases {
            assert_eq!(
                block_count_for_payload(STORAGE_SECTOR_SIZE, payload_len).unwrap(),
                expected_block_count
            );
        }

        assert!(block_count_for_payload(RedoBlockHeader::SIZE - 1, 1).is_err());
    }

    #[test]
    fn test_log_block_group_materializes_multi_block_group() {
        let cts = TrxID::new(11);
        let log = large_trx_log(cts);
        let payload_len = log.ser_len();
        let mut expected_payload = vec![0u8; payload_len];
        log.ser(&mut expected_payload[..], 0);
        let block_count = block_count_for_payload(STORAGE_SECTOR_SIZE, payload_len).unwrap();
        assert!(block_count > 1);
        let group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log).unwrap();

        let blocks = group
            .finish_with(|count| {
                (0..count)
                    .map(|_| DirectBuf::zeroed(STORAGE_SECTOR_SIZE))
                    .collect()
            })
            .unwrap();

        assert_eq!(blocks.len(), block_count);
        assert_eq!(
            block_count * STORAGE_SECTOR_SIZE,
            blocks.iter().map(DirectBuf::capacity).sum()
        );
        let mut actual_payload = Vec::with_capacity(payload_len);
        for (idx, block) in blocks.iter().enumerate() {
            assert_eq!(block.capacity(), STORAGE_SECTOR_SIZE);
            let (_, header) = RedoBlockHeader::deser(block.as_bytes(), 0).unwrap();
            header.verify_checksum(block.as_bytes()).unwrap();
            header.validate(STORAGE_SECTOR_SIZE).unwrap();
            assert_eq!(header.is_group_start(), idx == 0);
            assert_eq!(header.is_group_end(), idx + 1 == block_count);
            if idx + 1 < block_count {
                let capacity = if idx == 0 {
                    redo_start_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap()
                } else {
                    redo_continuation_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap()
                };
                assert_eq!(header.payload_len_usize(), capacity);
            }
            if idx == 0 {
                let (_, extension) =
                    RedoGroupStartExtension::deser(block.as_bytes(), RedoBlockHeader::SIZE)
                        .unwrap();
                assert_eq!(extension.group_payload_len_usize().unwrap(), payload_len);
                assert_eq!(extension.group_block_count_usize(), block_count);
                assert_eq!(extension.min_redo_cts, cts);
                assert_eq!(extension.max_redo_cts, cts);
            }
            let payload_start = if idx == 0 {
                RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE
            } else {
                RedoBlockHeader::SIZE
            };
            let payload_end = payload_start + header.payload_len_usize();
            actual_payload.extend_from_slice(&block.as_bytes()[payload_start..payload_end]);
            assert!(
                block.as_bytes()[payload_end..]
                    .iter()
                    .all(|&byte| byte == 0),
                "nonzero padding in block {idx}"
            );
        }
        assert_eq!(actual_payload, expected_payload);
    }
}
