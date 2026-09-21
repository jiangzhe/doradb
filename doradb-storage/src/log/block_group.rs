use crate::error::{DataIntegrityError, DataIntegrityResult, InternalError, InternalResult};
use crate::id::TrxID;
use crate::io::{DirectBuf, IOBuf};
use crate::log::format::{
    REDO_BLOCK_GROUP_END, REDO_BLOCK_GROUP_START, RedoBlockHeader, RedoGroupStartExtension,
    patch_redo_block_checksum, redo_continuation_block_payload_capacity,
    redo_start_block_payload_capacity,
};
use crate::log::redo::{RedoHeader, RedoLogs};
use crate::serde::{Deser, DeserResult, MinBytesHint, Ser, Serde, min_bytes_hint};
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
///
/// # Group format contract
///
/// This is the shared group contract for the writer, owning redo scan, and packed
/// recovery decoder. It describes the format selected by
/// [`REDO_FILE_FORMAT_VERSION`](crate::log::format::REDO_FILE_FORMAT_VERSION).
/// Transaction encoding is specified by [`TrxLog`]. All multibyte fields below
/// use little-endian encoding, with no implicit alignment or padding between fields.
///
/// Each group occupies consecutive fixed-size blocks within one file's data
/// region. Every block starts with the 11-byte [`RedoBlockHeader`]:
///
/// | Block offset | Field | Encoding |
/// | --- | --- | --- |
/// | 0 | `checksum` | `u32`, CRC32 of every remaining byte in this block |
/// | 4 | `flags` | `u8`, START = 1, END = 2; other bits are invalid |
/// | 5 | `payload_len` | `u16`, logical payload bytes in this block |
/// | 7 | `group_block_idx` | `u32`, zero-based position within the group |
///
/// Only the START block has the following 28-byte [`RedoGroupStartExtension`]
/// immediately after its common header:
///
/// | Block offset | Field | Encoding |
/// | --- | --- | --- |
/// | 11 | `group_payload_len` | `u64`, total logical bytes, including frame prefixes |
/// | 19 | `group_block_count` | `u32`, total physical blocks in this group |
/// | 23 | `min_redo_cts` | `u64`, inclusive lower transaction CTS bound |
/// | 31 | `max_redo_cts` | `u64`, inclusive upper transaction CTS bound |
///
/// Payload starts at byte 39 in the START block and byte 11 in continuation
/// blocks. Bytes after each block's payload must be zero; the checksum includes
/// this padding and, for the START block, the extension. Block indices must be
/// consecutive from zero, START appears only at index zero, and END appears only
/// on the final block. A one-block group carries both flags. Every block has
/// nonempty payload; nonfinal blocks fill their payload capacity completely.
/// The declared block count must match the count required for `group_payload_len`.
/// Group payload length and block count must be nonzero, and `min_redo_cts` must
/// not exceed `max_redo_cts`.
///
/// Concatenating only the blocks' payload bytes must yield exactly
/// `group_payload_len` bytes, with this logical layout:
///
/// ```text
/// group_body = transaction_frame ... transaction_frame
/// transaction_frame = data_len:u64 + transaction_body[data_len]
/// ```
///
/// There is no transaction count or padding in the logical body. A transaction,
/// including its length prefix, may cross physical block boundaries. Decoding
/// must advance for every frame, consume the complete body, and check each CTS
/// against the inclusive group bounds. The writer records the actual minimum and
/// maximum CTS; body decoders check containment, without requiring either bound
/// to occur or checking monotonic CTS order.
///
/// The writer puts multiple transactions together only when they fit in one
/// block; an oversized transaction gets a group of its own. This is a writer
/// batching policy, not an additional transaction-count check in the decoders.
///
/// # Validation and publication
///
/// The shared group reader validates physical blocks and assembles the body.
/// Its file-level policy distinguishes an acceptable unsealed crash tail from
/// corruption in required history; a discarded tail is never passed to a body
/// decoder. The owning iterator validates frames incrementally, while packed
/// decoding validates the entire body in one call. Both stream adapters must
/// finish validating every transaction before publishing any record from that
/// group. A decoding failure discards partial results and makes the stream
/// terminal; reading it again violates the stream protocol and panics. The raw
/// body decoders can exhaust an empty slice, but persisted groups are nonempty
/// because the physical reader enforces the extension rules above.
///
/// The two paths must agree on acceptance, decoded transaction contents, and
/// integrity error kind; diagnostic attachments may differ. The differential
/// corpus in `recovery::decode::tests` and
/// `recovery::stream::tests::both_adapters_reject_a_whole_group_and_remain_terminal`
/// cover this contract, including rejection of a corrupt later transaction.
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
    pub(crate) fn new(log_block_size: usize, trx_log: TrxLog) -> DataIntegrityResult<Self> {
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
    pub(crate) fn finish_with<F>(self, take_blocks: F) -> InternalResult<Vec<DirectBuf>>
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
    fn new(group: &'a LogBlockGroup, blocks: Vec<DirectBuf>) -> InternalResult<Self> {
        if blocks.len() != group.block_count {
            return Err(
                Report::new(InternalError::RedoFormatEncoding).attach(format!(
                    "block=redo-data, expected_block_count={}, actual_block_count={}",
                    group.block_count,
                    blocks.len()
                )),
            );
        }
        for (block_idx, block) in blocks.iter().enumerate() {
            if block.capacity() != group.log_block_size {
                return Err(Report::new(InternalError::RedoFormatEncoding).attach(format!(
                    "block=redo-data, block_idx={block_idx}, expected_block_size={}, actual_block_size={}",
                    group.log_block_size,
                    block.capacity()
                )));
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
    fn finish(mut self) -> InternalResult<Vec<DirectBuf>> {
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
    fn finalize_blocks(&mut self) -> InternalResult<()> {
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
    fn finalize_block(&mut self, idx: usize) -> InternalResult<()> {
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
        patch_redo_block_checksum(self.blocks[idx].as_bytes_mut());
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
///
/// # Transaction format contract
///
/// This is the shared transaction contract for [`Ser`], [`Deser`], and packed
/// recovery decoding. [`LogBlockGroup`] specifies the enclosing physical and
/// logical group format and its version. All fields are concatenated without
/// alignment padding. Multibyte integers and IEEE-754 floats are little-endian;
/// signed integers use two's complement. Table, row, page, and transaction IDs
/// occupy `u64`; index IDs occupy `u32`, and index slots occupy `u16`.
///
/// | Frame offset | Field | Encoding |
/// | --- | --- | --- |
/// | 0 | `data_len` | `u64`, bytes after this prefix through the end of this frame |
/// | 8 | `header.cts` | `u64` |
/// | 16 | `header.trx_kind` | `u8`: User = 0, System = 1 |
/// | 17 | `payload.ddl` | Optional DDL record, starting with its `u8` presence flag |
/// | variable | `payload.dml` | Table map, starting with its `u64` entry count |
///
/// An empty transaction has an 18-byte body: 9 header bytes, an absent-DDL flag,
/// and an empty table-map count. The frame prefix adds another 8 bytes. The length
/// must fit `usize`, cover at least this body, and stay within the input. All
/// nested reads are restricted to that frame; successful decoding must consume
/// it exactly. Extra bytes inside a frame are invalid. Bytes following a frame
/// belong to the next transaction and are consumed by the group decoder.
///
/// ## Collections and row operations
///
/// `option<T>` is a `u8` presence flag followed by `T` when present. Writers emit
/// 0 or 1; readers accept 0 as absent and **any nonzero flag** as present. A box
/// has the same encoding as its contents. `vec<T>` is a `u64` element count
/// followed by that many elements, without per-element framing. A map is a `u64`
/// entry count followed by key/value pairs. Counts must fit `usize` and pass the
/// shared minimum-size checks against the remaining frame before allocation.
///
/// ```text
/// table_map   = count:u64 + (table_id:u64 + row_map) * count
/// row_map     = count:u64 + (map_row_id:u64 + row_redo) * count
/// row_redo    = payload_row_id:u64 + operation_tag:u8 + operation_payload
/// update      = column_ordinal:u32 + value
/// primary_key = index_slot:u16 + vec<value>
/// ```
///
/// Writers emit maps in ascending key order. Readers accept arbitrary key order
/// and repeated keys, retain the last encoded value, and expose ascending map
/// order. Repeated table IDs replace the entire earlier row map. Every encoded
/// entry must be fully validated, even if later overwritten or filtered during
/// replay. The row-map key and payload row ID are separate identities and need
/// not match. Vectors retain encoded order, including repeated or unordered
/// update ordinals. Empty table/row maps and empty value/update/key vectors are
/// accepted.
///
/// [`RowRedoKind`](crate::log::redo::RowRedoKind) uses these tags and payloads:
///
/// | Tag (`u8`) | Operation | Payload after tag |
/// | --- | --- | --- |
/// | 1 | Insert | `page_id:u64 + vec<value>` |
/// | 2 | Delete | `option<page_id:u64>` |
/// | 3 | Update | `page_id:u64 + vec<update>` |
/// | 4 | DeleteByPrimaryKey | `primary_key` |
/// | 5 | UpdateByPrimaryKey | `primary_key + vec<update>` |
///
/// Keyed operations are structurally decoded for any table ID; restrictions on
/// where they may be replayed belong to the replay consumer. Likewise, schema,
/// column, and page validity are checked by replay rather than this wire grammar.
///
/// ## DDL and system operations
///
/// When the DDL option is present, [`DDLRedo`](crate::log::redo::DDLRedo) starts
/// with a `u8` tag and then the following fields in order. DML follows the DDL
/// record even when DDL is present, and may include both catalog and user tables.
///
/// | Tag (`u8`) | Operation | Payload after tag |
/// | --- | --- | --- |
/// | 129 | CreateTable | `table_id:u64` |
/// | 130 | DropTable | `table_id:u64` |
/// | 131 | CreateIndex | `table_id:u64 + index_id:u32 + index_slot:u16` |
/// | 132 | DropIndex | `table_id:u64 + index_id:u32 + index_slot:u16` |
/// | 133 | CreateRowPage | `table_id:u64 + page_id:u64 + start_row_id:u64 + end_row_id:u64` |
/// | 134 | DataCheckpoint | `table_id:u64 + pivot_row_id:u64 + checkpoint_ts:u64` |
/// | 135 | TableReplaySilentWatermark | `table_id:u64` |
///
/// `pivot_row_id` is spelled `pivor_row_id` in the current Rust variant.
///
/// ## Values
///
/// Each [`Val`](crate::value::Val) starts with a `u32` tag. Null is tag 0 with no
/// payload; the remaining tags follow [`ValKind`](crate::value::ValKind):
///
/// | Tag (`u32`) | Value kind | Payload after tag |
/// | --- | --- | --- |
/// | 0 | Null | None |
/// | 1, 2 | I8, U8 | 1 byte |
/// | 3, 4 | I16, U16 | 2 bytes |
/// | 5, 6, 7 | I32, U32, F32 | 4 bytes |
/// | 8, 9, 10 | I64, U64, F64 | 8 bytes |
/// | 11 | VarByte | `byte_len:u16 + bytes[byte_len]` |
///
/// Floating-point bits, including NaN payloads and signed zero, are preserved.
/// Variable bytes are opaque, may be empty, and may occupy the full `u16` length
/// range. Their encoding is independent of inline/outlined in-memory storage.
/// Unknown transaction, row-operation, DDL, or value tags, truncated fields,
/// invalid lengths, and inexact frame consumption yield
/// [`DataIntegrityError::InvalidPayload`].
///
/// ## Decoder responsibilities
///
/// [`TrxLog::deser`] decodes one owning frame and returns its end
/// offset. Group bounds, complete group consumption, and publication atomicity
/// are enforced by the consumers described on [`LogBlockGroup`]. Packed decoding
/// changes ownership and representation only: it must preserve the same decoded
/// meaning and acceptance/error-kind contract. The independent wire fixtures and
/// differential tests in `recovery::decode::tests` cover tags, scalar bits,
/// duplicates, option flags, truncations, and all operation variants. Changes to
/// the wire format must update this contract and both paths' parity coverage.
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

/// Decode one frame according to the transaction format contract on [`TrxLog`].
/// Group validation and publication follow the contract on [`LogBlockGroup`].
impl Deser for TrxLog {
    const MIN_BYTES_HINT: MinBytesHint =
        min_bytes_hint(mem::size_of::<u64>() + MIN_TRX_LOG_FRAME_LEN);

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (frame_end, _, frame) = read_trx_frame(input, start_idx)?;
        let data_len = frame.len();
        let (idx, header) = RedoHeader::deser(frame, 0)?;
        let (idx, payload) = RedoLogs::deser(frame, idx)?;
        validate_trx_frame_consumed(frame.len(), idx)?;
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

/// Encode the transaction format specified on [`TrxLog`], including its length prefix.
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

/// Read a checked transaction frame and its absolute body offset without decoding it.
#[inline]
pub(crate) fn read_trx_frame<S: Serde + ?Sized>(
    input: &S,
    start_idx: usize,
) -> DeserResult<(usize, usize, &[u8])> {
    let (frame_start, data_len) = input.deser_u64(start_idx)?;
    let data_len = usize::try_from(data_len).map_err(|_| {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach("block=redo-trx, trx_data_len_exceeds_usize")
    })?;
    // Check the advertised boundary before slicing so every nested checked
    // reader is restricted to this transaction, even within a larger group.
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
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-trx, trx_data_len={data_len}, min_trx_frame_len={MIN_TRX_LOG_FRAME_LEN}"
            )),
        );
    }
    let frame_end = frame_start + data_len;
    let (_, frame) = input.deser(frame_start, data_len)?;
    Ok((frame_end, frame_start, frame))
}

/// Require nested decoders to consume exactly the authoritative transaction frame.
#[inline]
pub(crate) fn validate_trx_frame_consumed(frame_len: usize, consumed: usize) -> DeserResult<()> {
    if consumed != frame_len {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-trx, trx_frame_len={frame_len}, consumed={consumed}"
            )),
        );
    }
    Ok(())
}

/// Validate forward progress and the inclusive group timestamp bounds after a full transaction.
#[inline]
pub(crate) fn validate_group_trx(
    start: usize,
    end: usize,
    cts: TrxID,
    min_cts: TrxID,
    max_cts: TrxID,
) -> DeserResult<()> {
    if end <= start {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach("block=redo-group, trx parser did not advance"));
    }
    if cts < min_cts || cts > max_cts {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-group, cts={cts}, min_cts={min_cts}, max_cts={max_cts}"
            )),
        );
    }
    Ok(())
}

/// Return the fixed-block count for a logical payload length.
#[inline]
pub(crate) fn block_count_for_payload(
    log_block_size: usize,
    payload_len: usize,
) -> DataIntegrityResult<usize> {
    let start_capacity = redo_start_block_payload_capacity(log_block_size)?;
    if payload_len <= start_capacity {
        return Ok(1);
    }
    let continuation_capacity = redo_continuation_block_payload_capacity(log_block_size)?;
    if continuation_capacity == 0 {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!("block=redo-data, log_block_size={log_block_size}")));
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
                row_id: RowID::new(100),
                kind: RowRedoKind::Insert(
                    test_page_id(5),
                    vec![Val::from(1u32), Val::from(&s[..])],
                ),
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

    /// Purpose: Protect transaction framing against stalled decoding and inexact consumption.
    /// Expected: Decoding must advance and consume each frame exactly; invalid boundaries report
    /// corrupt payloads.
    #[test]
    fn transaction_validation_rejects_nonprogress_and_inexact_consumption() {
        for end in [0, 10] {
            let err = validate_group_trx(10, end, TrxID::new(7), TrxID::new(7), TrxID::new(7))
                .unwrap_err();
            assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
        }
        validate_group_trx(10, 11, TrxID::new(7), TrxID::new(7), TrxID::new(7)).unwrap();
        for consumed in [9, 11] {
            let err = validate_trx_frame_consumed(10, consumed).unwrap_err();
            assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
        }
        validate_trx_frame_consumed(10, 10).unwrap();
    }

    /// Purpose: Reject transaction frames whose declared length exceeds the available group body.
    /// Expected: Decoding reports an invalid payload instead of accepting an incomplete frame.
    #[test]
    fn test_trx_log_rejects_frame_exceeding_group_body() {
        let mut bytes = vec![0u8; mem::size_of::<u64>()];
        bytes[..].ser_u64(0, 1);

        let err = TrxLog::deser(&bytes[..], 0).unwrap_err();

        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    /// Purpose: Reject trailing bytes left inside a declared transaction frame.
    /// Expected: Decoding reports an invalid payload when the transaction under-consumes its frame.
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

    /// Purpose: Reject transaction data that extends beyond its declared frame.
    /// Expected: Decoding reports an invalid payload when the transaction over-consumes its frame.
    #[test]
    fn test_trx_log_rejects_over_consumed_frame() {
        let log = simple_trx_log(TrxID::new(7));
        let mut bytes = vec![0u8; log.ser_len()];
        log.ser(&mut bytes[..], 0);
        bytes[..].ser_u64(0, 1);

        let err = TrxLog::deser(&bytes[..], 0).unwrap_err();

        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    /// Purpose: Track the commit timestamp range as transactions join a redo group.
    /// Expected: The exposed range spans the earliest and latest logged commit timestamps.
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

    /// Purpose: Protect a redo group when an appended transaction exceeds its capacity.
    /// Expected: The rejected transaction is returned and existing group accounting remains
    /// unchanged.
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

    /// Purpose: Protect materialization of a redo group that fits in a single block.
    /// Expected: The block has valid integrity, complete group flags, and matching payload and
    /// timestamp metadata.
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

    /// Purpose: Protect caller-supplied buffer ownership during redo group materialization.
    /// Expected: Materialization requests the required batch and returns the supplied block
    /// allocations.
    #[test]
    fn test_log_block_group_finish_with_uses_supplied_block_batch() {
        let cts = TrxID::new(13);
        let log = large_trx_log(cts);
        let group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log).unwrap();
        let supplied = vec![
            DirectBuf::zeroed(STORAGE_SECTOR_SIZE),
            DirectBuf::zeroed(STORAGE_SECTOR_SIZE),
        ];
        let supplied_ptrs: Vec<_> = supplied
            .iter()
            .map(|block| block.as_bytes().as_ptr())
            .collect();
        let mut requested_count = 0usize;

        let blocks = group
            .finish_with(|count| {
                requested_count = count;
                supplied
            })
            .unwrap();

        assert_eq!(requested_count, supplied_ptrs.len());
        assert_eq!(blocks.len(), supplied_ptrs.len());
        for (block, supplied_ptr) in blocks.iter().zip(supplied_ptrs) {
            assert_eq!(block.as_bytes().as_ptr(), supplied_ptr);
            assert_eq!(block.capacity(), STORAGE_SECTOR_SIZE);
        }
    }

    /// Purpose: Reject an allocator batch that cannot hold the requested redo group.
    /// Expected: Materialization reports an internal encoding error with expected and actual block
    /// counts.
    #[test]
    fn test_log_block_group_finish_with_rejects_wrong_block_count() {
        let cts = TrxID::new(13);
        let log = simple_trx_log(cts);
        let group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log).unwrap();

        let err = match group.finish_with(|_| Vec::new()) {
            Ok(_) => panic!("wrong block count must be rejected"),
            Err(err) => err,
        };

        assert_eq!(*err.current_context(), InternalError::RedoFormatEncoding);
        let report = format!("{err:?}");
        assert!(report.contains("expected_block_count=1"), "{report}");
        assert!(report.contains("actual_block_count=0"), "{report}");
    }

    /// Purpose: Reject supplied redo buffers whose capacity differs from the configured block size.
    /// Expected: Materialization reports an internal encoding error with expected and actual
    /// capacities.
    #[test]
    fn test_log_block_group_finish_with_rejects_wrong_block_capacity() {
        let log = simple_trx_log(TrxID::new(13));
        let group = LogBlockGroup::new(STORAGE_SECTOR_SIZE, log).unwrap();

        let err = group
            .finish_with(|_| vec![DirectBuf::zeroed(STORAGE_SECTOR_SIZE * 2)])
            .err()
            .expect("wrong block capacity must be rejected");

        assert_eq!(*err.current_context(), InternalError::RedoFormatEncoding);
        let report = format!("{err:?}");
        assert!(
            report.contains(&format!("expected_block_size={STORAGE_SECTOR_SIZE}")),
            "{report}"
        );
        assert!(
            report.contains(&format!("actual_block_size={}", STORAGE_SECTOR_SIZE * 2)),
            "{report}"
        );
    }

    /// Purpose: Protect block-count transitions at start and continuation payload boundaries.
    /// Expected: Exact fits retain their block count, overflow adds a block, and undersized blocks
    /// are rejected.
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

    /// Purpose: Protect redo payload splitting across start and continuation blocks.
    /// Expected: Reassembled bytes match the transaction, with valid checksums, group metadata,
    /// flags, and zero padding.
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
