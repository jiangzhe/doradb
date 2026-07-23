use crate::error::{DataIntegrityError, DataIntegrityResult, InternalError, InternalResult};
use crate::file::block_integrity::{
    BLOCK_INTEGRITY_HEADER_SIZE, BlockIntegritySpec, checksum_offset, validate_block,
    write_block_checksum, write_block_header,
};
use crate::id::TrxID;
use crate::io::STORAGE_SECTOR_SIZE;
use crate::serde::{Deser, DeserResult, MinBytesHint, Ser, Serde, min_bytes_hint};
use error_stack::{Report, ResultExt};
use std::mem;

/// Magic bytes stored in every redo file super-block header.
pub(crate) const REDO_FILE_MAGIC: [u8; 8] = *b"DREDO\0\0\0";
/// Redo file format version for framing and serialized redo payloads.
pub(crate) const REDO_FILE_FORMAT_VERSION: u64 = 5;
/// Shared block-integrity envelope used by redo super-block slots.
pub(crate) const REDO_SUPER_BLOCK_SPEC: BlockIntegritySpec =
    BlockIntegritySpec::new(REDO_FILE_MAGIC, REDO_FILE_FORMAT_VERSION);
/// Number of redundant super-block slots at the beginning of each redo file.
pub(crate) const REDO_SUPER_BLOCK_SLOT_COUNT: usize = 2;
/// Size of each super-block slot; one sector keeps headers aligned and isolated.
pub(crate) const REDO_SUPER_BLOCK_SLOT_SIZE: usize = STORAGE_SECTOR_SIZE;
/// Offset where redo groups begin after the redundant super-block slots.
pub(crate) const REDO_DEFAULT_DATA_START_OFFSET: usize =
    REDO_SUPER_BLOCK_SLOT_COUNT * REDO_SUPER_BLOCK_SLOT_SIZE;
/// Serialized size of the common header at the front of every redo data block.
pub(crate) const REDO_BLOCK_COMMON_HEADER_SIZE: usize =
    mem::size_of::<u32>() + mem::size_of::<u8>() + mem::size_of::<u16>() + mem::size_of::<u32>();
/// Serialized size of metadata present only on group-start redo data blocks.
pub(crate) const REDO_GROUP_START_EXTENSION_SIZE: usize =
    mem::size_of::<u64>() + mem::size_of::<u32>() + mem::size_of::<u64>() * 2;
/// Serialized size of redo metadata inside a super-block integrity envelope.
pub(crate) const REDO_SUPER_BLOCK_PAYLOAD_SIZE: usize =
    mem::size_of::<u32>() * 2 + mem::size_of::<u64>() * 6;

/// Flag stored on the first block of every logical redo group.
pub(crate) const REDO_BLOCK_GROUP_START: u8 = 0b0000_0001;
/// Flag stored on the final block of every logical redo group.
pub(crate) const REDO_BLOCK_GROUP_END: u8 = 0b0000_0010;
const REDO_BLOCK_VALID_FLAGS: u8 = REDO_BLOCK_GROUP_START | REDO_BLOCK_GROUP_END;

/// Common fixed header stored at the front of every redo data block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RedoBlockHeader {
    /// CRC32 of this complete fixed-size block excluding this field.
    pub(crate) checksum: u32,
    /// Block role flags.
    pub(crate) flags: u8,
    /// Number of logical payload bytes stored in this block.
    pub(crate) payload_len: u16,
    /// Zero-based index of this block within its logical group.
    pub(crate) group_block_idx: u32,
}

impl RedoBlockHeader {
    /// Serialized byte size of the common block header.
    pub(crate) const SIZE: usize = REDO_BLOCK_COMMON_HEADER_SIZE;

    /// Build a block header before checksum patching.
    #[inline]
    pub(crate) fn new(
        flags: u8,
        payload_len: usize,
        group_block_idx: usize,
    ) -> InternalResult<Self> {
        let payload_len = u16::try_from(payload_len).map_err(|_| {
            Report::new(InternalError::RedoFormatEncoding)
                .attach(format!("block=redo-data, payload_len={payload_len}"))
        })?;
        let group_block_idx = u32::try_from(group_block_idx).map_err(|_| {
            Report::new(InternalError::RedoFormatEncoding).attach(format!(
                "block=redo-data, group_block_idx={group_block_idx}"
            ))
        })?;
        Ok(Self {
            checksum: 0,
            flags,
            payload_len,
            group_block_idx,
        })
    }

    /// Returns true when this is the first block of a logical group.
    #[inline]
    pub(crate) fn is_group_start(self) -> bool {
        self.flags & REDO_BLOCK_GROUP_START != 0
    }

    /// Returns true when this is the final block of a logical group.
    #[inline]
    pub(crate) fn is_group_end(self) -> bool {
        self.flags & REDO_BLOCK_GROUP_END != 0
    }

    /// Return the encoded payload length as `usize`.
    #[inline]
    pub(crate) fn payload_len_usize(self) -> usize {
        usize::from(self.payload_len)
    }

    /// Validate common header invariants for a fixed-size block.
    #[inline]
    pub(crate) fn validate(self, log_block_size: usize) -> DataIntegrityResult<()> {
        if self.flags & !REDO_BLOCK_VALID_FLAGS != 0 {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("block=redo-data, flags={:02x}", self.flags)));
        }
        if self.payload_len == 0 {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=redo-data, payload_len=0"));
        }
        if self.is_group_start() != (self.group_block_idx == 0) {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "block=redo-data, flags={:02x}, group_block_idx={}",
                    self.flags, self.group_block_idx
                )),
            );
        }
        let capacity = redo_block_payload_capacity(log_block_size, self.flags)?;
        if self.payload_len_usize() > capacity {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "block=redo-data, payload_len={}, capacity={capacity}",
                    self.payload_len
                )),
            );
        }
        Ok(())
    }

    /// Verify that the persisted checksum matches this complete block image.
    #[inline]
    pub(crate) fn verify_checksum(self, block: &[u8]) -> DataIntegrityResult<()> {
        if block.len() < Self::SIZE {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "block=redo-data, invalid_block_len={}",
                    block.len()
                )),
            );
        }
        let actual = crc32fast::hash(&block[mem::size_of::<u32>()..]);
        if actual != self.checksum {
            return Err(
                Report::new(DataIntegrityError::ChecksumMismatch).attach(format!(
                    "block=redo-data, expected_checksum={:08x}, actual_checksum={actual:08x}",
                    self.checksum
                )),
            );
        }
        Ok(())
    }
}

impl Ser<'_> for RedoBlockHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        Self::SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u32(start_idx, self.checksum);
        let idx = out.ser_u8(idx, self.flags);
        let idx = out.ser_u16(idx, self.payload_len);
        out.ser_u32(idx, self.group_block_idx)
    }
}

impl Deser for RedoBlockHeader {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(RedoBlockHeader::SIZE);

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, checksum) = input.deser_u32(start_idx)?;
        let (idx, flags) = input.deser_u8(idx)?;
        let (idx, payload_len) = input.deser_u16(idx)?;
        let (idx, group_block_idx) = input.deser_u32(idx)?;
        Ok((
            idx,
            RedoBlockHeader {
                checksum,
                flags,
                payload_len,
                group_block_idx,
            },
        ))
    }
}

/// Group-level metadata stored immediately after the common header on a start block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RedoGroupStartExtension {
    /// Logical payload bytes in the complete group.
    pub(crate) group_payload_len: u64,
    /// Number of fixed data blocks in this logical group.
    pub(crate) group_block_count: u32,
    /// Lowest commit timestamp allowed in transaction records.
    pub(crate) min_redo_cts: TrxID,
    /// Highest commit timestamp allowed in transaction records.
    pub(crate) max_redo_cts: TrxID,
}

impl RedoGroupStartExtension {
    /// Serialized byte size of the group-start extension.
    pub(crate) const SIZE: usize = REDO_GROUP_START_EXTENSION_SIZE;

    /// Build a group-start extension.
    #[inline]
    pub(crate) fn new(
        group_payload_len: usize,
        group_block_count: usize,
        min_redo_cts: TrxID,
        max_redo_cts: TrxID,
    ) -> InternalResult<Self> {
        let group_block_count = u32::try_from(group_block_count).map_err(|_| {
            Report::new(InternalError::RedoFormatEncoding).attach(format!(
                "block=redo-data, group_block_count={group_block_count}"
            ))
        })?;
        Ok(Self {
            group_payload_len: group_payload_len as u64,
            group_block_count,
            min_redo_cts,
            max_redo_cts,
        })
    }

    /// Return the group payload length as `usize`.
    #[inline]
    pub(crate) fn group_payload_len_usize(self) -> DataIntegrityResult<usize> {
        usize::try_from(self.group_payload_len).map_err(|_| {
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-data, group_payload_len={}",
                self.group_payload_len
            ))
        })
    }

    /// Return the group block count as `usize`.
    #[inline]
    pub(crate) fn group_block_count_usize(self) -> usize {
        self.group_block_count as usize
    }

    /// Validate group-start invariants.
    #[inline]
    pub(crate) fn validate(self) -> DataIntegrityResult<()> {
        if self.group_payload_len == 0 {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=redo-data, group_payload_len=0"));
        }
        if self.group_block_count == 0 {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=redo-data, group_block_count=0"));
        }
        if self.min_redo_cts > self.max_redo_cts {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "block=redo-data, min_redo_cts={}, max_redo_cts={}",
                    self.min_redo_cts, self.max_redo_cts
                )),
            );
        }
        self.group_payload_len_usize()?;
        Ok(())
    }
}

impl Ser<'_> for RedoGroupStartExtension {
    #[inline]
    fn ser_len(&self) -> usize {
        Self::SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u64(start_idx, self.group_payload_len);
        let idx = out.ser_u32(idx, self.group_block_count);
        let idx = out.ser_u64(idx, self.min_redo_cts.as_u64());
        out.ser_u64(idx, self.max_redo_cts.as_u64())
    }
}

impl Deser for RedoGroupStartExtension {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(RedoGroupStartExtension::SIZE);

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, group_payload_len) = input.deser_u64(start_idx)?;
        let (idx, group_block_count) = input.deser_u32(idx)?;
        let (idx, min_redo_cts) = input.deser_u64(idx)?;
        let (idx, max_redo_cts) = input.deser_u64(idx)?;
        Ok((
            idx,
            RedoGroupStartExtension {
                group_payload_len,
                group_block_count,
                min_redo_cts: TrxID::new(min_redo_cts),
                max_redo_cts: TrxID::new(max_redo_cts),
            },
        ))
    }
}

/// Durable redo metadata stored inside a super-block integrity envelope.
///
/// On disk, each redo super-block slot uses the shared block-integrity layout:
///
/// ```text
/// | Bytes            | Field                         |
/// |------------------|-------------------------------|
/// | 0..16            | block-integrity header        |
/// | 16..72           | redo super-block payload      |
/// | 72..slot_size-32 | zero padding                  |
/// | slot_size-32..   | block-integrity BLAKE3 trailer |
/// ```
///
/// The redo payload is serialized at byte `16`:
///
/// ```text
/// | Payload bytes | Field              | Type | Encoding      |
/// |---------------|--------------------|------|---------------|
/// | 0..4          | file_seq           | u32  | little-endian |
/// | 4..8          | slot_no            | u32  | little-endian |
/// | 8..16         | log_block_size     | u64  | little-endian |
/// | 16..24        | file_max_size      | u64  | little-endian |
/// | 24..32        | generation         | u64  | little-endian |
/// | 32..40        | durable_end_offset | u64  | little-endian |
/// | 40..48        | min_redo_cts       | u64  | little-endian |
/// | 48..56        | max_redo_cts       | u64  | little-endian |
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoSuperBlock {
    /// Sequence number encoded in the redo file name.
    pub(crate) file_seq: u32,
    /// Slot number containing this super-block.
    pub(crate) slot_no: u32,
    /// Physical stride for normal redo groups.
    pub(crate) log_block_size: u64,
    /// Logical file limit, normalized to whole log blocks after the fixed slots.
    pub(crate) file_max_size: u64,
    /// Monotonic generation for choosing the newest valid super-block slot.
    pub(crate) generation: u64,
    /// Offset immediately after the last durable redo group in a sealed file.
    pub(crate) durable_end_offset: u64,
    /// Lowest commit timestamp serialized into durable redo groups in this file.
    pub(crate) min_redo_cts: u64,
    /// Highest commit timestamp serialized into durable redo groups in this file.
    pub(crate) max_redo_cts: u64,
}

impl RedoSuperBlock {
    /// Build the initial redo super-block payload for a newly created file.
    #[inline]
    pub(crate) fn initial(file_seq: u32, log_block_size: usize, file_max_size: usize) -> Self {
        RedoSuperBlock {
            file_seq,
            slot_no: 0,
            log_block_size: log_block_size as u64,
            file_max_size: file_max_size as u64,
            generation: 0,
            durable_end_offset: 0,
            min_redo_cts: 0,
            max_redo_cts: 0,
        }
    }

    /// Return true when this super-block publishes sealed segment metadata.
    #[inline]
    pub(crate) fn is_sealed(&self) -> bool {
        self.durable_end_offset != 0
    }

    /// Return true for a sealed file that contains no durable redo groups.
    #[inline]
    pub(crate) fn sealed_empty(&self) -> bool {
        self.durable_end_offset == REDO_DEFAULT_DATA_START_OFFSET as u64
            && self.min_redo_cts == 0
            && self.max_redo_cts == 0
    }

    /// Return the sealed redo CTS range for a non-empty sealed file.
    #[inline]
    pub(crate) fn sealed_redo_range(&self) -> Option<(TrxID, TrxID)> {
        (self.is_sealed() && !self.sealed_empty())
            .then(|| (TrxID::new(self.min_redo_cts), TrxID::new(self.max_redo_cts)))
    }

    /// Build a sealed super-block for the inactive slot of an open file.
    #[inline]
    pub(crate) fn sealed_from_open(
        open: &RedoSuperBlock,
        slot_no: u32,
        durable_end_offset: usize,
        redo_range: Option<(TrxID, TrxID)>,
    ) -> InternalResult<Self> {
        let generation = open.generation.checked_add(1).ok_or_else(|| {
            Report::new(InternalError::RedoFormatEncoding)
                .attach("block=redo-super-block, generation overflow while sealing")
        })?;
        let (min_redo_cts, max_redo_cts) = match redo_range {
            Some((min_redo_cts, max_redo_cts)) => (min_redo_cts.as_u64(), max_redo_cts.as_u64()),
            None => (0, 0),
        };
        let sealed = RedoSuperBlock {
            file_seq: open.file_seq,
            slot_no,
            log_block_size: open.log_block_size,
            file_max_size: open.file_max_size,
            generation,
            durable_end_offset: durable_end_offset as u64,
            min_redo_cts,
            max_redo_cts,
        };
        validate_super_block_fields(&sealed, sealed.file_seq, slot_no)
            .change_context(InternalError::RedoFormatEncoding)
            .attach("trusted sealed redo fields invalid")?;
        Ok(sealed)
    }
}

impl Ser<'_> for RedoSuperBlock {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u32>()
            + mem::size_of::<u32>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u32(start_idx, self.file_seq);
        let idx = out.ser_u32(idx, self.slot_no);
        let idx = out.ser_u64(idx, self.log_block_size);
        let idx = out.ser_u64(idx, self.file_max_size);
        let idx = out.ser_u64(idx, self.generation);
        let idx = out.ser_u64(idx, self.durable_end_offset);
        let idx = out.ser_u64(idx, self.min_redo_cts);
        out.ser_u64(idx, self.max_redo_cts)
    }
}

impl Deser for RedoSuperBlock {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(REDO_SUPER_BLOCK_PAYLOAD_SIZE);

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, file_seq) = input.deser_u32(start_idx)?;
        let (idx, slot_no) = input.deser_u32(idx)?;
        let (idx, log_block_size) = input.deser_u64(idx)?;
        let (idx, file_max_size) = input.deser_u64(idx)?;
        let (idx, generation) = input.deser_u64(idx)?;
        let (idx, durable_end_offset) = input.deser_u64(idx)?;
        let (idx, min_redo_cts) = input.deser_u64(idx)?;
        let (idx, max_redo_cts) = input.deser_u64(idx)?;
        Ok((
            idx,
            RedoSuperBlock {
                file_seq,
                slot_no,
                log_block_size,
                file_max_size,
                generation,
                durable_end_offset,
                min_redo_cts,
                max_redo_cts,
            },
        ))
    }
}

/// Return the payload capacity of a redo group-start block.
#[inline]
pub(crate) fn redo_start_block_payload_capacity(
    log_block_size: usize,
) -> DataIntegrityResult<usize> {
    redo_block_payload_capacity(log_block_size, REDO_BLOCK_GROUP_START)
}

/// Return the payload capacity of a redo continuation block.
#[inline]
pub(crate) fn redo_continuation_block_payload_capacity(
    log_block_size: usize,
) -> DataIntegrityResult<usize> {
    redo_block_payload_capacity(log_block_size, 0)
}

/// Return true when a fixed redo data block is all zeroes.
#[inline]
pub(crate) fn is_zero_redo_block(block: &[u8]) -> bool {
    block.iter().all(|&byte| byte == 0)
}

/// Compute and write the redo data block checksum into the first four bytes.
#[inline]
pub(crate) fn patch_redo_block_checksum(block: &mut [u8]) -> u32 {
    assert!(
        block.len() >= RedoBlockHeader::SIZE,
        "trusted redo block must fit its header before checksum patching: block_len={}, header_len={}",
        block.len(),
        RedoBlockHeader::SIZE
    );
    let checksum = crc32fast::hash(&block[mem::size_of::<u32>()..]);
    block.ser_u32(0, checksum);
    checksum
}

/// Return the byte offset for a super-block slot number.
#[inline]
pub(crate) fn slot_offset(slot_no: u32) -> usize {
    slot_no as usize * REDO_SUPER_BLOCK_SLOT_SIZE
}

/// Serialize one complete checksum-protected super-block slot.
#[inline]
pub(crate) fn serialize_redo_super_block(
    buf: &mut [u8],
    super_block: &RedoSuperBlock,
) -> InternalResult<()> {
    if buf.len() != REDO_SUPER_BLOCK_SLOT_SIZE {
        return Err(
            Report::new(InternalError::RedoFormatEncoding).attach(format!(
                "block=redo-super-block, invalid_slot_buffer_len={}",
                buf.len()
            )),
        );
    }
    validate_super_block_fields(super_block, super_block.file_seq, super_block.slot_no)
        .change_context(InternalError::RedoFormatEncoding)
        .attach("trusted redo super-block fields invalid")?;
    buf.fill(0);
    let payload_start = write_block_header(buf, REDO_SUPER_BLOCK_SPEC);
    debug_assert_eq!(payload_start, BLOCK_INTEGRITY_HEADER_SIZE);
    let payload_end = super_block.ser(buf, payload_start);
    debug_assert_eq!(
        payload_end,
        BLOCK_INTEGRITY_HEADER_SIZE + REDO_SUPER_BLOCK_PAYLOAD_SIZE
    );
    debug_assert!(payload_end <= checksum_offset(buf.len()));
    write_block_checksum(buf);
    Ok(())
}

/// Parse and validate one super-block slot.
///
/// The shared integrity envelope must validate before the redo payload can
/// participate in newest-super-block selection.
#[inline]
pub(crate) fn parse_redo_super_block(
    buf: &[u8],
    expected_file_seq: u32,
    expected_slot_no: u32,
) -> DataIntegrityResult<RedoSuperBlock> {
    if buf.len() != REDO_SUPER_BLOCK_SLOT_SIZE {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-super-block, invalid_slot_buffer_len={}",
                buf.len()
            )),
        );
    }
    let payload = validate_block(buf, REDO_SUPER_BLOCK_SPEC)?;
    let (payload_end, super_block) = RedoSuperBlock::deser(payload, 0)
        .map_err(|report| report.attach("block=redo-super-block, section=payload"))?;
    debug_assert_eq!(payload_end, REDO_SUPER_BLOCK_PAYLOAD_SIZE);
    if payload[payload_end..].iter().any(|&byte| byte != 0) {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-super-block, nonzero_padding_after_payload, payload_end={payload_end}"
            )),
        );
    }
    validate_super_block_fields(&super_block, expected_file_seq, expected_slot_no)?;
    Ok(super_block)
}

/// Select the newest valid redo super-block from redundant slots.
#[inline]
pub(crate) fn select_redo_super_block(
    file_bytes: &[u8],
    expected_file_seq: u32,
) -> DataIntegrityResult<RedoSuperBlock> {
    let mut selected: Option<RedoSuperBlock> = None;
    let mut first_err: Option<Report<DataIntegrityError>> = None;
    for slot_no in 0..REDO_SUPER_BLOCK_SLOT_COUNT as u32 {
        let offset = slot_offset(slot_no);
        let Some(slot) = file_bytes.get(offset..offset + REDO_SUPER_BLOCK_SLOT_SIZE) else {
            first_err.get_or_insert_with(|| {
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "missing redo super-block slot bytes: slot_no={slot_no}, offset={offset}"
                ))
            });
            continue;
        };
        match parse_redo_super_block(slot, expected_file_seq, slot_no) {
            Ok(super_block) => {
                if selected
                    .as_ref()
                    .is_none_or(|current| is_newer_super_block(&super_block, current))
                {
                    selected = Some(super_block);
                }
            }
            Err(report) => {
                first_err.get_or_insert(report);
            }
        }
    }
    match selected {
        Some(super_block) => Ok(super_block),
        None => Err(first_err
            .unwrap_or_else(|| Report::new(DataIntegrityError::InvalidPayload))
            .attach(format!(
                "no valid redo super-block slots: file_seq={expected_file_seq:08x}"
            ))),
    }
}

/// Return the serialized redo block header length implied by flags.
#[inline]
fn redo_block_header_len_for_flags(flags: u8) -> usize {
    if flags & REDO_BLOCK_GROUP_START != 0 {
        REDO_BLOCK_COMMON_HEADER_SIZE + REDO_GROUP_START_EXTENSION_SIZE
    } else {
        REDO_BLOCK_COMMON_HEADER_SIZE
    }
}

/// Return the redo block payload capacity implied by block size and flags.
#[inline]
fn redo_block_payload_capacity(log_block_size: usize, flags: u8) -> DataIntegrityResult<usize> {
    let header_len = redo_block_header_len_for_flags(flags);
    if log_block_size > u16::MAX as usize + header_len {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-data, unsupported_log_block_size={log_block_size}"
            )),
        );
    }
    log_block_size.checked_sub(header_len).ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "block=redo-data, log_block_size={log_block_size}, flags={flags:02x}"
        ))
    })
}

/// Compare two already validated super-block candidates by generation.
#[inline]
fn is_newer_super_block(candidate: &RedoSuperBlock, current: &RedoSuperBlock) -> bool {
    candidate.generation > current.generation
}

/// Validate super-block fields that must match the file and slot being read.
#[inline]
fn validate_super_block_fields(
    super_block: &RedoSuperBlock,
    expected_file_seq: u32,
    expected_slot_no: u32,
) -> DataIntegrityResult<()> {
    if super_block.file_seq != expected_file_seq {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, expected_file_seq={expected_file_seq:08x}, actual_file_seq={:08x}",
                super_block.file_seq
            )));
    }
    if super_block.slot_no != expected_slot_no
        || super_block.slot_no as usize >= REDO_SUPER_BLOCK_SLOT_COUNT
    {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-super-block, expected_slot_no={expected_slot_no}, actual_slot_no={}",
                super_block.slot_no
            )),
        );
    }
    if super_block.log_block_size > usize::MAX as u64
        || !is_valid_aligned_size(super_block.log_block_size as usize)
    {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "block=redo-super-block, invalid_log_block_size={}",
                super_block.log_block_size
            )),
        );
    }
    let log_block_size = super_block.log_block_size as usize;
    if !is_valid_file_max_size(super_block.file_max_size, log_block_size) {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_file_max_size={}, data_start={}, log_block_size={log_block_size}",
                super_block.file_max_size, REDO_DEFAULT_DATA_START_OFFSET
            )));
    }
    validate_sealed_segment_fields(super_block)?;
    Ok(())
}

/// Validate durable segment metadata combinations.
#[inline]
fn validate_sealed_segment_fields(super_block: &RedoSuperBlock) -> DataIntegrityResult<()> {
    let durable_end_offset = super_block.durable_end_offset;
    let min_redo_cts = super_block.min_redo_cts;
    let max_redo_cts = super_block.max_redo_cts;
    if durable_end_offset == 0 && min_redo_cts == 0 && max_redo_cts == 0 {
        return Ok(());
    }
    if durable_end_offset == REDO_DEFAULT_DATA_START_OFFSET as u64
        && min_redo_cts == 0
        && max_redo_cts == 0
    {
        return Ok(());
    }
    let file_max_size = super_block.file_max_size;
    if durable_end_offset > REDO_DEFAULT_DATA_START_OFFSET as u64
        && durable_end_offset <= file_max_size
        && durable_end_offset <= usize::MAX as u64
        && (durable_end_offset as usize).is_multiple_of(STORAGE_SECTOR_SIZE)
        && min_redo_cts > 0
        && min_redo_cts <= max_redo_cts
    {
        return Ok(());
    }
    Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "block=redo-super-block, invalid sealed fields: durable_end_offset={durable_end_offset}, min_redo_cts={min_redo_cts}, max_redo_cts={max_redo_cts}, file_max_size={file_max_size}"
        )))
}

/// Return true when a persisted size is usable for direct I/O.
#[inline]
fn is_valid_aligned_size(size: usize) -> bool {
    size >= STORAGE_SECTOR_SIZE
        && size <= u16::MAX as usize + 1
        && size.is_multiple_of(STORAGE_SECTOR_SIZE)
}

/// Return true when the file limit holds fixed slots plus whole log blocks.
#[inline]
fn is_valid_file_max_size(file_max_size: u64, log_block_size: usize) -> bool {
    if file_max_size <= REDO_DEFAULT_DATA_START_OFFSET as u64 || file_max_size > usize::MAX as u64 {
        return false;
    }
    let file_max_size = file_max_size as usize;
    file_max_size.is_multiple_of(STORAGE_SECTOR_SIZE)
        && (file_max_size - REDO_DEFAULT_DATA_START_OFFSET).is_multiple_of(log_block_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::block_integrity::BLOCK_INTEGRITY_TRAILER_SIZE;

    fn valid_super_block(slot_no: u32, generation: u64) -> RedoSuperBlock {
        let mut super_block =
            RedoSuperBlock::initial(7, STORAGE_SECTOR_SIZE * 2, STORAGE_SECTOR_SIZE * 8);
        super_block.slot_no = slot_no;
        super_block.generation = generation;
        super_block
    }

    fn serialized_slot(super_block: &RedoSuperBlock) -> Vec<u8> {
        let mut buf = vec![0u8; REDO_SUPER_BLOCK_SLOT_SIZE];
        serialize_redo_super_block(&mut buf, super_block).unwrap();
        buf
    }

    fn assert_integrity_error(err: Report<DataIntegrityError>, expected: DataIntegrityError) {
        assert_eq!(*err.current_context(), expected);
    }

    type CorruptCase = (&'static str, Box<dyn FnOnce(&mut [u8])>, DataIntegrityError);

    #[test]
    fn redo_block_header_serializes_fixed_layout() {
        let header = RedoBlockHeader {
            checksum: 0x1122_3344,
            flags: REDO_BLOCK_GROUP_START | REDO_BLOCK_GROUP_END,
            payload_len: 0x0102,
            group_block_idx: 0x0304_0506,
        };
        let mut buf = [0u8; RedoBlockHeader::SIZE];

        let idx = header.ser(&mut buf[..], 0);

        assert_eq!(idx, RedoBlockHeader::SIZE);
        assert_eq!(&buf[0..4], &0x1122_3344u32.to_le_bytes());
        assert_eq!(buf[4], REDO_BLOCK_GROUP_START | REDO_BLOCK_GROUP_END);
        assert_eq!(&buf[5..7], &0x0102u16.to_le_bytes());
        assert_eq!(&buf[7..11], &0x0304_0506u32.to_le_bytes());

        let (idx, parsed) = RedoBlockHeader::deser(&buf[..], 0).unwrap();
        assert_eq!(idx, RedoBlockHeader::SIZE);
        assert_eq!(parsed, header);
    }

    #[test]
    fn redo_group_start_extension_serializes_fixed_layout() {
        let extension = RedoGroupStartExtension {
            group_payload_len: 0x0102_0304_0506_0708,
            group_block_count: 0x1112_1314,
            min_redo_cts: TrxID::new(0x2122_2324_2526_2728),
            max_redo_cts: TrxID::new(0x3132_3334_3536_3738),
        };
        let mut buf = [0u8; RedoGroupStartExtension::SIZE];

        let idx = extension.ser(&mut buf[..], 0);

        assert_eq!(idx, RedoGroupStartExtension::SIZE);
        assert_eq!(&buf[0..8], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&buf[8..12], &0x1112_1314u32.to_le_bytes());
        assert_eq!(&buf[12..20], &0x2122_2324_2526_2728u64.to_le_bytes());
        assert_eq!(&buf[20..28], &0x3132_3334_3536_3738u64.to_le_bytes());

        let (idx, parsed) = RedoGroupStartExtension::deser(&buf[..], 0).unwrap();
        assert_eq!(idx, RedoGroupStartExtension::SIZE);
        assert_eq!(parsed, extension);
    }

    #[test]
    fn redo_block_checksum_covers_header_payload_and_padding() {
        let mut block = vec![0u8; STORAGE_SECTOR_SIZE];
        let payload_start = RedoBlockHeader::SIZE + RedoGroupStartExtension::SIZE;
        let header =
            RedoBlockHeader::new(REDO_BLOCK_GROUP_START | REDO_BLOCK_GROUP_END, 16, 0).unwrap();
        header.ser(&mut block[..], 0);
        RedoGroupStartExtension::new(16, 1, TrxID::new(10), TrxID::new(12))
            .unwrap()
            .ser(&mut block[..], RedoBlockHeader::SIZE);
        block[payload_start..payload_start + 16].copy_from_slice(&[7u8; 16]);

        let checksum = patch_redo_block_checksum(&mut block);
        assert_eq!(checksum, crc32fast::hash(&block[mem::size_of::<u32>()..]));
        let (_, parsed) = RedoBlockHeader::deser(&block[..RedoBlockHeader::SIZE], 0).unwrap();
        parsed.verify_checksum(&block).unwrap();

        for mutate in [
            mem::size_of::<u32>(),
            payload_start,
            STORAGE_SECTOR_SIZE - 1,
        ] {
            let mut corrupted = block.clone();
            corrupted[mutate] ^= 0x80;
            let err = parsed.verify_checksum(&corrupted).unwrap_err();
            assert_integrity_error(err, DataIntegrityError::ChecksumMismatch);
        }
    }

    #[test]
    fn redo_block_payload_capacity_uses_fixed_headers() {
        assert_eq!(
            redo_start_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap(),
            STORAGE_SECTOR_SIZE - RedoBlockHeader::SIZE - RedoGroupStartExtension::SIZE
        );
        assert_eq!(
            redo_continuation_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap(),
            STORAGE_SECTOR_SIZE - RedoBlockHeader::SIZE
        );
    }

    #[test]
    fn redo_block_validate_rejects_invalid_header_invariants() {
        let start_capacity = redo_start_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap();
        let continuation_capacity =
            redo_continuation_block_payload_capacity(STORAGE_SECTOR_SIZE).unwrap();
        let cases = [
            RedoBlockHeader {
                checksum: 1,
                flags: REDO_BLOCK_GROUP_START,
                payload_len: 0,
                group_block_idx: 0,
            },
            RedoBlockHeader {
                checksum: 1,
                flags: 0b1000_0000,
                payload_len: 1,
                group_block_idx: 0,
            },
            RedoBlockHeader {
                checksum: 1,
                flags: REDO_BLOCK_GROUP_START,
                payload_len: 1,
                group_block_idx: 1,
            },
            RedoBlockHeader {
                checksum: 1,
                flags: 0,
                payload_len: 1,
                group_block_idx: 0,
            },
            RedoBlockHeader {
                checksum: 1,
                flags: REDO_BLOCK_GROUP_START,
                payload_len: u16::try_from(start_capacity + 1).unwrap(),
                group_block_idx: 0,
            },
            RedoBlockHeader {
                checksum: 1,
                flags: 0,
                payload_len: u16::try_from(continuation_capacity + 1).unwrap(),
                group_block_idx: 1,
            },
        ];

        for header in cases {
            let err = header.validate(STORAGE_SECTOR_SIZE).unwrap_err();
            assert_integrity_error(err, DataIntegrityError::InvalidPayload);
        }
    }

    #[test]
    fn redo_group_start_extension_rejects_invalid_metadata() {
        let cases = [
            RedoGroupStartExtension {
                group_payload_len: 0,
                group_block_count: 1,
                min_redo_cts: TrxID::new(1),
                max_redo_cts: TrxID::new(1),
            },
            RedoGroupStartExtension {
                group_payload_len: 1,
                group_block_count: 0,
                min_redo_cts: TrxID::new(1),
                max_redo_cts: TrxID::new(1),
            },
            RedoGroupStartExtension {
                group_payload_len: 1,
                group_block_count: 1,
                min_redo_cts: TrxID::new(2),
                max_redo_cts: TrxID::new(1),
            },
        ];

        for extension in cases {
            let err = extension.validate().unwrap_err();
            assert_integrity_error(err, DataIntegrityError::InvalidPayload);
        }
    }

    #[test]
    fn redo_super_block_round_trips_with_standard_integrity_envelope() {
        let super_block = valid_super_block(0, 0);
        let buf = serialized_slot(&super_block);

        assert_eq!(&buf[0..8], &REDO_FILE_MAGIC);
        assert_eq!(&buf[8..16], &REDO_FILE_FORMAT_VERSION.to_le_bytes()[..]);
        assert_eq!(&buf[16..20], &7u32.to_le_bytes());
        assert_eq!(&buf[20..24], &0u32.to_le_bytes());
        assert_eq!(
            &buf[24..32],
            &(STORAGE_SECTOR_SIZE as u64 * 2).to_le_bytes()
        );
        assert_eq!(
            &buf[32..40],
            &(STORAGE_SECTOR_SIZE as u64 * 8).to_le_bytes()
        );
        assert_eq!(&buf[40..48], &0u64.to_le_bytes());
        assert_eq!(&buf[48..56], &0u64.to_le_bytes());
        assert_eq!(&buf[56..64], &0u64.to_le_bytes());
        assert_eq!(&buf[64..72], &0u64.to_le_bytes());
        assert_eq!(
            checksum_offset(REDO_SUPER_BLOCK_SLOT_SIZE),
            REDO_SUPER_BLOCK_SLOT_SIZE - BLOCK_INTEGRITY_TRAILER_SIZE
        );

        let parsed = parse_redo_super_block(&buf, 7, 0).unwrap();
        assert_eq!(parsed, super_block);
    }

    #[test]
    fn redo_super_block_rejects_invalid_fields() {
        let cases: Vec<CorruptCase> = vec![
            (
                "magic",
                Box::new(|buf| buf[0] = 0),
                DataIntegrityError::InvalidMagic,
            ),
            (
                "version",
                Box::new(|buf| {
                    let _ = buf.ser_u64(8, REDO_FILE_FORMAT_VERSION - 1);
                }),
                DataIntegrityError::InvalidVersion,
            ),
            (
                "file_seq",
                Box::new(|buf| {
                    let _ = buf.ser_u32(16, 8);
                    refresh_checksum(buf);
                }),
                DataIntegrityError::InvalidPayload,
            ),
            (
                "slot_no",
                Box::new(|buf| {
                    let _ = buf.ser_u32(20, 9);
                    refresh_checksum(buf);
                }),
                DataIntegrityError::InvalidPayload,
            ),
            (
                "log_block",
                Box::new(|buf| {
                    let _ = buf.ser_u64(24, 7);
                    refresh_checksum(buf);
                }),
                DataIntegrityError::InvalidPayload,
            ),
            (
                "file_max",
                Box::new(|buf| {
                    let _ = buf.ser_u64(32, REDO_DEFAULT_DATA_START_OFFSET as u64);
                    refresh_checksum(buf);
                }),
                DataIntegrityError::InvalidPayload,
            ),
            (
                "file_max_not_log_block_aligned",
                Box::new(|buf| {
                    let file_max_size = REDO_DEFAULT_DATA_START_OFFSET
                        + STORAGE_SECTOR_SIZE * 2
                        + STORAGE_SECTOR_SIZE;
                    let _ = buf.ser_u64(32, file_max_size as u64);
                    refresh_checksum(buf);
                }),
                DataIntegrityError::InvalidPayload,
            ),
            (
                "padding",
                Box::new(|buf| {
                    buf[BLOCK_INTEGRITY_HEADER_SIZE + REDO_SUPER_BLOCK_PAYLOAD_SIZE] = 1;
                    refresh_checksum(buf);
                }),
                DataIntegrityError::InvalidPayload,
            ),
            (
                "checksum",
                Box::new(|buf| {
                    buf[checksum_offset(REDO_SUPER_BLOCK_SLOT_SIZE)] ^= 0xff;
                }),
                DataIntegrityError::ChecksumMismatch,
            ),
        ];

        for (name, corrupt, expected) in cases {
            let super_block = valid_super_block(0, 0);
            let mut buf = serialized_slot(&super_block);
            corrupt(&mut buf);
            let err = parse_redo_super_block(&buf, 7, 0).unwrap_err();
            assert_integrity_error(err, expected);
            let _ = name;
        }
    }

    #[test]
    fn redo_super_block_accepts_valid_sealed_encodings() {
        let open = valid_super_block(0, 0);

        assert!(!open.is_sealed());
        assert!(!open.sealed_empty());
        assert_eq!(open.sealed_redo_range(), None);

        let empty =
            RedoSuperBlock::sealed_from_open(&open, 1, REDO_DEFAULT_DATA_START_OFFSET, None)
                .unwrap();
        assert!(empty.is_sealed());
        assert!(empty.sealed_empty());
        assert_eq!(empty.sealed_redo_range(), None);
        assert_eq!(empty.generation, 1);
        assert_eq!(empty.slot_no, 1);

        let non_empty = RedoSuperBlock::sealed_from_open(
            &open,
            1,
            REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE,
            Some((TrxID::new(5), TrxID::new(9))),
        )
        .unwrap();
        assert!(non_empty.is_sealed());
        assert!(!non_empty.sealed_empty());
        assert_eq!(
            non_empty.sealed_redo_range(),
            Some((TrxID::new(5), TrxID::new(9)))
        );
    }

    #[test]
    fn redo_super_block_rejects_invalid_sealed_field_combinations() {
        let open = valid_super_block(0, 0);
        let cases = [
            RedoSuperBlock {
                durable_end_offset: 0,
                min_redo_cts: 1,
                max_redo_cts: 1,
                ..open.clone()
            },
            RedoSuperBlock {
                durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET as u64,
                min_redo_cts: 1,
                max_redo_cts: 1,
                ..open.clone()
            },
            RedoSuperBlock {
                durable_end_offset: (REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE) as u64,
                min_redo_cts: 0,
                max_redo_cts: 1,
                ..open.clone()
            },
            RedoSuperBlock {
                durable_end_offset: (REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE) as u64,
                min_redo_cts: 2,
                max_redo_cts: 1,
                ..open.clone()
            },
            RedoSuperBlock {
                durable_end_offset: (REDO_DEFAULT_DATA_START_OFFSET + 1) as u64,
                min_redo_cts: 1,
                max_redo_cts: 1,
                ..open.clone()
            },
            RedoSuperBlock {
                durable_end_offset: REDO_DEFAULT_DATA_START_OFFSET as u64 - 1,
                min_redo_cts: 1,
                max_redo_cts: 1,
                ..open.clone()
            },
            RedoSuperBlock {
                durable_end_offset: open.file_max_size + STORAGE_SECTOR_SIZE as u64,
                min_redo_cts: 1,
                max_redo_cts: 1,
                ..open
            },
        ];

        for super_block in cases {
            let err = validate_super_block_fields(&super_block, 7, 0).unwrap_err();
            assert_integrity_error(err, DataIntegrityError::InvalidPayload);
        }
    }

    #[test]
    fn redo_super_block_internal_encoding_preserves_validation_source() {
        let invalid_slot_no = REDO_SUPER_BLOCK_SLOT_COUNT as u32;
        let open = valid_super_block(0, 0);
        let sealed_err = RedoSuperBlock::sealed_from_open(
            &open,
            invalid_slot_no,
            REDO_DEFAULT_DATA_START_OFFSET,
            None,
        )
        .unwrap_err();

        let invalid = RedoSuperBlock {
            slot_no: invalid_slot_no,
            ..open
        };
        let mut buf = vec![0u8; REDO_SUPER_BLOCK_SLOT_SIZE];
        let serialize_err = serialize_redo_super_block(&mut buf, &invalid).unwrap_err();

        for (err, stage) in [
            (sealed_err, "trusted sealed redo fields invalid"),
            (serialize_err, "trusted redo super-block fields invalid"),
        ] {
            assert_eq!(*err.current_context(), InternalError::RedoFormatEncoding);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );
            assert!(format!("{err:?}").contains(stage));
        }
    }

    #[test]
    fn slot_selection_chooses_newest_valid_generation() {
        let slot0 = serialized_slot(&valid_super_block(0, 0));
        let slot1 = serialized_slot(&valid_super_block(1, 1));
        let mut file = vec![0u8; REDO_DEFAULT_DATA_START_OFFSET];
        file[..REDO_SUPER_BLOCK_SLOT_SIZE].copy_from_slice(&slot0);
        file[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET].copy_from_slice(&slot1);

        let selected = select_redo_super_block(&file, 7).unwrap();
        assert_eq!(selected.slot_no, 1);
        assert_eq!(selected.generation, 1);
    }

    #[test]
    fn slot_selection_keeps_first_slot_when_generation_ties() {
        let slot0 = serialized_slot(&valid_super_block(0, 1));
        let slot1 = serialized_slot(&valid_super_block(1, 1));
        let mut file = vec![0u8; REDO_DEFAULT_DATA_START_OFFSET];
        file[..REDO_SUPER_BLOCK_SLOT_SIZE].copy_from_slice(&slot0);
        file[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET].copy_from_slice(&slot1);

        let selected = select_redo_super_block(&file, 7).unwrap();
        assert_eq!(selected.slot_no, 0);
        assert_eq!(selected.generation, 1);
    }

    #[test]
    fn slot_selection_falls_back_to_valid_older_slot() {
        let slot0 = serialized_slot(&valid_super_block(0, 0));
        let mut slot1 = serialized_slot(&valid_super_block(1, 1));
        slot1[64] = 1;
        let mut file = vec![0u8; REDO_DEFAULT_DATA_START_OFFSET];
        file[..REDO_SUPER_BLOCK_SLOT_SIZE].copy_from_slice(&slot0);
        file[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET].copy_from_slice(&slot1);

        let selected = select_redo_super_block(&file, 7).unwrap();
        assert_eq!(selected.slot_no, 0);
        assert_eq!(selected.generation, 0);
    }

    #[test]
    fn slot_selection_rejects_all_zero_slots() {
        let file = vec![0u8; REDO_DEFAULT_DATA_START_OFFSET];
        let err = select_redo_super_block(&file, 7).unwrap_err();
        assert_integrity_error(err, DataIntegrityError::InvalidMagic);
    }

    fn refresh_checksum(buf: &mut [u8]) {
        write_block_checksum(buf);
    }
}
