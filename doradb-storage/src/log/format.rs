use crate::error::{DataIntegrityError, Error, Result};
use crate::file::block_integrity::{
    BLOCK_INTEGRITY_HEADER_SIZE, BlockIntegritySpec, checksum_offset, validate_block,
    write_block_checksum, write_block_header,
};
use crate::id::TrxID;
use crate::io::{STORAGE_SECTOR_SIZE, align_to_sector_size};
use crate::serde::{Deser, Ser, Serde};
use error_stack::Report;
use std::mem;

/// Magic bytes stored in every redo file super-block header.
pub(crate) const REDO_FILE_MAGIC: [u8; 8] = *b"DREDO\0\0\0";
/// Redo file format version for checksum-protected group framing.
pub(crate) const REDO_FILE_FORMAT_VERSION: u64 = 2;
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
/// Serialized size of a redo group header.
pub(crate) const REDO_GROUP_HEADER_SIZE: usize = mem::size_of::<u32>() + mem::size_of::<u64>() * 3;
/// Serialized size of redo metadata inside a super-block integrity envelope.
pub(crate) const REDO_SUPER_BLOCK_PAYLOAD_SIZE: usize =
    mem::size_of::<u32>() * 2 + mem::size_of::<u64>() * 3;

/// Fixed prefix for one redo group.
///
/// The checksum covers every byte after the checksum field, including the
/// remaining header fields, the logical group body, and any sector padding.
/// `min_cts..=max_cts` is an integrity envelope for transaction records in
/// the body; replay rejects records outside that inclusive range.
///
/// On disk:
///
/// ```text
/// | Bytes  | Field    | Type | Encoding      |
/// |--------|----------|------|---------------|
/// | 0..4   | checksum | u32  | little-endian |
/// | 4..12  | body_len | u64  | little-endian |
/// | 12..20 | min_cts  | u64  | little-endian |
/// | 20..28 | max_cts  | u64  | little-endian |
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RedoGroupHeader {
    /// CRC32 of the physical group bytes excluding this field.
    pub(crate) checksum: u32,
    /// Logical body length in bytes, excluding this header and sector padding.
    pub(crate) body_len: u64,
    /// Lowest commit timestamp allowed in the group body.
    pub(crate) min_cts: TrxID,
    /// Highest commit timestamp allowed in the group body.
    pub(crate) max_cts: TrxID,
}

impl RedoGroupHeader {
    /// Serialized byte size of the group header.
    pub(crate) const SIZE: usize = REDO_GROUP_HEADER_SIZE;

    /// Build a header before checksum patching.
    #[inline]
    pub(crate) fn new(body_len: usize, min_cts: TrxID, max_cts: TrxID) -> Self {
        RedoGroupHeader {
            checksum: 0,
            body_len: body_len as u64,
            min_cts,
            max_cts,
        }
    }

    /// Convert the encoded body length to the host size type.
    #[inline]
    pub(crate) fn body_len_usize(self) -> Result<usize> {
        usize::try_from(self.body_len).map_err(|_| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-group, body_len_exceeds_usize={}",
                    self.body_len
                ))
                .into()
        })
    }

    /// Returns true for the all-zero header used as the redo file EOF marker.
    #[inline]
    pub(crate) fn is_zero_eof(self) -> bool {
        self.checksum == 0
            && self.body_len == 0
            && self.min_cts.as_u64() == 0
            && self.max_cts.as_u64() == 0
    }

    /// Validate logical header fields after the zero EOF marker is handled.
    #[inline]
    pub(crate) fn validate(self) -> Result<()> {
        if self.body_len == 0 {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=redo-group, body_len=0")
                .into());
        }
        if self.min_cts > self.max_cts {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-group, min_cts={}, max_cts={}",
                    self.min_cts, self.max_cts
                ))
                .into());
        }
        self.body_len_usize()?;
        Ok(())
    }

    /// Return the physical read stride for this logical group length.
    ///
    /// Normal groups occupy one log block even if the body is smaller. Larger
    /// groups are rounded up to the next sector so direct I/O alignment remains
    /// valid and checksum verification includes the persisted padding.
    #[inline]
    pub(crate) fn physical_len(self, log_block_size: usize) -> Result<usize> {
        let body_len = self.body_len_usize()?;
        let logical_len = Self::SIZE.checked_add(body_len).ok_or_else(|| {
            Error::from(
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("block=redo-group, body_len={body_len}")),
            )
        })?;
        if logical_len <= log_block_size {
            return Ok(log_block_size);
        }
        if logical_len > usize::MAX - (STORAGE_SECTOR_SIZE - 1) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("block=redo-group, logical_len={logical_len}"))
                .into());
        }
        Ok(align_to_sector_size(logical_len))
    }

    /// Compute and write the group checksum into the first four bytes.
    ///
    /// The caller must pass the full physical group buffer, including padding,
    /// because replay verifies exactly the same byte range.
    #[inline]
    pub(crate) fn patch_checksum(group_bytes: &mut [u8]) -> Result<u32> {
        if group_bytes.len() < mem::size_of::<u32>() {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-group, invalid_group_len={}",
                    group_bytes.len()
                ))
                .into());
        }
        let checksum = crc32fast::hash(&group_bytes[mem::size_of::<u32>()..]);
        group_bytes.ser_u32(0, checksum);
        Ok(checksum)
    }

    /// Verify that the persisted checksum matches the physical group bytes.
    #[inline]
    pub(crate) fn verify_checksum(self, group_bytes: &[u8]) -> Result<()> {
        if group_bytes.len() < Self::SIZE {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-group, invalid_group_len={}",
                    group_bytes.len()
                ))
                .into());
        }
        let actual = crc32fast::hash(&group_bytes[mem::size_of::<u32>()..]);
        if actual != self.checksum {
            return Err(Report::new(DataIntegrityError::ChecksumMismatch)
                .attach(format!(
                    "block=redo-group, expected_checksum={:08x}, actual_checksum={actual:08x}",
                    self.checksum
                ))
                .into());
        }
        Ok(())
    }
}

impl Ser<'_> for RedoGroupHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        Self::SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u32(start_idx, self.checksum);
        let idx = out.ser_u64(idx, self.body_len);
        let idx = out.ser_u64(idx, self.min_cts.as_u64());
        out.ser_u64(idx, self.max_cts.as_u64())
    }
}

impl Deser for RedoGroupHeader {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, checksum) = input.deser_u32(start_idx)?;
        let (idx, body_len) = input.deser_u64(idx)?;
        let (idx, min_cts) = input.deser_u64(idx)?;
        let (idx, max_cts) = input.deser_u64(idx)?;
        Ok((
            idx,
            RedoGroupHeader {
                checksum,
                body_len,
                min_cts: TrxID::new(min_cts),
                max_cts: TrxID::new(max_cts),
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
/// | 16..48           | redo super-block payload      |
/// | 48..slot_size-32 | zero padding                  |
/// | slot_size-32..   | block-integrity BLAKE3 trailer |
/// ```
///
/// The redo payload is serialized at byte `16`:
///
/// ```text
/// | Payload bytes | Field          | Type | Encoding      |
/// |---------------|----------------|------|---------------|
/// | 0..4          | file_seq       | u32  | little-endian |
/// | 4..8          | slot_no        | u32  | little-endian |
/// | 8..16         | log_block_size | u64  | little-endian |
/// | 16..24        | file_max_size  | u64  | little-endian |
/// | 24..32        | generation     | u64  | little-endian |
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
        }
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
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u32(start_idx, self.file_seq);
        let idx = out.ser_u32(idx, self.slot_no);
        let idx = out.ser_u64(idx, self.log_block_size);
        let idx = out.ser_u64(idx, self.file_max_size);
        out.ser_u64(idx, self.generation)
    }
}

impl Deser for RedoSuperBlock {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, file_seq) = input.deser_u32(start_idx)?;
        let (idx, slot_no) = input.deser_u32(idx)?;
        let (idx, log_block_size) = input.deser_u64(idx)?;
        let (idx, file_max_size) = input.deser_u64(idx)?;
        let (idx, generation) = input.deser_u64(idx)?;
        Ok((
            idx,
            RedoSuperBlock {
                file_seq,
                slot_no,
                log_block_size,
                file_max_size,
                generation,
            },
        ))
    }
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
) -> Result<()> {
    if buf.len() != REDO_SUPER_BLOCK_SLOT_SIZE {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_slot_buffer_len={}",
                buf.len()
            ))
            .into());
    }
    validate_super_block_fields(super_block, super_block.file_seq, super_block.slot_no)?;
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
) -> Result<RedoSuperBlock> {
    if buf.len() != REDO_SUPER_BLOCK_SLOT_SIZE {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_slot_buffer_len={}",
                buf.len()
            ))
            .into());
    }
    let payload = validate_block(buf, REDO_SUPER_BLOCK_SPEC).map_err(Error::from)?;
    let (payload_end, super_block) = RedoSuperBlock::deser(payload, 0).map_err(|err| {
        let reason = err
            .data_integrity_error()
            .unwrap_or(DataIntegrityError::InvalidPayload);
        Error::from(Report::new(reason).attach("block=redo-super-block, section=payload"))
    })?;
    debug_assert_eq!(payload_end, REDO_SUPER_BLOCK_PAYLOAD_SIZE);
    if payload[payload_end..].iter().any(|&byte| byte != 0) {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, nonzero_padding_after_payload, payload_end={payload_end}"
            ))
            .into());
    }
    validate_super_block_fields(&super_block, expected_file_seq, expected_slot_no)?;
    Ok(super_block)
}

/// Select the newest valid redo super-block from redundant slots.
#[inline]
pub(crate) fn select_redo_super_block(
    file_bytes: &[u8],
    expected_file_seq: u32,
) -> Result<RedoSuperBlock> {
    let mut selected: Option<RedoSuperBlock> = None;
    let mut first_err: Option<DataIntegrityError> = None;
    for slot_no in 0..REDO_SUPER_BLOCK_SLOT_COUNT as u32 {
        let offset = slot_offset(slot_no);
        let Some(slot) = file_bytes.get(offset..offset + REDO_SUPER_BLOCK_SLOT_SIZE) else {
            first_err.get_or_insert(DataIntegrityError::InvalidPayload);
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
            Err(err) => {
                first_err.get_or_insert(
                    err.data_integrity_error()
                        .unwrap_or(DataIntegrityError::InvalidPayload),
                );
            }
        }
    }
    selected.ok_or_else(|| {
        Report::new(first_err.unwrap_or(DataIntegrityError::InvalidPayload))
            .attach(format!(
                "no valid redo super-block slots: file_seq={expected_file_seq:08x}"
            ))
            .into()
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
) -> Result<()> {
    if super_block.file_seq != expected_file_seq {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, expected_file_seq={expected_file_seq:08x}, actual_file_seq={:08x}",
                super_block.file_seq
            ))
            .into());
    }
    if super_block.slot_no != expected_slot_no
        || super_block.slot_no as usize >= REDO_SUPER_BLOCK_SLOT_COUNT
    {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, expected_slot_no={expected_slot_no}, actual_slot_no={}",
                super_block.slot_no
            ))
            .into());
    }
    if super_block.log_block_size > usize::MAX as u64
        || !is_valid_aligned_size(super_block.log_block_size as usize)
    {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_log_block_size={}",
                super_block.log_block_size
            ))
            .into());
    }
    let log_block_size = super_block.log_block_size as usize;
    if !is_valid_file_max_size(super_block.file_max_size, log_block_size) {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_file_max_size={}, data_start={}, log_block_size={log_block_size}",
                super_block.file_max_size, REDO_DEFAULT_DATA_START_OFFSET
            ))
            .into());
    }
    Ok(())
}

/// Return true when a persisted size is usable for direct I/O.
#[inline]
fn is_valid_aligned_size(size: usize) -> bool {
    size >= STORAGE_SECTOR_SIZE && size.is_multiple_of(STORAGE_SECTOR_SIZE)
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

    fn assert_integrity_error(err: Error, expected: DataIntegrityError) {
        assert_eq!(err.data_integrity_error(), Some(expected));
    }

    type CorruptCase = (&'static str, Box<dyn FnOnce(&mut [u8])>, DataIntegrityError);

    #[test]
    fn redo_group_header_serializes_fixed_layout() {
        let header = RedoGroupHeader {
            checksum: 0x1122_3344,
            body_len: 0x0102_0304_0506_0708,
            min_cts: TrxID::new(0x1112_1314_1516_1718),
            max_cts: TrxID::new(0x2122_2324_2526_2728),
        };
        let mut buf = [0u8; RedoGroupHeader::SIZE];

        let idx = header.ser(&mut buf[..], 0);

        assert_eq!(idx, RedoGroupHeader::SIZE);
        assert_eq!(&buf[0..4], &0x1122_3344u32.to_le_bytes());
        assert_eq!(&buf[4..12], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&buf[12..20], &0x1112_1314_1516_1718u64.to_le_bytes());
        assert_eq!(&buf[20..28], &0x2122_2324_2526_2728u64.to_le_bytes());

        let (idx, parsed) = RedoGroupHeader::deser(&buf[..], 0).unwrap();
        assert_eq!(idx, RedoGroupHeader::SIZE);
        assert_eq!(parsed, header);
    }

    #[test]
    fn redo_group_checksum_covers_header_body_and_padding() {
        let mut group = vec![0u8; STORAGE_SECTOR_SIZE];
        let header = RedoGroupHeader::new(16, TrxID::new(10), TrxID::new(12));
        header.ser(&mut group[..], 0);
        group[RedoGroupHeader::SIZE..RedoGroupHeader::SIZE + 16].copy_from_slice(&[7u8; 16]);

        let checksum = RedoGroupHeader::patch_checksum(&mut group).unwrap();
        assert_eq!(checksum, crc32fast::hash(&group[mem::size_of::<u32>()..]));
        let (_, parsed) = RedoGroupHeader::deser(&group[..RedoGroupHeader::SIZE], 0).unwrap();
        parsed.verify_checksum(&group).unwrap();

        for mutate in [
            mem::size_of::<u32>(),
            RedoGroupHeader::SIZE,
            STORAGE_SECTOR_SIZE - 1,
        ] {
            let mut corrupted = group.clone();
            corrupted[mutate] ^= 0x80;
            let err = parsed.verify_checksum(&corrupted).unwrap_err();
            assert_integrity_error(err, DataIntegrityError::ChecksumMismatch);
        }
    }

    #[test]
    fn redo_group_physical_len_uses_normal_stride_or_sector_alignment() {
        let small = RedoGroupHeader::new(16, TrxID::new(1), TrxID::new(1));
        assert_eq!(
            small.physical_len(STORAGE_SECTOR_SIZE).unwrap(),
            STORAGE_SECTOR_SIZE
        );

        let large = RedoGroupHeader::new(STORAGE_SECTOR_SIZE, TrxID::new(1), TrxID::new(1));
        assert_eq!(
            large.physical_len(STORAGE_SECTOR_SIZE).unwrap(),
            STORAGE_SECTOR_SIZE * 2
        );
    }

    #[test]
    fn redo_group_validate_rejects_invalid_header_invariants() {
        let empty = RedoGroupHeader {
            checksum: 1,
            body_len: 0,
            min_cts: TrxID::new(1),
            max_cts: TrxID::new(1),
        };
        let err = empty.validate().unwrap_err();
        assert_integrity_error(err, DataIntegrityError::InvalidPayload);

        let inverted_cts = RedoGroupHeader {
            checksum: 1,
            body_len: 1,
            min_cts: TrxID::new(2),
            max_cts: TrxID::new(1),
        };
        let err = inverted_cts.validate().unwrap_err();
        assert_integrity_error(err, DataIntegrityError::InvalidPayload);
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
                    let _ = buf.ser_u64(8, 999);
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
