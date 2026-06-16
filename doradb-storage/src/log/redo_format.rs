use crate::error::{DataIntegrityError, Error, Result};
use crate::io::STORAGE_SECTOR_SIZE;
use crate::serde::{Deser, Ser, Serde};
use error_stack::Report;
use std::mem;

pub(crate) const REDO_FILE_MAGIC: [u8; 8] = *b"DREDO\0\0\0";
pub(crate) const REDO_FILE_FORMAT_VERSION: u64 = 2;
pub(crate) const REDO_SUPER_BLOCK_SLOT_COUNT: usize = 2;
pub(crate) const REDO_SUPER_BLOCK_SLOT_SIZE: usize = STORAGE_SECTOR_SIZE;
pub(crate) const REDO_DEFAULT_DATA_START_OFFSET: usize =
    REDO_SUPER_BLOCK_SLOT_COUNT * REDO_SUPER_BLOCK_SLOT_SIZE;
const REDO_SUPER_BLOCK_FOOTER_SIZE: usize = mem::size_of::<RedoFileFooter>();
const REDO_SUPER_BLOCK_FOOTER_OFFSET: usize =
    REDO_SUPER_BLOCK_SLOT_SIZE - REDO_SUPER_BLOCK_FOOTER_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoFileHeader {
    pub(crate) magic: [u8; 8],
    pub(crate) version: u64,
    pub(crate) file_seq: u32,
    pub(crate) header_slot_no: u32,
    pub(crate) log_block_size: u64,
    pub(crate) file_max_size: u64,
    pub(crate) generation: u64,
}

impl RedoFileHeader {
    #[inline]
    pub(crate) fn initial(file_seq: u32, log_block_size: usize, file_max_size: usize) -> Self {
        RedoFileHeader {
            magic: REDO_FILE_MAGIC,
            version: REDO_FILE_FORMAT_VERSION,
            file_seq,
            header_slot_no: 0,
            log_block_size: log_block_size as u64,
            file_max_size: file_max_size as u64,
            generation: 0,
        }
    }
}

impl Ser<'_> for RedoFileHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<[u8; 8]>()
            + mem::size_of::<u64>()
            + mem::size_of::<u32>()
            + mem::size_of::<u32>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.magic);
        let idx = out.ser_u64(idx, self.version);
        let idx = out.ser_u32(idx, self.file_seq);
        let idx = out.ser_u32(idx, self.header_slot_no);
        let idx = out.ser_u64(idx, self.log_block_size);
        let idx = out.ser_u64(idx, self.file_max_size);
        out.ser_u64(idx, self.generation)
    }
}

impl Deser for RedoFileHeader {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic) = input.deser_byte_array::<8>(start_idx)?;
        let (idx, version) = input.deser_u64(idx)?;
        let (idx, file_seq) = input.deser_u32(idx)?;
        let (idx, header_slot_no) = input.deser_u32(idx)?;
        let (idx, log_block_size) = input.deser_u64(idx)?;
        let (idx, file_max_size) = input.deser_u64(idx)?;
        let (idx, generation) = input.deser_u64(idx)?;
        Ok((
            idx,
            RedoFileHeader {
                magic,
                version,
                file_seq,
                header_slot_no,
                log_block_size,
                file_max_size,
                generation,
            },
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoFileFooter {
    pub(crate) b3sum: [u8; 32],
    pub(crate) generation: u64,
    pub(crate) file_seq: u32,
    pub(crate) header_slot_no: u32,
}

impl Ser<'_> for RedoFileFooter {
    #[inline]
    fn ser_len(&self) -> usize {
        REDO_SUPER_BLOCK_FOOTER_SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.b3sum);
        let idx = out.ser_u64(idx, self.generation);
        let idx = out.ser_u32(idx, self.file_seq);
        out.ser_u32(idx, self.header_slot_no)
    }
}

impl Deser for RedoFileFooter {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, b3sum) = input.deser_byte_array::<32>(start_idx)?;
        let (idx, generation) = input.deser_u64(idx)?;
        let (idx, file_seq) = input.deser_u32(idx)?;
        let (idx, header_slot_no) = input.deser_u32(idx)?;
        Ok((
            idx,
            RedoFileFooter {
                b3sum,
                generation,
                file_seq,
                header_slot_no,
            },
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoFileSuperBlock {
    pub(crate) header: RedoFileHeader,
    pub(crate) footer: RedoFileFooter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SelectedRedoFileHeader {
    pub(crate) header: RedoFileHeader,
    pub(crate) slot_no: u32,
    pub(crate) slot_offset: usize,
}

#[inline]
pub(crate) fn slot_offset(slot_no: u32) -> usize {
    slot_no as usize * REDO_SUPER_BLOCK_SLOT_SIZE
}

#[inline]
pub(crate) fn serialize_redo_super_block(buf: &mut [u8], header: &RedoFileHeader) -> Result<()> {
    if buf.len() != REDO_SUPER_BLOCK_SLOT_SIZE {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_slot_buffer_len={}",
                buf.len()
            ))
            .into());
    }
    validate_header_fields(header, header.file_seq, header.header_slot_no)?;
    buf.fill(0);
    let body_end = header.ser(buf, 0);
    debug_assert!(body_end <= REDO_SUPER_BLOCK_FOOTER_OFFSET);
    let b3sum = blake3::hash(&buf[..REDO_SUPER_BLOCK_FOOTER_OFFSET]);
    let footer = RedoFileFooter {
        b3sum: *b3sum.as_bytes(),
        generation: header.generation,
        file_seq: header.file_seq,
        header_slot_no: header.header_slot_no,
    };
    let footer_end = footer.ser(buf, REDO_SUPER_BLOCK_FOOTER_OFFSET);
    debug_assert_eq!(footer_end, REDO_SUPER_BLOCK_SLOT_SIZE);
    Ok(())
}

#[inline]
pub(crate) fn parse_redo_super_block(
    buf: &[u8],
    expected_file_seq: u32,
    expected_slot_no: u32,
) -> Result<RedoFileSuperBlock> {
    if buf.len() != REDO_SUPER_BLOCK_SLOT_SIZE {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_slot_buffer_len={}",
                buf.len()
            ))
            .into());
    }
    let (_, header) = RedoFileHeader::deser(buf, 0).map_err(|err| {
        let reason = err
            .data_integrity_error()
            .unwrap_or(DataIntegrityError::InvalidPayload);
        Error::from(Report::new(reason).attach("block=redo-super-block, section=header"))
    })?;
    validate_header_fields(&header, expected_file_seq, expected_slot_no)?;
    let (_, footer) = RedoFileFooter::deser(buf, REDO_SUPER_BLOCK_FOOTER_OFFSET).map_err(|_| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=redo-super-block, section=footer"),
        )
    })?;
    if header.generation != footer.generation
        || header.file_seq != footer.file_seq
        || header.header_slot_no != footer.header_slot_no
    {
        return Err(Report::new(DataIntegrityError::TornWrite)
            .attach(format!(
                "block=redo-super-block, header_generation={}, footer_generation={}, header_file_seq={}, footer_file_seq={}, header_slot_no={}, footer_slot_no={}",
                header.generation,
                footer.generation,
                header.file_seq,
                footer.file_seq,
                header.header_slot_no,
                footer.header_slot_no
            ))
            .into());
    }
    let b3sum = blake3::hash(&buf[..REDO_SUPER_BLOCK_FOOTER_OFFSET]);
    if b3sum.as_bytes() != &footer.b3sum {
        return Err(Report::new(DataIntegrityError::ChecksumMismatch)
            .attach(format!(
                "block=redo-super-block, file_seq={expected_file_seq:08x}, slot_no={expected_slot_no}"
            ))
            .into());
    }
    Ok(RedoFileSuperBlock { header, footer })
}

#[inline]
pub(crate) fn select_redo_file_header(
    file_bytes: &[u8],
    expected_file_seq: u32,
) -> Result<SelectedRedoFileHeader> {
    let mut selected: Option<SelectedRedoFileHeader> = None;
    let mut first_err: Option<DataIntegrityError> = None;
    for slot_no in 0..REDO_SUPER_BLOCK_SLOT_COUNT as u32 {
        let offset = slot_offset(slot_no);
        let Some(slot) = file_bytes.get(offset..offset + REDO_SUPER_BLOCK_SLOT_SIZE) else {
            first_err.get_or_insert(DataIntegrityError::InvalidPayload);
            continue;
        };
        match parse_redo_super_block(slot, expected_file_seq, slot_no) {
            Ok(super_block) => {
                let candidate = SelectedRedoFileHeader {
                    header: super_block.header,
                    slot_no,
                    slot_offset: offset,
                };
                if selected
                    .as_ref()
                    .is_none_or(|current| is_newer_header(&candidate, current))
                {
                    selected = Some(candidate);
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

#[inline]
fn is_newer_header(candidate: &SelectedRedoFileHeader, current: &SelectedRedoFileHeader) -> bool {
    (candidate.header.generation, candidate.slot_no) > (current.header.generation, current.slot_no)
}

#[inline]
fn validate_header_fields(
    header: &RedoFileHeader,
    expected_file_seq: u32,
    expected_slot_no: u32,
) -> Result<()> {
    if header.magic != REDO_FILE_MAGIC {
        return Err(Report::new(DataIntegrityError::InvalidMagic)
            .attach(format!(
                "block=redo-super-block, expected_magic={REDO_FILE_MAGIC:?}, actual_magic={:?}",
                header.magic
            ))
            .into());
    }
    if header.version != REDO_FILE_FORMAT_VERSION {
        return Err(Report::new(DataIntegrityError::InvalidVersion)
            .attach(format!(
                "block=redo-super-block, expected_version={REDO_FILE_FORMAT_VERSION}, actual_version={}",
                header.version
            ))
            .into());
    }
    if header.file_seq != expected_file_seq {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, expected_file_seq={expected_file_seq:08x}, actual_file_seq={:08x}",
                header.file_seq
            ))
            .into());
    }
    if header.header_slot_no != expected_slot_no
        || header.header_slot_no as usize >= REDO_SUPER_BLOCK_SLOT_COUNT
    {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, expected_slot_no={expected_slot_no}, actual_slot_no={}",
                header.header_slot_no
            ))
            .into());
    }
    if header.log_block_size > usize::MAX as u64
        || !is_valid_aligned_size(header.log_block_size as usize)
    {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_log_block_size={}",
                header.log_block_size
            ))
            .into());
    }
    if !is_valid_file_max_size(header.file_max_size) {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "block=redo-super-block, invalid_file_max_size={}, min_file_max_size={}",
                header.file_max_size, REDO_DEFAULT_DATA_START_OFFSET
            ))
            .into());
    }
    Ok(())
}

#[inline]
fn is_valid_aligned_size(size: usize) -> bool {
    size >= STORAGE_SECTOR_SIZE && size.is_multiple_of(STORAGE_SECTOR_SIZE)
}

#[inline]
fn is_valid_file_max_size(file_max_size: u64) -> bool {
    file_max_size > REDO_DEFAULT_DATA_START_OFFSET as u64
        && file_max_size <= usize::MAX as u64
        && (file_max_size as usize).is_multiple_of(STORAGE_SECTOR_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_header(slot_no: u32, generation: u64) -> RedoFileHeader {
        let mut header =
            RedoFileHeader::initial(7, STORAGE_SECTOR_SIZE * 2, STORAGE_SECTOR_SIZE * 8);
        header.header_slot_no = slot_no;
        header.generation = generation;
        header
    }

    fn serialized_slot(header: &RedoFileHeader) -> Vec<u8> {
        let mut buf = vec![0u8; REDO_SUPER_BLOCK_SLOT_SIZE];
        serialize_redo_super_block(&mut buf, header).unwrap();
        buf
    }

    fn assert_integrity_error(err: Error, expected: DataIntegrityError) {
        assert_eq!(err.data_integrity_error(), Some(expected));
    }

    type CorruptCase = (&'static str, Box<dyn FnOnce(&mut [u8])>, DataIntegrityError);

    #[test]
    fn redo_file_header_round_trips() {
        let header = valid_header(0, 0);
        let buf = serialized_slot(&header);
        let super_block = parse_redo_super_block(&buf, 7, 0).unwrap();
        assert_eq!(super_block.header, header);
        assert_eq!(super_block.footer.generation, 0);
        assert_eq!(super_block.footer.file_seq, 7);
        assert_eq!(super_block.footer.header_slot_no, 0);
    }

    #[test]
    fn redo_file_header_rejects_invalid_fields() {
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
                    refresh_checksum(buf);
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
                "footer_redundancy",
                Box::new(|buf| {
                    let _ = buf.ser_u64(REDO_SUPER_BLOCK_FOOTER_OFFSET + 32, 99);
                }),
                DataIntegrityError::TornWrite,
            ),
            (
                "checksum",
                Box::new(|buf| {
                    buf[64] = 1;
                }),
                DataIntegrityError::ChecksumMismatch,
            ),
        ];

        for (name, corrupt, expected) in cases {
            let header = valid_header(0, 0);
            let mut buf = serialized_slot(&header);
            corrupt(&mut buf);
            let err = parse_redo_super_block(&buf, 7, 0).unwrap_err();
            assert_integrity_error(err, expected);
            let _ = name;
        }
    }

    #[test]
    fn slot_selection_chooses_newest_valid_generation() {
        let slot0 = serialized_slot(&valid_header(0, 0));
        let slot1 = serialized_slot(&valid_header(1, 1));
        let mut file = vec![0u8; REDO_DEFAULT_DATA_START_OFFSET];
        file[..REDO_SUPER_BLOCK_SLOT_SIZE].copy_from_slice(&slot0);
        file[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET].copy_from_slice(&slot1);

        let selected = select_redo_file_header(&file, 7).unwrap();
        assert_eq!(selected.slot_no, 1);
        assert_eq!(selected.header.generation, 1);
    }

    #[test]
    fn slot_selection_falls_back_to_valid_older_slot() {
        let slot0 = serialized_slot(&valid_header(0, 0));
        let mut slot1 = serialized_slot(&valid_header(1, 1));
        slot1[64] = 1;
        let mut file = vec![0u8; REDO_DEFAULT_DATA_START_OFFSET];
        file[..REDO_SUPER_BLOCK_SLOT_SIZE].copy_from_slice(&slot0);
        file[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET].copy_from_slice(&slot1);

        let selected = select_redo_file_header(&file, 7).unwrap();
        assert_eq!(selected.slot_no, 0);
        assert_eq!(selected.header.generation, 0);
    }

    #[test]
    fn slot_selection_rejects_all_zero_slots() {
        let file = vec![0u8; REDO_DEFAULT_DATA_START_OFFSET];
        let err = select_redo_file_header(&file, 7).unwrap_err();
        assert_integrity_error(err, DataIntegrityError::InvalidMagic);
    }

    fn refresh_checksum(buf: &mut [u8]) {
        let b3sum = blake3::hash(&buf[..REDO_SUPER_BLOCK_FOOTER_OFFSET]);
        let _ = buf.ser_byte_array(REDO_SUPER_BLOCK_FOOTER_OFFSET, b3sum.as_bytes());
    }
}
