use crate::error::{DataIntegrityError, Result};
use crate::id::TrxID;
use crate::io::{DirectBuf, IOBuf};
use crate::log::format::RedoGroupHeader;
use crate::log::redo::{RedoHeader, RedoLogs};
use crate::serde::{Deser, MinBytesHint, Ser, Serde, min_bytes_hint};
use error_stack::Report;
use std::mem;

/// Direct I/O buffer for one persisted redo group.
///
/// The buffer reserves header space first, appends one or more length-prefixed
/// transaction records, and tracks the inclusive commit timestamp range needed
/// for the group header.
pub(crate) struct LogBuf {
    /// Sector-aligned bytes that will be submitted to the redo writer.
    buf: DirectBuf,
    /// Inclusive commit timestamp range among appended transaction frames.
    cts_range: Option<(TrxID, TrxID)>,
}

impl LogBuf {
    /// Create a zeroed log buffer and reserve space for the group header.
    #[inline]
    pub(crate) fn new(len: usize) -> Self {
        let mut buf = DirectBuf::zeroed(len);
        buf.truncate(RedoGroupHeader::SIZE);
        LogBuf {
            buf,
            cts_range: None,
        }
    }

    /// Reuse an existing direct buffer and reserve space for the group header.
    #[inline]
    pub(crate) fn with_buffer(mut buf: DirectBuf) -> Self {
        buf.truncate(RedoGroupHeader::SIZE);
        LogBuf {
            buf,
            cts_range: None,
        }
    }

    /// Append one transaction frame and extend the group commit timestamp range.
    #[inline]
    pub(crate) fn append_trx_log(&mut self, trx_log: &TrxLog) {
        let offset = self.buf.len();
        let ser_len = trx_log.ser_len();
        debug_assert!(offset + ser_len <= self.buf.capacity());
        let new_offset = trx_log.ser(self.buf.as_bytes_mut(), offset);
        self.buf.truncate(new_offset);
        let cts = trx_log.header.cts;
        self.cts_range = Some(self.cts_range.map_or((cts, cts), |(min_cts, max_cts)| {
            (min_cts.min(cts), max_cts.max(cts))
        }));
    }

    /// Write the group header, checksum the physical buffer, and return it.
    ///
    /// Padding bytes after the logical body are zeroed before checksum patching
    /// so replay can verify the complete direct-I/O write range deterministically.
    #[inline]
    pub(crate) fn finish(mut self) -> DirectBuf {
        let len = self.buf.len();
        debug_assert!(len >= RedoGroupHeader::SIZE);
        let body_len = len - RedoGroupHeader::SIZE;
        debug_assert!(body_len > 0);
        self.buf.as_bytes_mut()[len..].fill(0);
        let (min_cts, max_cts) = self
            .cts_range
            .unwrap_or_else(|| (TrxID::new(0), TrxID::new(0)));
        let header = RedoGroupHeader::new(body_len, min_cts, max_cts);
        let header_end = header.ser(self.buf.as_bytes_mut(), 0);
        debug_assert_eq!(header_end, RedoGroupHeader::SIZE);
        let checksum_res = RedoGroupHeader::patch_checksum(self.buf.as_bytes_mut());
        debug_assert!(checksum_res.is_ok());
        self.buf
    }

    /// Return the logical group length for a body of `data_len` bytes.
    #[inline]
    pub(crate) fn actual_len(data_len: usize) -> usize {
        RedoGroupHeader::SIZE + data_len
    }

    /// Return the physical buffer capacity.
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Return the real serialized redo CTS range tracked for this group.
    #[inline]
    pub(crate) fn redo_cts_range(&self) -> Option<(TrxID, TrxID)> {
        self.cts_range
    }

    /// Return whether an additional serialized frame of `len` bytes fits.
    #[inline]
    pub(crate) fn capable_for(&self, len: usize) -> bool {
        self.buf
            .len()
            .checked_add(len)
            .is_some_and(|next_len| next_len <= self.buf.capacity())
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

/// Smallest valid transaction frame: redo header plus an empty redo payload.
const MIN_TRX_LOG_FRAME_LEN: usize =
    mem::size_of::<TrxID>() + mem::size_of::<u8>() + mem::size_of::<u8>() + mem::size_of::<u64>();

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
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
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
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-trx, trx_data_len={data_len}, remaining_group_body={remaining}"
                ))
                .into());
        }
        // A frame too small to hold an empty transaction record is always
        // corrupt and would otherwise fail later with less useful context.
        if data_len < MIN_TRX_LOG_FRAME_LEN {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-trx, trx_data_len={data_len}, min_trx_frame_len={MIN_TRX_LOG_FRAME_LEN}"
                ))
                .into());
        }
        let frame_end = frame_start + data_len;
        let (_, frame) = input.deser(frame_start, data_len)?;
        let (idx, header) = RedoHeader::deser(frame, 0)?;
        let (idx, payload) = RedoLogs::deser(frame, idx)?;
        // The length prefix is authoritative: nested payload parsers must
        // consume exactly the advertised frame, with no trailing garbage.
        if idx != frame.len() {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "block=redo-trx, trx_frame_len={}, consumed={idx}",
                    frame.len()
                ))
                .into());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::redo::RedoTrxKind;

    fn simple_trx_log(cts: TrxID) -> TrxLog {
        TrxLog::new(
            RedoHeader {
                cts,
                trx_kind: RedoTrxKind::System,
            },
            RedoLogs::default(),
        )
    }

    #[test]
    fn test_trx_log_rejects_frame_exceeding_group_body() {
        let mut bytes = vec![0u8; mem::size_of::<u64>()];
        bytes[..].ser_u64(0, 1);

        let err = TrxLog::deser(&bytes[..], 0).unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_trx_log_rejects_under_consumed_frame() {
        let log = simple_trx_log(TrxID::new(7));
        let mut bytes = vec![0u8; log.ser_len() + 1];
        let idx = log.ser(&mut bytes[..], 0);
        bytes[..].ser_u64(0, (log.data_len + 1) as u64);
        bytes[idx] = 99;

        let err = TrxLog::deser(&bytes[..], 0).unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_trx_log_rejects_over_consumed_frame() {
        let log = simple_trx_log(TrxID::new(7));
        let mut bytes = vec![0u8; log.ser_len()];
        log.ser(&mut bytes[..], 0);
        bytes[..].ser_u64(0, 1);

        let err = TrxLog::deser(&bytes[..], 0).unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_log_buf_exposes_serialized_redo_cts_range() {
        let mut buf = LogBuf::new(128);
        assert_eq!(buf.redo_cts_range(), None);

        buf.append_trx_log(&simple_trx_log(TrxID::new(7)));
        assert_eq!(buf.redo_cts_range(), Some((TrxID::new(7), TrxID::new(7))));

        buf.append_trx_log(&simple_trx_log(TrxID::new(9)));
        assert_eq!(buf.redo_cts_range(), Some((TrxID::new(7), TrxID::new(9))));
    }
}
