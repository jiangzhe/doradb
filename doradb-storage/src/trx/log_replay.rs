use crate::error::{DataIntegrityError, Result};
use crate::io::{DirectBuf, IOBuf};
use crate::io::{STORAGE_SECTOR_SIZE, align_to_sector_size};
use crate::serde::{Deser, LenPrefixPod, Ser, Serde};
use crate::trx::log::RedoLogInitializer;
use crate::trx::redo::{RedoHeader, RedoLogs};
use error_stack::Report;
use memmap2::Mmap;
use std::collections::VecDeque;
use std::fs::File;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::path::Path;

/// Log buffer to hold logs of one or more transaction(s).
pub(crate) struct LogBuf(DirectBuf);

impl LogBuf {
    /// Create a new log buffer.
    #[inline]
    pub(crate) fn new(len: usize) -> Self {
        let mut buf = DirectBuf::zeroed(len);
        // reserve 8-byte for length.
        buf.truncate(mem::size_of::<u64>());
        LogBuf(buf)
    }

    /// Create a log buffer with given DirectBuf.
    #[inline]
    pub(crate) fn with_buffer(mut buf: DirectBuf) -> Self {
        buf.truncate(mem::size_of::<u64>());
        LogBuf(buf)
    }

    /// Serialize data at the end of the log buffer.
    #[inline]
    pub(crate) fn ser<'a, T: Ser<'a>>(&mut self, data: &T) {
        let offset = self.0.len();
        let ser_len = data.ser_len();
        debug_assert!(offset + ser_len <= self.0.capacity());
        let new_offset = data.ser(self.0.as_bytes_mut(), offset);
        self.0.truncate(new_offset);
    }

    /// Complete the serialization and return the underlying direct IO buffer.
    #[inline]
    pub(crate) fn finish(mut self) -> DirectBuf {
        let len = self.0.len();
        debug_assert!(len >= mem::size_of::<u64>());
        let data_len = len - mem::size_of::<u64>();
        // persist length field at the beginning of log buffer.
        self.0.as_bytes_mut().ser_u64(0, data_len as u64);
        self.0
    }

    /// Returns actual length of given data.
    #[inline]
    pub(crate) fn actual_len(data_len: usize) -> usize {
        mem::size_of::<u64>() + data_len
    }

    /// Returns capacity of log buffer.
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Returns whether the buffer is capable for additional data.
    #[inline]
    pub(crate) fn capable_for(&self, len: usize) -> bool {
        self.0.len() + len <= self.0.capacity()
    }
}

pub(crate) struct TrxLog(LenPrefixPod<RedoHeader, RedoLogs>);

impl TrxLog {
    #[inline]
    pub(crate) fn new(header: RedoHeader, payload: RedoLogs) -> Self {
        TrxLog(LenPrefixPod::new(header, payload))
    }

    #[inline]
    pub(crate) fn into_inner(self) -> (RedoHeader, RedoLogs) {
        (self.0.header, self.0.payload)
    }
}

impl Deref for TrxLog {
    type Target = LenPrefixPod<RedoHeader, RedoLogs>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TrxLog {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deser for TrxLog {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, pod) = <LenPrefixPod<RedoHeader, RedoLogs>>::deser(input, start_idx)?;
        Ok((idx, TrxLog(pod)))
    }
}

impl Ser<'_> for TrxLog {
    #[inline]
    fn ser_len(&self) -> usize {
        self.0.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        self.0.ser(out, start_idx)
    }
}

/// Result of log read.
pub(crate) enum ReadLog<'a> {
    /// Log is ended with empty page.
    DataEnd,
    /// File reach maximum size limit.
    SizeLimit,
    /// Data in file is corrupted.
    DataCorrupted,
    /// A group of log.
    Some(LogGroup<'a>),
}

pub(crate) struct LogGroup<'a> {
    data: &'a [u8],
}

impl LogGroup<'_> {
    #[inline]
    #[cfg(test)]
    pub(crate) fn data(&self) -> &[u8] {
        self.data
    }

    #[inline]
    pub(crate) fn try_next(&mut self) -> Result<Option<TrxLog>> {
        if self.data.is_empty() {
            return Ok(None);
        }
        let (offset, res) = TrxLog::deser(self.data, 0)?;
        self.data = &self.data[offset..];
        Ok(Some(res))
    }
}

pub(crate) struct RedoLogStream {
    pub(super) initializer: RedoLogInitializer,
    pub(super) reader: Option<MmapLogReader>,
    pub(super) buffer: VecDeque<TrxLog>,
}

impl Deref for RedoLogStream {
    type Target = VecDeque<TrxLog>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl RedoLogStream {
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
            let reader = self.initializer.next_reader()?;
            if reader.is_none() {
                return Ok(());
            }
            self.reader = reader;
        }
    }

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

    #[inline]
    pub(super) fn into_initializer(self) -> RedoLogInitializer {
        debug_assert!(self.reader.is_none());
        debug_assert!(self.buffer.is_empty());
        self.initializer
    }
}

pub(crate) struct MmapLogReader {
    m: Mmap,
    page_size: usize,
    max_file_size: usize,
    offset: usize,
}

impl MmapLogReader {
    #[inline]
    pub(crate) fn new(
        log_file_path: impl AsRef<Path>,
        page_size: usize,
        max_file_size: usize,
        offset: usize,
    ) -> Result<Self> {
        let file = File::open(log_file_path.as_ref())?;
        // SAFETY: the file handle stays alive for the duration of mapping
        // creation, and the returned `Mmap` owns the mapping afterward.
        let m = unsafe { Mmap::map(&file)? };
        Ok(MmapLogReader {
            m,
            page_size,
            max_file_size,
            offset,
        })
    }

    #[inline]
    pub(crate) fn read(&mut self) -> ReadLog<'_> {
        if self.offset >= self.max_file_size {
            return ReadLog::SizeLimit; // file is exhausted.
        }
        // Always read multiple pages.
        debug_assert!(self.offset.is_multiple_of(STORAGE_SECTOR_SIZE));
        // Try single page first.
        // This may fail as log is incomplete.
        if let Some(mut buf) = self.m.get(self.offset..self.offset + self.page_size) {
            // Log buffer has prefix 8-byte integer, indicating the data length.
            if let Ok((idx, data_len)) = buf.deser_u64(0) {
                // Log file is truncated to certain size, and if no data is written, the header of page
                // will always be zeroed. This is the default behavior on Linux.
                if data_len == 0 {
                    // empty group means end of file.
                    return ReadLog::DataEnd;
                }
                let group_len = data_len as usize + mem::size_of::<u64>();
                if group_len > buf.len() {
                    // this can happen if a group exceeds the single page size.
                    let aligned_len = align_to_sector_size(group_len);
                    if let Some(new_buf) = self.m.get(self.offset..self.offset + aligned_len) {
                        buf = new_buf;
                    } else {
                        return ReadLog::DataCorrupted; // file is incomplete.
                    }
                    self.offset += aligned_len;
                } else {
                    self.offset += self.page_size;
                }
                return ReadLog::Some(LogGroup {
                    data: &buf[idx..idx + data_len as usize],
                });
            } else {
                return ReadLog::DataCorrupted;
            }
        }
        ReadLog::DataCorrupted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::test_page_id;
    use crate::id::{RowID, TableID, TrxID};
    use crate::io::IOBuf;
    use crate::trx::redo::{
        DDLRedo, RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind, TableDML,
    };
    use std::collections::BTreeMap;
    use std::io::Write;

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
        let mut buf = LogBuf::new(log1.ser_len());
        buf.ser(&log1);

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
        buf.ser(&log2);

        let direct_buf = buf.finish();

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(direct_buf.as_bytes()).unwrap();
        file.flush().unwrap();

        println!("file path {:?}", file_path);
        let mut reader = MmapLogReader::new(file_path, 4096, 1024 * 1024, 0).unwrap();
        match reader.read() {
            ReadLog::DataCorrupted | ReadLog::DataEnd | ReadLog::SizeLimit => {
                panic!("invalid data")
            }
            ReadLog::Some(mut group) => {
                let log1 = group.try_next().unwrap().unwrap();
                assert!(log1.header.trx_kind == RedoTrxKind::System);
                let log2 = group.try_next().unwrap().unwrap();
                assert!(log2.header.trx_kind == RedoTrxKind::User);
            }
        }
    }
}
