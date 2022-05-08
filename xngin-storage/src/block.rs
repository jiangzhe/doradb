use crate::attr::{Attr, SerAttrHeader, LEN_ATTR_HDR};
use crate::error::{Error, Result};
use std::io::{Cursor, Write};
use std::sync::Arc;

/// Block collects multiple tuples and aggregate synopses for analytical query.
/// On-disk format is described as below.
///
/// Block:
/// 1. Block header.
/// 2. Block data.
pub struct Block {
    pub n_records: u16,
    pub data: BlockData,
}

impl Block {
    /// Create a new block with given data.
    #[inline]
    pub fn new(n_records: u16, data: BlockData) -> Self {
        Block { n_records, data }
    }

    /// Serializable block.
    #[inline]
    pub fn ser(&self) -> SerBlock<'_> {
        // offset of first attribute data is 16 + N * 48.
        let mut offset = OFFSET_START_ATTR_HDR + self.data.len() * LEN_ATTR_HDR;
        let mut header;
        let mut ser_attrs = Vec::with_capacity(self.data.len());
        for attr in &self.data {
            (header, offset) = attr.ser_header(offset);
            ser_attrs.push((attr, header));
        }
        SerBlock {
            n_records: self.n_records,
            ser_bytes: offset,
            data: ser_attrs,
        }
    }
}

/// Block Header:
/// 1. Number of records: 2B with 2B padding.
/// 2. Number of attributes: 2B with 2B padding.
/// 3. Reserved padding: 8B.
/// 4. Attribute header, ..., Attribute header: N * 48 B.
///
/// Block Data:
/// 1. Attribute data, ..., Attribute data: variable length.
pub struct SerBlock<'a> {
    n_records: u16,
    ser_bytes: usize,
    data: Vec<(&'a Attr, SerAttrHeader)>,
}

impl<'a> SerBlock<'a> {
    /// Create an empty serializable block.
    #[inline]
    pub fn empty(n_records: u16) -> Self {
        SerBlock {
            n_records,
            ser_bytes: OFFSET_START_ATTR_HDR,
            data: vec![],
        }
    }

    /// Write this block in byte format.
    #[inline]
    pub fn store<W: Write>(&self, writer: &mut W, buf: &mut Vec<u8>) -> Result<usize> {
        // buffer header before writing to storage.
        buf.clear();
        let mut offset = 0;
        let mut cursor = Cursor::new(buf);
        // number of records
        cursor.write_all(&self.n_records.to_ne_bytes())?;
        cursor.write_all(&[0u8; 2])?;
        // number of attributes
        cursor.write_all(&(self.data.len() as u16).to_ne_bytes())?;
        cursor.write_all(&[0u8; 2])?;
        // padding
        cursor.write_all(&[0u8; 8])?;
        // attribute headers
        for (_, header) in &self.data {
            header.store(&mut cursor)?;
        }
        let buf = cursor.into_inner();
        writer.write_all(&buf[..])?;
        offset += buf.len();
        buf.clear();
        // attributes
        // The start of next attribute is also the end of previous attribute.
        // That means we need to fill the gap between neighbor attributes when storing them
        // together.
        let attr_iter = self
            .data
            .iter()
            .map(|(_, hdr)| hdr.start_offset())
            .chain(std::iter::once(self.ser_bytes))
            .skip(1)
            .zip(self.data.iter());
        for (end_offset, (attr, hdr)) in attr_iter {
            let total_bytes = end_offset - hdr.start_offset();
            offset += attr.store(writer, buf, total_bytes)?;
        }
        debug_assert_eq!(offset, self.ser_bytes);
        Ok(offset)
    }
}

/// Block data is just a list of attributes.
pub type BlockData = Vec<Attr>;

// block level offset
const OFFSET_START_N_RECORDS: usize = 0;
const OFFSET_END_N_RECORDS: usize = 2;
const OFFSET_START_N_ATTRS: usize = 4;
const OFFSET_END_N_ATTRS: usize = 6;
const OFFSET_START_ATTR_HDR: usize = 16;

/// RawBlock is the serialized byte format of Block, which could
/// be written directly to disk, and could be also read
/// directly from disk.
#[derive(Clone)]
pub struct RawBlock {
    /// Reference counted byte array.
    /// All data derived from this byte array must clone the
    /// reference counter to make sure data is immutable and
    /// always valid through the whole lifetime.
    inner: Arc<[u8]>,
}

impl RawBlock {
    #[inline]
    pub fn new(bytes: Arc<[u8]>) -> Self {
        Self { inner: bytes }
    }

    /// Returns number of bytes of underlying byte array.
    #[inline]
    pub fn n_bytes(&self) -> usize {
        self.inner.len()
    }

    /// Returns number of records.
    #[inline]
    pub fn n_records(&self) -> u16 {
        u16::from_ne_bytes(
            self.inner[OFFSET_START_N_RECORDS..OFFSET_END_N_RECORDS]
                .try_into()
                .unwrap(),
        )
    }

    /// Returns number of attributes.
    #[inline]
    pub fn n_attrs(&self) -> u16 {
        u16::from_ne_bytes(
            self.inner[OFFSET_START_N_ATTRS..OFFSET_END_N_ATTRS]
                .try_into()
                .unwrap(),
        )
    }

    /// Returns attribute header.
    #[inline]
    fn attr_header(&self, attr_id: usize) -> Option<SerAttrHeader> {
        if attr_id >= self.n_attrs() as usize {
            return None;
        }
        let start = OFFSET_START_ATTR_HDR + attr_id * LEN_ATTR_HDR;
        let end = start + LEN_ATTR_HDR;
        let attr_header = SerAttrHeader::try_from(&self.inner[start..end]).unwrap();
        Some(attr_header)
    }

    /// Load a new block with all attributes.
    #[inline]
    pub fn load_all(&self) -> Result<Block> {
        let n_records = self.n_records();
        let n_attrs = self.n_attrs() as usize;
        let mut data = Vec::with_capacity(n_attrs);
        for i in 0..n_attrs {
            let header = self.attr_header(i).ok_or(Error::InvalidFormat)?;
            let attr = Attr::load(&self.inner, n_records, &header)?;
            data.push(attr);
        }
        Ok(Block::new(n_records, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Single;
    use crate::sel::Sel;
    use std::io::Cursor;
    use std::sync::Arc;
    use xngin_datatype::PreciseType;

    #[test]
    fn test_block_single_store_and_load() {
        let attr1 = Attr::new_null(PreciseType::i32(), 1024);
        let attr2 = Attr::new_single(PreciseType::i32(), Single::new(1i32, 1024), Sel::All(1024));
        let block = Block::new(1024, vec![attr1, attr2]);
        let mut bs: Vec<u8> = vec![];
        let mut cursor = Cursor::new(&mut bs);
        let mut buf = Vec::with_capacity(4096);
        let ser_block = block.ser();
        let total_bytes = ser_block.ser_bytes;
        let written = ser_block.store(&mut cursor, &mut buf).unwrap();
        assert_eq!(total_bytes, written);
        let raw_block = RawBlock::new(Arc::from(bs.into_boxed_slice()));
        assert_eq!(written, raw_block.n_bytes());
        assert_eq!(1024, raw_block.n_records());
        assert_eq!(2, raw_block.n_attrs());
        let new_block = raw_block.load_all().unwrap();
        let attr1_new = &new_block.data[0];
        assert_eq!(PreciseType::i32(), attr1_new.ty);
        assert!(attr1_new.validity.is_none() && attr1_new.sma.is_none());
        let attr2_new = &new_block.data[1];
        assert_eq!(PreciseType::i32(), attr2_new.ty);
        assert!(attr2_new.validity.is_all());
        let value = attr2_new.codec.as_single().unwrap().view::<i32>();
        assert_eq!(1i32, value);
    }

    #[test]
    fn test_block_array_store_and_load() {
        let data1: Vec<i32> = (0i32..1024).collect();
        let attr1 = Attr::from(data1.clone().into_iter());
        let attr2 = Attr::from_iter(data1.iter().map(|v| Some(*v)));
        let block = Block::new(1024, vec![attr1, attr2]);
        let mut bs: Vec<u8> = vec![];
        let mut cursor = Cursor::new(&mut bs);
        let mut buf = Vec::with_capacity(4096);
        let ser_block = block.ser();
        let total_bytes = ser_block.ser_bytes;
        let written = ser_block.store(&mut cursor, &mut buf).unwrap();
        assert_eq!(total_bytes, written);
        let raw_block = RawBlock::new(Arc::from(bs.into_boxed_slice()));
        assert_eq!(written, raw_block.n_bytes());
        assert_eq!(1024, raw_block.n_records());
        assert_eq!(2, raw_block.n_attrs());
        let new_block = raw_block.load_all().unwrap();
        let attr1_new = &new_block.data[0];
        assert_eq!(PreciseType::i32(), attr1_new.ty);
        assert!(attr1_new.validity.is_all() && attr1_new.sma.is_none());
        let arr1 = attr1_new.codec.as_array().unwrap().cast_slice::<i32>();
        assert_eq!(&data1, arr1);
        let attr2_new = &new_block.data[1];
        assert_eq!(PreciseType::i32(), attr2_new.ty);
        assert!(!attr2_new.validity.is_all() && attr2_new.sma.is_none());
        let arr2 = attr2_new.codec.as_array().unwrap().cast_slice::<i32>();
        assert_eq!(&data1, arr2);
    }
}
