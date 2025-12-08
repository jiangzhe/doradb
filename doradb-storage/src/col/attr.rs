use crate::col::alloc::align_u128;
use crate::col::array::Array;
use crate::col::bitmap::Bitmap;
use crate::col::codec::{Codec, Single};
use crate::col::repr::ByteRepr;
use crate::col::sel::Sel;
use crate::col::sma::{PosKind, PosTbl, SMA};
use crate::error::{Error, Result};
use bitflags::bitflags;
use doradb_datatype::{PreciseType, StaticTyped};
use smallvec::SmallVec;
use std::io;
use std::sync::Arc;

// attribute header level offset
const ATTR_HDR_OFFSET_START_FMT: usize = 0;
const ATTR_HDR_OFFSET_END_FMT: usize = 8;
const ATTR_HDR_OFFSET_START_VALID: usize = 8;
const ATTR_HDR_OFFSET_END_VALID: usize = 16;
const ATTR_HDR_OFFSET_START_DATA: usize = 16;
const ATTR_HDR_OFFSET_END_DATA: usize = 24;
const ATTR_HDR_OFFSET_START_SMA: usize = 24;
const ATTR_HDR_OFFSET_END_SMA: usize = 32;
const ATTR_HDR_OFFSET_START_DICT: usize = 32;
const ATTR_HDR_OFFSET_END_DICT: usize = 40;
const ATTR_HDR_OFFSET_START_STR: usize = 40;
const ATTR_HDR_OFFSET_END_STR: usize = 48;
pub(crate) const LEN_ATTR_HDR: usize = 48;

/// Attribute data:
/// 1. Validity.
/// 2. Compressed data: fixed-length data, length depends on compression method.
/// 3. SMA data: contains MinValue and MaxValue, length depends on data type.
///    and lookup(PSMA) table: lookup by first non-zero byte, length depends on data type.
/// 4. Dict data
/// 5. String data: variable-length data.
///
/// Lookup table:
/// 1. Entry count: 2B with 6B padding.
/// 2. Lookup ranges: 256 * 4 * 2 * N B. N is byte width of value. e.g. N of u16 is 2.
///    Key of lookup range is first non-zero byte. Range contains
///    start offset and end offset(exclusive). end offset = 0 means missing.
///
/// Dict data:
/// 1. Entry count: 4B with 4B padding.
/// 2. Entry offsets: (N + 1) * 4 B, with padding to make multiple of 8.
///    Offset starts from first byte of "String data".
///
/// Single Codec:
/// 1. Number of bytes: 4B.
/// 2. Number of identical values: 4B.
/// 3. Value Bytes: variable number of bytes.
///
/// Array Codec(Non-string): Fixed-length array.
#[derive(Debug)]
pub struct Attr {
    pub ty: PreciseType,
    pub codec: Codec,
    pub validity: Sel,
    pub sma: Option<Arc<SMA>>,
}

impl Attr {
    /// Create new attribute with single codec.
    #[inline]
    pub fn new_single(ty: PreciseType, single: Single, validity: Sel) -> Attr {
        debug_assert_eq!(single.len as usize, validity.n_records());
        debug_assert!(!validity.is_none());
        Attr {
            ty,
            validity,
            codec: Codec::Single(single),
            sma: None,
        }
    }

    #[inline]
    pub fn new_null(ty: PreciseType, n_records: u16) -> Attr {
        Attr {
            ty,
            validity: Sel::None(n_records),
            codec: Codec::Single(Single::new_null(n_records)),
            sma: None,
        }
    }

    /// Create a bitmap attribute.
    #[inline]
    pub fn new_bitmap(bitmap: Bitmap, validity: Sel) -> Attr {
        debug_assert_eq!(bitmap.len(), validity.n_records());
        Attr {
            ty: PreciseType::bool(),
            codec: Codec::new_bitmap(bitmap),
            validity,
            sma: None,
        }
    }

    #[inline]
    pub fn arc_bitmap(arc_bitmap: Arc<Bitmap>, validity: Sel) -> Attr {
        debug_assert_eq!(arc_bitmap.len(), validity.n_records());
        Attr {
            ty: PreciseType::bool(),
            codec: Codec::Bitmap(arc_bitmap),
            validity,
            sma: None,
        }
    }

    #[inline]
    pub fn new_array(ty: PreciseType, array: Array, validity: Sel, sma: Option<Arc<SMA>>) -> Attr {
        debug_assert_eq!(array.len(), validity.n_records());
        Attr {
            ty,
            codec: Codec::new_array(array),
            validity,
            sma,
        }
    }

    /// Create an empty attribute
    #[inline]
    pub fn empty(ty: PreciseType) -> Attr {
        Attr {
            ty,
            validity: Sel::None(0),
            codec: Codec::Empty,
            sma: None,
        }
    }

    /// Convert self to owned.
    #[inline]
    pub fn to_owned(&self) -> Self {
        Attr {
            ty: self.ty,
            validity: self.validity.clone_to_owned(),
            codec: Codec::to_owned(&self.codec),
            sma: self.sma.as_ref().map(SMA::clone_to_owned),
        }
    }

    /// Returns valid flag and raw bytes at given index.
    /// If it's null, length of returned byte slice is zero.
    #[inline]
    pub fn val_at(&self, idx: usize) -> Result<(bool, &[u8])> {
        let valid = self.is_valid(idx)?;
        if !valid {
            return Ok((false, &[]));
        }
        let bs = match &self.codec {
            Codec::Empty => return Err(Error::IndexOutOfBound),
            Codec::Single(s) => &s.data[..],
            Codec::Bitmap(b) => {
                if b.get(idx)? {
                    &[0x01]
                } else {
                    &[0x00]
                }
            }
            Codec::Array(a) => {
                let val_len = self.ty.val_len().unwrap();
                let byte_idx = idx * val_len;
                &a.raw()[byte_idx..byte_idx + val_len]
            }
        };
        Ok((true, bs))
    }

    /// Returns valid flag and bool value at given index.
    /// The codec must be single or bitmap.
    #[inline]
    pub fn bool_at(&self, idx: usize) -> Result<(bool, bool)> {
        let valid = self.is_valid(idx)?;
        if !valid {
            return Ok((false, false));
        }
        match &self.codec {
            Codec::Single(s) => Ok((true, s.view_bool())),
            Codec::Bitmap(b) => Ok((true, b.get(idx)?)),
            _ => Err(Error::InvalidDatatype),
        }
    }

    /// Returns whether the value at given index is valid.
    #[inline]
    pub fn is_valid(&self, idx: usize) -> Result<bool> {
        self.validity.selected(idx)
    }

    /// Returns number of records.
    #[inline]
    pub fn n_records(&self) -> usize {
        self.codec.n_records()
    }

    /// Make a new header of this attribute based on given offset.
    #[inline]
    pub fn ser_header(&self, offset: usize) -> (SerAttrHeader, usize) {
        match &self.codec {
            Codec::Single(s) => {
                assert!(self.sma.is_none() && (self.validity.is_all() || self.validity.is_none()));
                let fields = SerFields::VALID;
                let format_desc = SerFormatDesc {
                    ty: self.ty,
                    method: SerMethod::Single,
                    fields,
                };
                // 1. validity
                let (offset_valid, offset) = ser_validity_offset(&self.validity, offset);
                // 2. data
                let (offset_data, offset) = if self.validity.is_none() {
                    (offset, offset)
                } else {
                    (offset, offset + align_u128(s.data.len() + 4))
                };
                (
                    SerAttrHeader {
                        format_desc,
                        offset_data,
                        offset_valid,
                        offset_sma: offset,
                        offset_dict: offset,
                        offset_str: offset,
                    },
                    offset,
                )
            }
            Codec::Array(a) => {
                let mut fields = SerFields::VALID; // always serialize validity
                // 1. validity
                let (offset_valid, offset) = ser_validity_offset(&self.validity, offset);
                // 2. data
                // array codec does not support dict and string
                let (offset_data, offset) = (offset, align_u128(offset + a.total_bytes()));
                // 3. sma(optional)
                let (offset_sma, offset) = if let Some(sma) = self.sma.as_ref() {
                    fields.insert(SerFields::SMA);
                    // array codec does not allow varlen values, so sma values must be fixed length.
                    // encoded as length + sma
                    let sma_bytes = align_u128(sma.val_bytes() + sma.kind_bytes())
                        + align_u128(sma.pos_bytes());
                    (offset, align_u128(offset + sma_bytes))
                } else {
                    (offset, offset)
                };
                let format_desc = SerFormatDesc {
                    ty: self.ty,
                    method: SerMethod::Array,
                    fields,
                };
                (
                    SerAttrHeader {
                        format_desc,
                        offset_data,
                        offset_valid,
                        offset_sma,
                        offset_dict: offset,
                        offset_str: offset,
                    },
                    offset,
                )
            }
            Codec::Bitmap(_) => todo!(),
            Codec::Empty => todo!(),
        }
    }

    /// Write attribute in byte format.
    #[inline]
    pub fn store<W: io::Write>(
        &self,
        writer: &mut W,
        buf: &mut Vec<u8>,
        total_bytes: usize,
    ) -> Result<usize> {
        let n = match &self.codec {
            Codec::Single(s) => {
                // write validity
                let mut n = write_validity(&self.validity, writer, buf)?;
                // write data
                if !self.validity.is_none() {
                    buf.clear();
                    // always prefix with data length
                    buf.extend_from_slice(&(s.data.len() as u32).to_ne_bytes());
                    buf.extend_from_slice(&s.data);
                    // fill gap
                    buf.extend(std::iter::repeat_n(0u8, align_u128(buf.len()) - buf.len()));
                    writer.write_all(buf)?;
                    n += buf.len();
                }
                n
            }
            Codec::Array(a) => {
                // write validity
                let mut n = write_validity(&self.validity, writer, buf)?;
                // write data
                n += write_align_u128(a.raw(), writer, buf)?;
                // write sma
                if let Some(sma) = self.sma.as_ref() {
                    n += write_sma(sma, writer, buf)?;
                }
                n
            }
            Codec::Bitmap(_) => todo!(),
            Codec::Empty => todo!(),
        };
        debug_assert_eq!(total_bytes, n);
        Ok(n)
    }

    /// Load attribute from byte format.
    #[inline]
    pub fn load(raw: &Arc<[u8]>, n_records: u16, header: &SerAttrHeader) -> Result<Self> {
        match header.format_desc.method {
            SerMethod::Single => {
                debug_assert!(header.format_desc.fields.contains(SerFields::VALID));
                // 1. validity
                let validity = load_validity(raw, header.offset_valid, n_records)?;
                // 2. data
                if validity.is_none() {
                    Ok(Attr::new_null(header.format_desc.ty, n_records))
                } else {
                    let bytes_slice: [u8; 4] =
                        raw[header.offset_data..header.offset_data + 4].try_into()?;
                    let n_bytes = u32::from_ne_bytes(bytes_slice) as usize;
                    let mut data = SmallVec::with_capacity(n_bytes);

                    data.extend_from_slice(
                        &raw[header.offset_data + 4..header.offset_data + 4 + n_bytes],
                    );
                    Ok(Attr::new_single(
                        header.format_desc.ty,
                        Single::new_raw(data, n_records),
                        validity,
                    ))
                }
            }
            SerMethod::Array => {
                debug_assert!(header.format_desc.fields.contains(SerFields::VALID));
                // 1. validity
                let validity = load_validity(raw, header.offset_valid, n_records)?;
                // 2. data
                let arr = load_array(raw, n_records, header.format_desc.ty, header.offset_data)?;
                // 3. sma
                let sma = if header.format_desc.fields.contains(SerFields::SMA) {
                    let sma = load_fixed_len_sma(raw, header.format_desc.ty, header.offset_sma)?;
                    Some(Arc::new(sma))
                } else {
                    None
                };
                Ok(Attr::new_array(header.format_desc.ty, arr, validity, sma))
            }
        }
    }

    /// Setup SMA based on array codec.
    /// If all values are null, this method will convert
    /// array codec to single codec.
    #[inline]
    pub fn setup_sma(&mut self) {
        if self.sma.is_some() {
            return; // do not compute agian
        }
        // Currently SMA is only available for array codec.
        if let Codec::Array(a) = &self.codec {
            let sma = match self.ty {
                PreciseType::Int(4, false) => {
                    let data = a.cast_slice::<i32>();
                    SMA::with_validity(data, &self.validity).map(Arc::new)
                }
                _ => todo!(),
            };
            if sma.is_none() {
                // SMA is not available because all values are null.
                // Update codec to single.
                let n_records = self.n_records();
                self.codec = Codec::Single(Single::new_null(n_records as u16));
                self.validity = Sel::None(n_records as u16);
            } else {
                self.sma = sma;
            }
        }
    }
}

impl From<Sel> for Attr {
    #[inline]
    fn from(sel: Sel) -> Self {
        match sel {
            Sel::All(len) => Attr::new_single(
                PreciseType::bool(),
                Single::new_bool(true, len),
                Sel::All(len),
            ),
            Sel::None(len) => Attr::new_single(
                PreciseType::bool(),
                Single::new_bool(false, len),
                Sel::All(len),
            ),
            Sel::Index {
                count,
                len,
                indexes,
            } => {
                let mut bm = Bitmap::zeroes(len as usize);
                for idx in &indexes[..count as usize] {
                    bm.set(*idx as usize, true).unwrap();
                }
                Attr::new_bitmap(bm, Sel::All(len))
            }
            Sel::Bitmap(bm) => {
                if bm.as_ref().is_owned() {
                    let len = bm.len() as u16;
                    Attr::arc_bitmap(bm, Sel::All(len))
                } else {
                    Attr::arc_bitmap(Bitmap::clone_to_owned(&bm), Sel::All(bm.len() as u16))
                }
            }
        }
    }
}

#[inline]
fn write_validity<W: io::Write>(
    validity: &Sel,
    writer: &mut W,
    buf: &mut Vec<u8>,
) -> Result<usize> {
    let mut n = 0;
    match validity {
        Sel::All(_) => {
            let mut data = [0u8; 16];
            data[0] = SerValidType::All as u8;
            writer.write_all(&data)?;
            n += 16;
        }
        Sel::None(_) => {
            let mut data = [0u8; 16];
            data[0] = SerValidType::None as u8;
            writer.write_all(&data)?;
            n += 16;
        }
        Sel::Index { count, indexes, .. } => {
            let mut data = [0u8; 16];
            data[0] = SerValidType::Index as u8;
            data[1] = *count;
            data[2..2 + 12].copy_from_slice(bytemuck::cast_slice::<_, u8>(indexes));
            writer.write_all(&data)?;
            n += 16;
        }
        Sel::Bitmap(bm) => {
            let mut data = [0u8; 16];
            data[0] = SerValidType::Bitmap as u8;
            writer.write_all(&data)?;
            n += 16;
            n += write_align_u128(bm.raw(), writer, buf)?;
        }
    }
    Ok(n)
}

#[inline]
fn write_sma<W: io::Write>(sma: &SMA, writer: &mut W, buf: &mut Vec<u8>) -> Result<usize> {
    let mut n = 0;
    buf.clear();
    // array codec does not support var length values.
    // min value, max value and kind
    buf.extend_from_slice(&sma.min);
    buf.extend_from_slice(&sma.max);
    buf.push(sma.kind as u8);
    let padding = align_u128(buf.len()) - buf.len();
    if padding > 0 {
        buf.extend(std::iter::repeat_n(0u8, padding));
    }
    // kind
    writer.write_all(buf)?;
    n += buf.len();
    // position lookup
    n += write_align_u128(sma.raw_pos_tbl(), writer, buf)?;
    Ok(n)
}

#[inline]
fn ser_validity_offset(validity: &Sel, offset: usize) -> (usize, usize) {
    let vlen = match validity {
        Sel::All(_) | Sel::None(_) => align_u128(1),
        Sel::Index { .. } => align_u128(1 + 1 + 12),
        Sel::Bitmap(bm) => align_u128(1) + align_u128(bm.total_bytes()),
    };
    (offset, offset + vlen)
}

#[inline]
fn load_validity(raw: &Arc<[u8]>, offset: usize, n_records: u16) -> Result<Sel> {
    let res = match SerValidType::try_from(raw[offset])? {
        SerValidType::All => Sel::All(n_records),
        SerValidType::None => Sel::None(n_records),
        SerValidType::Index => {
            let count = raw[offset + 1];
            let mut indexes = [0u16; 6];
            bytemuck::cast_slice_mut::<_, u8>(&mut indexes)
                .copy_from_slice(&raw[offset + 2..offset + 2 + 12]);
            Sel::Index {
                count,
                len: n_records,
                indexes,
            }
        }
        SerValidType::Bitmap => {
            let bm = Bitmap::new_borrowed(Arc::clone(raw), n_records as usize, offset + 16);
            Sel::Bitmap(Arc::new(bm))
        }
    };
    Ok(res)
}

#[inline]
fn load_array(
    raw: &Arc<[u8]>,
    n_records: u16,
    ty: PreciseType,
    start_bytes: usize,
) -> Result<Array> {
    let arr = match ty {
        PreciseType::Int(4, _) => {
            Array::new_borrowed::<i32>(raw.clone(), n_records as usize, start_bytes)
        }
        _ => return Err(Error::DataTypeNotSupported),
    };
    Ok(arr)
}

#[inline]
fn load_fixed_len_sma(raw: &Arc<[u8]>, ty: PreciseType, start_bytes: usize) -> Result<SMA> {
    let val_len = ty.val_len().unwrap(); // won't fail
    // read min value
    let mut min = SmallVec::with_capacity(val_len);
    let start = start_bytes;
    let end = start + val_len;
    min.extend_from_slice(&raw[start..end]);
    // read max value
    let mut max = SmallVec::with_capacity(val_len);
    let (start, end) = (end, end + val_len);
    max.extend_from_slice(&raw[start..end]);
    // read kind
    let kind = PosKind::try_from(raw[end])?;
    // align and read sma lookup table
    let start = align_u128(end + 1);
    let pos = PosTbl::new_borrowed(raw.clone(), kind.n_slots(), start);
    Ok(SMA::new(min, max, kind, pos))
}

#[inline]
fn write_align_u128<W: io::Write>(bs: &[u8], writer: &mut W, buf: &mut Vec<u8>) -> Result<usize> {
    writer.write_all(bs)?;
    let total_bytes = align_u128(bs.len());
    if total_bytes > bs.len() {
        buf.clear();
        buf.extend(std::iter::repeat_n(0u8, total_bytes - bs.len()));
        writer.write_all(buf)?;
    }
    Ok(total_bytes)
}

impl<T: ByteRepr + StaticTyped + Default> FromIterator<Option<T>> for Attr {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let iter_size = match iter.size_hint() {
            (_, Some(hb)) => hb.max(64),
            _ => 64,
        };
        let mut validity = Bitmap::zeroes(iter_size);
        let (mut validity_u64s, _) = validity.u64s_mut();
        let mut data = Array::new_owned::<T>(iter_size);
        let mut data_slice = data.cast_slice_mut::<T>(iter_size).unwrap();

        let mut len = 0usize;
        let mut buffer = Vec::with_capacity(64);
        let mut bitmask = 1u64;
        let mut word = 0u64;
        for item in iter {
            if let Some(v) = item {
                word |= bitmask;
                buffer.push(v);
            } else {
                buffer.push(T::default());
            }
            len += 1;
            if len & 63 == 0 {
                // add to validity
                let vidx = len / 64 - 1;
                if vidx == validity_u64s.len() {
                    validity_u64s = validity.reserve_u64s(vidx * 2 * 64);
                }
                validity_u64s[vidx] = word;
                word = 0;
                bitmask = 1;
                // add to data
                if len > data_slice.len() {
                    let new_len = len.max(data_slice.len() * 2);
                    data_slice = data.cast_slice_mut::<T>(new_len).unwrap();
                }
                data_slice[len - 64..len].clone_from_slice(&buffer);
                buffer.clear();
            } else {
                bitmask <<= 1;
            }
        }
        if !buffer.is_empty() {
            // add to validity
            let vidx = len / 64;
            if vidx == validity_u64s.len() {
                validity_u64s = validity.reserve_u64s(vidx * 2 * 64);
            }
            validity_u64s[vidx] = word;
            // add to data
            if len > data_slice.len() {
                let new_len = len.max(data_slice.len() * 2);
                data_slice = data.cast_slice_mut::<T>(new_len).unwrap();
            }
            data_slice[len & !63..len].clone_from_slice(&buffer);
            buffer.clear();
        }
        // update length
        unsafe { data.set_len(len) };
        unsafe { validity.set_len(len) };
        Attr::new_array(T::static_pty(), data, Sel::from(validity), None)
    }
}

impl<T, I> From<I> for Attr
where
    T: ByteRepr + StaticTyped,
    I: ExactSizeIterator<Item = T>,
{
    #[inline]
    fn from(src: I) -> Self {
        let len = src.len();
        assert!(len <= u16::MAX as usize);
        let mut data = Array::new_owned::<T>(len);
        let data_slice = data.cast_slice_mut::<T>(len).unwrap();
        for (t, s) in data_slice.iter_mut().zip(src) {
            *t = s;
        }
        unsafe { data.set_len(len) };
        Attr::new_array(T::static_pty(), data, Sel::All(len as u16), None)
    }
}

/// Attribute header:
/// 1. Format Descriptor: 8B.
/// 2. Offset data: 8B.
/// 3. Offset validity: 8B.
/// 4. Offset SMA: 8B.
/// 5. Offset dict: 8B. 0 means there is no dict.
/// 6. Offset string: 8B. 0 means there is no string.
///
/// All offsets listed above start from first byte this block.
/// Attribute header has length 48B in total.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SerAttrHeader {
    pub format_desc: SerFormatDesc, // 0..8
    pub offset_valid: usize,        // 8..16
    pub offset_data: usize,         // 16..24
    pub offset_sma: usize,          // 24..32
    pub offset_dict: usize,         // 32..40
    pub offset_str: usize,          // 40..48
}

impl SerAttrHeader {
    /// Returns start offset of payload.
    #[inline]
    pub fn start_offset(&self) -> usize {
        self.offset_valid
    }

    /// Write the header in byte format.
    #[inline]
    pub fn store<W: io::Write>(&self, writer: &mut W) -> Result<usize> {
        let mut n = 0;
        n += self.format_desc.store(writer)?;
        writer.write_all(&(self.offset_valid as u64).to_ne_bytes())?;
        writer.write_all(&(self.offset_data as u64).to_ne_bytes())?;
        writer.write_all(&(self.offset_sma as u64).to_ne_bytes())?;
        writer.write_all(&(self.offset_dict as u64).to_ne_bytes())?;
        writer.write_all(&(self.offset_str as u64).to_ne_bytes())?;
        n += 8 * 5;
        Ok(n)
    }
}

impl TryFrom<&[u8]> for SerAttrHeader {
    type Error = Error;
    #[inline]
    fn try_from(src: &[u8]) -> Result<Self> {
        let format_desc =
            SerFormatDesc::try_from(&src[ATTR_HDR_OFFSET_START_FMT..ATTR_HDR_OFFSET_END_FMT])?;
        // validity
        let offset_valid = u64::from_ne_bytes(
            src[ATTR_HDR_OFFSET_START_VALID..ATTR_HDR_OFFSET_END_VALID].try_into()?,
        ) as usize;
        // data
        let offset_data = u64::from_ne_bytes(
            src[ATTR_HDR_OFFSET_START_DATA..ATTR_HDR_OFFSET_END_DATA].try_into()?,
        ) as usize;
        // sma
        let offset_sma =
            u64::from_ne_bytes(src[ATTR_HDR_OFFSET_START_SMA..ATTR_HDR_OFFSET_END_SMA].try_into()?)
                as usize;
        // dict
        let offset_dict = u64::from_ne_bytes(
            src[ATTR_HDR_OFFSET_START_DICT..ATTR_HDR_OFFSET_END_DICT].try_into()?,
        ) as usize;
        // str
        let offset_str =
            u64::from_ne_bytes(src[ATTR_HDR_OFFSET_START_STR..ATTR_HDR_OFFSET_END_STR].try_into()?)
                as usize;
        Ok(SerAttrHeader {
            format_desc,
            offset_valid,
            offset_sma,
            offset_dict,
            offset_data,
            offset_str,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SerFormatDesc {
    pub ty: PreciseType,
    pub method: SerMethod,
    pub fields: SerFields,
}

impl SerFormatDesc {
    /// store the format descriptor in byte format.
    #[inline]
    pub fn store<W: io::Write>(&self, writer: &mut W) -> Result<usize> {
        let mut n = 0;
        n += self.ty.write_to(writer)?;
        writer.write_all(&[self.method as u8])?;
        writer.write_all(&[self.fields.bits()])?;
        writer.write_all(&[0u8; 2])?;
        n += 4;
        Ok(n)
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SerMethod {
    Single = 0,
    Array = 1,
    // Trunc1B = 2,
    // Trunc2B = 3,
    // Trunc4B = 4,
    // Dict1B = 5,
    // Dict2B = 6,
    // Dict4B = 7,
}

impl TryFrom<&[u8]> for SerFormatDesc {
    type Error = Error;
    #[inline]
    fn try_from(src: &[u8]) -> Result<Self> {
        if src.len() < 6 {
            return Err(Error::InvalidFormat);
        }
        let ty = PreciseType::try_from(&src[..4]).map_err(|_| Error::InvalidFormat)?;
        let method = match src[4] {
            0 => SerMethod::Single,
            1 => SerMethod::Array,
            _ => return Err(Error::InvalidFormat),
        };
        let fields = SerFields::from_bits(src[5]).ok_or(Error::InvalidFormat)?;
        Ok(SerFormatDesc { ty, method, fields })
    }
}

bitflags! {
    pub struct SerFields: u8 {
        const VALID = 0x01;
        const SMA = 0x02;
        const DICT = 0x04;
        const STR = 0x08;
    }
}

#[repr(u8)]
pub enum SerValidType {
    All = 1,
    None = 2,
    Index = 3,
    Bitmap = 4,
}

impl TryFrom<u8> for SerValidType {
    type Error = Error;
    #[inline]
    fn try_from(src: u8) -> Result<Self> {
        let res = match src {
            1 => SerValidType::All,
            2 => SerValidType::None,
            3 => SerValidType::Index,
            4 => SerValidType::Bitmap,
            _ => return Err(Error::InvalidFormat),
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_codec_from_iter() {
        let attr = Attr::from_iter(vec![Some(1i32), None, Some(3)]);
        let bm = &attr.validity;
        assert!(bm.selected(0).unwrap());
        assert!(!bm.selected(1).unwrap());
        assert!(bm.selected(2).unwrap());
        assert_eq!(
            &[1, 0, 3],
            attr.codec.as_array().unwrap().cast_slice::<i32>()
        );
    }

    #[test]
    fn test_attr_single_make_header() {
        let single = Single::new(1i32, 1024);
        let attr = Attr::new_single(PreciseType::i32(), single, Sel::All(1024));
        let offset = 64;
        let (header, new_offset) = attr.ser_header(offset);
        assert_eq!(
            SerFormatDesc {
                ty: attr.ty,
                method: SerMethod::Single,
                fields: SerFields::VALID
            },
            header.format_desc
        );
        assert_eq!(64, header.offset_valid);
        assert_eq!(64 + align_u128(1), header.offset_data);
        assert_eq!(64 + align_u128(1) + 16, new_offset);
    }

    #[test]
    fn test_attr_array_non_valid_make_header() {
        let attr = Attr::from(0..1024i32);
        let offset = 64;
        let (header, new_offset) = attr.ser_header(offset);
        assert_eq!(
            SerFormatDesc {
                ty: attr.ty,
                method: SerMethod::Array,
                fields: SerFields::VALID,
            },
            header.format_desc
        );
        assert_eq!(64, header.offset_valid);
        assert_eq!(64 + align_u128(1), header.offset_data);
        assert_eq!(64 + align_u128(1) + 1024 * 4, new_offset);
    }

    #[test]
    fn test_attr_array_valid_make_header() {
        let attr = Attr::from_iter((0..1024i32).map(|i| Some(i)));
        let offset = 64;
        let (header, new_offset) = attr.ser_header(offset);
        assert_eq!(
            SerFormatDesc {
                ty: attr.ty,
                method: SerMethod::Array,
                fields: SerFields::VALID
            },
            header.format_desc
        );
        assert_eq!(64, header.offset_valid);
        assert_eq!(
            64 + align_u128(1) + align_u128(1024 / 8),
            header.offset_data
        );
        assert_eq!(
            64 + align_u128(1) + align_u128(1024 / 8) + 1024 * 4,
            new_offset
        );
    }

    #[test]
    fn test_attr_ser_header() {
        use std::io::Cursor;
        // single codec header
        let single = Single::new(1i32, 1024);
        let attr = Attr::new_single(PreciseType::i32(), single, Sel::All(1024));
        let offset = 64;
        let (header, _) = attr.ser_header(offset);
        let mut bs: Vec<u8> = vec![];
        let mut cursor = Cursor::new(&mut bs);
        header.store(&mut cursor).unwrap();
        let new_header = SerAttrHeader::try_from(&bs[..]).unwrap();
        assert_eq!(header, new_header);
        // array codec header
        let attr = Attr::from_iter((0..1024i32).map(|i| Some(i)));
        let offset = 64;
        let (header, _) = attr.ser_header(offset);
        let mut bs: Vec<u8> = vec![];
        let mut cursor = Cursor::new(&mut bs);
        header.store(&mut cursor).unwrap();
        let new_header = SerAttrHeader::try_from(&bs[..]).unwrap();
        assert_eq!(header, new_header);
    }

    #[test]
    fn test_attr_single_store_and_load() {
        use std::io::Cursor;
        let single = Single::new(1i32, 1024);
        let mut attr = Attr::new_single(PreciseType::i32(), single, Sel::All(1024));
        attr.setup_sma(); // no-op
        let mut bs: Vec<u8> = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(&mut bs);
        let mut buf = vec![];
        let (header, total_bytes) = attr.ser_header(0);
        let written = attr.store(&mut cursor, &mut buf, total_bytes).unwrap();
        assert_eq!(written, total_bytes);
        let raw: Arc<[u8]> = Arc::from(bs.into_boxed_slice());
        let new_attr = Attr::load(&raw, 1024, &header).unwrap();
        assert_eq!(attr.ty, new_attr.ty);
        assert!(new_attr.validity.is_all());
        let value = new_attr.codec.as_single().unwrap().view::<i32>();
        assert!(value == 1);
    }

    #[test]
    fn test_attr_array_store_and_load() {
        use std::io::Cursor;
        let nums: Vec<i32> = (0i32..1024).collect();
        let attr = Attr::from(nums.clone().into_iter());
        let mut bs: Vec<u8> = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(&mut bs);
        let mut buf = vec![];
        let (header, total_bytes) = attr.ser_header(0);
        let written = attr.store(&mut cursor, &mut buf, total_bytes).unwrap();
        assert_eq!(written, total_bytes);
        let raw: Arc<[u8]> = Arc::from(bs.into_boxed_slice());
        let new_attr = Attr::load(&raw, 1024, &header).unwrap();
        assert_eq!(attr.ty, new_attr.ty);
        assert!(new_attr.validity.is_all());
        let vals = new_attr.codec.as_array().unwrap().cast_slice::<i32>();
        assert_eq!(&nums, vals);
    }

    #[test]
    fn test_attr_sma_store_and_load() {
        use std::io::Cursor;
        let nums: Vec<i32> = (0i32..1024).collect();
        let mut attr = Attr::from(nums.clone().into_iter());
        // setup sma
        attr.setup_sma();
        assert!(attr.sma.is_some());
        let mut bs: Vec<u8> = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(&mut bs);
        let mut buf = vec![];
        let (header, total_bytes) = attr.ser_header(0);
        let written = attr.store(&mut cursor, &mut buf, total_bytes).unwrap();
        assert_eq!(written, total_bytes);
        let raw: Arc<[u8]> = Arc::from(bs.into_boxed_slice());
        let new_attr = Attr::load(&raw, 1024, &header).unwrap();
        assert_eq!(attr.ty, new_attr.ty);
        assert!(new_attr.validity.is_all());
        assert!(new_attr.sma.is_some());
        let vals = new_attr.codec.as_array().unwrap().cast_slice::<i32>();
        assert_eq!(&nums, vals);
        // compare sma tables
        let sma_tbl = attr.sma.as_ref().map(|sma| sma.pos_tbl()).unwrap();
        let new_sma_tbl = new_attr.sma.as_ref().map(|sma| sma.pos_tbl()).unwrap();
        assert_eq!(sma_tbl, new_sma_tbl);
        // array with validity and sma
        let array = Array::from(nums.into_iter());
        let mut attr = Attr::new_array(
            PreciseType::i32(),
            array,
            Sel::new_indexes(1024, vec![3, 4, 5, 1021, 1022]),
            None,
        );
        attr.setup_sma();
        let mut bs: Vec<u8> = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(&mut bs);
        let mut buf = vec![];
        let (header, total_bytes) = attr.ser_header(0);
        let written = attr.store(&mut cursor, &mut buf, total_bytes).unwrap();
        assert_eq!(written, total_bytes);
        let written = attr.store(&mut cursor, &mut buf, total_bytes).unwrap();
        assert_eq!(written, total_bytes);
        let raw: Arc<[u8]> = Arc::from(bs.into_boxed_slice());
        let new_attr = Attr::load(&raw, 1024, &header).unwrap();
        assert_eq!(1024, new_attr.validity.n_records());
        assert_eq!(5, new_attr.validity.n_filtered());
        assert!(new_attr.is_valid(3).unwrap());
        assert!(!new_attr.is_valid(8).unwrap());
        assert!(new_attr.sma.is_some());
        if let Some(sma) = new_attr.sma {
            let max = sma.max_val::<i32>();
            assert_eq!(1022, max);
            let min = sma.min_val::<i32>();
            assert_eq!(3, min);
        }
    }

    #[test]
    fn test_attr_from_sel() {
        // from sel all
        let s = Sel::All(1024);
        let a = Attr::from(s);
        assert_eq!(1024, a.n_records());
        let (valid, val) = a.bool_at(0).unwrap();
        assert!(valid && val);
        // from sel none
        let s = Sel::None(1024);
        let a = Attr::from(s);
        assert_eq!(1024, a.n_records());
        let (valid, val) = a.bool_at(0).unwrap();
        assert!(valid && !val);
        // from sel index
        let s = Sel::new_indexes(1024, vec![1, 5, 10]);
        let a = Attr::from(s);
        assert_eq!(1024, a.n_records());
        let (valid, val) = a.bool_at(1).unwrap();
        assert!(valid && val);
        let (valid, val) = a.bool_at(2).unwrap();
        assert!(valid && !val);
        // from sel bitmap
        let s = Sel::Bitmap(Arc::new(Bitmap::zeroes(1024)));
        let a = Attr::from(s);
        assert_eq!(1024, a.n_records());
        let (valid, val) = a.bool_at(0).unwrap();
        assert!(valid && !val);
        // from sel bitmap borrowed
        let raw: Arc<[u8]> = Arc::from(vec![0u8; 128].into_boxed_slice());
        let bm = Bitmap::new_borrowed(raw, 128, 0);
        let a = Attr::from(Sel::Bitmap(Arc::new(bm)));
        assert_eq!(128, a.n_records());
        let (valid, val) = a.bool_at(4).unwrap();
        assert!(valid && !val);
    }
}
