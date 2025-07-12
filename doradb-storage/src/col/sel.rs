use crate::col::array::Array;
use crate::col::attr::Attr;
use crate::col::bitmap::Bitmap;
use crate::col::codec::{Codec, Single};
use crate::error::{Error, Result};
use doradb_datatype::PreciseType;
use smallvec::SmallVec;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Sel encodes filter indexes into bitmap, single or none.
#[derive(Debug)]
pub enum Sel {
    /// bitmap of which returned value marked as 1.
    Bitmap(Arc<Bitmap>),
    /// Index stores individual indexes of selected values.
    Index {
        count: u8,
        len: u16,
        indexes: [u16; 6], // at most 6 values
    },
    /// no value returned.
    None(u16),
    /// all values returned.
    All(u16),
}

impl Sel {
    /// Create a new selection with given selected indexes.
    #[inline]
    pub fn new_indexes(len: u16, sel: Vec<u16>) -> Sel {
        assert!(!sel.is_empty() && sel.len() <= 6);
        let mut indexes = [0u16; 6];
        indexes[..sel.len()].copy_from_slice(&sel);
        let count = sel.len() as u8;
        Sel::Index {
            count,
            len,
            indexes,
        }
    }

    /// Returns total number of records.
    #[inline]
    pub fn n_records(&self) -> usize {
        match self {
            Sel::Bitmap(b) => b.len(),
            Sel::Index { len, .. } | Sel::None(len) | Sel::All(len) => *len as usize,
        }
    }

    /// Returns whether selection is all.
    #[inline]
    pub fn is_all(&self) -> bool {
        matches!(self, Sel::All(_))
    }

    /// Returns whether selection is none.
    #[inline]
    pub fn is_none(&self) -> bool {
        matches!(self, Sel::None(_))
    }

    /// Returns number of filtered records.
    #[inline]
    pub fn n_filtered(&self) -> usize {
        match self {
            Sel::Bitmap(b) => b.true_count(),
            Sel::Index { count, .. } => *count as usize,
            Sel::None(_) => 0,
            Sel::All(len) => *len as usize,
        }
    }

    /// Returns if given index is selected.
    #[inline]
    pub fn selected(&self, idx: usize) -> Result<bool> {
        if idx >= self.n_records() {
            return Err(Error::IndexOutOfBound);
        }
        match self {
            Sel::All(_) => Ok(true),
            Sel::None(_) => Ok(false),
            Sel::Bitmap(b) => b.get(idx),
            Sel::Index { count, indexes, .. } => Ok(indexes[..*count as usize]
                .iter()
                .any(|i| idx == *i as usize)),
        }
    }

    /// Intersects another selection.
    #[inline]
    pub fn intersect(&self, that: &Sel) -> Result<Sel> {
        if self.n_records() != that.n_records() {
            return Err(Error::ValueCountMismatch);
        }
        let res = match (self, that) {
            (_, Sel::Bitmap(bm)) => return self.intersect_bm(bm),
            (Sel::All(_), other) => other.clone_to_owned(),
            (other, Sel::All(_)) => other.clone_to_owned(),
            (Sel::None(len), _) => Sel::None(*len),
            (_, Sel::None(len)) => Sel::None(*len),
            (
                Sel::Index {
                    count: c1,
                    indexes: i1,
                    len,
                },
                Sel::Index {
                    count: c2,
                    indexes: i2,
                    ..
                },
            ) => {
                let mut indexes = [0u16; 6];
                let mut count = 0;
                let idx1 = &i1[..*c1 as usize];
                let idx2 = &i2[..*c2 as usize];
                for i in idx1 {
                    if idx2.contains(i) {
                        indexes[count] = *i;
                        count += 1;
                    }
                }
                if count == 0 {
                    Sel::None(*len)
                } else {
                    Sel::Index {
                        count: count as u8,
                        len: *len,
                        indexes,
                    }
                }
            }
            (Sel::Bitmap(bm), Sel::Index { count, indexes, .. }) => {
                intersect_index_bitmap(&indexes[..*count as usize], bm)?
            }
        };
        Ok(res)
    }

    /// Intersect given bitmap.
    #[inline]
    pub fn intersect_bm(&self, bm: &Bitmap) -> Result<Sel> {
        let res = match self {
            Sel::All(_) => Sel::Bitmap(Arc::new(bm.to_owned())),
            Sel::None(_) => Sel::None(bm.len() as u16),
            Sel::Index { count, indexes, .. } => {
                intersect_index_bitmap(&indexes[..*count as usize], bm)?
            }
            Sel::Bitmap(bm1) => {
                let mut res = bm1.as_ref().to_owned();
                res.intersect(bm)?;
                Sel::from(res)
            }
        };
        Ok(res)
    }

    /// Union given selection.
    #[inline]
    pub fn union(&self, that: &Sel) -> Result<Sel> {
        if self.n_records() != that.n_records() {
            return Err(Error::ValueCountMismatch);
        }
        let res = match (self, that) {
            (_, Sel::Bitmap(bm)) => return self.union_bm(bm),
            (Sel::All(len), _) | (_, Sel::All(len)) => Sel::All(*len),
            (Sel::None(_), other) | (other, Sel::None(_)) => other.clone_to_owned(),
            (Sel::Bitmap(bm), Sel::Index { count, indexes, .. }) => {
                let mut bm = bm.as_ref().to_owned();
                for idx in &indexes[..*count as usize] {
                    bm.set(*idx as usize, true)?;
                }
                Sel::from(bm)
            }
            (
                Sel::Index {
                    count: c1,
                    len,
                    indexes: i1,
                },
                Sel::Index {
                    count: c2,
                    indexes: i2,
                    ..
                },
            ) => {
                let mut set = BTreeSet::new();
                for i in &i1[..*c1 as usize] {
                    set.insert(*i);
                }
                for i in &i2[..*c2 as usize] {
                    set.insert(*i);
                }
                if set.len() <= 6 {
                    let mut indexes = [0u16; 6];
                    let mut count = 0;
                    for i in set {
                        indexes[count] = i;
                        count += 1;
                    }
                    Sel::Index {
                        count: count as u8,
                        len: *len,
                        indexes,
                    }
                } else {
                    let mut bm = Bitmap::zeroes(*len as usize);
                    for i in set {
                        bm.set(i as usize, true)?;
                    }
                    // must be bitmap
                    Sel::Bitmap(Arc::new(bm))
                }
            }
        };
        Ok(res)
    }

    /// Union given bitmap.
    #[inline]
    pub fn union_bm(&self, bm: &Bitmap) -> Result<Sel> {
        let res = match self {
            Sel::All(_) => Sel::All(bm.len() as u16),
            Sel::None(_) => Sel::Bitmap(Arc::new(bm.to_owned())),
            Sel::Index { count, indexes, .. } => {
                let mut bm = bm.to_owned();
                for idx in &indexes[..*count as usize] {
                    bm.set(*idx as usize, true)?;
                }
                Sel::from(bm)
            }
            Sel::Bitmap(bm1) => {
                let mut res = bm1.as_ref().to_owned();
                res.union(bm)?;
                Sel::from(res)
            }
        };
        Ok(res)
    }

    /// Returns inverse of current selection.
    #[inline]
    pub fn inverse(&self) -> Sel {
        match self {
            Sel::All(len) => Sel::None(*len),
            Sel::None(len) => Sel::All(*len),
            Sel::Bitmap(b) => {
                let mut b = b.as_ref().to_owned();
                b.inverse();
                Sel::from(b)
            }
            Sel::Index {
                count,
                len,
                indexes,
            } => {
                let mut bm = Bitmap::ones(*len as usize);
                for idx in &indexes[..*count as usize] {
                    bm.set(*idx as usize, false).unwrap();
                }
                Sel::Bitmap(Arc::new(bm))
            }
        }
    }

    /// Apply selection to given attribute.
    /// It generates a new attribute with only filtered values.
    #[inline]
    pub fn apply(&self, attr: &Attr) -> Result<Attr> {
        debug_assert_eq!(self.n_records(), attr.n_records());
        let res = match self {
            Sel::Bitmap(sel) => {
                let n_filtered = sel.true_count();
                match n_filtered {
                    0 => Attr::empty(attr.ty),
                    1 => {
                        let idx = sel.first_true().unwrap(); // won't fail
                        sel_one(attr, idx)?
                    }
                    n if n == attr.n_records() => {
                        // retain all rows, just copy
                        attr.to_owned()
                    }
                    _ => {
                        // apply data
                        match &attr.codec {
                            Codec::Empty => unreachable!(),
                            Codec::Single(s) => {
                                let validity =
                                    validity_with_sel_bitmap(&attr.validity, sel, n_filtered)?;
                                if validity.is_none() {
                                    Attr::new_null(attr.ty, n_filtered as u16)
                                } else {
                                    Attr::new_single(
                                        attr.ty,
                                        Single::new_raw(s.data.clone(), n_filtered as u16),
                                        validity,
                                    )
                                }
                            }
                            Codec::Bitmap(b) => {
                                debug_assert_eq!(PreciseType::bool(), attr.ty);
                                // extend data
                                let mut data = Bitmap::with_capacity(n_filtered);
                                let mut idx = 0;
                                for (flag, n) in sel.range_iter() {
                                    if flag {
                                        data.extend_range(b, idx..idx + n)?;
                                    }
                                    idx += n;
                                }
                                debug_assert_eq!(data.len(), n_filtered);
                                // extend validity
                                let validity =
                                    validity_with_sel_bitmap(&attr.validity, sel, n_filtered)?;
                                Attr::new_bitmap(data, validity)
                            }
                            Codec::Array(a) => {
                                let val_len = attr.ty.val_len().unwrap(); // won't fail
                                let mut data = Array::with_capacity(n_filtered * val_len);
                                let data_raw = data.raw_mut().unwrap(); // won't fail
                                let src_raw = a.raw();
                                let mut src_idx = 0;
                                let mut tgt_offset = 0;
                                for (flag, n) in sel.range_iter() {
                                    if flag {
                                        let src_offset = src_idx * val_len;
                                        let raw_len = n * val_len;
                                        data_raw[tgt_offset..tgt_offset + raw_len].copy_from_slice(
                                            &src_raw[src_offset..src_offset + raw_len],
                                        );
                                        tgt_offset += raw_len;
                                    }
                                    src_idx += n;
                                }
                                debug_assert_eq!(n_filtered * val_len, tgt_offset);
                                // SAFETY
                                //
                                // length is guaranteed to be number of filtered elements.
                                unsafe { data.set_len(n_filtered) };
                                // apply validity
                                let validity =
                                    validity_with_sel_bitmap(&attr.validity, sel, n_filtered)?;
                                Attr::new_array(attr.ty, data, validity, None)
                            }
                        }
                    }
                }
            }
            Sel::Index { count, indexes, .. } => {
                if *count == 1 {
                    sel_one(attr, indexes[0] as usize)?
                } else {
                    sel_indexes(attr, &indexes[..*count as usize])?
                }
            }
            Sel::None { .. } => Attr::empty(attr.ty),
            Sel::All { .. } => attr.to_owned(),
        };
        Ok(res)
    }

    /// Clone to owned with atomic reference.
    #[inline]
    pub fn clone_to_owned(&self) -> Self {
        match self {
            Sel::Bitmap(bm) => Sel::Bitmap(Bitmap::clone_to_owned(bm)),
            Sel::All(len) => Sel::All(*len),
            Sel::Index {
                count,
                len,
                indexes,
            } => Sel::Index {
                count: *count,
                len: *len,
                indexes: *indexes,
            },
            Sel::None(len) => Sel::None(*len),
        }
    }
}

#[inline]
fn intersect_index_bitmap(idx: &[u16], bm: &Bitmap) -> Result<Sel> {
    let mut indexes = [0u16; 6];
    let mut count = 0;
    for i in idx {
        if bm.get(*i as usize)? {
            indexes[count] = *i;
            count += 1;
        }
    }
    let res = if count == 0 {
        Sel::None(bm.len() as u16)
    } else {
        Sel::Index {
            count: count as u8,
            len: bm.len() as u16,
            indexes,
        }
    };
    Ok(res)
}

impl From<Bitmap> for Sel {
    #[inline]
    fn from(bm: Bitmap) -> Self {
        if let Some(res) = compact_bitmap(&bm) {
            res
        } else {
            Sel::Bitmap(Arc::new(bm))
        }
    }
}

/// Compaction of bitmap handle three special cases:
/// 1. all falses => Sel::None
/// 2. single true => Sel::Single
/// 3. all trues => Sel::All
#[inline]
pub fn compact_bitmap(bm: &Bitmap) -> Option<Sel> {
    let mut new_count = 0;
    let mut new_indexes = [0u16; 6];
    let mut idx = 0;
    'INDEX: for (flag, n) in bm.range_iter() {
        let n = n as u16;
        if flag {
            for i in idx..idx + n {
                if new_count == 6 {
                    new_count += 1;
                    break 'INDEX;
                }
                new_indexes[new_count] = i;
                new_count += 1;
            }
        }
        idx += n;
    }
    match new_count {
        0 => Some(Sel::None(bm.len() as u16)),
        1..=6 => Some(Sel::Index {
            count: new_count as u8,
            len: bm.len() as u16,
            indexes: new_indexes,
        }),
        _ => None,
    }
}

/// Build Selection from attribute.
/// The attribute type must be bool.
/// The conversion will treat null as false.
#[inline]
pub fn null_as_false(attr: &Attr) -> Result<Sel> {
    debug_assert_eq!(PreciseType::bool(), attr.ty);
    // Only bitmap and single could be converted to selection.
    let res = match &attr.codec {
        Codec::Single(s) => match &attr.validity {
            Sel::All(_) => {
                let flag = s.view::<u8>();
                if flag == 0 {
                    Sel::None(s.len)
                } else {
                    Sel::All(s.len)
                }
            }
            // here we treat null as false.
            Sel::None(_) => Sel::None(s.len),
            Sel::Index { .. } | Sel::Bitmap(_) => {
                let flag = s.view::<u8>();
                if flag == 0 {
                    Sel::None(s.len)
                } else {
                    attr.validity.clone_to_owned()
                }
            }
        },
        Codec::Bitmap(bm) => match &attr.validity {
            Sel::Bitmap(validity) => {
                let mut bm = bm.as_ref().to_owned();
                bm.intersect(validity)?;
                Sel::from(bm)
            }
            Sel::All(_) => {
                if let Some(bm) = compact_bitmap(bm) {
                    bm
                } else {
                    Sel::Bitmap(Bitmap::clone_to_owned(bm))
                }
            }
            Sel::None(_) => Sel::None(attr.n_records() as u16),
            Sel::Index { count, indexes, .. } => {
                let mut new_indexes = [0u16; 6];
                let mut new_count = 0;
                for vidx in &indexes[..*count as usize] {
                    let vidx = *vidx as usize;
                    if bm.get(vidx)? {
                        new_indexes[new_count] = vidx as u16;
                        new_count += 1;
                    }
                }
                if new_count == 0 {
                    Sel::None(attr.n_records() as u16)
                } else {
                    Sel::Index {
                        count: new_count as u8,
                        len: attr.n_records() as u16,
                        indexes: new_indexes,
                    }
                }
            }
        },
        _ => return Err(Error::InvalidCodecForSel),
    };
    Ok(res)
}

#[inline]
fn validity_with_sel_bitmap(validity: &Sel, sel: &Bitmap, n_filtered: usize) -> Result<Sel> {
    let res = match &validity {
        Sel::Bitmap(vm) => {
            let mut tmp = Bitmap::with_capacity(n_filtered);
            let mut idx = 0;
            for (flag, n) in sel.range_iter() {
                if flag {
                    tmp.extend_range(vm, idx..idx + n)?;
                }
                idx += n;
            }
            debug_assert_eq!(tmp.len(), n_filtered);
            Sel::Bitmap(Arc::new(tmp))
        }
        Sel::All(_) => Sel::All(n_filtered as u16),
        Sel::None(_) => Sel::None(n_filtered as u16),
        Sel::Index { count, indexes, .. } => {
            // we will have no more than `count` valid values,
            // result validity must be Sel::Index
            let mut valids = [0u16; 6];
            let mut valid_count = 0;
            let mut cands: SmallVec<[u16; 6]> =
                indexes[..*count as usize].iter().rev().cloned().collect();
            let mut idx = 0;
            let mut sel_idx = 0;
            'OUTER: for (flag, n) in sel.range_iter() {
                loop {
                    if let Some(vidx) = cands.last() {
                        let vidx = *vidx as usize;
                        if vidx < idx {
                            cands.pop().unwrap();
                        } else if vidx < idx + n {
                            // within range
                            if flag {
                                valids[valid_count] = (vidx - idx + sel_idx) as u16;
                                valid_count += 1;
                            }
                            cands.pop().unwrap();
                        } else {
                            // larger than current range
                            break;
                        }
                    } else {
                        // all candidates are visited, break outer loop
                        break 'OUTER;
                    }
                }
                if flag {
                    sel_idx += n;
                }
                idx += n;
            }
            // check valid count
            if valid_count == 0 {
                Sel::None(n_filtered as u16)
            } else {
                Sel::Index {
                    count: valid_count as u8,
                    len: n_filtered as u16,
                    indexes: valids,
                }
            }
        }
    };
    Ok(res)
}

#[inline]
fn sel_one(attr: &Attr, sel: usize) -> Result<Attr> {
    let (valid, raw_val) = attr.val_at(sel)?;
    let res = if valid {
        let mut data: SmallVec<_> = SmallVec::with_capacity(raw_val.len());
        data.extend_from_slice(raw_val);
        Attr::new_single(attr.ty, Single::new_raw(data, 1), Sel::All(1))
    } else {
        Attr::new_null(attr.ty, 1)
    };
    Ok(res)
}

#[inline]
fn sel_indexes(attr: &Attr, sel: &[u16]) -> Result<Attr> {
    // bools should be handled specially
    let res = if attr.ty == PreciseType::bool() {
        let mut bm = Bitmap::zeroes(sel.len());
        let mut validity = Bitmap::with_capacity(sel.len());
        let orig = attr.codec.as_bitmap().unwrap();
        for (idx, i) in sel.iter().enumerate() {
            let i = *i as usize;
            let valid = attr.validity.selected(i)?;
            if valid {
                bm.set(idx, orig.get(i)?)?;
            }
            validity.add(valid);
        }
        Attr::new_bitmap(bm, Sel::from(validity))
    } else {
        let val_len = attr.ty.val_len().unwrap();
        let mut arr = Array::with_capacity(sel.len());
        let arr_raw = arr.raw_mut().unwrap();
        let mut validity = Bitmap::with_capacity(sel.len());
        for (idx, i) in sel.iter().enumerate() {
            let i = *i as usize;
            let (valid, value) = attr.val_at(i)?;
            if valid {
                arr_raw[idx * val_len..idx * val_len + val_len].copy_from_slice(value);
                validity.add(true);
            } else {
                validity.add(false);
            }
        }
        Attr::new_array(attr.ty, arr, Sel::from(validity), None)
    };
    Ok(res)
}

impl TryFrom<&Attr> for Sel {
    type Error = Error;
    #[inline]
    fn try_from(src: &Attr) -> Result<Self> {
        if src.ty != PreciseType::bool() {
            return Err(Error::InvalidDatatype);
        }
        null_as_false(src) // by default, treat null as false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sel_apply_single() {
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(3, true).unwrap();
        bm.set(5, true).unwrap();
        bm.set(6, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let attr = Attr::new_single(PreciseType::i32(), Single::new(10i32, 1024), Sel::All(1024));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(3, res.n_records());
        let attr = Attr::new_null(PreciseType::i32(), 1024);
        let res = sel.apply(&attr).unwrap();
        assert_eq!(3, res.n_records());
        assert!(res.validity.is_none());
    }

    #[test]
    fn test_sel_apply_array() {
        let attr = Attr::from(0i32..1024);
        // none
        let sel = Sel::None(1024);
        let res = sel.apply(&attr).unwrap();
        assert_eq!(0, res.n_records());
        // single
        let sel = Sel::Index {
            count: 1,
            len: 1024,
            indexes: [1, 0, 0, 0, 0, 0],
        };
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1, res.n_records());
        assert_eq!(1, res.codec.as_single().unwrap().view::<i32>());
        // single null
        let sel = Sel::Index {
            count: 1,
            len: 1024,
            indexes: [1, 0, 0, 0, 0, 0],
        };
        let mut bm = Bitmap::from_iter(vec![true; 1024]);
        bm.set(1, false).unwrap();
        let attr2 = Attr::new_array(
            PreciseType::i32(),
            Array::from(0i32..1024),
            Sel::Bitmap(Arc::new(bm)),
            None,
        );
        let res = sel.apply(&attr2).unwrap();
        assert_eq!(1, res.n_records());
        assert!(res.validity.is_none());
        // array 0
        let bm = Bitmap::from_iter(vec![false; 1024]);
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(0, res.n_records());
        // array 1
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(10, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1, res.n_records());
        // array 3
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(3, true).unwrap();
        bm.set(5, true).unwrap();
        bm.set(6, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(3, res.n_records());
        assert_eq!(
            &[3i32, 5, 6],
            res.codec.as_array().unwrap().cast_slice::<i32>()
        );
        // array all
        let bm = Bitmap::from_iter(vec![true; 1024]);
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1024, res.n_records());
    }

    #[test]
    fn test_sel_apply_bitmap() {
        let attr = Attr::new_bitmap(Bitmap::from_iter(vec![true; 1024]), Sel::All(1024));
        // none
        let sel = Sel::None(1024);
        let res = sel.apply(&attr).unwrap();
        assert_eq!(0, res.n_records());
        // single
        let sel = Sel::Index {
            count: 1,
            len: 1024,
            indexes: [1, 0, 0, 0, 0, 0],
        };
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1, res.n_records());
        assert_eq!(1, res.codec.as_single().unwrap().view::<u8>());
        // array 0
        let bm = Bitmap::from_iter(vec![false; 1024]);
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(0, res.n_records());
        // array 1
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(10, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1, res.n_records());
        // array 3
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(3, true).unwrap();
        bm.set(5, true).unwrap();
        bm.set(6, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(3, res.n_records());
        assert_eq!(
            vec![true, true, true],
            res.codec
                .as_bitmap()
                .unwrap()
                .bools()
                .collect::<Vec<bool>>()
        );
        // array with validity
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(3, true).unwrap();
        bm.set(5, true).unwrap();
        bm.set(6, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let mut vm = Bitmap::from_iter(vec![true; 1024]);
        vm.set(3, false).unwrap();
        let attr2 = Attr::new_bitmap(
            Bitmap::from_iter(vec![true; 1024]),
            Sel::Bitmap(Arc::new(vm)),
        );
        let res = sel.apply(&attr2).unwrap();
        assert_eq!(3, res.n_records());
        let (valid, _) = res.val_at(0).unwrap();
        assert!(!valid);
    }

    #[test]
    fn test_sel_intersect() {
        let s1 = Sel::All(64);
        assert_eq!(64, s1.n_filtered());
        let s2 = Sel::None(64);
        assert_eq!(0, s2.n_filtered());
        let s3 = Sel::Index {
            count: 5,
            len: 64,
            indexes: [0, 1, 2, 4, 5, 0],
        };
        let mut bm = Bitmap::ones(64);
        for i in [0, 1, 2] {
            bm.set(i, false).unwrap();
        }
        let s4 = Sel::Bitmap(Arc::new(bm));
        let mut bm = Bitmap::ones(64);
        for i in [1, 10, 50] {
            bm.set(i, false).unwrap();
        }
        let s5 = Sel::Bitmap(Arc::new(bm));
        // all vs all
        let res = s1.intersect(&s1).unwrap();
        assert!(res.is_all());
        // all vs none
        let res = s1.intersect(&s2).unwrap();
        assert!(res.is_none());
        // all vs index
        let res = s1.intersect(&s3).unwrap();
        assert_eq!(5, res.n_filtered());
        // all vs bitmap
        let res = s1.intersect(&s4).unwrap();
        assert_eq!(64 - 3, res.n_filtered());
        // none vs all
        let res = s2.intersect(&s1).unwrap();
        assert!(res.is_none());
        // none vs none
        let res = s2.intersect(&s2).unwrap();
        assert!(res.is_none());
        // none vs index
        let res = s2.intersect(&s3).unwrap();
        assert!(res.is_none());
        // none vs bitmap
        let res = s2.intersect(&s4).unwrap();
        assert!(res.is_none());
        // index vs all
        let res = s3.intersect(&s1).unwrap();
        assert_eq!(5, res.n_filtered());
        // index vs none
        let res = s3.intersect(&s2).unwrap();
        assert!(res.is_none());
        // index vs index
        let res = s3.intersect(&s3).unwrap();
        assert_eq!(5, res.n_filtered());
        // index vs bitmap
        let res = s3.intersect(&s4).unwrap();
        assert_eq!(2, res.n_filtered());
        // bitmap vs all
        let res = s4.intersect(&s1).unwrap();
        assert_eq!(64 - 3, res.n_filtered());
        // bitmap vs none
        let res = s4.intersect(&s2).unwrap();
        assert!(res.is_none());
        // bitmap vs index
        let res = s4.intersect(&s3).unwrap();
        assert_eq!(2, res.n_filtered());
        // bitmap vs bitmap
        let res = s4.intersect(&s4).unwrap();
        assert_eq!(64 - 3, res.n_filtered());
        let res = s4.intersect(&s5).unwrap();
        assert_eq!(64 - 5, res.n_filtered());
    }

    #[test]
    fn test_sel_union() {
        let s1 = Sel::All(64);
        let s2 = Sel::None(64);
        let s3 = Sel::Index {
            count: 5,
            len: 64,
            indexes: [0, 1, 2, 4, 5, 0],
        };
        let mut bm = Bitmap::ones(64);
        for i in [0, 10, 20] {
            bm.set(i, false).unwrap();
        }
        let s4 = Sel::Bitmap(Arc::new(bm));
        let mut bm = Bitmap::ones(64);
        for i in [1, 10, 50] {
            bm.set(i, false).unwrap();
        }
        let s5 = Sel::Bitmap(Arc::new(bm));
        let s6 = Sel::new_indexes(64, vec![7, 8, 9, 10]);
        // all vs all
        let res = s1.union(&s1).unwrap();
        assert!(res.is_all());
        // all vs none
        let res = s1.union(&s2).unwrap();
        assert!(res.is_all());
        // all vs index
        let res = s1.union(&s3).unwrap();
        assert!(res.is_all());
        // all vs bitmap
        let res = s1.union(&s4).unwrap();
        assert!(res.is_all());
        // none vs all
        let res = s2.union(&s1).unwrap();
        assert!(res.is_all());
        // none vs none
        let res = s2.union(&s2).unwrap();
        assert!(res.is_none());
        // none vs index
        let res = s2.union(&s3).unwrap();
        assert_eq!(5, res.n_filtered());
        // none vs bitmap
        let res = s2.union(&s4).unwrap();
        assert_eq!(64 - 3, res.n_filtered());
        // index vs all
        let res = s3.union(&s1).unwrap();
        assert!(res.is_all());
        // index vs none
        let res = s3.union(&s2).unwrap();
        assert_eq!(5, res.n_filtered());
        // index vs index
        let res = s3.union(&s3).unwrap();
        assert_eq!(5, res.n_filtered());
        let res = s3.union(&s6).unwrap();
        assert_eq!(9, res.n_filtered());
        // index vs bitmap
        let res = s3.union(&s4).unwrap();
        assert_eq!(64 - 2, res.n_filtered());
        // bitmap vs all
        let res = s4.union(&s1).unwrap();
        assert!(res.is_all());
        // bitmap vs none
        let res = s4.union(&s2).unwrap();
        assert_eq!(64 - 3, res.n_filtered());
        // bitmap vs index
        let res = s4.union(&s3).unwrap();
        assert_eq!(64 - 2, res.n_filtered());
        // bitmap vs bitmap
        let res = s4.union(&s4).unwrap();
        assert_eq!(64 - 3, res.n_filtered());
        let res = s4.union(&s5).unwrap();
        assert_eq!(64 - 1, res.n_filtered());
    }

    #[test]
    fn test_sel_inverse() {
        let s1 = Sel::All(64);
        let s2 = Sel::None(64);
        let s3 = Sel::new_indexes(64, vec![0, 1, 2, 4, 5]);
        let mut bm = Bitmap::ones(64);
        for i in [0, 10, 20] {
            bm.set(i, false).unwrap();
        }
        let s4 = Sel::Bitmap(Arc::new(bm));
        let res = s1.inverse();
        assert_eq!(0, res.n_filtered());
        let res = s2.inverse();
        assert_eq!(64, res.n_filtered());
        let res = s3.inverse();
        assert_eq!(64 - 5, res.n_filtered());
        let res = s4.inverse();
        assert_eq!(3, res.n_filtered());
    }

    #[test]
    fn test_sel_from_attr() {
        let attr = Attr::new_null(PreciseType::bool(), 64);
        let res = Sel::try_from(&attr).unwrap();
        assert_eq!(0, res.n_filtered());
        let attr = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(true, 64),
            Sel::All(64),
        );
        let res = Sel::try_from(&attr).unwrap();
        assert_eq!(64, res.n_filtered());
        let attr = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(true, 64),
            Sel::new_indexes(64, vec![0, 2, 3]),
        );
        let res = Sel::try_from(&attr).unwrap();
        assert_eq!(3, res.n_filtered());
        let mut bm = Bitmap::ones(64);
        bm.set(1, false).unwrap();
        let attr = Attr::new_bitmap(bm.to_owned(), Sel::All(64));
        let res = Sel::try_from(&attr).unwrap();
        assert_eq!(63, res.n_filtered());
        let mut sel = Bitmap::zeroes(64);
        for i in 0..10 {
            sel.set(i, true).unwrap();
        }
        let attr = Attr::new_bitmap(bm.to_owned(), Sel::Bitmap(Arc::new(sel)));
        let res = Sel::try_from(&attr).unwrap();
        assert_eq!(9, res.n_filtered());
        let attr = Attr::new_bitmap(Bitmap::ones(64), Sel::None(64));
        let res = Sel::try_from(&attr).unwrap();
        assert_eq!(0, res.n_filtered());
        let attr = Attr::new_bitmap(Bitmap::ones(64), Sel::new_indexes(64, vec![0, 1, 2]));
        let res = Sel::try_from(&attr).unwrap();
        assert_eq!(3, res.n_filtered());
    }
}
