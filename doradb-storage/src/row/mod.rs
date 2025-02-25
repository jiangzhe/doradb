pub mod ops;

use crate::buffer::frame::{BufferFrame, BufferFrameAware};
use crate::buffer::page::PAGE_SIZE;
use crate::buffer::BufferPool;
use crate::catalog::TableSchema;
use crate::row::ops::{Delete, InsertRow, Select, SelectKey, Update, UpdateCol};
use crate::trx::undo::UndoMap;
use crate::value::*;
use std::fmt;
use std::mem;
use std::slice;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

pub type RowID = u64;
pub const INVALID_ROW_ID: RowID = !0;

const _: () = assert!(
    { std::mem::size_of::<RowPageHeader>() % 8 == 0 },
    "RowPageHeader should have size align to 8 bytes"
);

/// RowPage is the core data structure of row-store.
/// It is designed to be fast in both TP and AP scenarios.
/// It follows design of PAX format.
///
/// Header:
///
/// | field                   | length(B) |
/// |-------------------------|-----------|
/// | start_row_id            | 8         |
/// | row_count               | 2         |
/// | var_field_offset        | 2         |
/// | max_row_count           | 2         |
/// | col_count               | 2         |
/// | del_bitset_offset       | 2         |
/// | null_bitset_list_offset | 2         |
/// | col_offset_list_offset  | 2         |
/// | fix_field_offset        | 2         |
/// | fix_field_end           | 2         |
/// | padding                 | 6         |
///
/// Data:
///
/// | field            | length(B)                                     |
/// |------------------|-----------------------------------------------|
/// | del_bitset       | (count + 63) / 64 * 8                         |
/// | null_bitmap_list | (col_count + 7) / 8 * count, align to 8 bytes |
/// | col_offset_list  | col_count * 2, align to 8 bytes               |
/// | c_0              | depends on column type, align to 8 bytes      |
/// | c_1              | same as above                                 |
/// | ...              | ...                                           |
/// | c_n              | same as above                                 |
/// | free_space       | free space                                    |
/// | var_len_data     | data of var-len column                        |
///
pub struct RowPage {
    pub header: RowPageHeader,
    pub data: [u8; PAGE_SIZE - mem::size_of::<RowPageHeader>()],
}

impl RowPage {
    /// Initialize row page.
    #[inline]
    pub fn init(&mut self, start_row_id: u64, max_row_count: usize, schema: &TableSchema) {
        debug_assert!(max_row_count <= 0xffff);
        self.header.start_row_id = start_row_id;
        self.header.max_row_count = max_row_count as u16;
        self.header
            .store_row_count_and_var_field_offset(0, PAGE_SIZE - mem::size_of::<RowPageHeader>());
        self.header.col_count = schema.col_count() as u16;
        // initialize offset fields.
        self.header.del_bitset_offset = 0; // always starts at data_ptr().
        self.header.null_bitset_list_offset =
            self.header.del_bitset_offset + del_bitset_len(max_row_count) as u16;
        self.header.col_offset_list_offset = self.header.null_bitset_list_offset
            + null_bitset_list_len(max_row_count, schema.col_count()) as u16;
        self.header.fix_field_offset =
            self.header.col_offset_list_offset + col_offset_list_len(schema.col_count()) as u16;
        self.init_col_offset_list_and_fix_field_end(schema, max_row_count as u16);
        self.init_bitsets_and_row_ids();
        debug_assert!({
            (self.header.row_count_and_var_field_offset().0..self.header.max_row_count as usize)
                .all(|i| {
                    let row = self.row(i);
                    row.is_deleted()
                })
        });
    }

    #[inline]
    fn init_col_offset_list_and_fix_field_end(&mut self, schema: &TableSchema, row_count: u16) {
        debug_assert!(schema.col_count() >= 2); // at least RowID and one user column.
        debug_assert!(schema.layout(0) == Layout::Byte8); // first column must be RowID, with 8-byte layout.
        debug_assert!(self.header.col_offset_list_offset != 0);
        debug_assert!(self.header.fix_field_offset != 0);
        let mut col_offset = self.header.fix_field_offset;
        for (i, ty) in schema.types().iter().enumerate() {
            *self.col_offset_mut(i) = col_offset;
            col_offset += col_inline_len(&ty.kind.layout(), row_count as usize) as u16;
        }
        self.header.fix_field_end = col_offset;
    }

    #[inline]
    fn init_bitsets_and_row_ids(&mut self) {
        unsafe {
            // initialize del_bitset to all ones.
            {
                let count =
                    (self.header.null_bitset_list_offset - self.header.del_bitset_offset) as usize;
                let ptr = self
                    .data_ptr_mut()
                    .add(self.header.del_bitset_offset as usize);
                std::ptr::write_bytes(ptr, 0xff, count);
            }
            // initialize null_bitset_list to all zeros.
            {
                let count = (self.header.col_offset_list_offset
                    - self.header.null_bitset_list_offset) as usize;
                let ptr = self
                    .data_ptr_mut()
                    .add(self.header.null_bitset_list_offset as usize);
                std::ptr::write_bytes(ptr, 0xff, count);
            }
        }
    }

    /// Returns index of the row within page.
    #[inline]
    pub fn row_idx(&self, row_id: RowID) -> usize {
        debug_assert!(self.row_id_in_valid_range(row_id));
        (row_id - self.header.start_row_id) as usize
    }

    /// Returns row id for given index.
    #[inline]
    pub fn row_id(&self, row_idx: usize) -> RowID {
        debug_assert!(row_idx < self.header.row_count());
        self.header.start_row_id + row_idx as u64
    }

    /// Returns whether row id is in valid range.
    #[inline]
    pub fn row_id_in_valid_range(&self, row_id: RowID) -> bool {
        row_id >= self.header.start_row_id
            && row_id < self.header.start_row_id + self.header.row_count() as u64
    }

    /// Returns row id list in this page.
    #[inline]
    pub fn row_ids(&self) -> &[RowID] {
        self.vals::<RowID>(0)
    }

    #[inline]
    pub fn row_by_id(&self, row_id: RowID) -> Option<Row> {
        if !self.row_id_in_valid_range(row_id) {
            return None;
        }
        Some(self.row(self.row_idx(row_id)))
    }

    /// Returns free space of current page.
    /// The free space is used to hold data of var-len columns.
    #[inline]
    pub fn free_space(&self) -> u16 {
        self.header.var_field_offset() as u16 - self.header.fix_field_end
    }

    /// Request one new row id and addtional space for var-len data.
    /// This method uses atomic operation to update both fields.
    #[inline]
    pub fn request_row_idx_and_free_space(&self, var_len: usize) -> Option<(usize, usize)> {
        loop {
            let (row_count, var_field_offset) = self.header.row_count_and_var_field_offset();
            if row_count == self.header.max_row_count as usize {
                return None;
            }
            if self.header.fix_field_end as usize + var_len > var_field_offset {
                return None;
            }
            if self.header.compare_exchange_row_count_and_var_field_offset(
                (row_count, var_field_offset),
                (row_count + 1, var_field_offset - var_len),
            ) {
                return Some((row_count, var_field_offset - var_len));
            }
        }
    }

    /// Request addtitional space for var-len data.
    #[inline]
    pub fn request_free_space(&self, var_len: usize) -> Option<usize> {
        loop {
            let (row_count, var_field_offset) = self.header.row_count_and_var_field_offset();
            if self.header.fix_field_end as usize + var_len > var_field_offset {
                return None;
            }
            if self.header.compare_exchange_row_count_and_var_field_offset(
                (row_count, var_field_offset),
                (row_count, var_field_offset - var_len),
            ) {
                return Some(var_field_offset - var_len);
            }
        }
    }

    /// Insert a new row in page.
    #[inline]
    pub fn insert(&self, schema: &TableSchema, user_cols: &[Val]) -> InsertRow {
        debug_assert!(schema.col_count() == self.header.col_count as usize);
        // insert row does not include RowID, as RowID is auto-generated.
        debug_assert!(user_cols.len() + 1 == self.header.col_count as usize);

        let var_len = var_len_for_insert(schema, &user_cols);
        let (row_idx, var_offset) =
            if let Some((row_idx, var_offset)) = self.request_row_idx_and_free_space(var_len) {
                (row_idx, var_offset)
            } else {
                return InsertRow::NoFreeSpaceOrRowID;
            };
        let mut new_row = self.new_row(row_idx as usize, var_offset);
        for v in user_cols {
            match v {
                Val::Null => new_row.add_null(),
                Val::Byte1(v1) => new_row.add_val(*v1),
                Val::Byte2(v2) => new_row.add_val(*v2),
                Val::Byte4(v4) => new_row.add_val(*v4),
                Val::Byte8(v8) => new_row.add_val(*v8),
                Val::VarByte(var) => new_row.add_var(var.as_bytes()),
            }
        }
        InsertRow::Ok(new_row.finish())
    }

    /// delete row in page.
    /// This method will only mark the row as deleted.
    #[inline]
    pub fn delete(&self, row_id: RowID) -> Delete {
        if !self.row_id_in_valid_range(row_id) {
            return Delete::NotFound;
        }
        let row_idx = self.row_idx(row_id);
        if self.is_deleted(row_idx) {
            return Delete::AlreadyDeleted;
        }
        self.set_deleted(row_idx, true);
        Delete::Ok
    }

    /// Update in-place in current page.
    #[inline]
    pub fn update(
        &mut self,
        schema: &TableSchema,
        row_id: RowID,
        user_cols: &[UpdateCol],
    ) -> Update {
        // column indexes must be in range
        debug_assert!(
            {
                user_cols
                    .iter()
                    .all(|uc| uc.idx < self.header.col_count as usize)
            },
            "update column indexes must be in range"
        );
        // column indexes should be in order.
        debug_assert!(
            {
                user_cols.is_empty()
                    || user_cols
                        .iter()
                        .zip(user_cols.iter().skip(1))
                        .all(|(l, r)| l.idx < r.idx)
            },
            "update columns should be in order"
        );
        if !self.row_id_in_valid_range(row_id) {
            return Update::NotFound;
        }
        let row_idx = self.row_idx(row_id);
        if self.row(row_idx).is_deleted() {
            return Update::Deleted;
        }
        let var_len = self.var_len_for_update(row_idx, user_cols);
        let var_offset = if let Some(var_offset) = self.request_free_space(var_len) {
            var_offset
        } else {
            let row = self.row(row_idx);
            let vals = row.clone_vals(schema, false);
            return Update::NoFreeSpace(vals);
        };
        let mut row = self.row_mut(row_idx, var_offset, var_offset + var_len);
        for uc in user_cols {
            row.update_user_col(uc.idx, &uc.val);
        }
        row.finish();
        Update::Ok(row_id)
    }

    /// Select single row by row id.
    #[inline]
    pub fn select(&self, row_id: RowID) -> Select {
        if !self.row_id_in_valid_range(row_id) {
            return Select::NotFound;
        }
        let row_idx = self.row_idx(row_id);
        let row = self.row(row_idx);
        if row.is_deleted() {
            return Select::RowDeleted(row);
        }
        Select::Ok(row)
    }

    #[inline]
    pub fn var_len_for_update(&self, row_idx: usize, user_cols: &[UpdateCol]) -> usize {
        let row = self.row(row_idx);
        user_cols
            .iter()
            .map(|uc| match &uc.val {
                Val::VarByte(var) => {
                    let col = row.var(uc.idx);
                    let orig_var_len = PageVar::outline_len(col);
                    let upd_var_len = PageVar::outline_len(var.as_bytes());
                    if upd_var_len > orig_var_len {
                        upd_var_len
                    } else {
                        0
                    }
                }
                _ => 0,
            })
            .sum()
    }

    /// Creates a new row in page.
    #[inline]
    pub(crate) fn new_row(&self, row_idx: usize, var_offset: usize) -> NewRow {
        let mut row = NewRow {
            page: self,
            row_idx,
            col_idx: 0,
            var_offset,
        };
        // always add RowID as first column
        row.add_val(self.row_id(row_idx));
        row
    }

    /// Returns row by given index in page.
    #[inline]
    pub(crate) fn row(&self, row_idx: usize) -> Row {
        debug_assert!(row_idx < self.header.max_row_count as usize);
        Row {
            page: self,
            row_idx,
        }
    }

    /// Returns mutable row by given index in page.
    #[inline]
    pub(crate) fn row_mut(&self, row_idx: usize, var_offset: usize, var_end: usize) -> RowMut {
        debug_assert!(row_idx < self.header.row_count() as usize);
        RowMut {
            page: self,
            row_idx,
            var_offset,
            var_end,
        }
    }

    /// Returns all values of given column.
    #[inline]
    fn vals<V: Value>(&self, col_idx: usize) -> &[V] {
        let len = self.header.row_count() as usize;
        unsafe { self.vals_unchecked(col_idx, len) }
    }

    /// Returns all mutable values of given column.
    #[inline]
    fn vals_mut<V: Value>(&mut self, col_idx: usize) -> &mut [V] {
        let len = self.header.row_count() as usize;
        unsafe { self.vals_mut_unchecked(col_idx, len) }
    }

    #[inline]
    unsafe fn vals_unchecked<V: Value>(&self, col_idx: usize, len: usize) -> &[V] {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *const V = mem::transmute(ptr);
        std::slice::from_raw_parts(data, len)
    }

    #[inline]
    unsafe fn vals_mut_unchecked<V: Value>(&mut self, col_idx: usize, len: usize) -> &mut [V] {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr_mut().add(offset);
        let data: *mut V = mem::transmute(ptr);
        std::slice::from_raw_parts_mut(data, len)
    }

    #[inline]
    unsafe fn val_unchecked<V: Value>(&self, row_idx: usize, col_idx: usize) -> &V {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *const V = mem::transmute(ptr);
        &*data.add(row_idx)
    }

    #[inline]
    unsafe fn val_mut_unchecked<V: Value>(&mut self, row_idx: usize, col_idx: usize) -> &mut V {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *mut V = mem::transmute(ptr);
        &mut *data.add(row_idx)
    }

    #[inline]
    fn val_offset(&self, row_idx: usize, col_idx: usize, col_inline_len: usize) -> usize {
        unsafe {
            let list_start = self.header.col_offset_list_offset as usize;
            let ptr = self.data_ptr().add(list_start) as *const u16;
            let col_offset = *ptr.add(col_idx);
            col_offset as usize + row_idx * col_inline_len
        }
    }

    #[inline]
    pub(crate) fn update_val<V: ToValue>(&self, row_idx: usize, col_idx: usize, val: &V) {
        unsafe {
            let val = val.to_val();
            let offset = self.val_offset(row_idx, col_idx, mem::size_of::<V>());
            let ptr = self.data_ptr().add(offset);
            val.atomic_store(ptr as *mut _);
        }
    }

    #[inline]
    pub(crate) fn update_var(&self, row_idx: usize, col_idx: usize, var: PageVar) {
        debug_assert!(mem::size_of::<PageVar>() == mem::size_of::<u64>());
        self.update_val::<u64>(row_idx, col_idx, unsafe { mem::transmute(&var) });
    }

    #[inline]
    pub(crate) fn add_var(&self, input: &[u8], var_offset: usize) -> (PageVar, usize) {
        let len = input.len();
        if len <= PAGE_VAR_LEN_INLINE {
            return (PageVar::inline(input), var_offset);
        }
        // copy data to given offset.
        // this is safe because we atomically assign space for var-len data
        // so that no conflict will occur when modifing the allocated memory area.
        // for read, row lock will protect all row data.
        unsafe {
            let ptr = self.data_ptr().add(var_offset);
            let target = slice::from_raw_parts_mut(ptr as *mut _, len);
            target.copy_from_slice(input);
        }
        (
            PageVar::outline(len as u16, var_offset as u16, &input[..PAGE_VAR_LEN_PREFIX]),
            var_offset + len,
        )
    }

    #[inline]
    unsafe fn var_unchecked(&self, row_idx: usize, col_idx: usize) -> &PageVar {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *const PageVar = mem::transmute(ptr);
        &*data.add(row_idx)
    }

    #[inline]
    unsafe fn var_mut_unchecked(&mut self, row_idx: usize, col_idx: usize) -> &mut PageVar {
        let offset = self.col_offset(col_idx) as usize;
        let ptr = self.data_ptr().add(offset);
        let data: *mut PageVar = mem::transmute(ptr);
        &mut *data.add(row_idx)
    }

    #[inline]
    fn data_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    #[inline]
    fn data_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    /// Returns whether given row is deleted.
    #[inline]
    pub fn is_deleted(&self, row_idx: usize) -> bool {
        unsafe {
            let offset = self.header.del_bit_offset(row_idx);
            let ptr = self.data_ptr().add(offset);
            let bit_mask = 1 << (row_idx % 8);
            (*ptr) & bit_mask != 0
        }
    }

    /// Mark given row as deleted.
    #[inline]
    pub(crate) fn set_deleted(&self, row_idx: usize, deleted: bool) {
        unsafe {
            let offset = self.header.del_bit_offset(row_idx);
            let ptr = self.data_ptr().add(offset);
            let bit_mask = 1 << (row_idx % 8);
            let atom = AtomicU8::from_ptr(ptr as *mut _);
            loop {
                let current = atom.load(Ordering::Acquire);
                if deleted {
                    if current & bit_mask != 0 {
                        return; // already deleted.
                    }
                    let new = current | bit_mask;
                    if atom
                        .compare_exchange_weak(current, new, Ordering::SeqCst, Ordering::Relaxed)
                        .is_ok()
                    {
                        return;
                    }
                } else {
                    if current & bit_mask == 0 {
                        return; //already not deleted.
                    }
                    let new = current & !bit_mask;
                    if atom
                        .compare_exchange_weak(current, new, Ordering::SeqCst, Ordering::Relaxed)
                        .is_ok()
                    {
                        return;
                    }
                }
            }
        }
    }

    #[inline]
    fn is_null(&self, row_idx: usize, col_idx: usize) -> bool {
        unsafe {
            let offset = self.header.null_bit_offset(row_idx, col_idx);
            let ptr = self.data_ptr().add(offset);
            let bit_mask = 1 << (row_idx % 8);
            (*ptr) & bit_mask != 0
        }
    }

    #[inline]
    pub(crate) fn set_null(&self, row_idx: usize, col_idx: usize, null: bool) {
        unsafe {
            let offset = self.header.null_bit_offset(row_idx, col_idx);
            let ptr = self.data_ptr().add(offset);
            let atom = AtomicU8::from_ptr(ptr as *mut _);
            let bit_mask = 1 << (row_idx % 8);
            loop {
                let current = atom.load(Ordering::Acquire);
                let new = if null {
                    current | bit_mask
                } else {
                    current & !bit_mask
                };
                if atom
                    .compare_exchange_weak(current, new, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    return;
                }
            }
        }
    }

    #[inline]
    fn col_offset(&self, col_idx: usize) -> u16 {
        let offset = self.header.col_offset_list_offset as usize;
        unsafe {
            let ptr = self.data_ptr().add(offset) as *const u16;
            *ptr.add(col_idx)
        }
    }

    #[inline]
    fn col_offset_mut(&mut self, col_idx: usize) -> &mut u16 {
        let offset = self.header.col_offset_list_offset as usize;
        unsafe {
            let ptr = self.data_ptr_mut().add(offset) as *mut u16;
            &mut *ptr.add(col_idx)
        }
    }
}

impl BufferFrameAware for RowPage {
    #[inline]
    fn on_alloc<P: BufferPool>(pool: P, frame: &mut BufferFrame) {
        let page_id = frame.page_id;
        if let Some(undo_map) = pool.load_orphan_undo_map(page_id) {
            let res = frame.undo_map.replace(undo_map);
            debug_assert!(res.is_none());
        }
    }

    #[inline]
    fn on_dealloc<P: BufferPool>(pool: P, frame: &mut BufferFrame) {
        if let Some(undo_map) = frame.undo_map.take() {
            if undo_map.occupied() > 0 {
                let page_id = frame.page_id;
                pool.save_orphan_undo_map(page_id, undo_map);
            }
        }
    }

    #[inline]
    fn after_init<P: BufferPool>(_pool: P, frame: &mut BufferFrame) {
        if frame.undo_map.is_none() {
            let len = unsafe { Self::get(frame) }.header.max_row_count as usize;
            frame.undo_map = Some(UndoMap::new(len));
        }
    }
}

#[repr(C)]
pub struct RowPageHeader {
    pub start_row_id: u64,
    // higher two bytes is row count.
    // lower two bytes is var field offset.
    row_count_and_var_field_offset: AtomicU32,
    pub max_row_count: u16,
    pub col_count: u16,
    pub del_bitset_offset: u16,
    pub null_bitset_list_offset: u16,
    pub col_offset_list_offset: u16,
    pub fix_field_offset: u16,
    pub fix_field_end: u16,

    padding: [u8; 6],
}

impl RowPageHeader {
    #[inline]
    pub fn row_count(&self) -> usize {
        let value = self.row_count_and_var_field_offset.load(Ordering::Relaxed);
        ((value >> 16) & 0xffff) as usize
    }

    #[inline]
    pub fn var_field_offset(&self) -> usize {
        let value = self.row_count_and_var_field_offset.load(Ordering::Relaxed);
        (value & 0xffff) as usize
    }

    #[inline]
    pub fn row_count_and_var_field_offset(&self) -> (usize, usize) {
        let value = self.row_count_and_var_field_offset.load(Ordering::Relaxed);
        (((value >> 16) & 0xffff) as usize, (value & 0xffff) as usize)
    }

    #[inline]
    pub fn compare_exchange_row_count_and_var_field_offset(
        &self,
        (old_row_count, old_var_field_offset): (usize, usize),
        (row_count, var_field_offset): (usize, usize),
    ) -> bool {
        let old = ((old_row_count & 0xffff) << 16) as u32 | (old_var_field_offset & 0xffff) as u32;
        let new = ((row_count & 0xffff) << 16) as u32 | (var_field_offset & 0xffff) as u32;
        self.row_count_and_var_field_offset
            .compare_exchange_weak(old, new, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    #[inline]
    pub fn store_row_count_and_var_field_offset(&self, row_count: usize, var_field_offset: usize) {
        let new = ((row_count & 0xffff) << 16) as u32 | (var_field_offset & 0xffff) as u32;
        self.row_count_and_var_field_offset
            .store(new, Ordering::Relaxed);
    }

    #[inline]
    pub fn null_bit_offset(&self, row_idx: usize, col_idx: usize) -> usize {
        let len = align8(self.max_row_count as usize) / 8;
        let start = self.null_bitset_list_offset as usize;
        start + len * col_idx + row_idx / 8
    }

    #[inline]
    pub fn del_bit_offset(&self, row_idx: usize) -> usize {
        self.del_bitset_offset as usize + row_idx / 8
    }
}

impl fmt::Debug for RowPageHeader {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (row_count, var_field_offset) = self.row_count_and_var_field_offset();
        f.debug_struct("RowPageHeader")
            .field("start_row_id", &self.start_row_id)
            .field("max_row_count", &self.max_row_count)
            .field("row_count", &row_count)
            .field("col_count", &self.col_count)
            .field("del_bitset_offset", &self.del_bitset_offset)
            .field("null_bitset_list_offset", &self.null_bitset_list_offset)
            .field("col_offset_list_offset", &self.col_offset_list_offset)
            .field("fix_field_offset", &self.fix_field_offset)
            .field("fix_field_end", &self.fix_field_end)
            .field("var_field_offset", &var_field_offset)
            .finish()
    }
}

/// NewRow wraps the page to provide convenient method
/// to add values to new row.
pub struct NewRow<'a> {
    page: &'a RowPage,
    row_idx: usize,
    col_idx: usize,
    var_offset: usize,
}

impl<'a> NewRow<'a> {
    /// add one value to current row.
    #[inline]
    pub fn add_val<V: ToValue>(&mut self, val: V) {
        debug_assert!(self.col_idx < self.page.header.col_count as usize);
        self.page.update_val(self.row_idx, self.col_idx, &val);
        self.page.set_null(self.row_idx, self.col_idx, false);
        self.col_idx += 1;
    }

    /// Add variable-length value to current row.
    #[inline]
    pub fn add_var(&mut self, input: &[u8]) {
        debug_assert!(self.col_idx < self.page.header.col_count as usize);
        let (var, offset) = self.page.add_var(input, self.var_offset);
        self.page.update_var(self.row_idx, self.col_idx, var);
        self.page.set_null(self.row_idx, self.col_idx, false);
        self.var_offset = offset;
        self.col_idx += 1;
    }

    /// Add string value to current row, same as add_var().
    #[inline]
    pub fn add_str_atomic(&mut self, input: &str) {
        self.add_var(input.as_bytes())
    }

    /// Add null value to current row.
    #[inline]
    pub fn add_null(&mut self) {
        debug_assert!(self.col_idx < self.page.header.col_count as usize);
        self.page.set_null(self.row_idx, self.col_idx, true);
        self.col_idx += 1;
    }

    /// Finish current row.
    #[inline]
    pub fn finish(self) -> RowID {
        debug_assert!(self.col_idx == self.page.header.col_count as usize);
        self.page.set_deleted(self.row_idx, false);
        self.page.row_id(self.row_idx)
    }
}

/// Common trait to read values from a row.
pub trait RowRead {
    /// Page of current row.
    fn page(&self) -> &RowPage;

    /// Row index on the page.
    fn row_idx(&self) -> usize;

    /// Returns value by given column index.
    #[inline]
    fn val<T: Value>(&self, col_idx: usize) -> &T {
        unsafe { self.page().val_unchecked::<T>(self.row_idx(), col_idx) }
    }

    /// Returns value by given column index.(row id is excluded)
    #[inline]
    fn user_val<T: Value>(&self, user_col_idx: usize) -> &T {
        self.val(user_col_idx + 1)
    }

    /// Returns variable-length value by given column index.
    #[inline]
    fn var(&self, col_idx: usize) -> &[u8] {
        unsafe {
            let var = self.page().var_unchecked(self.row_idx(), col_idx);
            var.as_bytes(self.page().data_ptr())
        }
    }

    /// Returns variable-length value by given column index.(row id is excluded)
    #[inline]
    fn user_var(&self, user_col_idx: usize) -> &[u8] {
        self.var(user_col_idx + 1)
    }

    /// Returns string.
    #[inline]
    fn user_str(&self, user_col_idx: usize) -> &str {
        self.str(user_col_idx + 1)
    }

    /// Returns string.
    #[inline]
    fn str(&self, col_idx: usize) -> &str {
        unsafe {
            let var = self.page().var_unchecked(self.row_idx(), col_idx);
            std::str::from_utf8_unchecked(var.as_bytes(self.page().data_ptr()))
        }
    }

    /// Returns RowID of current row.
    /// Row id is always the first column of a row, with 8-byte width.
    #[inline]
    fn row_id(&self) -> RowID {
        *self.val::<RowID>(0)
    }

    /// Returns whether current row is deleted.
    /// The page is initialized as all rows are deleted.
    /// And insert should set the delete flag to false.
    #[inline]
    fn is_deleted(&self) -> bool {
        self.page().is_deleted(self.row_idx())
    }

    /// /// Returns whether column by given index is null.
    #[inline]
    fn is_null(&self, col_idx: usize) -> bool {
        self.page().is_null(self.row_idx(), col_idx)
    }

    /// Returns whether column by given index is null. (row id is excluded)
    #[inline]
    fn is_user_null(&self, user_col_idx: usize) -> bool {
        self.is_null(user_col_idx + 1)
    }

    /// Returns additional variable length space required for this update.
    #[inline]
    fn var_len_for_update(&self, user_cols: &[UpdateCol]) -> usize {
        self.page().var_len_for_update(self.row_idx(), user_cols)
    }

    /// Clone single value with given column index.
    #[inline]
    fn clone_user_val(&self, schema: &TableSchema, user_col_idx: usize) -> Val {
        self.clone_val(schema, user_col_idx + 1)
    }

    /// Clone index values.
    #[inline]
    fn clone_index_vals(&self, schema: &TableSchema, index_no: usize) -> Vec<Val> {
        schema.indexes[index_no]
            .keys
            .iter()
            .map(|key| self.clone_user_val(schema, key.user_col_idx as usize))
            .collect()
    }

    /// Clone single value and its var-len offset with given column index.
    #[inline]
    fn clone_user_val_with_var_offset(
        &self,
        schema: &TableSchema,
        user_col_idx: usize,
    ) -> (Val, Option<u16>) {
        self.clone_val_with_var_offset(schema, user_col_idx + 1)
    }

    /// Clone single value with given column index.
    /// NOTE: input column index includes RowID.
    #[inline]
    fn clone_val(&self, schema: &TableSchema, col_idx: usize) -> Val {
        if self.is_null(col_idx) {
            return Val::Null;
        }
        match schema.layout(col_idx) {
            Layout::Byte1 => {
                let v = self.val::<Byte1Val>(col_idx);
                Val::from(*v)
            }
            Layout::Byte2 => {
                let v = self.val::<Byte2Val>(col_idx);
                Val::from(*v)
            }
            Layout::Byte4 => {
                let v = self.val::<Byte4Val>(col_idx);
                Val::from(*v)
            }
            Layout::Byte8 => {
                let v = self.val::<Byte8Val>(col_idx);
                Val::from(*v)
            }
            Layout::VarByte => {
                let v = self.var(col_idx);
                Val::VarByte(MemVar::from(v))
            }
        }
    }

    #[inline]
    fn clone_val_with_var_offset(
        &self,
        schema: &TableSchema,
        col_idx: usize,
    ) -> (Val, Option<u16>) {
        if self.is_null(col_idx) {
            return (Val::Null, None);
        }
        match schema.layout(col_idx) {
            Layout::Byte1 => {
                let v = self.val::<Byte1Val>(col_idx);
                (Val::from(*v), None)
            }
            Layout::Byte2 => {
                let v = self.val::<Byte2Val>(col_idx);
                (Val::from(*v), None)
            }
            Layout::Byte4 => {
                let v = self.val::<Byte4Val>(col_idx);
                (Val::from(*v), None)
            }
            Layout::Byte8 => {
                let v = self.val::<Byte8Val>(col_idx);
                (Val::from(*v), None)
            }
            Layout::VarByte => {
                // let v = self.var(col_idx);
                let pv = unsafe { self.page().var_unchecked(self.row_idx(), col_idx) };
                let v = pv.as_bytes(self.page().data_ptr());
                let offset = pv.offset().map(|os| os as u16);
                (Val::VarByte(MemVar::from(v)), offset)
            }
        }
    }

    /// Clone all values.
    #[inline]
    fn clone_vals(&self, schema: &TableSchema, include_row_id: bool) -> Vec<Val> {
        let skip = if include_row_id { 0 } else { 1 };
        let mut vals = Vec::with_capacity(schema.col_count() - skip);
        for (col_idx, _) in schema.types().iter().enumerate().skip(skip) {
            vals.push(self.clone_val(schema, col_idx));
        }
        vals
    }

    /// Clone all values with var-len offset.
    #[inline]
    fn clone_vals_with_var_offsets(
        &self,
        schema: &TableSchema,
        include_row_id: bool,
    ) -> Vec<(Val, Option<u16>)> {
        let skip = if include_row_id { 0 } else { 1 };
        let mut vals = Vec::with_capacity(schema.col_count() - skip);
        for (col_idx, _) in schema.types().iter().enumerate().skip(skip) {
            vals.push(self.clone_val_with_var_offset(schema, col_idx));
        }
        vals
    }

    /// Clone values for given read set. (row id is excluded)
    #[inline]
    fn clone_vals_for_read_set(&self, schema: &TableSchema, user_read_set: &[usize]) -> Vec<Val> {
        let mut vals = Vec::with_capacity(user_read_set.len());
        for user_col_idx in user_read_set {
            vals.push(self.clone_user_val(schema, *user_col_idx))
        }
        vals
    }

    /// Returns whether the key of current row is different from given value.
    #[inline]
    fn is_key_different(&self, schema: &TableSchema, key: &SelectKey) -> bool {
        debug_assert!(!key.vals.is_empty());
        schema.indexes[key.index_no]
            .keys
            .iter()
            .map(|k| k.user_col_idx as usize + 1)
            .zip(&key.vals)
            .any(|(col_idx, val)| self.is_different(schema, col_idx, val))
    }

    /// Returns whether the value of current row at given column index is different from given value.
    #[inline]
    fn is_different(&self, schema: &TableSchema, col_idx: usize, value: &Val) -> bool {
        match (value, self.is_null(col_idx), schema.layout(col_idx)) {
            (Val::Null, true, _) => false,
            (Val::Null, false, _) => true,
            (_, true, _) => true,
            (Val::Byte1(new), false, lo) => {
                debug_assert!(lo == Layout::Byte1);
                let old = self.val::<Byte1Val>(col_idx);
                old != new
            }
            (Val::Byte2(new), false, lo) => {
                debug_assert!(lo == Layout::Byte2);
                let old = self.val::<Byte2Val>(col_idx);
                old != new
            }
            (Val::Byte4(new), false, lo) => {
                debug_assert!(lo == Layout::Byte4);
                let old = self.val::<Byte4Val>(col_idx);
                old != new
            }
            (Val::Byte8(new), false, lo) => {
                debug_assert!(lo == Layout::Byte8);
                let old = self.val::<Byte8Val>(col_idx);
                old != new
            }
            (Val::VarByte(new), false, lo) => {
                debug_assert!(lo == Layout::VarByte);
                let old = self.var(col_idx);
                old != new.as_bytes()
            }
        }
    }

    /// Returns whether the value of current row at given column index is different
    /// from given value. (row id is excluded)
    #[inline]
    fn is_user_different(&self, schema: &TableSchema, user_col_idx: usize, value: &Val) -> bool {
        self.is_different(schema, user_col_idx + 1, value)
    }

    /// Returns the old value if different from given index and new value.
    #[inline]
    fn user_different(
        &self,
        schema: &TableSchema,
        user_col_idx: usize,
        value: &Val,
    ) -> Option<(Val, Option<u16>)> {
        if !self.is_user_different(schema, user_col_idx, value) {
            return None;
        }
        Some(self.clone_user_val_with_var_offset(schema, user_col_idx))
    }

    /// Calculate delta between given values and current row.
    #[inline]
    fn calc_delta(&self, schema: &TableSchema, user_vals: &[Val]) -> Vec<UpdateCol> {
        let user_types = schema.user_types();
        debug_assert!(user_types.len() == user_vals.len());
        user_vals
            .iter()
            .enumerate()
            .filter_map(|(user_col_idx, val)| {
                if self.is_user_different(schema, user_col_idx, val) {
                    Some(UpdateCol {
                        idx: user_col_idx,
                        val: val.clone(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Row abstract a logical row in the page.
#[derive(Clone)]
pub struct Row<'a> {
    page: &'a RowPage,
    row_idx: usize,
}

impl<'a> RowRead for Row<'a> {
    #[inline]
    fn page(&self) -> &RowPage {
        self.page
    }

    #[inline]
    fn row_idx(&self) -> usize {
        self.row_idx
    }
}

/// RowMut is mutable row in the page.
pub struct RowMut<'a> {
    page: &'a RowPage,
    row_idx: usize,
    var_offset: usize,
    var_end: usize,
}

impl<'a> RowRead for RowMut<'a> {
    #[inline]
    fn page(&self) -> &RowPage {
        self.page
    }

    #[inline]
    fn row_idx(&self) -> usize {
        self.row_idx
    }
}

impl<'a> RowMut<'a> {
    /// Update column by given index and value.
    /// the column index is
    #[inline]
    pub fn update_user_col(&mut self, user_col_idx: usize, value: &Val) {
        let col_idx = user_col_idx + 1;
        match value {
            Val::Null => {
                self.page.set_null(self.row_idx, col_idx, true);
            }
            Val::Byte1(v1) => {
                self.page.update_val(self.row_idx, col_idx, v1);
                self.page.set_null(self.row_idx, col_idx, false);
            }
            Val::Byte2(v2) => {
                self.page.update_val(self.row_idx, col_idx, v2);
                self.page.set_null(self.row_idx, col_idx, false);
            }
            Val::Byte4(v4) => {
                self.page.update_val(self.row_idx, col_idx, v4);
                self.page.set_null(self.row_idx, col_idx, false);
            }
            Val::Byte8(v8) => {
                self.page.update_val(self.row_idx, col_idx, v8);
                self.page.set_null(self.row_idx, col_idx, false);
            }
            Val::VarByte(var) => {
                self.update_user_var(user_col_idx, var.as_bytes());
                self.page.set_null(self.row_idx, col_idx, false);
            }
        }
    }

    /// Update variable-length value.
    #[inline]
    pub fn update_user_var(&mut self, user_col_idx: usize, input: &[u8]) {
        let col_idx = user_col_idx + 1;
        if input.len() <= PAGE_VAR_LEN_INLINE {
            // inlined var can be directly updated,
            // without overwriting original var-len data in page.
            let var = PageVar::inline(input);
            self.page.update_var(self.row_idx, col_idx, var);
            return;
        }
        // todo: reuse released space by update.
        // if update value is longer than original value,
        // the original space is wasted.
        // there can be optimization that additionally record
        // the head free offset of released var-len space at the page header.
        // and any released space is at lest 7 bytes(larger than VAR_LEN_INLINE)
        // long and is enough to connect the free list.
        unsafe {
            let old_var = self.page.var_unchecked(self.row_idx, col_idx);
            if input.len() <= old_var.len() {
                let offset = old_var.offset().unwrap();
                // overwrite original var data.
                let (var, _) = self.page.add_var(input, offset);
                self.page.update_var(self.row_idx, col_idx, var);
            } else {
                // use free space.
                let (var, var_offset) = self.page.add_var(input, self.var_offset);
                self.page.update_var(self.row_idx, col_idx, var);
                self.var_offset = var_offset;
            }
        }
    }

    /// Set null bit by given column index.
    #[inline]
    pub fn set_user_null(&mut self, user_col_idx: usize, null: bool) {
        self.page.set_null(self.row_idx, user_col_idx + 1, null);
    }

    /// Set delete flag by given row.
    #[inline]
    pub fn set_deleted(&mut self, deleted: bool) {
        self.page.set_deleted(self.row_idx, deleted);
    }

    #[inline]
    pub fn finish(self) {
        debug_assert!(self.var_offset == self.var_end);
    }
}

#[inline]
const fn align8(len: usize) -> usize {
    (len + 7) / 8 * 8
}

#[inline]
const fn align64(len: usize) -> usize {
    (len + 63) / 64 * 64
}

/// delete bitset length, align to 8 bytes.
#[inline]
const fn del_bitset_len(count: usize) -> usize {
    align64(count) / 8
}

// null bitset length, align to 8 bytes.
#[inline]
const fn null_bitset_list_len(row_count: usize, col_count: usize) -> usize {
    align8(align8(row_count) / 8 * col_count)
}

// column offset list len, align to 8 bytes.
#[inline]
const fn col_offset_list_len(col_count: usize) -> usize {
    align8(mem::size_of::<u16>() * col_count)
}

// column inline length, align to 8 bytes.
#[inline]
const fn col_inline_len(col: &Layout, row_count: usize) -> usize {
    align8(col.inline_len() * row_count)
}

/// Returns estimation of maximum row count of a new page with average row length
/// equal to given row length.
#[inline]
pub const fn estimate_max_row_count(row_len: usize, col_count: usize) -> usize {
    let body_len = PAGE_SIZE
        - mem::size_of::<RowPageHeader>() // header
        - col_count * 2; // col offset (approx)
    let estimated_row_size = row_len
        + 1 // del bitset (approx)
        + (col_count + 7) / 8; // null bitset (approx)
    body_len / estimated_row_size
}

/// Returns additional space of var-len data of the new row to be inserted.
#[inline]
pub fn var_len_for_insert(schema: &TableSchema, user_cols: &[Val]) -> usize {
    schema
        .var_cols
        .iter()
        .map(|idx| match &user_cols[*idx - 1] {
            Val::VarByte(var) => {
                if var.len() > PAGE_VAR_LEN_INLINE {
                    var.len()
                } else {
                    0
                }
            }
            _ => 0,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use core::str;

    use crate::catalog::{IndexKey, IndexSchema};
    use mem::MaybeUninit;

    use super::*;

    #[test]
    fn test_estimate_max_row_count() {
        for row_len in (100..600).step_by(100) {
            for col_count in 1..6 {
                let row_count = estimate_max_row_count(row_len, col_count);
                println!(
                    "row_len={},col_count={},est_row_count={}",
                    row_len, col_count, row_count
                );
            }
        }
    }

    #[test]
    fn test_row_page_init() {
        let schema = TableSchema::new(
            vec![ValKind::I32.nullable(false)],
            vec![IndexSchema {
                keys: vec![IndexKey::new(0)],
                unique: true,
            }],
        );
        let mut page = create_row_page();
        page.init(100, 105, &schema);
        println!("page header={:?}", page.header);
        assert!(page.header.start_row_id == 100);
        assert!(page.header.max_row_count == 105);
        assert!(page.header.row_count() == 0);
        assert!(page.header.del_bitset_offset == 0);
        assert!(page.header.null_bitset_list_offset % 8 == 0);
        assert!(page.header.col_offset_list_offset % 8 == 0);
        assert!(page.header.fix_field_offset % 8 == 0);
        assert!(page.header.fix_field_end % 8 == 0);
        assert!(page.header.var_field_offset() % 8 == 0);
    }

    #[test]
    fn test_row_page_new_row() {
        let schema = TableSchema::new(
            vec![ValKind::I32.nullable(false)],
            vec![IndexSchema {
                keys: vec![IndexKey::new(0)],
                unique: true,
            }],
        );
        let mut page = create_row_page();
        page.init(100, 200, &schema);
        assert!(page.header.row_count() == 0);
        assert!(page.header.col_count == 2);
        let insert = vec![Val::Byte8(1u64)];
        assert!(page.insert(&schema, &insert).is_ok());
        assert!(page.header.row_count() == 1);
        let insert = vec![Val::Byte8(2u64)];
        assert!(page.insert(&schema, &insert).is_ok());
        assert!(page.header.row_count() == 2);
    }

    #[test]
    fn test_row_page_read_write_row() {
        let schema = TableSchema::new(
            vec![
                ValKind::I32.nullable(false),
                ValKind::VarByte.nullable(false),
            ],
            vec![IndexSchema {
                keys: vec![IndexKey::new(0)],
                unique: true,
            }],
        );
        let mut page = create_row_page();
        page.init(100, 200, &schema);

        let insert = vec![Val::from(1_000_000i32), Val::from("hello")];
        assert!(page.insert(&schema, &insert).is_ok());

        let row1 = page.row(0);
        assert!(row1.row_id() == 100);
        assert!(*row1.val::<Byte4Val>(1) as i32 == 1_000_000i32);
        assert!(row1.var(2) == b"hello");

        let insert = vec![
            Val::from(2_000_000i32),
            Val::from("this value is not inline"),
        ];
        assert!(page.insert(&schema, &insert).is_ok());

        let row2 = page.row(1);
        assert!(row2.row_id() == 101);
        assert!(*row2.val::<Byte4Val>(1) as i32 == 2_000_000i32);
        let s = row2.var(2);
        println!("len={:?}, s={:?}", s.len(), str::from_utf8(&s[..24]));
        assert!(row2.var(2) == b"this value is not inline");

        let row_id = row2.row_id();
        let update = vec![
            UpdateCol {
                idx: 0,
                val: Val::Null,
            },
            UpdateCol {
                idx: 1,
                val: Val::from("update to non-inline value"),
            },
        ];
        assert!(page.update(&schema, row_id, &update).is_ok());
    }

    #[test]
    fn test_row_page_crud() {
        let schema = TableSchema::new(
            vec![
                ValKind::I8.nullable(false),
                ValKind::I16.nullable(false),
                ValKind::I32.nullable(false),
                ValKind::I64.nullable(false),
                ValKind::VarByte.nullable(false),
            ],
            vec![IndexSchema {
                keys: vec![IndexKey::new(2)],
                unique: true,
            }],
        );
        let mut page = create_row_page();
        page.init(100, 200, &schema);
        let short = b"short";
        let long = b"very loooooooooooooooooong";

        let insert = vec![
            Val::Byte1(1),
            Val::Byte2(1000),
            Val::Byte4(1_000_000),
            Val::Byte8(1 << 35),
            Val::from(&short[..]),
        ];
        let res = page.insert(&schema, &insert);
        assert!(matches!(res, InsertRow::Ok(100)));
        assert!(!page.row(0).is_deleted());

        let row_id = 100;
        let update = vec![
            UpdateCol {
                idx: 0,
                val: Val::Byte1(2),
            },
            UpdateCol {
                idx: 1,
                val: Val::Byte2(2000),
            },
            UpdateCol {
                idx: 2,
                val: Val::Byte4(2_000_000),
            },
            UpdateCol {
                idx: 3,
                val: Val::Byte8(2 << 35),
            },
            UpdateCol {
                idx: 4,
                val: Val::VarByte(MemVar::from(&long[..])),
            },
        ];
        let res = page.update(&schema, row_id, &update);
        assert!(res.is_ok());

        let res = page.delete(row_id);
        assert!(matches!(res, Delete::Ok));

        let select = page.select(row_id);
        assert!(matches!(select, Select::RowDeleted(_)));
    }

    fn create_row_page() -> RowPage {
        unsafe {
            let new = MaybeUninit::<RowPage>::uninit();
            new.assume_init()
        }
    }
}
