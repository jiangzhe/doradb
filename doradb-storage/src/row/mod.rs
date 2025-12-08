pub mod ops;

use crate::buffer::page::{BufferPage, PAGE_SIZE};
use crate::catalog::TableMetadata;
use crate::row::ops::{Delete, InsertRow, Recover, Select, SelectKey, Update, UpdateCol};
use crate::value::*;
use std::fmt;
use std::mem;
use std::slice;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

pub type RowID = u64;
pub const INVALID_ROW_ID: RowID = !0;

const _: () = assert!(
    { std::mem::size_of::<RowPageHeader>().is_multiple_of(8) },
    "RowPageHeader should have size align to 8 bytes"
);

/// RowPage is the core data structure of row-store.
/// Uses PAX format in order to be fast in both TP and
/// AP scenarios.
///
/// Header:
///
/// |-------------------------|-----------|
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
/// |-------------------------|-----------|
///
/// Data:
///
/// |------------------|-----------------------------------------------|
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
/// |------------------|-----------------------------------------------|
pub struct RowPage {
    pub header: RowPageHeader,
    pub data: [u8; PAGE_SIZE - mem::size_of::<RowPageHeader>()],
}

impl RowPage {
    /// Initialize row page.
    #[inline]
    pub fn init(&mut self, start_row_id: u64, max_row_count: usize, metadata: &TableMetadata) {
        debug_assert!(max_row_count <= 0xffff);
        self.header.start_row_id = start_row_id;
        self.header.max_row_count = max_row_count as u16;
        self.header
            .store_row_count_and_var_field_offset(0, PAGE_SIZE - mem::size_of::<RowPageHeader>());
        self.header.col_count = metadata.col_count() as u16;
        // initialize offset fields.
        self.header.del_bitset_offset = 0; // always starts at data_ptr().
        self.header.null_bitset_list_offset =
            self.header.del_bitset_offset + del_bitset_len(max_row_count) as u16;
        self.header.col_offset_list_offset = self.header.null_bitset_list_offset
            + null_bitset_list_len(max_row_count, metadata.col_count()) as u16;
        self.header.fix_field_offset =
            self.header.col_offset_list_offset + col_offset_list_len(metadata.col_count()) as u16;
        self.init_col_offset_list_and_fix_field_end(metadata, max_row_count as u16);
        self.init_bitsets();
        debug_assert!({
            (self.header.row_count_and_var_field_offset().0..self.header.max_row_count as usize)
                .all(|i| {
                    let row = self.row(i);
                    row.is_deleted()
                })
        });
    }

    #[inline]
    fn init_col_offset_list_and_fix_field_end(&mut self, schema: &TableMetadata, row_count: u16) {
        debug_assert!(schema.col_count() >= 1); // at least one user column.
        debug_assert!(self.header.col_offset_list_offset != 0);
        debug_assert!(self.header.fix_field_offset != 0);
        let mut col_offset = self.header.fix_field_offset;
        for (i, ty) in schema.col_types().iter().enumerate() {
            *self.col_offset_mut(i) = col_offset;
            col_offset += col_inline_len(&ty.kind.layout(), row_count as usize) as u16;
        }
        self.header.fix_field_end = col_offset;
    }

    #[inline]
    fn init_bitsets(&mut self) {
        unsafe {
            // initialize del_bitset to all ones.
            {
                let count =
                    (self.header.null_bitset_list_offset - self.header.del_bitset_offset) as usize;
                let ptr = self
                    .data_ptr_mut()
                    .add(self.header.del_bitset_offset as usize);
                ptr.write_bytes(0xff, count);
            }
            // initialize null_bitset_list to all zeros.
            {
                let count = (self.header.col_offset_list_offset
                    - self.header.null_bitset_list_offset) as usize;
                let ptr = self
                    .data_ptr_mut()
                    .add(self.header.null_bitset_list_offset as usize);
                ptr.write_bytes(0xff, count);
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
            && row_id < self.header.start_row_id + self.header.max_row_count as u64
    }

    /// Returns row id list in this page.
    #[inline]
    pub fn row_ids(&self) -> &[RowID] {
        self.vals::<RowID>(0)
    }

    #[inline]
    pub fn row_by_id(&self, row_id: RowID) -> Option<Row<'_>> {
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

    #[inline]
    pub fn update_count_to_include_row_id(&mut self, row_id: RowID) {
        debug_assert!(row_id >= self.header.start_row_id);
        debug_assert!(row_id < self.header.start_row_id + self.header.max_row_count as u64);
        let row_count = self.header.row_count();
        let new_count = row_id - self.header.start_row_id + 1;
        if row_count < new_count as usize {
            self.header.update_row_count(new_count as usize);
        }
    }

    /// Insert a new row in page.
    #[inline]
    pub fn insert(&self, schema: &TableMetadata, user_cols: &[Val]) -> InsertRow {
        debug_assert!(schema.col_count() == self.header.col_count as usize);
        // insert row does not include RowID, as RowID is auto-generated.
        debug_assert!(user_cols.len() == self.header.col_count as usize);

        let var_len = var_len_for_insert(schema, user_cols);
        let (row_idx, var_offset) =
            if let Some((row_idx, var_offset)) = self.request_row_idx_and_free_space(var_len) {
                (row_idx, var_offset)
            } else {
                return InsertRow::NoFreeSpaceOrRowID;
            };
        let mut new_row = self.new_row(row_idx, var_offset);
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
        metadata: &TableMetadata,
        row_id: RowID,
        cols: &[UpdateCol],
    ) -> Update {
        // column indexes must be in range
        debug_assert!(
            {
                cols.iter()
                    .all(|uc| uc.idx < self.header.col_count as usize)
            },
            "update column indexes must be in range"
        );
        // column indexes should be in order.
        debug_assert!(
            {
                cols.is_empty()
                    || cols
                        .iter()
                        .zip(cols.iter().skip(1))
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
        let var_len = self.var_len_for_update(row_idx, cols);
        let var_offset = if let Some(var_offset) = self.request_free_space(var_len) {
            var_offset
        } else {
            let row = self.row(row_idx);
            let vals = row.clone_vals(metadata);
            return Update::NoFreeSpace(vals);
        };
        let mut row = self.row_mut(row_idx, var_offset, var_offset + var_len);
        for uc in cols {
            row.update_col(uc.idx, &uc.val);
        }
        row.finish();
        Update::Ok(row_id)
    }

    /// Select single row by row id.
    #[inline]
    pub fn select(&self, row_id: RowID) -> Select<'_> {
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
    pub(crate) fn new_row(&self, row_idx: usize, var_offset: usize) -> NewRow<'_> {
        let row_id = self.row_id(row_idx);
        NewRow {
            page: self,
            row_idx,
            col_idx: 0,
            var_offset,
            row_id,
        }
    }

    /// Returns row by given index in page.
    #[inline]
    pub(crate) fn row(&self, row_idx: usize) -> Row<'_> {
        debug_assert!(row_idx < self.header.max_row_count as usize);
        Row {
            page: self,
            row_idx,
        }
    }

    /// Returns mutable row by given index in page.
    #[inline]
    pub(crate) fn row_mut(&self, row_idx: usize, var_offset: usize, var_end: usize) -> RowMut<'_> {
        debug_assert!(row_idx < self.header.row_count());
        RowMut {
            page: self,
            row_idx,
            var_offset,
            var_end,
        }
    }

    #[inline]
    pub(crate) fn row_mut_exclusive(
        &mut self,
        row_idx: usize,
        var_offset: usize,
        var_end: usize,
    ) -> RowMutExclusive<'_> {
        RowMutExclusive {
            page: self,
            row_idx,
            var_offset,
            var_end,
        }
    }

    /// Returns all values of given column.
    #[inline]
    fn vals<V: Value>(&self, col_idx: usize) -> &[V] {
        let len = self.header.row_count();
        unsafe { self.vals_unchecked(col_idx, len) }
    }

    /// Returns all mutable values of given column.
    #[inline]
    fn vals_mut<V: Value>(&mut self, col_idx: usize) -> &mut [V] {
        let len = self.header.row_count();
        unsafe { self.vals_mut_unchecked(col_idx, len) }
    }

    #[inline]
    unsafe fn vals_unchecked<V: Value>(&self, col_idx: usize, len: usize) -> &[V] {
        unsafe {
            let offset = self.col_offset(col_idx) as usize;
            let ptr = self.data_ptr().add(offset);
            let data: *const V = mem::transmute(ptr);
            std::slice::from_raw_parts(data, len)
        }
    }

    #[inline]
    unsafe fn vals_mut_unchecked<V: Value>(&mut self, col_idx: usize, len: usize) -> &mut [V] {
        unsafe {
            let offset = self.col_offset(col_idx) as usize;
            let ptr = self.data_ptr_mut().add(offset);
            let data: *mut V = mem::transmute(ptr);
            std::slice::from_raw_parts_mut(data, len)
        }
    }

    #[inline]
    unsafe fn val_unchecked<V: Value>(&self, row_idx: usize, col_idx: usize) -> &V {
        unsafe {
            let offset = self.col_offset(col_idx) as usize;
            let ptr = self.data_ptr().add(offset);
            let data: *const V = mem::transmute(ptr);
            &*data.add(row_idx)
        }
    }

    #[inline]
    unsafe fn val_mut_unchecked<V: Value>(&mut self, row_idx: usize, col_idx: usize) -> &mut V {
        unsafe {
            let offset = self.col_offset(col_idx) as usize;
            let ptr = self.data_ptr().add(offset);
            let data: *mut V = mem::transmute(ptr);
            &mut *data.add(row_idx)
        }
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
    pub(crate) fn update_val<V: Value>(&self, row_idx: usize, col_idx: usize, val: V) {
        unsafe {
            let offset = self.val_offset(row_idx, col_idx, mem::size_of::<V>());
            let ptr = self.data_ptr().add(offset);
            val.atomic_store(ptr);
        }
    }

    #[inline]
    pub fn update_col(
        &self,
        row_idx: usize,
        col_idx: usize,
        value: &Val,
        mut var_offset: usize,
        old_exists: bool,
    ) -> usize {
        match value {
            Val::Null => {
                self.set_null(row_idx, col_idx, true);
            }
            Val::Byte1(v1) => {
                self.update_val(row_idx, col_idx, *v1);
                self.set_null(row_idx, col_idx, false);
            }
            Val::Byte2(v2) => {
                self.update_val(row_idx, col_idx, *v2);
                self.set_null(row_idx, col_idx, false);
            }
            Val::Byte4(v4) => {
                self.update_val(row_idx, col_idx, *v4);
                self.set_null(row_idx, col_idx, false);
            }
            Val::Byte8(v8) => {
                self.update_val(row_idx, col_idx, *v8);
                self.set_null(row_idx, col_idx, false);
            }
            Val::VarByte(var) => {
                if let Some(new_offset) =
                    self.modify_var(row_idx, col_idx, var.as_bytes(), var_offset, old_exists)
                {
                    var_offset = new_offset;
                }
                self.set_null(row_idx, col_idx, false);
            }
        }
        var_offset
    }

    #[inline]
    pub fn update_col_exclusive(
        &mut self,
        row_idx: usize,
        col_idx: usize,
        value: &Val,
        mut var_offset: usize,
        old_exists: bool,
    ) -> usize {
        match value {
            Val::Null => {
                self.set_null_exclusive(row_idx, col_idx, true);
            }
            Val::Byte1(v1) => {
                self.update_val_exclusive(row_idx, col_idx, *v1);
                self.set_null_exclusive(row_idx, col_idx, false);
            }
            Val::Byte2(v2) => {
                self.update_val_exclusive(row_idx, col_idx, *v2);
                self.set_null_exclusive(row_idx, col_idx, false);
            }
            Val::Byte4(v4) => {
                self.update_val_exclusive(row_idx, col_idx, *v4);
                self.set_null_exclusive(row_idx, col_idx, false);
            }
            Val::Byte8(v8) => {
                self.update_val_exclusive(row_idx, col_idx, *v8);
                self.set_null_exclusive(row_idx, col_idx, false);
            }
            Val::VarByte(var) => {
                if let Some(new_offset) =
                    self.modify_var(row_idx, col_idx, var.as_bytes(), var_offset, old_exists)
                {
                    var_offset = new_offset;
                }
                self.set_null_exclusive(row_idx, col_idx, false);
            }
        }
        var_offset
    }

    /// Update variable-length value.
    /// If old value exists, we will try to reuse space occupied by old value.
    /// Returns the updated var length offset.
    #[inline]
    pub fn modify_var(
        &self,
        row_idx: usize,
        col_idx: usize,
        input: &[u8],
        var_offset: usize,
        old_exists: bool,
    ) -> Option<usize> {
        if input.len() <= PAGE_VAR_LEN_INLINE {
            // inlined var can be directly updated,
            // without overwriting original var-len data in page.
            let var = PageVar::inline(input);
            self.update_var(row_idx, col_idx, var);
            return None;
        }
        // todo: reuse released space by update.
        // if update value is longer than original value,
        // the original space is wasted.
        // there can be optimization that additionally record
        // the head free offset of released var-len space at the page header.
        // and any released space is at lest 7 bytes(larger than VAR_LEN_INLINE)
        // long and is enough to connect the free list.
        if !old_exists {
            // use free space.
            let (var, var_offset) = self.add_var(input, var_offset);
            self.update_var(row_idx, col_idx, var);
            return Some(var_offset);
        }

        unsafe {
            let old_var = self.var_unchecked(row_idx, col_idx);
            if input.len() <= old_var.len() {
                let offset = old_var.offset().unwrap();
                // overwrite original var data.
                let (var, _) = self.add_var(input, offset);
                self.update_var(row_idx, col_idx, var);
                None
            } else {
                // use free space.
                let (var, var_offset) = self.add_var(input, var_offset);
                self.update_var(row_idx, col_idx, var);
                Some(var_offset)
            }
        }
    }

    #[inline]
    pub(crate) fn update_val_exclusive<V: Value>(
        &mut self,
        row_idx: usize,
        col_idx: usize,
        val: V,
    ) {
        unsafe {
            let offset = self.val_offset(row_idx, col_idx, mem::size_of::<V>());
            let ptr = self.data_ptr().add(offset);
            val.store(ptr as *mut _);
        }
    }

    #[inline]
    pub(crate) fn update_var(&self, row_idx: usize, col_idx: usize, var: PageVar) {
        debug_assert!(mem::size_of::<PageVar>() == mem::size_of::<u64>());
        self.update_val::<u64>(row_idx, col_idx, unsafe {
            mem::transmute::<PageVar, u64>(var)
        });
    }

    #[inline]
    pub(crate) fn update_var_exclusive(&mut self, row_idx: usize, col_idx: usize, var: PageVar) {
        debug_assert!(mem::size_of::<PageVar>() == mem::size_of::<u64>());
        self.update_val_exclusive::<u64>(row_idx, col_idx, unsafe {
            mem::transmute::<PageVar, u64>(var)
        });
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
    pub(crate) fn add_var_exclusive(
        &mut self,
        input: &[u8],
        var_offset: usize,
    ) -> (PageVar, usize) {
        self.add_var(input, var_offset)
    }

    #[inline]
    unsafe fn var_unchecked(&self, row_idx: usize, col_idx: usize) -> &PageVar {
        unsafe {
            let offset = self.col_offset(col_idx) as usize;
            let ptr = self.data_ptr().add(offset);
            let data: *const PageVar = mem::transmute(ptr);
            &*data.add(row_idx)
        }
    }

    #[inline]
    unsafe fn var_mut_unchecked(&mut self, row_idx: usize, col_idx: usize) -> &mut PageVar {
        unsafe {
            let offset = self.col_offset(col_idx) as usize;
            let ptr = self.data_ptr().add(offset);
            let data: *mut PageVar = mem::transmute(ptr);
            &mut *data.add(row_idx)
        }
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
    pub(crate) fn set_deleted_exclusive(&mut self, row_idx: usize, deleted: bool) {
        unsafe {
            let offset = self.header.del_bit_offset(row_idx);
            let ptr = self.data_ptr_mut().add(offset);
            let current = *ptr;
            let bit_mask = 1 << (row_idx % 8);
            *ptr = if deleted {
                current | bit_mask
            } else {
                current & !bit_mask
            };
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

    /// Set null bit of given row given column.
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

    /// Set null bit of given row given column.
    /// Page is owned exclusively, so no need to perform atomicly.
    #[inline]
    pub(crate) fn set_null_exclusive(&mut self, row_idx: usize, col_idx: usize, null: bool) {
        unsafe {
            let offset = self.header.null_bit_offset(row_idx, col_idx);
            let ptr = self.data_ptr_mut().add(offset);
            let current = *ptr;
            let bit_mask = 1 << (row_idx % 8);
            *ptr = if null {
                current | bit_mask
            } else {
                current & !bit_mask
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

impl BufferPage for RowPage {}

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
    /// Returns row count of this page.
    #[inline]
    pub fn row_count(&self) -> usize {
        let value = self.row_count_and_var_field_offset.load(Ordering::Relaxed);
        ((value >> 16) & 0xffff) as usize
    }

    /// Update row count of this page.
    #[inline]
    pub fn update_row_count(&mut self, row_count: usize) {
        debug_assert!(row_count <= self.max_row_count as usize);
        let value = self.row_count_and_var_field_offset.load(Ordering::Relaxed);
        let new_value = ((row_count as u32) << 16) | (value & 0xffff);
        self.row_count_and_var_field_offset
            .store(new_value, Ordering::Relaxed);
    }

    /// Returns var-length field offset of this page.
    #[inline]
    pub fn var_field_offset(&self) -> usize {
        let value = self.row_count_and_var_field_offset.load(Ordering::Relaxed);
        (value & 0xffff) as usize
    }

    /// Returns row count and var-length field offset of this page.
    #[inline]
    pub fn row_count_and_var_field_offset(&self) -> (usize, usize) {
        let value = self.row_count_and_var_field_offset.load(Ordering::Relaxed);
        (((value >> 16) & 0xffff) as usize, (value & 0xffff) as usize)
    }

    /// Atomically update(CAS) row count and var-length field offset.
    /// This is required when multiple threads are inserting/updating on the same page.
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

    /// Store row count and var-length field offset.
    #[inline]
    pub fn store_row_count_and_var_field_offset(&self, row_count: usize, var_field_offset: usize) {
        let new = ((row_count & 0xffff) << 16) as u32 | (var_field_offset & 0xffff) as u32;
        self.row_count_and_var_field_offset
            .store(new, Ordering::Relaxed);
    }

    /// Returns offset of null bits.
    #[inline]
    pub fn null_bit_offset(&self, row_idx: usize, col_idx: usize) -> usize {
        let len = align8(self.max_row_count as usize) / 8;
        let start = self.null_bitset_list_offset as usize;
        start + len * col_idx + row_idx / 8
    }

    /// Returns offset of delete bits.
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
    row_id: RowID,
}

impl NewRow<'_> {
    /// add one value to current row.
    #[inline]
    pub(crate) fn add_val<V: Value>(&mut self, val: V) {
        debug_assert!(self.col_idx < self.page.header.col_count as usize);
        self.page.update_val(self.row_idx, self.col_idx, val);
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
        self.row_id
    }
}

/// Common trait to read values from a row.
pub(crate) trait RowRead {
    /// Page of current row.
    fn page(&self) -> &RowPage;

    /// Row index on the page.
    fn row_idx(&self) -> usize;

    /// Returns value by given column index.
    #[inline]
    fn val<T: Value>(&self, col_idx: usize) -> &T {
        unsafe { self.page().val_unchecked::<T>(self.row_idx(), col_idx) }
    }

    /// Returns variable-length value by given column index.
    #[inline]
    fn var(&self, col_idx: usize) -> &[u8] {
        unsafe {
            let var = self.page().var_unchecked(self.row_idx(), col_idx);
            var.as_bytes(self.page().data_ptr())
        }
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
        self.page().header.start_row_id + self.row_idx() as RowID
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

    /// Returns additional variable length space required for this update.
    #[inline]
    fn var_len_for_update(&self, cols: &[UpdateCol]) -> usize {
        self.page().var_len_for_update(self.row_idx(), cols)
    }

    /// Clone index values.
    #[inline]
    fn clone_index_vals(&self, metadata: &TableMetadata, index_no: usize) -> Vec<Val> {
        metadata.index_specs[index_no]
            .index_cols
            .iter()
            .map(|key| self.clone_val(metadata, key.col_no as usize))
            .collect()
    }

    /// Clone single value with given column index.
    /// NOTE: input column index includes RowID.
    #[inline]
    fn clone_val(&self, metadata: &TableMetadata, col_idx: usize) -> Val {
        if self.is_null(col_idx) {
            return Val::Null;
        }
        match metadata.col_layout(col_idx) {
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
        metadata: &TableMetadata,
        col_idx: usize,
    ) -> (Val, Option<u16>) {
        if self.is_null(col_idx) {
            return (Val::Null, None);
        }
        match metadata.col_layout(col_idx) {
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
            Layout::VarByte => unsafe {
                let pv = self.page().var_unchecked(self.row_idx(), col_idx);
                let v = pv.as_bytes(self.page().data_ptr());
                let offset = pv.offset().map(|os| os as u16);
                (Val::VarByte(MemVar::from(v)), offset)
            },
        }
    }

    /// Clone all values.
    #[inline]
    fn clone_vals(&self, metadata: &TableMetadata) -> Vec<Val> {
        (0..metadata.col_count())
            .map(|col_idx| self.clone_val(metadata, col_idx))
            .collect()
    }

    /// Clone all values with var-len offset.
    #[inline]
    fn clone_vals_with_var_offsets(&self, metadata: &TableMetadata) -> Vec<(Val, Option<u16>)> {
        (0..metadata.col_count())
            .map(|col_idx| self.clone_val_with_var_offset(metadata, col_idx))
            .collect()
    }

    /// Clone values for given read set. (row id is excluded)
    #[inline]
    fn clone_vals_for_read_set(&self, metadata: &TableMetadata, read_set: &[usize]) -> Vec<Val> {
        read_set
            .iter()
            .map(|col_idx| self.clone_val(metadata, *col_idx))
            .collect()
    }

    /// Returns whether the key of current row is different from given value.
    #[inline]
    fn is_key_different(&self, metadata: &TableMetadata, key: &SelectKey) -> bool {
        debug_assert!(!key.vals.is_empty());
        metadata.index_specs[key.index_no]
            .index_cols
            .iter()
            .zip(&key.vals)
            .any(|(key, val)| self.is_different(metadata, key.col_no as usize, val))
    }

    /// Returns whether the value of current row at given column index is different from given value.
    #[inline]
    fn is_different(&self, metadata: &TableMetadata, col_idx: usize, value: &Val) -> bool {
        match (value, self.is_null(col_idx), metadata.col_layout(col_idx)) {
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

    /// Returns the old value if different from given index and new value.
    #[inline]
    fn different(
        &self,
        metadata: &TableMetadata,
        col_idx: usize,
        value: &Val,
    ) -> Option<(Val, Option<u16>)> {
        if !self.is_different(metadata, col_idx, value) {
            return None;
        }
        Some(self.clone_val_with_var_offset(metadata, col_idx))
    }

    /// Calculate delta between given values and current row.
    #[inline]
    fn calc_delta(&self, metadata: &TableMetadata, vals: &[Val]) -> Vec<UpdateCol> {
        let col_types = metadata.col_types();
        debug_assert!(col_types.len() == vals.len());
        vals.iter()
            .enumerate()
            .filter_map(|(col_idx, val)| {
                if self.is_different(metadata, col_idx, val) {
                    Some(UpdateCol {
                        idx: col_idx,
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

impl RowRead for Row<'_> {
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

impl RowRead for RowMut<'_> {
    #[inline]
    fn page(&self) -> &RowPage {
        self.page
    }

    #[inline]
    fn row_idx(&self) -> usize {
        self.row_idx
    }
}

impl RowMut<'_> {
    /// Update column by given index and value.
    #[inline]
    pub fn update_col(&mut self, col_idx: usize, value: &Val) {
        self.var_offset = self
            .page
            .update_col(self.row_idx, col_idx, value, self.var_offset, true);
    }

    /// Set null bit by given column index.
    #[inline]
    pub fn set_null(&mut self, col_idx: usize, null: bool) {
        self.page.set_null(self.row_idx, col_idx, null);
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

/// RowRecover is the row to recover in this page.
pub struct RowMutExclusive<'a> {
    page: &'a mut RowPage,
    row_idx: usize,
    var_offset: usize,
    var_end: usize,
}

impl RowRead for RowMutExclusive<'_> {
    #[inline]
    fn page(&self) -> &RowPage {
        self.page
    }

    #[inline]
    fn row_idx(&self) -> usize {
        self.row_idx
    }
}

impl RowMutExclusive<'_> {
    /// Update column by given index and value.
    #[inline]
    pub fn update_col(&mut self, col_idx: usize, value: &Val, old_exists: bool) {
        self.var_offset = self.page.update_col_exclusive(
            self.row_idx,
            col_idx,
            value,
            self.var_offset,
            old_exists,
        );
    }

    /// Finish row replace.
    #[inline]
    pub fn finish_insert(self) -> Recover {
        debug_assert!(self.var_offset == self.var_end);
        self.page.set_deleted(self.row_idx, false);
        Recover::Ok
    }

    #[inline]
    pub fn finish_update(self) -> Recover {
        debug_assert!(self.var_offset == self.var_end);
        debug_assert!(!self.page.is_deleted(self.row_idx));
        Recover::Ok
    }
}

#[allow(clippy::manual_div_ceil)]
#[inline]
const fn align8(len: usize) -> usize {
    (len + 7) / 8 * 8
}

#[allow(clippy::manual_div_ceil)]
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
#[allow(clippy::manual_div_ceil)]
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
pub fn var_len_for_insert(schema: &TableMetadata, cols: &[Val]) -> usize {
    schema
        .var_cols
        .iter()
        .map(|idx| match &cols[*idx] {
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

    use doradb_catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexOrder, IndexSpec,
    };
    use doradb_datatype::{Collation, PreciseType};
    use mem::MaybeUninit;
    use semistr::SemiStr;

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
        let metadata = TableMetadata::new(
            vec![ColumnSpec {
                column_name: SemiStr::new("id"),
                column_type: PreciseType::Int(4, false),
                column_attributes: ColumnAttributes::empty(),
            }],
            vec![IndexSpec {
                index_name: SemiStr::new("idx_tb1_id"),
                index_cols: vec![IndexKey {
                    col_no: 0,
                    order: IndexOrder::Asc,
                }],
                index_attributes: IndexAttributes::PK,
            }],
        );
        let mut page = create_row_page();
        page.init(100, 105, &metadata);
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
        let metadata = TableMetadata::new(
            vec![ColumnSpec {
                column_name: SemiStr::new("id"),
                column_type: PreciseType::Int(4, false),
                column_attributes: ColumnAttributes::empty(),
            }],
            vec![IndexSpec {
                index_name: SemiStr::new("idx_tb1_id"),
                index_cols: vec![IndexKey::new(0)],
                index_attributes: IndexAttributes::PK,
            }],
        );
        let mut page = create_row_page();
        page.init(100, 200, &metadata);
        assert!(page.header.row_count() == 0);
        assert!(page.header.col_count == 1);
        let insert = vec![Val::Byte8(1u64)];
        assert!(page.insert(&metadata, &insert).is_ok());
        assert!(page.header.row_count() == 1);
        let insert = vec![Val::Byte8(2u64)];
        assert!(page.insert(&metadata, &insert).is_ok());
        assert!(page.header.row_count() == 2);
    }

    #[test]
    fn test_row_page_read_write_row() {
        let metadata = TableMetadata::new(
            vec![
                ColumnSpec {
                    column_name: SemiStr::new("id"),
                    column_type: PreciseType::Int(4, false),
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("name"),
                    column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                    column_attributes: ColumnAttributes::empty(),
                },
            ],
            vec![IndexSpec {
                index_name: SemiStr::new("idx_tb1_id"),
                index_cols: vec![IndexKey::new(0)],
                index_attributes: IndexAttributes::PK,
            }],
        );
        let mut page = create_row_page();
        page.init(100, 200, &metadata);

        let insert = vec![Val::from(1_000_000i32), Val::from("hello")];
        assert!(page.insert(&metadata, &insert).is_ok());

        let row1 = page.row(0);
        assert!(row1.row_id() == 100);
        assert!(*row1.val::<Byte4Val>(0) as i32 == 1_000_000i32);
        assert!(row1.var(1) == b"hello");

        let insert = vec![
            Val::from(2_000_000i32),
            Val::from("this value is not inline"),
        ];
        assert!(page.insert(&metadata, &insert).is_ok());

        let row2 = page.row(1);
        assert!(row2.row_id() == 101);
        assert!(*row2.val::<Byte4Val>(0) as i32 == 2_000_000i32);
        let s = row2.var(1);
        println!("len={:?}, s={:?}", s.len(), str::from_utf8(&s[..24]));
        assert!(row2.var(1) == b"this value is not inline");

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
        assert!(page.update(&metadata, row_id, &update).is_ok());
    }

    #[test]
    fn test_row_page_crud() {
        let schema = TableMetadata::new(
            vec![
                ColumnSpec {
                    column_name: SemiStr::new("col1"),
                    column_type: PreciseType::Int(1, true),
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("col2"),
                    column_type: PreciseType::Int(2, true),
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("col3"),
                    column_type: PreciseType::Int(4, true),
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("col4"),
                    column_type: PreciseType::Int(8, true),
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("col5"),
                    column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                    column_attributes: ColumnAttributes::empty(),
                },
            ],
            vec![IndexSpec {
                index_name: SemiStr::new("idx_tb1_col3"),
                index_cols: vec![IndexKey::new(2)],
                index_attributes: IndexAttributes::PK,
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
            let new = MaybeUninit::<RowPage>::zeroed();
            new.assume_init()
        }
    }
}
