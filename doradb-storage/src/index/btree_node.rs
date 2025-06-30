//! B+Tree index is the most commonly used data structure for database indexing.
//! This module provide an implementation of B+Tree backed by buffer pool and hybrid latch.

use crate::buffer::page::{BufferPage, PageID, PAGE_SIZE};
use crate::index::btree::{BTreeDelete, BTreeUpdate, BTreeValue};
use crate::row::{RowID, INVALID_ROW_ID};
use crate::trx::TrxID;
use std::cmp::Ordering;
use std::mem;

const _: () = assert!(mem::size_of::<BTreeHeader>() % mem::size_of::<Slot>() == 0);

const _: () = assert!(mem::size_of::<BTreeNode>() == PAGE_SIZE);

/// BTree header, in total 64 bytes.
/// Memory layout as below:
///                          
/// ┌──────────────────────┐
/// │ height(2)            │
/// ├──────────────────────┤
/// │ count(2)             │
/// ├──────────────────────┤
/// │ start_offset(2)      │
/// ├──────────────────────┤
/// │ end_offset(2)        │
/// ├──────────────────────┤
/// │ timestamp(8)         │
/// ├──────────────────────┤
/// │ effective_space(2)   │
/// ├──────────────────────┤
/// │ prefix_len(2)        │
/// ├──────────────────────┤
/// │ initialized(1)       │
/// ├──────────────────────┤
/// │ padding(3)           │
/// ├──────────────────────┤
/// │ lower fence slot(8)  │
/// ├──────────────────────┤
/// │ lower fence value(8) │
/// ├──────────────────────┤
/// │ upper fence slot(8)  │
/// └──────────────────────┘
///                          
/// Lower fence is not located in header but the first slot at beginning of
/// node body. Lower fence is always the first one inserted into node
/// and it can be valid or invalid(deleted).
#[repr(C)]
pub struct BTreeHeader {
    /// Height of the node.
    /// 0 means leaf.
    height: u16,
    /// Count of entries.
    /// For leaf node, it's key-value pairs.
    /// For branch node, it's key-pointer pairs.
    count: u16,
    /// offset of next available entry.
    start_offset: u16,
    /// offset of variable length data at end of page.
    /// free space is calculated by subtracting end_offset and
    /// start_offset.
    end_offset: u16,
    /// Snapshot timestamp of latest write transaction.
    /// This field has two usage:
    /// 1. On leaf page, support covering index to eliminate MVCC
    ///    visibility check with best effort.
    /// 2. On root page, support Copy-on-Write batch inserts.
    ts: TrxID,
    /// Effective space used of b-tree node body.
    /// In case of update, there might be wasted space if key length changes.
    /// If key is longer than original one, orignal key length + 8 bytes are wasted.
    /// If key is shorter than original one, the subtraction of key length are wasted.
    /// We records effective space used to allow compaction if space is insufficient
    /// when insert/update/delete happens.
    ///
    /// Note: both effective space and end offset should always include common prefix,
    /// lower fence data and high fence data.
    effective_space: u16,
    /// Common prefix length of all values in this node.
    prefix_len: u16,
    /// Whether this node is initialized. This flag is used for SMO validity check.
    initialized: bool,
    /// Padding for memory layout.
    _padding: [u8; 3],
    /// Lower fence key of this node, inclusive.
    lower_fence: Slot,
    /// Value of lower fence, used only in branch node,
    /// which is child page id.
    lower_fence_value: PageID,
    /// Upper fence key of this node, exclusive.
    /// If offset is zero, upper fence is None.
    upper_fence: Slot,
}

const KEY_HEAD_LEN: usize = 4;

const _: () = assert!(mem::size_of::<Slot>() == 8);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Slot {
    len: u16,
    offset: u16,
    head: [u8; KEY_HEAD_LEN],
}

impl Slot {
    /// Returns an inline slot.
    /// This can only be used to create upper fence, because
    /// all other slot will have at least 8-byte value and
    /// does not fit inline requirement.
    #[inline]
    pub fn inline(value: &[u8]) -> Self {
        debug_assert!(value.len() <= KEY_HEAD_LEN);
        let mut head = [0u8; KEY_HEAD_LEN];
        head[..value.len()].copy_from_slice(value);
        Slot {
            len: value.len() as u16,
            offset: 0,
            head,
        }
    }

    /// Returns whether the slot is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

pub type BTreeBody = [u8; PAGE_SIZE - mem::size_of::<BTreeHeader>()];

/// BTree node. This is the fixed-size B-Tree node, including leaf and branch.
/// Memory layout as below:
///                                                     
/// ┌───────────────────────┐                           
/// │ header                │                           
/// ├───────────────────────┤ ◄─ start offset(original)
/// │ slot 0                │                           
/// ├───────────────────────┤                           
/// │ slot 1                │                           
/// ├───────────────────────┤                           
/// │ ...                   │                           
/// ├───────────────────────┤                           
/// │ slot N(N=count-1)     │                           
/// ├───────────────────────┤ ◄─ start offset(current)  
/// │                       │                           
/// │ free space            │                           
/// │                       │                           
/// ├───────────────────────┤ ◄─ end offset(current)    
/// │ slot data N(N=coun1)  │                           
/// ├───────────────────────┤                           
/// │ ...                   │                           
/// ├───────────────────────┤                           
/// │ slot data 1           │                           
/// ├───────────────────────┤                           
/// │ slot data 0           │                           
/// ├───────────────────────┤ ◄─ end offset(original)   
/// │ upper fence key data  │                           
/// ├───────────────────────┤                           
/// │ lower fence key data  │                           
/// ├───────────────────────┤                           
/// │ common prefix         │                           
/// └───────────────────────┘                           
///                                                     
/// Lower fence is inclusive, and just slot 0, but initially marked as invalid.
/// High fence is exclusive.
///
/// Every slot occupies 8 bytes.
///                                        
/// ┌──────────┬─────────────┬───────────┐
/// │key len(2)│key offset(2)│key head(4)│
/// └──────────┴─────────────┴───────────┘
///                                        
/// Every slot data occupies X+8 bytes, X is key length, 8 is value length,
/// which is always 8 for row id(in leaf node) or page id(in branch node).
///                         
/// ┌───────────┬────────────────────┐
/// │key data(x)│row id or page id(8)│
/// └───────────┴────────────────────┘
///                         
/// This BTree index is only for secondary index, of which value can only be
/// row id or page id. In future, more columns may be included, in order to
/// support "wider" covering index, but currently we do not consider it.
///
#[repr(C)]
pub struct BTreeNode {
    header: BTreeHeader,
    body: BTreeBody,
}

impl BufferPage for BTreeNode {}

impl BTreeNode {
    /// Initialize B-Tree node.
    /// Common prefix, lower fence and upper fence are initialized using this method
    /// and will be immutable
    #[inline]
    pub fn init(
        &mut self,
        height: u16,
        ts: TrxID,
        lower_fence: &[u8],
        lower_fence_value: PageID,
        upper_fence: &[u8],
    ) {
        self.header.height = height;
        self.header.count = 0;
        self.header.start_offset = 0;
        self.header.end_offset = mem::size_of::<BTreeBody>() as u16;
        self.header.ts = ts;
        self.header.effective_space = 0;

        // 1. Calculate and set common prefix.
        let prefix_len = common_prefix_len(lower_fence, upper_fence);
        self.set_common_prefix(&lower_fence[..prefix_len]);

        // 2. Assign lower fence and its value.
        let lf = &lower_fence[prefix_len..];
        if lf.len() <= KEY_HEAD_LEN {
            self.header.lower_fence = Slot::inline(lf);
        } else {
            self.header.lower_fence = self.new_slot_without_value(lower_fence);
        }
        self.header.lower_fence_value = lower_fence_value;

        // 3. Assign upper fence and its value.
        let uf = &upper_fence[prefix_len..];
        if uf.len() <= KEY_HEAD_LEN {
            self.header.upper_fence = Slot::inline(uf);
        } else {
            // for upper fence, we do not store any value for it.
            self.header.upper_fence = self.new_slot_without_value(upper_fence);
        }

        // 4. Mark as initialized.
        self.header.initialized = true;
    }

    /// Returns whether this node is leaf node.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.header.height == 0
    }

    /// Returns height of this node.
    #[inline]
    pub fn height(&self) -> usize {
        self.header.height as usize
    }

    /// Returns the timestamp of latest write on this page.
    #[inline]
    pub fn ts(&self) -> TrxID {
        self.header.ts
    }

    /// Update timestamp on this page.
    #[inline]
    pub fn update_ts(&mut self, ts: TrxID) {
        if self.header.ts < ts {
            self.header.ts = ts;
        }
    }

    /// Returns whether the node has no upper fence.
    #[inline]
    pub fn has_no_upper_fence(&self) -> bool {
        self.header.prefix_len == 0 && self.header.upper_fence.len == 0
    }

    /// Checks whether the input key is within boundary.
    #[inline]
    pub fn within_boundary(&self, key: &[u8]) -> bool {
        if key.len() < self.header.prefix_len as usize {
            // if key is shorter than prefix, must be out of range.
            return false;
        }
        let k = &key[self.header.prefix_len as usize..];
        // inclusive lower fence.
        match self.cmp_key_without_prefix(k, &self.header.lower_fence) {
            Ordering::Equal => true,
            Ordering::Less => false,
            Ordering::Greater => {
                if self.has_no_upper_fence() {
                    return true;
                }
                // exclusive upper fence.
                match self.cmp_key_without_prefix(k, &self.header.upper_fence) {
                    Ordering::Greater | Ordering::Equal => false,
                    Ordering::Less => true,
                }
            }
        }
    }

    #[inline]
    fn cmp_key_without_prefix(&self, key: &[u8], slot: &Slot) -> Ordering {
        if slot.len as usize <= KEY_HEAD_LEN {
            return key.cmp(&slot.head[..slot.len as usize]);
        }
        let k2 = unsafe {
            std::slice::from_raw_parts(self.body().add(slot.offset as usize), slot.len as usize)
        };
        key.cmp(k2)
    }

    /// Insert a new key value pair to current node.
    /// Returns inserted slot number.
    #[inline]
    pub fn insert<V: BTreeValue>(&mut self, key: &[u8], value: V) -> usize {
        debug_assert!(self.can_insert(key));
        let (slot_idx, overwrite) = match self.search::<V>(key) {
            SearchResult::GreaterThan(idx) => {
                // insert at next position.
                (idx + 1, false)
            }
            SearchResult::EqualDeleted(idx, _) => {
                // same key but already deleted.
                (idx, true)
            }
            SearchResult::LessThanAllSlots => {
                debug_assert!(self.within_boundary(key));
                (0, false)
            }
            SearchResult::LessThanLowerFence | SearchResult::GreaterEqualUpperFence => {
                unreachable!()
            }
            SearchResult::Equal(..) => {
                // duplicate key found.
                panic!("BTreeNode does not support duplicate key");
            }
        };
        let slot = self.new_slot_with_value(key, value);
        unsafe {
            self.insert_slot_at(slot_idx, slot, overwrite);
        }
        slot_idx
    }

    #[inline]
    pub fn insert_at<V: BTreeValue>(&mut self, idx: usize, key: &[u8], value: V) {
        debug_assert!(idx <= self.header.count as usize);
        let slot = self.new_slot_with_value(key, value);
        unsafe {
            self.insert_slot_at(idx, slot, false);
        }
    }

    /// Delete an existing key value pair in current node.
    /// If key or value does not match, returns false.
    #[inline]
    pub fn delete<V: BTreeValue>(&mut self, key: &[u8], value: V) -> BTreeDelete {
        let idx = match self.search::<V>(key) {
            SearchResult::Equal(idx, v) | SearchResult::EqualDeleted(idx, v) => {
                if v != value {
                    return BTreeDelete::ValueMismatch;
                }
                idx
            }
            _ => return BTreeDelete::NotFound,
        };
        self.delete_at::<V>(idx);
        BTreeDelete::Ok
    }

    /// Mark an existing key value pair in current node as deleted.
    /// If key or value does not match, returns false.
    #[inline]
    pub fn mark_as_deleted<V: BTreeValue>(&mut self, key: &[u8], value: V) -> BTreeUpdate<V> {
        let idx = match self.search::<V>(key) {
            SearchResult::Equal(idx, v) | SearchResult::EqualDeleted(idx, v) => {
                if v != value {
                    return BTreeUpdate::ValueMismatch(v);
                }
                idx
            }
            _ => return BTreeUpdate::NotFound,
        };
        let old_value = self.update_value(idx, value.deleted());
        BTreeUpdate::Ok(old_value)
    }

    /// Update an existing key value pair in current node to new value.
    /// The delete bit is ignored when comparing old values.
    /// That means old value can be marked as deleted, any new value may be
    /// an un-deleted one. In such case, we "undo" the deletion, which
    /// is common when a transaction rollback all its changes including
    /// modification on index.
    #[inline]
    pub fn update<V: BTreeValue>(
        &mut self,
        key: &[u8],
        old_value: V,
        new_value: V,
    ) -> BTreeUpdate<V> {
        let idx = match self.search::<V>(key) {
            SearchResult::Equal(idx, v) | SearchResult::EqualDeleted(idx, v) => {
                if old_value.value() != v {
                    return BTreeUpdate::ValueMismatch(v);
                }
                idx
            }
            _ => return BTreeUpdate::NotFound,
        };
        let old_value = self.update_value(idx, new_value);
        BTreeUpdate::Ok(old_value)
    }

    #[inline]
    fn delete_at<V: BTreeValue>(&mut self, idx: usize) {
        let payload_len = self.payload_len::<V>(idx);
        if idx + 1 < self.header.count as usize {
            // not last row, shift one slot left.
            unsafe {
                let dst = (self.body_mut() as *mut Slot).add(idx);
                let src = dst.add(1);
                std::ptr::copy(src, dst, self.header.count as usize - idx - 1);
            }
        }
        self.header.count -= 1;
        self.header.start_offset -= mem::size_of::<Slot>() as u16;
        // Note: we do not release payload of the deleted key, because
        // it requires relocate payloads of other keys.
        // But we decrease effective space so if we want more space,
        // we know exactly how much remains after compaction.
        self.header.effective_space -= (payload_len + mem::size_of::<Slot>()) as u16;
    }

    #[inline]
    fn payload_len<V: BTreeValue>(&self, idx: usize) -> usize {
        let slot = self.slot(idx);
        if slot.len as usize <= KEY_HEAD_LEN {
            mem::size_of::<V>()
        } else {
            slot.len as usize + mem::size_of::<V>()
        }
    }

    /// Insert key value to the end of node.
    #[inline]
    pub fn insert_at_end<V: BTreeValue>(&mut self, key: &[u8], value: V) {
        debug_assert!(self.can_insert(key));
        debug_assert!(self.within_boundary(key));
        let slot = self.new_slot_with_value(key, value);
        unsafe {
            self.insert_slot_at(self.header.count as usize, slot, false);
        }
        // single entry or in order.
        debug_assert!(
            self.header.count == 1
                || self.cmp_slot_key(
                    self.header.count as usize - 1,
                    self.header.count as usize - 2
                ) == Ordering::Greater
        );
    }

    /// Lookup child by given key.
    /// Returns value of the last key no more than input.
    /// This method is used when searching key from root to path.
    #[inline]
    pub fn lookup_child(&self, key: &[u8]) -> Option<PageID> {
        match self.search(key) {
            SearchResult::GreaterThan(idx) => Some(self.value(idx as usize)),
            SearchResult::Equal(_, value) => Some(value),
            SearchResult::LessThanAllSlots => {
                if self.header.lower_fence_value == PageID::INVALID_VALUE {
                    None
                } else {
                    Some(self.header.lower_fence_value)
                }
            }
            SearchResult::EqualDeleted(..)
            | SearchResult::LessThanLowerFence
            | SearchResult::GreaterEqualUpperFence => None,
        }
    }

    /// Returns the entry count.
    #[inline]
    pub fn count(&self) -> usize {
        self.header.count as usize
    }

    /// Returns prefix length of this node.
    #[inline]
    pub fn prefix_len(&self) -> usize {
        self.header.prefix_len as usize
    }

    /// Find the separator when the node is full.
    /// Separator strategy may vary in different scenarios.
    /// e.g. if data is inserted in ascending order, the best
    /// separator is to split the node into a large left side
    /// and small right side.
    #[inline]
    pub fn find_separator(&self) -> usize {
        // todo: implements different strategy.
        self.count() / 2 + 1
    }

    /// Create a new slot with given key and value.
    #[inline]
    fn new_slot_with_value<V: BTreeValue>(&mut self, key: &[u8], value: V) -> Slot {
        debug_assert!(key.len() >= self.header.prefix_len as usize);
        let k = &key[self.header.prefix_len as usize..];
        if k.len() <= KEY_HEAD_LEN {
            // Only value inserted at end of page.
            // Copy head.
            let mut head = [0u8; KEY_HEAD_LEN];
            head[..k.len()].copy_from_slice(k);
            // Copy value.
            self.header.end_offset -= mem::size_of::<V>() as u16;
            unsafe {
                (self.body_mut().add(self.header.end_offset as usize) as *mut V)
                    .write_unaligned(value);
            }
            self.header.effective_space += mem::size_of::<V>() as u16;
            return Slot {
                len: k.len() as u16,
                offset: self.header.end_offset,
                head,
            };
        }
        // Key suffix and value inserted at end of page.
        // copy head.
        let mut head = [0u8; KEY_HEAD_LEN];
        head.copy_from_slice(&k[..KEY_HEAD_LEN]);
        // copy value.
        self.header.end_offset -= (k.len() + mem::size_of::<V>()) as u16;
        unsafe {
            let dst = self.body_mut().add(self.header.end_offset as usize);
            std::ptr::copy_nonoverlapping(k.as_ptr(), dst, k.len());
            let dst = dst.add(k.len());
            (dst as *mut V).write_unaligned(value);
        }
        self.header.effective_space += (k.len() + mem::size_of::<V>()) as u16;
        Slot {
            len: k.len() as u16,
            offset: self.header.end_offset,
            head,
        }
    }

    #[inline]
    fn new_slot_without_value(&mut self, key: &[u8]) -> Slot {
        debug_assert!(key.len() >= self.header.prefix_len as usize);
        let k = &key[self.header.prefix_len as usize..];
        if k.len() <= KEY_HEAD_LEN {
            // No value inserted to page.
            // Copy head.
            let mut head = [0u8; KEY_HEAD_LEN];
            head[..k.len()].copy_from_slice(k);
            return Slot {
                len: k.len() as u16,
                offset: self.header.end_offset,
                head,
            };
        }
        // Key suffix inserted at end of page.
        // copy head.
        let mut head = [0u8; KEY_HEAD_LEN];
        head.copy_from_slice(&k[..KEY_HEAD_LEN]);
        // copy key suffix.
        self.header.end_offset -= k.len() as u16;
        unsafe {
            let dst = self.body_mut().add(self.header.end_offset as usize);
            std::ptr::copy_nonoverlapping(k.as_ptr(), dst, k.len());
        }
        self.header.effective_space += k.len() as u16;
        Slot {
            len: k.len() as u16,
            offset: self.header.end_offset,
            head,
        }
    }

    /// Returns whether the node has space for the insert.
    #[inline]
    pub(super) fn can_insert(&self, key: &[u8]) -> bool {
        let space_needed = self.space_needed(key);
        // todo: compact if neccessary.
        space_needed <= self.free_space()
    }

    /// Returns start pointer of body.
    #[inline]
    fn body(&self) -> *const u8 {
        self.body.as_ptr()
    }

    #[inline]
    unsafe fn body_end(&self) -> *const u8 {
        self.body().add(mem::size_of::<BTreeBody>())
    }

    #[inline]
    unsafe fn body_end_mut(&mut self) -> *mut u8 {
        self.body_mut().add(mem::size_of::<BTreeBody>())
    }

    /// Returns mutable start pointer of body.
    #[inline]
    fn body_mut(&mut self) -> *mut u8 {
        self.body.as_mut_ptr()
    }

    /// Returns how many bytes a new key value pair is needed.
    #[inline]
    fn space_needed(&self, key: &[u8]) -> usize {
        debug_assert!(key.len() >= self.header.prefix_len as usize);
        let klen = key.len() - self.header.prefix_len as usize;
        // We can inline key inside key head.
        // When extracting key, we need to maintain buffer to copy key out.
        mem::size_of::<Slot>()
            + mem::size_of::<RowID>()
            + if klen <= KEY_HEAD_LEN { 0 } else { klen }
    }

    /// Returns free space.
    #[inline]
    fn free_space(&self) -> usize {
        (self.header.end_offset - self.header.start_offset) as usize
    }

    /// Returns free space after compaction.
    #[inline]
    fn free_space_after_compaction(&self) -> usize {
        mem::size_of::<BTreeBody>() - self.header.effective_space as usize
    }

    /// Returns slice of slots.
    #[inline]
    fn slots(&self) -> &[Slot] {
        let len = self.header.count as usize;
        unsafe { std::slice::from_raw_parts(self.body() as *const Slot, len) }
    }

    /// Returns reference to slot at given position.
    #[inline]
    fn slot(&self, idx: usize) -> &Slot {
        debug_assert!(idx < self.header.count as usize);
        unsafe { self.slot_unchecked(idx) }
    }

    #[inline]
    unsafe fn slot_unchecked(&self, idx: usize) -> &Slot {
        &*(self.body() as *const Slot).add(idx)
    }

    /// insert slot at given position.
    /// If overwrite is set to true, the old value is overwritten.
    #[inline]
    unsafe fn insert_slot_at(&mut self, idx: usize, slot: Slot, overwrite: bool) {
        let dst = (self.body_mut() as *mut Slot).add(idx);
        if !overwrite {
            if idx < self.header.count as usize {
                // shift all elements starting from destination by one position.
                let next = dst.add(1);
                std::ptr::copy(dst, next, self.header.count as usize - idx);
            }
            self.header.count += 1;
            self.header.start_offset += mem::size_of::<Slot>() as u16;
            self.header.effective_space += mem::size_of::<Slot>() as u16;
        }
        *dst = slot;
    }

    /// Returns common prefix of all values in this node.
    /// The common prefix is stored at end of the node.
    #[inline]
    fn common_prefix(&self) -> &[u8] {
        unsafe {
            let len = self.header.prefix_len as usize;
            if len == 0 {
                return &[];
            }
            std::slice::from_raw_parts(self.body_end().sub(len), len)
        }
    }

    #[inline]
    fn set_common_prefix(&mut self, common_prefix: &[u8]) {
        let l = common_prefix.len();
        self.header.prefix_len = l as u16;
        if l > 0 {
            unsafe {
                let dst = self.body_end_mut().sub(l);
                std::ptr::copy_nonoverlapping(common_prefix.as_ptr(), dst, l);
            }
            self.header.end_offset -= l as u16;
            self.header.effective_space += l as u16;
        }
    }

    /// Search given key in current node.
    #[inline]
    pub(super) fn search<V: BTreeValue>(&self, key: &[u8]) -> SearchResult<V> {
        let prefix_len = self.header.prefix_len as usize;
        if prefix_len == 0 {
            return self.search_without_prefix(key);
        }
        // Key is shorter than prefix, compare and return.
        if key.len() < prefix_len {
            let l = key.len().min(prefix_len);
            let prefix = self.common_prefix();
            return match key[..l].cmp(&prefix[..l]) {
                Ordering::Greater => {
                    if self.has_no_upper_fence() {
                        SearchResult::GreaterThan(self.header.count as usize - 1)
                    } else {
                        SearchResult::GreaterEqualUpperFence
                    }
                }
                Ordering::Less | Ordering::Equal => SearchResult::LessThanLowerFence,
            };
        }
        // Check if prefix matches.
        return match key[..prefix_len].cmp(self.common_prefix()) {
            Ordering::Greater => {
                if self.has_no_upper_fence() {
                    SearchResult::GreaterThan(self.header.count as usize - 1)
                } else {
                    SearchResult::GreaterEqualUpperFence
                }
            }
            Ordering::Less => SearchResult::LessThanLowerFence,
            Ordering::Equal => self.search_without_prefix(&key[prefix_len..]),
        };
    }

    /// Search without prefix. The prefix of input key should be trimed.
    #[inline]
    fn search_without_prefix<V: BTreeValue>(&self, k: &[u8]) -> SearchResult<V> {
        let body = self.body();
        match self.slots().binary_search_by(|s| {
            if s.len as usize <= KEY_HEAD_LEN {
                let sk = &s.head[..s.len as usize];
                sk.cmp(k)
            } else {
                let sk = unsafe {
                    std::slice::from_raw_parts(body.add(s.offset as usize), s.len as usize)
                };
                sk.cmp(k)
            }
        }) {
            Ok(idx) => {
                let value = self.value::<V>(idx);
                if value.is_deleted() {
                    SearchResult::EqualDeleted(idx, value.value())
                } else {
                    SearchResult::Equal(idx, value)
                }
            }
            Err(idx) => {
                if idx == 0 {
                    // input key is less than the first slot key.
                    SearchResult::LessThanAllSlots
                } else {
                    SearchResult::GreaterThan(idx - 1)
                }
            }
        }
    }

    #[inline]
    pub(super) fn key(&self, idx: usize) -> Vec<u8> {
        debug_assert!(idx < self.header.count as usize);
        let mut res = vec![];
        self.extract_key(idx, &mut res);
        res
    }

    #[inline]
    pub(super) fn lower_fence_key(&self) -> Vec<u8> {
        let mut res = vec![];
        self.extract_slot_key(&self.header.lower_fence, &mut res);
        res
    }

    #[inline]
    pub(super) fn lower_fence_value(&self) -> PageID {
        self.header.lower_fence_value
    }

    #[inline]
    pub(super) fn upper_fence_key(&self) -> Vec<u8> {
        if self.has_no_upper_fence() {
            return vec![];
        }
        let mut res = vec![];
        self.extract_slot_key(&self.header.upper_fence, &mut res);
        res
    }

    /// Extract key into buffer.
    /// Previous content in buffer is discarded.
    #[inline]
    pub(super) fn extract_key(&self, idx: usize, res: &mut Vec<u8>) {
        debug_assert!(idx < self.header.count as usize);
        let slot = self.slot(idx);
        self.extract_slot_key(slot, res);
    }

    #[inline]
    fn extract_slot_key(&self, slot: &Slot, res: &mut Vec<u8>) {
        let len = (self.header.prefix_len + slot.len) as usize;
        res.clear();
        res.reserve(len);
        res.extend_from_slice(self.common_prefix());
        if slot.len as usize <= KEY_HEAD_LEN {
            res.extend_from_slice(&slot.head[..slot.len as usize]);
        } else {
            unsafe {
                let dst = res.as_mut_ptr().add(self.header.prefix_len as usize);
                res.set_len(len);
                std::ptr::copy_nonoverlapping(
                    self.body().add(slot.offset as usize),
                    dst,
                    slot.len as usize,
                );
            }
        }
    }

    #[inline]
    pub(super) fn value<V: BTreeValue>(&self, idx: usize) -> V {
        debug_assert!(idx < self.header.count as usize);
        let slot = self.slot(idx);
        let offset = if slot.len as usize <= KEY_HEAD_LEN {
            // key is inlined.
            slot.offset
        } else {
            // should shift key length.
            slot.offset + slot.len
        } as usize;
        unsafe {
            let ptr = self.body().add(offset);
            std::ptr::read_unaligned::<V>(ptr as *const V)
        }
    }

    /// Returns all values in this node.
    #[inline]
    pub(super) fn values<V: BTreeValue>(&self) -> Vec<V> {
        (0..self.header.count as usize)
            .map(|idx| self.value(idx))
            .collect()
    }

    #[inline]
    pub(super) fn update_value<V: BTreeValue>(&mut self, idx: usize, value: V) -> V {
        debug_assert!(idx < self.header.count as usize);
        let slot = self.slot(idx);
        let offset = if slot.len as usize <= KEY_HEAD_LEN {
            // key is inlined.
            slot.offset
        } else {
            // should shift key length.
            slot.offset + slot.len
        } as usize;
        unsafe {
            let ptr = self.body_mut().add(offset);
            let old_value = std::ptr::read_unaligned::<V>(ptr as *const V);
            std::ptr::write_unaligned::<V>(ptr as *mut V, value);
            old_value
        }
    }

    /// Returns separate key of given position.
    /// If truncate is set to true, will truncate unneccessary suffix to
    /// identify key at idx-1 and idx.
    #[inline]
    pub(super) fn create_sep_key(&self, idx: usize, truncate: bool) -> Vec<u8> {
        debug_assert!(idx > 0);
        if !truncate {
            return self.key(idx);
        }
        let s1 = self.slot(idx); // separator key
        let s2 = self.slot(idx - 1); // preceding one.
        let l = s1.len.min(s2.len) as usize;
        if l <= KEY_HEAD_LEN {
            // compare head is enough
            for (i, (a, b)) in s1.head[..l].iter().zip(&s2.head[..l]).enumerate() {
                if a != b {
                    let mut res = Vec::<u8>::with_capacity(self.header.prefix_len as usize + i);
                    res.extend_from_slice(self.common_prefix());
                    res.extend_from_slice(&s1.head[..i + 1]);
                    return res;
                }
            }
            // As s1 is greater than s2, and the common part is identical.
            // s1 must be longer than s2.
            debug_assert!(s1.len > s2.len);
            let diff_len = l + 1;
            let mut res = Vec::<u8>::with_capacity(self.header.prefix_len as usize + diff_len);
            res.extend_from_slice(self.common_prefix());
            if diff_len <= KEY_HEAD_LEN {
                res.extend_from_slice(&s1.head[..diff_len]);
            } else {
                let src = unsafe {
                    std::slice::from_raw_parts(self.body().add(s1.offset as usize), diff_len)
                };
                res.extend_from_slice(src);
            }
            return res;
        }
        // compare key without prefix.
        unsafe {
            let k1 = std::slice::from_raw_parts(self.body().add(s1.offset as usize), l);
            let k2 = std::slice::from_raw_parts(self.body().add(s2.offset as usize), l);
            for (i, (a, b)) in k1[..l].iter().zip(&k2[..l]).enumerate() {
                if a != b {
                    let mut res = Vec::<u8>::with_capacity(self.header.prefix_len as usize + i);
                    res.extend_from_slice(self.common_prefix());
                    res.extend_from_slice(&k1[..i + 1]);
                    return res;
                }
            }
            debug_assert!(k1.len() > k2.len());
            let diff_len = l + 1;
            let mut res = Vec::<u8>::with_capacity(self.header.prefix_len as usize + diff_len);
            res.extend_from_slice(self.common_prefix());
            res.extend_from_slice(&k1[..diff_len]);
            res
        }
    }

    /// Extend slots from other node.
    #[inline]
    pub fn extend_slots_from<V: BTreeValue>(
        &mut self,
        src_node: &BTreeNode,
        src_slot_idx: usize,
        count: usize,
    ) {
        if self.header.prefix_len == src_node.header.prefix_len {
            // only allow copy to end of current node.
            let dst_slot_idx = self.header.count as usize;
            unsafe {
                // fast path to memcpy slots.
                let src_slot = (src_node.body() as *const Slot).add(src_slot_idx);
                let dst_slot = (self.body_mut() as *mut Slot).add(dst_slot_idx);
                std::ptr::copy_nonoverlapping(src_slot, dst_slot, count);
                {
                    // update space and offset.

                    let slot_space = (mem::size_of::<Slot>() * count) as u16;
                    self.header.start_offset += slot_space;
                    self.header.effective_space += slot_space;
                }

                // copy keys and values.
                let mut offset = self.header.end_offset as usize;
                for i in 0..count {
                    let s = &*src_slot.add(i);
                    let len = if s.len as usize <= KEY_HEAD_LEN {
                        mem::size_of::<V>()
                    } else {
                        s.len as usize + mem::size_of::<V>()
                    };
                    offset -= len;

                    std::ptr::copy_nonoverlapping(
                        src_node.body().add(s.offset as usize),
                        self.body_mut().add(offset),
                        len,
                    );

                    // update slot offset
                    let d = &mut *dst_slot.add(i);
                    d.offset = offset as u16;
                }
                // update space and offset.
                {
                    let payload_space = self.header.end_offset - offset as u16;
                    self.header.end_offset = offset as u16;
                    self.header.effective_space += payload_space;
                }
                self.header.count += count as u16;
            }
            return;
        }
        // Slow path to copy key value one by one
        let mut key = vec![]; // key buffer
        for idx in src_slot_idx..src_slot_idx + count {
            src_node.extract_key(idx, &mut key);
            let value = src_node.value::<V>(idx);
            self.insert_at_end(&key, value);
        }
    }

    #[inline]
    fn cmp_slot_key(&self, idx1: usize, idx2: usize) -> Ordering {
        let s1 = self.slot(idx1);
        let s2 = self.slot(idx2);
        let l = s1.len.min(s2.len) as usize;
        if l <= KEY_HEAD_LEN {
            match s1.head[..l].cmp(&s2.head[..l]) {
                Ordering::Greater => Ordering::Greater,
                Ordering::Less => Ordering::Less,
                Ordering::Equal => s1.len.cmp(&s2.len),
            }
        } else {
            unsafe {
                let k1 = std::slice::from_raw_parts(
                    self.body().add(s1.offset as usize),
                    s1.len as usize,
                );
                let k2 = std::slice::from_raw_parts(
                    self.body().add(s2.offset as usize),
                    s2.len as usize,
                );
                k1.cmp(k2)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SearchResult<V: BTreeValue> {
    /// Less than lower fence.
    LessThanLowerFence,
    /// Less than all values.
    LessThanAllSlots,
    /// Exactly match at given position.
    Equal(usize, V),
    /// Special case of existing entry.
    /// The value is marked as deleted.
    EqualDeleted(usize, V),
    /// Not found.
    /// Greater than key at given position but less than following one.
    GreaterThan(usize),
    /// Greater than or equal to upper fence.
    GreaterEqualUpperFence,
}

#[inline]
fn common_prefix_len(key1: &[u8], key2: &[u8]) -> usize {
    let l = key1.len().min(key2.len());
    match key1.iter().zip(key2).position(|(a, b)| a != b) {
        Some(idx) => idx,
        None => l,
    }
}

const BTREE_VALUE_U64_DELETE_BIT: u64 = 1u64 << 63;

impl BTreeValue for RowID {
    const INVALID_VALUE: Self = INVALID_ROW_ID;

    #[inline]
    fn deleted(self) -> Self {
        self | BTREE_VALUE_U64_DELETE_BIT
    }

    #[inline]
    fn value(self) -> Self {
        self & !BTREE_VALUE_U64_DELETE_BIT
    }

    #[inline]
    fn is_deleted(self) -> bool {
        self & BTREE_VALUE_U64_DELETE_BIT != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, FixedBufferPool};
    use crate::lifetime::StaticLifetime;

    #[test]
    fn test_btree_node_insert() {
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(64usize * 1024 * 1024).unwrap();

            {
                let mut page_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let node = page_guard.page_mut();
                node.init(0, 0, &[], !0, &[]);
                for i in 0u64..10 {
                    let k = i.to_be_bytes();
                    let slot_idx = node.insert(&k, i);
                    println!("inserted, slot_idx={}", slot_idx);
                }

                for i in 0u64..10 {
                    let k = i.to_be_bytes();
                    let res = node.search::<u64>(&k);
                    assert_eq!(res, SearchResult::Equal(i as usize, i));
                }

                let res = node.search::<u64>(&11u64.to_be_bytes());
                assert_eq!(res, SearchResult::GreaterThan(9));
            }

            unsafe {
                StaticLifetime::drop_static(buf_pool);
            }
        })
    }

    #[test]
    fn test_btree_node_delete() {
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(64usize * 1024 * 1024).unwrap();

            {
                let mut page_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let node = page_guard.page_mut();
                node.init(0, 0, &[], !0, &[]);

                // Insert test data
                for i in 0u64..10 {
                    let k = i.to_be_bytes();
                    node.insert(&k, i);
                }

                // Test normal delete
                assert_eq!(node.delete(&5u64.to_be_bytes(), 5), BTreeDelete::Ok);
                assert_eq!(
                    node.search::<u64>(&5u64.to_be_bytes()),
                    SearchResult::GreaterThan(4)
                );

                // Test delete non-existent key
                assert_eq!(node.delete(&15u64.to_be_bytes(), 15), BTreeDelete::NotFound);

                // Test value mismatch
                assert_eq!(
                    node.delete(&6u64.to_be_bytes(), 7),
                    BTreeDelete::ValueMismatch
                );
            }

            unsafe {
                StaticLifetime::drop_static(buf_pool);
            }
        })
    }

    #[test]
    fn test_btree_node_mark_as_deleted() {
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(64usize * 1024 * 1024).unwrap();

            {
                let mut page_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let node = page_guard.page_mut();
                node.init(0, 0, &[], !0, &[]);

                // Insert test data
                for i in 0u64..10 {
                    let k = i.to_be_bytes();
                    node.insert(&k, i);
                }

                // Test normal mark as deleted
                assert_eq!(
                    node.mark_as_deleted(&3u64.to_be_bytes(), 3),
                    BTreeUpdate::Ok(3)
                );
                match node.search::<u64>(&3u64.to_be_bytes()) {
                    SearchResult::EqualDeleted(_, v) => assert_eq!(v, 3),
                    _ => panic!("Expected EqualDeleted"),
                };

                // Test mark already deleted
                assert_eq!(
                    node.mark_as_deleted(&3u64.to_be_bytes(), 3),
                    BTreeUpdate::Ok(3.deleted())
                );

                // Test mark non-existent key
                assert_eq!(
                    node.mark_as_deleted(&15u64.to_be_bytes(), 15),
                    BTreeUpdate::NotFound
                );

                // Test value mismatch
                assert_eq!(
                    node.mark_as_deleted(&4u64.to_be_bytes(), 5),
                    BTreeUpdate::ValueMismatch(4)
                );
            }

            unsafe {
                StaticLifetime::drop_static(buf_pool);
            }
        })
    }

    #[test]
    fn test_btree_node_update() {
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(64usize * 1024 * 1024).unwrap();

            {
                let mut page_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let node = page_guard.page_mut();
                node.init(0, 0, &[], !0, &[]);

                // Insert test data
                for i in 0u64..10 {
                    let k = i.to_be_bytes();
                    node.insert(&k, i);
                }

                // Test normal update
                assert_eq!(node.update(&5u64.to_be_bytes(), 5, 50), BTreeUpdate::Ok(5));
                assert_eq!(
                    node.search::<u64>(&5u64.to_be_bytes()),
                    SearchResult::Equal(5, 50)
                );

                // Test update deleted entry
                node.mark_as_deleted(&6u64.to_be_bytes(), 6);
                assert_eq!(
                    node.update(&6u64.to_be_bytes(), 6, 60),
                    BTreeUpdate::Ok(6.deleted())
                );
                assert_eq!(
                    node.search::<u64>(&6u64.to_be_bytes()),
                    SearchResult::Equal(6, 60)
                );

                // Test update non-existent key
                assert_eq!(
                    node.update(&15u64.to_be_bytes(), 15, 150),
                    BTreeUpdate::NotFound
                );

                // Test old value mismatch
                assert_eq!(
                    node.update(&7u64.to_be_bytes(), 8, 70),
                    BTreeUpdate::ValueMismatch(7)
                );
            }

            unsafe {
                StaticLifetime::drop_static(buf_pool);
            }
        })
    }
}
