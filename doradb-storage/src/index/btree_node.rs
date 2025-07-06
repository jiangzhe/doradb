//! B+Tree index is the most commonly used data structure for database indexing.
//! This module provide an implementation of B+Tree backed by buffer pool and hybrid latch.

use crate::buffer::page::{BufferPage, PageID, PAGE_SIZE};
use crate::index::btree::{BTreeDelete, BTreeUpdate, BTreeValue};
use crate::row::{RowID, INVALID_ROW_ID};
use crate::trx::TrxID;
use smallvec::SmallVec;
use std::cmp;
use std::cmp::Ordering;
use std::mem::{self, MaybeUninit};

const _: () = assert!(mem::size_of::<BTreeHeader>() % mem::size_of::<Slot>() == 0);

const _: () = assert!(mem::size_of::<BTreeNode>() == PAGE_SIZE);

/// BTree header, in total 48 bytes.
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
/// │ lower fence slot(8)  │
/// ├──────────────────────┤
/// │ lower fence value(8) │
/// ├──────────────────────┤
/// │ upper fence slot(8)  │
/// ├──────────────────────┤
/// │ effective_space(4)   │
/// ├──────────────────────┤
/// │ initialized(1)       │
/// ├──────────────────────┤
/// │ padding(1)           │
/// ├──────────────────────┤
/// │ prefix_len(2)        │
/// ├──────────────────────┤
/// │ inline prefix(16)    │
/// └──────────────────────┘
///                          
/// Lower fence is not located in header but the first slot at beginning of
/// node body. Lower fence is always the first one inserted into node
/// and it can be valid or invalid(deleted).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
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
    /// Lower fence key of this node, inclusive.
    lower_fence: Slot,
    /// Value of lower fence, used only in branch node,
    /// which is child page id.
    lower_fence_value: PageID,
    /// Upper fence key of this node, exclusive.
    /// If offset is zero, upper fence is None.
    upper_fence: Slot,
    /// Effective space used of b-tree node.
    /// It includes header space, slot space and payload space.
    /// In insert-only scenario, used space is equal to effective space.
    /// In case of update, there might be wasted space if key length changes.
    /// If key is longer than original one, orignal key and payload space are wasted.
    /// In case of delete, payload space is wasted.
    /// These actions will also modify effective space, so we can estimate fast the
    /// result space if a compaction is executed.
    effective_space: u32,
    /// Whether this node is initialized. This flag is used for SMO validity check.
    initialized: bool,
    /// Padding for memory layout.
    _padding: [u8; 1],
    /// Common prefix length of all values in this node.
    prefix_len: u16,
    /// Inline prefix data.
    /// If prefix is less than 16 bytes, it will be stored here instead of end of page.
    inline_prefix: [u8; INLINE_PREFIX_LEN],
}

const INLINE_PREFIX_LEN: usize = 16;

const KEY_HEAD_LEN: usize = 4;

pub type KeyHead = [u8; KEY_HEAD_LEN];

const _: () = assert!(mem::size_of::<Slot>() == 8);

pub type KeyVec = SmallVec<[u8; 16]>;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Slot {
    len: u16,
    offset: u16,
    head: KeyHead,
}

impl Slot {
    /// Returns an inline slot.
    /// This can only be used to create upper fence, because
    /// all other slot will have at least 8-byte value and
    /// does not fit inline requirement.
    #[inline]
    pub fn inline(k: &[u8]) -> Self {
        debug_assert!(k.len() <= KEY_HEAD_LEN);
        Slot {
            len: k.len() as u16,
            offset: 0,
            head: head(k),
        }
    }

    /// Returns whether the slot is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// convert head to u32 value.
    #[inline]
    pub fn head_as_u32(&self) -> u32 {
        u32::from_be_bytes(self.head)
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
/// │ common prefix(outline)│                           
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
#[derive(Clone)]
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
        // Include header in effective space.
        self.header.effective_space = mem::size_of::<BTreeHeader>() as u32;

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

    /// Returns effective space used by this node,
    /// including node header.
    #[inline]
    pub fn effective_space(&self) -> usize {
        self.header.effective_space as usize
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
    fn cmp_key_without_prefix(&self, k: &[u8], slot: &Slot) -> Ordering {
        match head(k).cmp(&slot.head) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                let l = k.len().min(slot.len as usize);
                if l <= KEY_HEAD_LEN {
                    k.len().cmp(&(slot.len as usize))
                } else {
                    let k2 = self.k(slot);
                    k.cmp(k2)
                }
            }
        }
    }

    /// Insert a new key value pair to current node.
    /// Returns inserted slot number.
    #[inline]
    pub(super) fn insert<V: BTreeValue>(&mut self, key: &[u8], value: V) -> usize {
        debug_assert!(self.can_insert(key));
        let slot_idx = match self.search_key(key) {
            SearchKey::GreaterThan(idx) => {
                // insert at next position.
                idx + 1
            }
            SearchKey::LowerFence => 0,
            SearchKey::Equal(..) => {
                // duplicate key found.
                panic!("BTreeNode does not support duplicate key");
            }
        };
        let slot = self.new_slot_with_value(key, value);
        unsafe {
            self.insert_slot_at(slot_idx, slot);
        }
        slot_idx
    }

    #[inline]
    pub fn insert_at<V: BTreeValue>(&mut self, idx: usize, key: &[u8], value: V) {
        debug_assert!(idx <= self.header.count as usize);
        let slot = self.new_slot_with_value(key, value);
        unsafe {
            self.insert_slot_at(idx, slot);
        }
    }

    /// Delete an existing key value pair in current node.
    /// If key or value does not match, returns false.
    #[inline]
    pub fn delete<V: BTreeValue>(&mut self, key: &[u8], value: V) -> BTreeDelete {
        let idx = match self.search_value::<V>(key) {
            SearchValue::Equal(idx, v) | SearchValue::EqualDeleted(idx, v) => {
                if v != value {
                    return BTreeDelete::ValueMismatch;
                }
                idx
            }
            _ => return BTreeDelete::NotFound,
        };
        self.delete_at(idx, mem::size_of::<V>());
        BTreeDelete::Ok
    }

    /// Mark an existing key value pair in current node as deleted.
    /// If key or value does not match, returns false.
    #[inline]
    pub fn mark_as_deleted<V: BTreeValue>(&mut self, key: &[u8], value: V) -> BTreeUpdate<V> {
        let idx = match self.search_value::<V>(key) {
            SearchValue::Equal(idx, v) | SearchValue::EqualDeleted(idx, v) => {
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
        let idx = match self.search_value::<V>(key) {
            SearchValue::Equal(idx, v) | SearchValue::EqualDeleted(idx, v) => {
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

    /// Compact source node into target node.
    /// Target node must be unitialized.
    #[inline]
    pub unsafe fn compact_into<V: BTreeValue>(&self, dst: &mut BTreeNode) {
        debug_assert!(!dst.header.initialized);
        let lower_fence_key = self.lower_fence_key();
        let upper_fence_key = self.upper_fence_key();
        dst.init(
            self.height() as u16,
            self.ts(),
            &lower_fence_key,
            self.lower_fence_value(),
            &upper_fence_key,
        );
        dst.extend_slots_from::<V>(self, 0, self.count());
        debug_assert!(self.free_space_after_compaction() == dst.free_space());
    }

    /// Self compact.
    #[inline]
    pub fn self_compact<V: BTreeValue>(&mut self) {
        let tmp_node = unsafe {
            let mut tmp = MaybeUninit::<BTreeNode>::zeroed();
            self.compact_into::<V>(tmp.assume_init_mut());
            tmp.assume_init()
        };
        debug_assert!(self.free_space_after_compaction() == tmp_node.free_space());
        *self = tmp_node;
    }

    /// Delete key value at given position.
    #[inline]
    pub fn delete_at(&mut self, idx: usize, value_size: usize) {
        let payload_len = self.payload_len(idx, value_size);
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
        self.header.effective_space -= (payload_len + mem::size_of::<Slot>()) as u32;
    }

    #[inline]
    fn payload_len(&self, idx: usize, value_size: usize) -> usize {
        let slot = self.slot(idx);
        if slot.len as usize <= KEY_HEAD_LEN {
            value_size
        } else {
            slot.len as usize + value_size
        }
    }

    /// Insert key value to the end of node.
    #[inline]
    pub fn insert_at_end<V: BTreeValue>(&mut self, key: &[u8], value: V) {
        debug_assert!(self.can_insert(key));
        debug_assert!(self.within_boundary(key));
        let slot = self.new_slot_with_value(key, value);
        unsafe {
            self.insert_slot_at(self.header.count as usize, slot);
        }
        debug_assert!(&self.key(self.header.count as usize - 1)[..] == key);
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
    pub fn lookup_child_slow(&self, key: &[u8]) -> LookupChild {
        match self.search_value_slow(key) {
            SearchValue::GreaterThan(idx) => LookupChild::Slot(idx, self.value(idx as usize)),
            SearchValue::Equal(idx, value) => LookupChild::Slot(idx, value),
            SearchValue::LessThanAllSlots => {
                if self.header.lower_fence_value == PageID::INVALID_VALUE {
                    LookupChild::NotFound
                } else {
                    LookupChild::LowerFence(self.header.lower_fence_value)
                }
            }
            SearchValue::EqualDeleted(..)
            | SearchValue::LessThanLowerFence
            | SearchValue::GreaterEqualUpperFence => LookupChild::NotFound,
        }
    }

    #[inline]
    pub fn lookup_child(&self, key: &[u8]) -> LookupChild {
        match self.search_value(key) {
            SearchValue::GreaterThan(idx) => LookupChild::Slot(idx, self.value(idx as usize)),
            SearchValue::Equal(idx, value) => LookupChild::Slot(idx, value),
            SearchValue::LessThanAllSlots => {
                if self.header.lower_fence_value == PageID::INVALID_VALUE {
                    LookupChild::NotFound
                } else {
                    LookupChild::LowerFence(self.header.lower_fence_value)
                }
            }
            SearchValue::EqualDeleted(..)
            | SearchValue::LessThanLowerFence
            | SearchValue::GreaterEqualUpperFence => LookupChild::NotFound,
        }
    }

    /// Returns child index of given key.
    /// If hit on lower fence, -1 will be returned.
    #[inline]
    pub fn lookup_child_idx(&self, key: &[u8]) -> Option<isize> {
        match self.lookup_child_slow(key) {
            LookupChild::Slot(idx, _) => Some(idx as isize),
            LookupChild::LowerFence(_) => Some(-1),
            LookupChild::NotFound => None,
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
        let head = head(k);
        if k.len() <= KEY_HEAD_LEN {
            // Only value inserted at end of page.
            // Copy value.
            self.header.end_offset -= mem::size_of::<V>() as u16;
            unsafe {
                (self.body_mut().add(self.header.end_offset as usize) as *mut V)
                    .write_unaligned(value);
            }
            self.header.effective_space += mem::size_of::<V>() as u32;
            return Slot {
                len: k.len() as u16,
                offset: self.header.end_offset,
                head,
            };
        }
        // Key suffix and value inserted at end of page.
        // copy value.
        self.header.end_offset -= (k.len() + mem::size_of::<V>()) as u16;
        unsafe {
            let dst = self.body_mut().add(self.header.end_offset as usize);
            std::ptr::copy_nonoverlapping(k.as_ptr(), dst, k.len());
            let dst = dst.add(k.len());
            (dst as *mut V).write_unaligned(value);
        }
        self.header.effective_space += (k.len() + mem::size_of::<V>()) as u32;
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
        let head = head(k);
        if k.len() <= KEY_HEAD_LEN {
            // No value inserted to page.
            return Slot {
                len: k.len() as u16,
                offset: self.header.end_offset,
                head,
            };
        }
        // Key suffix inserted at end of page.
        // copy key suffix.
        self.header.end_offset -= k.len() as u16;
        unsafe {
            let dst = self.body_mut().add(self.header.end_offset as usize);
            std::ptr::copy_nonoverlapping(k.as_ptr(), dst, k.len());
        }
        self.header.effective_space += k.len() as u32;
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
    pub fn free_space(&self) -> usize {
        (self.header.end_offset - self.header.start_offset) as usize
    }

    /// Returns used space.
    #[inline]
    pub fn used_space(&self) -> usize {
        mem::size_of::<BTreeNode>() - self.free_space()
    }

    /// Returns free space after compaction.
    #[inline]
    pub fn free_space_after_compaction(&self) -> usize {
        mem::size_of::<BTreeNode>() - self.header.effective_space as usize
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

    #[inline]
    fn slot_mut(&mut self, idx: usize) -> &mut Slot {
        debug_assert!(idx < self.header.count as usize);
        unsafe { self.slot_mut_unchecked(idx) }
    }

    #[inline]
    unsafe fn slot_mut_unchecked(&mut self, idx: usize) -> &mut Slot {
        &mut *(self.body_mut() as *mut Slot).add(idx)
    }

    /// insert slot at given position.
    /// If overwrite is set to true, the old value is overwritten.
    #[inline]
    unsafe fn insert_slot_at(&mut self, idx: usize, slot: Slot) {
        let dst = (self.body_mut() as *mut Slot).add(idx);
        if idx < self.header.count as usize {
            // shift all elements starting from destination by one position.
            let next = dst.add(1);
            std::ptr::copy(dst, next, self.header.count as usize - idx);
        }
        self.header.count += 1;
        self.header.start_offset += mem::size_of::<Slot>() as u16;
        self.header.effective_space += mem::size_of::<Slot>() as u32;
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
            if len <= INLINE_PREFIX_LEN {
                return &self.header.inline_prefix[..len];
            }
            std::slice::from_raw_parts(self.body_end().sub(len), len)
        }
    }

    #[inline]
    fn set_common_prefix(&mut self, common_prefix: &[u8]) {
        let l = common_prefix.len();
        self.header.prefix_len = l as u16;
        if l == 0 {
            return;
        }
        if l <= INLINE_PREFIX_LEN {
            self.header.inline_prefix[..l].copy_from_slice(common_prefix);
            return;
        }
        // not inline, stored at end of page.
        unsafe {
            let dst = self.body_end_mut().sub(l);
            std::ptr::copy_nonoverlapping(common_prefix.as_ptr(), dst, l);
        }
        self.header.end_offset -= l as u16;
        self.header.effective_space += l as u32;
    }

    /// Search given key in current node.
    #[inline]
    pub fn search_value_slow<V: BTreeValue>(&self, key: &[u8]) -> SearchValue<V> {
        let prefix_len = self.header.prefix_len as usize;
        if prefix_len == 0 {
            return self.search_value_without_prefix(key);
        }
        // Key is shorter than prefix, compare and return.
        if key.len() < prefix_len {
            let l = key.len().min(prefix_len);
            let prefix = self.common_prefix();
            return match key[..l].cmp(&prefix[..l]) {
                Ordering::Greater => {
                    if self.has_no_upper_fence() {
                        SearchValue::GreaterThan(self.header.count as usize - 1)
                    } else {
                        SearchValue::GreaterEqualUpperFence
                    }
                }
                Ordering::Less | Ordering::Equal => SearchValue::LessThanLowerFence,
            };
        }
        // Check if prefix matches.
        return match key[..prefix_len].cmp(self.common_prefix()) {
            Ordering::Greater => {
                if self.has_no_upper_fence() {
                    SearchValue::GreaterThan(self.header.count as usize - 1)
                } else {
                    SearchValue::GreaterEqualUpperFence
                }
            }
            Ordering::Less => SearchValue::LessThanLowerFence,
            Ordering::Equal => self.search_value_without_prefix(&key[prefix_len..]),
        };
    }

    /// This is used for B-tree which always guarantee the prefix matching
    /// with its structure.
    #[inline]
    pub fn search_value<V: BTreeValue>(&self, key: &[u8]) -> SearchValue<V> {
        debug_assert!(
            key.len() >= self.header.prefix_len as usize
                && &key[..self.header.prefix_len as usize] == self.common_prefix()
        );
        self.search_value_without_prefix::<V>(&key[self.header.prefix_len as usize..])
    }

    /// Search without prefix. The prefix of input key should be trimed.
    #[inline]
    fn search_value_without_prefix<V: BTreeValue>(&self, k: &[u8]) -> SearchValue<V> {
        let head = head_as_u32(k);
        match self
            .slots()
            .binary_search_by(|s| match s.head_as_u32().cmp(&head) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => {
                    let l = k.len().min(s.len as usize);
                    if l <= KEY_HEAD_LEN {
                        (s.len as usize).cmp(&k.len())
                    } else {
                        let sk = self.k(s);
                        sk.cmp(k)
                    }
                }
            }) {
            Ok(idx) => {
                let value = self.value::<V>(idx);
                if value.is_deleted() {
                    SearchValue::EqualDeleted(idx, value.value())
                } else {
                    SearchValue::Equal(idx, value)
                }
            }
            Err(idx) => {
                if idx == 0 {
                    // input key is less than the first slot key.
                    SearchValue::LessThanAllSlots
                } else {
                    SearchValue::GreaterThan(idx - 1)
                }
            }
        }
    }

    #[inline]
    pub fn search_key(&self, key: &[u8]) -> SearchKey {
        debug_assert!(
            key.len() >= self.header.prefix_len as usize
                && &key[..self.header.prefix_len as usize] == self.common_prefix()
        );
        let k = &key[self.header.prefix_len as usize..];
        let head = head_as_u32(k);
        match self
            .slots()
            .binary_search_by(|s| match s.head_as_u32().cmp(&head) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => {
                    let l = k.len().min(s.len as usize);
                    if l <= KEY_HEAD_LEN {
                        (s.len as usize).cmp(&k.len())
                    } else {
                        let sk = self.k(s);
                        sk.cmp(k)
                    }
                }
            }) {
            Ok(idx) => SearchKey::Equal(idx),
            Err(idx) => {
                if idx == 0 {
                    // input key is less than the first slot key.
                    SearchKey::LowerFence
                } else {
                    SearchKey::GreaterThan(idx - 1)
                }
            }
        }
    }

    #[inline]
    pub(super) fn key(&self, idx: usize) -> KeyVec {
        debug_assert!(idx < self.header.count as usize);
        let mut res = KeyVec::new();
        self.extract_key(idx, &mut res);
        res
    }

    #[inline]
    fn long_key_suffix(&self, slot: &Slot) -> &[u8] {
        debug_assert!(slot.len as usize > KEY_HEAD_LEN);
        unsafe { self.payload(slot.offset as usize, slot.len as usize) }
    }

    #[inline]
    unsafe fn payload(&self, offset: usize, len: usize) -> &[u8] {
        std::slice::from_raw_parts(self.body().add(offset), len)
    }

    #[inline]
    fn key_len(&self, idx: usize) -> u16 {
        debug_assert!(idx < self.header.count as usize);
        self.header.prefix_len + self.slot(idx).len
    }

    #[inline]
    pub(super) fn lower_fence_key(&self) -> KeyVec {
        let mut res = KeyVec::new();
        self.extract_slot_key(&self.header.lower_fence, &mut res);
        res
    }

    #[inline]
    pub(super) fn extract_lower_fence_key(&self, res: &mut KeyVec) {
        self.extract_slot_key(&self.header.lower_fence, res);
    }

    #[inline]
    pub(super) fn lower_fence_key_len(&self) -> u16 {
        self.header.prefix_len + self.header.lower_fence.len
    }

    #[inline]
    pub(super) fn lower_fence_value(&self) -> PageID {
        self.header.lower_fence_value
    }

    #[inline]
    pub(super) fn upper_fence_key(&self) -> KeyVec {
        if self.has_no_upper_fence() {
            return KeyVec::new();
        }
        let mut res = KeyVec::new();
        self.extract_slot_key(&self.header.upper_fence, &mut res);
        res
    }

    #[inline]
    pub(super) fn extract_upper_fence_key(&self, res: &mut KeyVec) {
        self.extract_slot_key(&self.header.upper_fence, res);
    }

    #[inline]
    pub(super) fn upper_fence_key_len(&self) -> u16 {
        self.header.prefix_len + self.header.upper_fence.len
    }

    /// Extract key into buffer.
    /// Previous content in buffer is discarded.
    #[inline]
    pub(super) fn extract_key(&self, idx: usize, res: &mut KeyVec) {
        debug_assert!(idx < self.header.count as usize);
        let slot = self.slot(idx);
        self.extract_slot_key(slot, res);
    }

    #[inline]
    fn extract_slot_key(&self, slot: &Slot, res: &mut KeyVec) {
        let len = (self.header.prefix_len + slot.len) as usize;
        res.clear();
        res.reserve(len);
        res.extend_from_slice(self.common_prefix());
        res.extend_from_slice(self.k(slot));
    }

    #[inline]
    pub(super) fn value<V: BTreeValue>(&self, idx: usize) -> V {
        debug_assert!(idx < self.header.count as usize);
        let slot = self.slot(idx);
        unsafe { self.slot_value(slot) }
    }

    #[inline]
    unsafe fn slot_value<V: BTreeValue>(&self, slot: &Slot) -> V {
        let offset = if slot.len as usize <= KEY_HEAD_LEN {
            // key is inlined.
            slot.offset
        } else {
            // should shift key length.
            slot.offset + slot.len
        } as usize;
        let ptr = self.body().add(offset);
        std::ptr::read_unaligned::<V>(ptr as *const V)
    }

    /// Returns all values in this node.
    #[inline]
    pub(super) fn values<V: BTreeValue>(&self) -> Vec<V> {
        self.slots()
            .iter()
            .map(|slot| unsafe { self.slot_value(slot) })
            .collect()
    }

    #[inline]
    fn update_value<V: BTreeValue>(&mut self, idx: usize, value: V) -> V {
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
    pub fn create_sep_key(&self, idx: usize, truncate: bool) -> KeyVec {
        debug_assert!(idx > 0);
        debug_assert!(idx < self.count());
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
                    let mut res = KeyVec::with_capacity(self.header.prefix_len as usize + i);
                    res.extend_from_slice(self.common_prefix());
                    res.extend_from_slice(&s1.head[..i + 1]);
                    return res;
                }
            }
            // As s1 is greater than s2, and the common part is identical.
            // s1 must be longer than s2.
            debug_assert!(s1.len > s2.len);
            let diff_len = l + 1;
            let mut res = KeyVec::with_capacity(self.header.prefix_len as usize + diff_len);
            res.extend_from_slice(self.common_prefix());
            if diff_len <= KEY_HEAD_LEN {
                res.extend_from_slice(&s1.head[..diff_len]);
            } else {
                let src = unsafe { self.payload(s1.offset as usize, diff_len) };
                res.extend_from_slice(src);
            }
            return res;
        }
        // compare key without prefix.
        let k1 = self.long_key_suffix(s1);
        let k2 = self.long_key_suffix(s2);
        for (i, (a, b)) in k1[..l].iter().zip(&k2[..l]).enumerate() {
            if a != b {
                let mut res = KeyVec::with_capacity(self.header.prefix_len as usize + i);
                res.extend_from_slice(self.common_prefix());
                res.extend_from_slice(&k1[..i + 1]);
                return res;
            }
        }
        debug_assert!(k1.len() > k2.len());
        let diff_len = l + 1;
        let mut res = KeyVec::with_capacity(self.header.prefix_len as usize + diff_len);
        res.extend_from_slice(self.common_prefix());
        res.extend_from_slice(&k1[..diff_len]);
        res
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
                    let slot_space = mem::size_of::<Slot>() * count;
                    self.header.start_offset += slot_space as u16;
                    self.header.effective_space += slot_space as u32;
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
                    self.header.effective_space += payload_space as u32;
                }
                self.header.count += count as u16;
            }
            debug_assert!(self.header.start_offset <= self.header.end_offset);
            return;
        }
        // Slow path to copy key value one by one.
        let mut key = KeyVec::new(); // key buffer
        for idx in src_slot_idx..src_slot_idx + count {
            src_node.extract_key(idx, &mut key);
            let value = src_node.value::<V>(idx);
            self.insert_at_end(&key, value);
        }
    }

    /// Returns space estimation of self node.
    #[inline]
    pub fn space_estimation(&self, value_size: usize) -> SpaceEstimation {
        SpaceEstimation::new(
            self.header.prefix_len,
            self.lower_fence_key_len(),
            self.upper_fence_key_len(),
            value_size,
        )
    }

    /// Prepare to update key in the node.
    /// When merging children, there might be cases that lower fence of
    /// a child is changed(as moved to left neighbor), then we need to
    /// regenerate separator key and update it in parent node.
    /// Returns false if there is no space to update key in-place.
    #[inline]
    pub fn prepare_update_key<V: BTreeValue>(&mut self, idx: usize, key: &[u8]) -> bool {
        debug_assert!(idx < self.header.count as usize);
        debug_assert!(self.preserve_order_with_key_replacement(idx, key));
        let slot = self.slot(idx);
        let kl = key.len() - self.header.prefix_len as usize;
        if kl <= KEY_HEAD_LEN {
            return true;
        }
        if kl <= slot.len as usize {
            return true;
        }
        if kl + mem::size_of::<V>() <= self.free_space() {
            return true;
        }
        false
    }

    /// Update key in-place.
    /// prepare_update_key() should be called before calling this method
    #[inline]
    pub fn update_key<V: BTreeValue>(&mut self, idx: usize, key: &[u8]) {
        debug_assert!(self.prepare_update_key::<V>(idx, key));
        let slot = self.slot(idx);
        let offset = slot.offset as usize;
        let old_len = slot.len as usize;
        let k = &key[self.header.prefix_len as usize..];
        if k.len() <= KEY_HEAD_LEN || k.len() <= old_len {
            self.update_key_in_place::<V>(idx, k, offset, old_len);
            return;
        }
        // out of place update
        let value = self.value::<V>(idx);
        let old_payload_len = self.payload_len(idx, mem::size_of::<V>());
        let slot = self.new_slot_with_value(key, value);
        *self.slot_mut(idx) = slot;
        self.header.effective_space -= old_payload_len as u32;
    }

    #[inline]
    fn update_key_in_place<V: BTreeValue>(
        &mut self,
        idx: usize,
        k: &[u8],
        old_offset: usize,
        old_len: usize,
    ) {
        let head = head(k);
        if old_len <= KEY_HEAD_LEN {
            // no extra payload.
            let slot = self.slot_mut(idx);
            // update head.
            slot.head = head;
            // update length.
            slot.len = k.len() as u16;
            // no value change.
            // no change on effective space.
            return;
        }
        // old value has extra payload.
        if k.len() == old_len {
            // update extra payload
            unsafe {
                std::ptr::copy_nonoverlapping(k.as_ptr(), self.body_mut().add(old_offset), k.len());
            }
            // update head.
            let slot = self.slot_mut(idx);
            slot.head = head;
            // no value change.
            // no change on effective space.
            return;
        }
        let value = unsafe { (self.body().add(old_offset + old_len) as *const V).read_unaligned() };
        if k.len() <= KEY_HEAD_LEN {
            // update value
            unsafe {
                (self.body_mut().add(old_offset) as *mut V).write_unaligned(value);
            }
            // update head
            let slot = self.slot_mut(idx);
            slot.head = head;
            // update length
            slot.len = k.len() as u16;
            // update effective space, as extra payload is not effective.
            self.header.effective_space -= old_len as u32;
            return;
        }
        debug_assert!(k.len() < old_len);
        // update extra payload and value
        unsafe {
            std::ptr::copy_nonoverlapping(k.as_ptr(), self.body_mut().add(old_offset), k.len());
            (self.body_mut().add(old_offset + k.len()) as *mut V).write_unaligned(value);
        }
        // update head
        let slot = self.slot_mut(idx);
        slot.head = head;
        // update length
        slot.len = k.len() as u16;
        self.header.effective_space -= (old_len - k.len()) as u32;
    }

    #[inline]
    fn preserve_order_with_key_replacement(&self, idx: usize, key: &[u8]) -> bool {
        if !self.within_boundary(key) {
            return false;
        }
        if self.header.count == 1 {
            return true;
        }
        // at least two keys.
        let k = &key[self.header.prefix_len as usize..];
        if idx == 0 {
            let r = self.slot(1);
            return self.cmp_key_without_prefix(k, r) == cmp::Ordering::Less;
        }
        if idx + 1 == self.header.count as usize {
            let l = self.slot(idx - 1);
            return self.cmp_key_without_prefix(k, l) == cmp::Ordering::Greater;
        }
        // at least three keys.
        let l = self.slot(idx - 1);
        let r = self.slot(idx + 1);
        self.cmp_key_without_prefix(k, l) == cmp::Ordering::Greater
            && self.cmp_key_without_prefix(k, r) == cmp::Ordering::Less
    }

    #[inline]
    fn cmp_slot_key(&self, idx1: usize, idx2: usize) -> Ordering {
        let s1 = self.slot(idx1);
        let s2 = self.slot(idx2);
        match s1.head.cmp(&s2.head) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let l = s1.len.min(s2.len) as usize;
                if l <= KEY_HEAD_LEN {
                    s1.len.cmp(&s2.len)
                } else {
                    let k1 = self.long_key_suffix(s1);
                    let k2 = self.long_key_suffix(s2);
                    k1.cmp(k2)
                }
            }
        }
    }

    #[inline]
    fn k<'a>(&'a self, slot: &'a Slot) -> &'a [u8] {
        if slot.len as usize <= KEY_HEAD_LEN {
            &slot.head[..slot.len as usize]
        } else {
            unsafe {
                let ptr = self.body().add(slot.offset as usize);
                std::slice::from_raw_parts(ptr, slot.len as usize)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchValue<V: BTreeValue> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchKey {
    LowerFence,
    Equal(usize),
    GreaterThan(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupChild {
    Slot(usize, PageID),
    LowerFence(PageID),
    NotFound,
}

#[inline]
pub(super) fn common_prefix_len(key1: &[u8], key2: &[u8]) -> usize {
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

/// Estimate the space of one node after it absorbs another node's data.
/// The bytes used is not very precise, we only consider lower fence of
/// left node and upper fence of right node as fence keys of merged node.
#[derive(Debug, Clone)]
pub struct SpaceEstimation {
    prefix_len: u16,
    value_size: usize,
    slots: u16,
    total_space: usize,
}

impl SpaceEstimation {
    #[inline]
    pub fn new(
        prefix_len: u16,
        lower_fence_key_len: u16,
        upper_fence_key_len: u16,
        value_size: usize,
    ) -> Self {
        debug_assert!(lower_fence_key_len >= prefix_len);
        debug_assert!(upper_fence_key_len >= prefix_len);
        let lower_fence_space = if (lower_fence_key_len - prefix_len) as usize <= KEY_HEAD_LEN {
            0
        } else {
            (lower_fence_key_len - prefix_len) as usize
        };
        let upper_fence_space = if (upper_fence_key_len - prefix_len) as usize <= KEY_HEAD_LEN {
            0
        } else {
            (upper_fence_key_len - prefix_len) as usize
        };
        let total_space = mem::size_of::<BTreeHeader>()
            + prefix_len as usize
            + lower_fence_space
            + upper_fence_space;
        SpaceEstimation {
            prefix_len,
            value_size,
            slots: 0,
            total_space,
        }
    }

    #[inline]
    pub fn with_fences(lower_fence_key: &[u8], upper_fence_key: &[u8], value_size: usize) -> Self {
        let prefix_len = common_prefix_len(lower_fence_key, upper_fence_key);
        SpaceEstimation::new(
            prefix_len as u16,
            lower_fence_key.len() as u16,
            upper_fence_key.len() as u16,
            value_size,
        )
    }

    #[inline]
    pub fn add_key_range(&mut self, node: &BTreeNode, start_idx: usize, end_idx: usize) {
        debug_assert!(start_idx <= end_idx);
        for idx in start_idx..end_idx {
            self.add_key(node.key_len(idx));
        }
    }

    #[inline]
    pub fn add_key(&mut self, len: u16) -> usize {
        debug_assert!(len >= self.prefix_len);
        self.add_key_suffix(len - self.prefix_len)
    }

    #[inline]
    pub fn add_key_suffix(&mut self, len: u16) -> usize {
        self.slots += 1;
        if len as usize > KEY_HEAD_LEN {
            self.total_space += len as usize;
        }
        // slot and value space are always added.
        self.total_space += mem::size_of::<Slot>() + self.value_size;
        self.total_space
    }

    #[inline]
    pub fn total_space(&self) -> usize {
        self.total_space
    }

    /// Grow space until reach threshold.
    /// Return 0 if no key can be added.
    /// Return node.count() if all keys can be added.
    #[inline]
    pub fn grow_until_threshold(&mut self, node: &BTreeNode, threshold: usize) -> usize {
        let mut i = 0usize;
        if self.prefix_len == node.header.prefix_len {
            for slot in node.slots() {
                let total_space = self.add_key_suffix(slot.len);
                if total_space > threshold {
                    break;
                }
                i += 1;
            }
        } else {
            for slot in node.slots() {
                let total_space = self.add_key(slot.len + node.header.prefix_len);
                if total_space > threshold {
                    break;
                }
                i += 1;
            }
        }
        i
    }
}

#[inline]
fn head(k: &[u8]) -> KeyHead {
    match k.len() {
        0 => [0; 4],
        1 => [k[0], 0, 0, 0],
        2 => [k[0], k[1], 0, 0],
        3 => [k[0], k[1], k[2], 0],
        _ => [k[0], k[1], k[2], k[3]],
    }
}

#[inline]
fn head_as_u32(k: &[u8]) -> u32 {
    u32::from_be_bytes(head(k))
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
                    let res = node.search_value_slow::<u64>(&k);
                    assert_eq!(res, SearchValue::Equal(i as usize, i));
                }

                let res = node.search_value_slow::<u64>(&11u64.to_be_bytes());
                assert_eq!(res, SearchValue::GreaterThan(9));
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
                    node.search_value_slow::<u64>(&5u64.to_be_bytes()),
                    SearchValue::GreaterThan(4)
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
                match node.search_value_slow::<u64>(&3u64.to_be_bytes()) {
                    SearchValue::EqualDeleted(_, v) => assert_eq!(v, 3),
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
                    node.search_value_slow::<u64>(&5u64.to_be_bytes()),
                    SearchValue::Equal(5, 50)
                );

                // Test update deleted entry
                node.mark_as_deleted(&6u64.to_be_bytes(), 6);
                assert_eq!(
                    node.update(&6u64.to_be_bytes(), 6, 60),
                    BTreeUpdate::Ok(6.deleted())
                );
                assert_eq!(
                    node.search_value_slow::<u64>(&6u64.to_be_bytes()),
                    SearchValue::Equal(6, 60)
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

    #[test]
    fn test_btree_node_compact_non_empty() {
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(64usize * 1024 * 1024).unwrap();

            {
                // Create source leaf node with data
                let mut src_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let src_node = src_guard.page_mut();
                src_node.init(0, 1, &[], !0, &[]);

                // Insert test data
                for i in 0u64..10 {
                    let k = i.to_be_bytes();
                    src_node.insert(&k, i);
                }

                // Create empty destination node
                let mut dst_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let dst_node = dst_guard.page_mut();

                // Compact source to destination
                unsafe { src_node.compact_into::<u64>(dst_node) };

                // Verify compaction results
                assert_eq!(dst_node.height(), 0);
                assert_eq!(dst_node.ts(), 1);
                assert_eq!(dst_node.count(), src_node.count());
                assert_eq!(&dst_node.lower_fence_key()[..], &[0u8; 0][..]);
                assert_eq!(&dst_node.upper_fence_key()[..], &[0u8; 0][..]);
                assert_eq!(
                    dst_node.free_space(),
                    src_node.free_space_after_compaction()
                );

                // Verify all slots are copied correctly
                for i in 0..src_node.count() {
                    assert_eq!(dst_node.key(i), src_node.key(i));
                    assert_eq!(dst_node.value::<u64>(i), src_node.value::<u64>(i));
                }
            }

            unsafe {
                StaticLifetime::drop_static(buf_pool);
            }
        })
    }

    #[test]
    fn test_btree_node_compact_empty() {
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(64usize * 1024 * 1024).unwrap();

            {
                // Create empty source node
                let mut src_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let src_node = src_guard.page_mut();
                src_node.init(0, 3, &[], !0, &[]);

                // Create empty destination node
                let mut dst_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let dst_node = dst_guard.page_mut();

                // Compact source to destination
                unsafe { src_node.compact_into::<u64>(dst_node) };

                // Verify compaction results
                assert_eq!(dst_node.height(), 0);
                assert_eq!(dst_node.ts(), 3);
                assert_eq!(dst_node.count(), 0);
                assert_eq!(dst_node.lower_fence_key().as_slice(), &[0u8; 0][..]);
                assert_eq!(dst_node.upper_fence_key().as_slice(), &[0u8; 0][..]);
                assert_eq!(
                    dst_node.free_space(),
                    src_node.free_space_after_compaction()
                );
            }

            unsafe {
                StaticLifetime::drop_static(buf_pool);
            }
        })
    }

    #[test]
    fn test_space_estimation() {
        let mut mse = SpaceEstimation::new(10, 20, 20, mem::size_of::<u64>());
        assert!(mse.total_space() == mem::size_of::<BTreeHeader>() + 30);

        assert_eq!(mse.add_key(11), mem::size_of::<BTreeHeader>() + 30 + 16);
        assert_eq!(mse.add_key(12), mem::size_of::<BTreeHeader>() + 30 + 32);
        assert_eq!(mse.add_key(15), mem::size_of::<BTreeHeader>() + 30 + 48 + 5);
    }

    #[test]
    fn test_btree_node_space_estimation() {
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(64usize * 1024 * 1024).unwrap();

            {
                let mut page1_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let node1 = page1_guard.page_mut();
                node1.init(1, 1, &[], RowID::INVALID_VALUE, &10u64.to_be_bytes());

                // Insert test data for node 1
                for i in 0u64..10 {
                    let k = i.to_be_bytes();
                    node1.insert(&k, i);
                }
                // prefix=0, lower_fence="", upper_fence=10u64.
                assert_eq!(
                    node1.effective_space(),
                    std::mem::size_of::<BTreeHeader>() + 8 + 10 * 24
                );

                let mut page2_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let node2 = page2_guard.page_mut();
                node2.init(
                    1,
                    2,
                    &10u64.to_be_bytes(),
                    RowID::INVALID_VALUE,
                    &20u64.to_be_bytes(),
                );

                // Insert test data for node 2
                for i in 10u64..20 {
                    let k = i.to_be_bytes();
                    node2.insert(&k, i);
                }
                assert_eq!(
                    node2.effective_space(),
                    std::mem::size_of::<BTreeHeader>() + 10 * 16
                );

                let lower_fence = node1.lower_fence_key();
                let upper_fence = node2.upper_fence_key();
                let mut estimation =
                    SpaceEstimation::with_fences(&lower_fence, &upper_fence, mem::size_of::<u64>());
                estimation.add_key_range(node1, 0, node1.count());
                assert_eq!(estimation.total_space(), node1.effective_space());
                estimation.add_key_range(node2, 0, node2.count());
                println!("left={}", node1.effective_space());
                println!("right={}", node2.effective_space());
                // Merged space can be larger than sum of two nodes, because
                // prefix length may be reduced.
                println!("merged={}", estimation.total_space());
            }

            unsafe {
                StaticLifetime::drop_static(buf_pool);
            }
        })
    }

    #[test]
    fn test_btree_node_update_key() {
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(64usize * 1024 * 1024).unwrap();

            {
                let mut page_guard = buf_pool.allocate_page::<BTreeNode>().await;
                let node = page_guard.page_mut();
                node.init(0, 0, &[], !0, &[]);

                // Insert test data with short keys (<= KEY_HEAD_LEN)
                for i in 0u64..5 {
                    // k00, k10, k20, k30, k40
                    let k = format!("k{}0", i);
                    node.insert(k.as_bytes(), i);
                }

                // Insert test data with long keys (> KEY_HEAD_LEN)
                for i in 5u64..10 {
                    // long-key-50, long-key-60, long-key-70, long-key-80, long-key-90
                    let k = format!("long-key-{}0", i).into_bytes();
                    node.insert(&k, i);
                }
                println!("effective space {}", node.effective_space());

                // Test 1: Update short key to another short key
                {
                    let idx = match node.search_value_slow::<u64>(b"k20") {
                        SearchValue::Equal(idx, _) => idx,
                        _ => panic!("wrong search result"),
                    };
                    let new_key = KeyVec::from_slice("k21".as_bytes());
                    assert!(node.prepare_update_key::<u64>(idx, &new_key));
                    node.update_key::<u64>(idx, &new_key);
                    assert_eq!(node.key(idx), new_key);
                    assert_eq!(node.value::<u64>(idx), 2);
                }

                // Test 2: Update long key to another long key (same length)
                {
                    let idx = match node.search_value_slow::<u64>(b"long-key-70") {
                        SearchValue::Equal(idx, _) => idx,
                        _ => panic!("wrong search result"),
                    };
                    let new_key = KeyVec::from_slice(format!("long-key-75").as_bytes());
                    assert!(node.prepare_update_key::<u64>(idx, &new_key));
                    node.update_key::<u64>(idx, &new_key);
                    assert_eq!(node.key(idx), new_key);
                    assert_eq!(node.value::<u64>(idx), 7);
                }

                // Test 3: Update short key to long key
                {
                    let idx = match node.search_value_slow::<u64>(b"k10") {
                        SearchValue::Equal(idx, _) => idx,
                        _ => panic!("wrong search result"),
                    };
                    let new_key = KeyVec::from_slice(b"k100000000000000");
                    assert!(node.prepare_update_key::<u64>(idx, &new_key));
                    node.update_key::<u64>(idx, &new_key);
                    assert_eq!(node.key(idx), new_key);
                    assert_eq!(node.value::<u64>(idx), 1);
                }

                // Test 4: Update long key to short key
                {
                    let idx = match node.search_value_slow::<u64>(b"long-key-50") {
                        SearchValue::Equal(idx, _) => idx,
                        _ => panic!("wrong search result"),
                    };
                    let new_key = KeyVec::from_slice(b"lon");
                    assert!(node.prepare_update_key::<u64>(idx, &new_key));
                    node.update_key::<u64>(idx, &new_key);
                    assert_eq!(node.key(idx), new_key);
                    assert_eq!(node.value::<u64>(idx), 5);
                }

                // Test 5: Verify order is preserved after updates
                for i in 1..node.count() {
                    assert!(node.cmp_slot_key(i, i - 1) == Ordering::Greater);
                }
            }

            unsafe {
                StaticLifetime::drop_static(buf_pool);
            }
        })
    }
}
