use crate::row::RowID;
use crate::value::Val;
use parking_lot::RwLock;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem::MaybeUninit;

/// Simple index interface for single key.
/// The purpose is to demonstrate MVCC functionality with index
/// lookup.
/// A full-featured index interface and implementation is to be
/// developed soon.
pub trait SingleKeyIndex {
    fn lookup(&self, key: &Val) -> Option<RowID>;

    fn insert(&self, key: Val, row_id: RowID) -> Option<RowID>;

    fn insert_if_not_exists(&self, key: Val, row_id: RowID) -> Option<RowID>;

    fn delete(&self, key: &Val) -> Option<RowID>;

    fn compare_exchange(&self, key: &Val, old_row_id: RowID, new_row_id: RowID) -> bool;

    // todo: scan
}

pub const INDEX_PARTITIONS: usize = 64;

// Simple partitioned index implementation backed by BTreeMap in standard library.
pub struct PartitionIntIndex([RwLock<BTreeMap<i32, RowID>>; INDEX_PARTITIONS]);

impl PartitionIntIndex {
    #[inline]
    pub fn empty() -> Self {
        let mut init: MaybeUninit<[RwLock<BTreeMap<i32, RowID>>; INDEX_PARTITIONS]> =
            MaybeUninit::uninit();
        let array = unsafe {
            for ptr in init.assume_init_mut().iter_mut() {
                (ptr as *mut RwLock<BTreeMap<i32, RowID>>).write(RwLock::new(BTreeMap::new()));
            }
            init.assume_init()
        };
        PartitionIntIndex(array)
    }

    #[inline]
    fn select(&self, key: i32) -> &RwLock<BTreeMap<i32, RowID>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        &self.0[hash as usize % INDEX_PARTITIONS]
    }

    #[inline]
    fn key_to_int(&self, key: &Val) -> i32 {
        match key {
            Val::Byte4(v) => *v as i32,
            _ => panic!("other key type not support"),
        }
    }
}

impl SingleKeyIndex for PartitionIntIndex {
    #[inline]
    fn lookup(&self, key: &Val) -> Option<RowID> {
        let key = self.key_to_int(key);
        let tree = self.select(key);
        let g = tree.read();
        g.get(&key).cloned()
    }

    #[inline]
    fn insert(&self, key: Val, row_id: RowID) -> Option<RowID> {
        let key = self.key_to_int(&key);
        let tree = self.select(key);
        let mut g = tree.write();
        g.insert(key, row_id)
    }

    #[inline]
    fn insert_if_not_exists(&self, key: Val, row_id: RowID) -> Option<RowID> {
        let key = self.key_to_int(&key);
        let tree = self.select(key);
        let mut g = tree.write();
        match g.entry(key) {
            Entry::Occupied(occ) => Some(*occ.get()),
            Entry::Vacant(vac) => {
                vac.insert(row_id);
                None
            }
        }
    }

    #[inline]
    fn delete(&self, key: &Val) -> Option<RowID> {
        let key = self.key_to_int(key);
        let tree = self.select(key);
        let mut g = tree.write();
        g.remove(&key)
    }

    #[inline]
    fn compare_exchange(&self, key: &Val, old_row_id: RowID, new_row_id: RowID) -> bool {
        let key = self.key_to_int(key);
        let tree = self.select(key);
        let mut g = tree.write();
        match g.get_mut(&key) {
            Some(row_id) => {
                if *row_id == old_row_id {
                    *row_id = new_row_id;
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}
