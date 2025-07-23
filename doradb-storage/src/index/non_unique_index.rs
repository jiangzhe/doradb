use crate::index::btree::BTree;
use crate::index::btree_key::BTreeKeyEncoder;
use crate::row::RowID;
use crate::value::Val;
use std::future::Future;

/// Abstraction of non-unique index.
pub trait NonUniqueIndex: Send + Sync + 'static {
    /// Lookup key and put associated values into given collection.
    fn lookup(&self, key: &[Val], res: &mut Vec<RowID>) -> impl Future<Output = ()>;

    /// Insert key value pair into the index.
    /// Value is always RowID.
    fn insert(&self, key: &[Val], row_id: RowID) -> impl Future<Output = ()>;

    /// Delete key value pair from the index.
    /// Value is always RowID.
    fn delete(&self, key: &[Val], row_id: RowID) -> impl Future<Output = bool>;

    /// Update existing key value pair to new value.
    fn update(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> impl Future<Output = bool>;
}

pub struct NonUniqueBTreeIndex {
    tree: BTree,
    encoder: BTreeKeyEncoder,
}

impl NonUniqueBTreeIndex {
    #[inline]
    pub fn new(tree: BTree, encoder: BTreeKeyEncoder) -> Self {
        NonUniqueBTreeIndex { tree, encoder }
    }
}

impl NonUniqueIndex for NonUniqueBTreeIndex {
    #[inline]
    async fn lookup(&self, key: &[Val], res: &mut Vec<RowID>) {
        todo!()
    }

    #[inline]
    async fn insert(&self, key: &[Val], row_id: RowID) {
        todo!()
    }

    #[inline]
    async fn delete(&self, key: &[Val], row_id: RowID) -> bool {
        todo!()
    }

    #[inline]
    async fn update(&self, key: &[Val], old_row_id: RowID, new_row_id: RowID) -> bool {
        todo!()
    }
}
