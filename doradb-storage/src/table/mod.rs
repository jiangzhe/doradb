pub mod layout;
pub mod mvcc;

use crate::buffer::FixedBufferPool;
use crate::index::block_index::BlockIndex;
use crate::table::layout::Layout;
use crate::table::mvcc::MvccTable;

pub type Schema = Vec<Layout>;
pub type SchemaRef<'a> = &'a [Layout];
// todo: integrate with doradb_catalog::TableID.
pub type TableID = u64;

pub struct Table<'a> {
    pub table_id: TableID,
    pub buf_pool: &'a FixedBufferPool,
    pub schema: Schema,
    pub blk_idx: BlockIndex<'a>,
    // todo: secondary indexes.
}

impl<'a> Table<'a> {
    #[inline]
    pub fn mvcc(&'a self) -> MvccTable<'a> {
        MvccTable(self)
    }
}
