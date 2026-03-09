use crate::buffer::guard::PageGuard;
use crate::buffer::page::{BufferPage, PageID};
use crate::buffer::{BufferPool, FixedBufferPool};
use crate::catalog::storage::CatalogStorage;
use crate::catalog::table::TableMetadata;
use crate::catalog::{ObjID, TableID, USER_OBJ_ID_START};
use crate::error::{Error, Result};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MultiTableFile, MutableMultiTableFile,
};
use crate::index::{
    COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_MAX_BRANCH_ENTRIES, COLUMN_BLOCK_MAX_ENTRIES,
    COLUMN_BRANCH_ENTRY_SIZE, COLUMN_PAGE_PAYLOAD_SIZE, ColumnBlockIndex, ColumnBlockNodeHeader,
    ColumnBlockPageReader, ColumnPagePayload, ColumnPagePayloadPatch, OffloadedBitmapPatch,
};
use crate::io::{AIOBuf, DirectBuf};
use crate::lwc::{LwcBuilder, LwcData, LwcPage};
use crate::ptr::UnsafePtr;
use crate::row::ops::SelectKey;
use crate::row::{InsertRow, RowID, RowPage};
use crate::trx::redo::RowRedoKind;
use crate::trx::sys::{CatalogCheckpointBatch, CatalogRedoEntry};
use crate::value::{Val, ValKind};
use bytemuck::pod_read_unaligned;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::mem;
use std::num::NonZeroU64;
use std::pin::Pin;

struct PendingLwcPage {
    start_row_id: RowID,
    buf: DirectBuf,
}

#[derive(Clone, Copy)]
struct CatalogIndexEntry {
    start_row_id: RowID,
    payload: ColumnPagePayload,
}

struct RowRecord {
    row_id: RowID,
    vals: Vec<Val>,
}

struct ExistingVisibleRow {
    row_id: RowID,
    start_row_id: RowID,
    vals: Vec<Val>,
    deleted: bool,
}

struct PendingInsertRow {
    row_id: RowID,
    vals: Vec<Val>,
    deleted: bool,
}

struct CatalogMtbIndexPageReader<'a> {
    mtb: &'a MultiTableFile,
}

impl<'a> CatalogMtbIndexPageReader<'a> {
    #[inline]
    fn new(mtb: &'a MultiTableFile) -> Self {
        CatalogMtbIndexPageReader { mtb }
    }
}

impl ColumnBlockPageReader for CatalogMtbIndexPageReader<'_> {
    fn read_page<'a>(
        &'a self,
        page_id: PageID,
    ) -> Pin<Box<dyn Future<Output = Result<DirectBuf>> + Send + 'a>> {
        Box::pin(async move {
            let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
            // SAFETY: `DirectBuf` memory is sector-aligned and valid until async read completes.
            unsafe {
                self.mtb
                    .read_page_into_ptr(page_id, UnsafePtr(buf.as_bytes_mut().as_mut_ptr()))
                    .await?;
            }
            Ok(buf)
        })
    }
}

impl CatalogStorage {
    pub async fn apply_checkpoint_batch(
        &self,
        batch: CatalogCheckpointBatch,
        next_user_obj_id: ObjID,
    ) -> Result<()> {
        let CatalogCheckpointBatch {
            from_exclusive,
            safe_cts,
            catalog_ops,
            ..
        } = batch;
        let snapshot = self.mtb.load_snapshot()?;
        let last_catalog_checkpoint_cts = snapshot.checkpoint_cts;
        if last_catalog_checkpoint_cts != from_exclusive {
            if last_catalog_checkpoint_cts >= safe_cts {
                return Ok(());
            }
            return Err(Error::InvalidState);
        }
        if safe_cts <= last_catalog_checkpoint_cts {
            return Ok(());
        }

        if catalog_ops.is_empty() {
            let mut mutable = MutableMultiTableFile::fork(&self.mtb);
            mutable.apply_checkpoint_metadata(
                safe_cts,
                next_user_obj_id.max(USER_OBJ_ID_START),
                snapshot.meta.table_roots,
            )?;
            let (_, old_root) = mutable.commit().await?;
            drop(old_root);
            return Ok(());
        }

        let mut ops_by_table: Vec<Vec<RowRedoKind>> =
            (0..self.tables.len()).map(|_| Vec::new()).collect();
        for CatalogRedoEntry { table_id, kind } in catalog_ops {
            let table_idx = table_id as usize;
            if table_idx >= ops_by_table.len() {
                continue;
            }
            ops_by_table[table_idx].push(kind);
        }

        let mut mutable = MutableMultiTableFile::fork(&self.mtb);
        let mut new_roots = snapshot.meta.table_roots;
        for (idx, table) in self.tables.iter().enumerate() {
            if idx >= CATALOG_TABLE_ROOT_DESC_COUNT {
                break;
            }
            if ops_by_table[idx].is_empty() {
                continue;
            }
            let current_root = new_roots[idx];
            let new_root = self
                .apply_table_ops(
                    &mut mutable,
                    idx as TableID,
                    table.metadata(),
                    current_root,
                    &ops_by_table[idx],
                    safe_cts,
                )
                .await?;
            new_roots[idx] = new_root;
        }

        mutable.apply_checkpoint_metadata(
            safe_cts,
            next_user_obj_id.max(USER_OBJ_ID_START),
            new_roots,
        )?;
        let (_, old_root) = mutable.commit().await?;
        drop(old_root);
        Ok(())
    }

    async fn apply_table_ops(
        &self,
        mutable: &mut MutableMultiTableFile,
        table_id: TableID,
        metadata: &TableMetadata,
        root: CatalogTableRootDesc,
        table_ops: &[RowRedoKind],
        checkpoint_cts: u64,
    ) -> Result<CatalogTableRootDesc> {
        // Step 1: Validate root invariants and construct a base index snapshot for reads.
        if root.root_page_id.is_none() && root.pivot_row_id != 0 {
            return Err(Error::InvalidState);
        }
        let root_page_id = root.root_page_id.map_or(0, NonZeroU64::get);
        let entries = if root_page_id == 0 {
            Vec::new()
        } else {
            self.collect_index_entries(root_page_id).await?
        };
        let reader = CatalogMtbIndexPageReader::new(&self.mtb);
        let base_index =
            ColumnBlockIndex::new_with_page_reader(root_page_id, root.pivot_row_id, &reader);
        let mut next_row_id = root.pivot_row_id;

        // Step 2: Preload existing visible rows only when delete-by-key appears.
        let need_delete_lookup = table_ops
            .iter()
            .any(|kind| matches!(kind, RowRedoKind::DeleteByUniqueKey(_)));
        let mut existing_rows = if need_delete_lookup {
            self.load_visible_rows_for_delete_lookup(metadata, &entries, &base_index)
                .await?
        } else {
            Vec::new()
        };
        let mut pending_rows = Vec::new();
        let mut delete_deltas: BTreeMap<RowID, BTreeSet<u32>> = BTreeMap::new();

        // Step 3: Replay table ops into in-memory pending rows and deletion deltas.
        for kind in table_ops {
            match kind {
                RowRedoKind::Insert(vals) => {
                    if vals.len() != metadata.col_count() {
                        return Err(Error::InvalidFormat);
                    }
                    pending_rows.push(PendingInsertRow {
                        row_id: next_row_id,
                        vals: vals.clone(),
                        deleted: false,
                    });
                    next_row_id = next_row_id.saturating_add(1);
                }
                RowRedoKind::DeleteByUniqueKey(key) => {
                    if let Some(row) = pending_rows
                        .iter_mut()
                        .rev()
                        .find(|row| !row.deleted && row_matches_key(metadata, &row.vals, key))
                    {
                        row.deleted = true;
                        continue;
                    }
                    if let Some(row) = existing_rows
                        .iter_mut()
                        .find(|row| !row.deleted && row_matches_key(metadata, &row.vals, key))
                    {
                        row.deleted = true;
                        let delta = row
                            .row_id
                            .checked_sub(row.start_row_id)
                            .ok_or(Error::InvalidState)?;
                        if delta > u32::MAX as u64 {
                            return Err(Error::InvalidState);
                        }
                        delete_deltas
                            .entry(row.start_row_id)
                            .or_default()
                            .insert(delta as u32);
                    }
                }
                RowRedoKind::Delete | RowRedoKind::Update(_) => {
                    return Err(Error::InvalidState);
                }
            }
        }

        // Step 4: Build the live-insert batch after canceling same-batch insert+delete rows.
        let mut current_root_page_id = root_page_id;
        let mut current_end_row_id = root.pivot_row_id;
        let mut entries_changed = false;
        let mut live_inserts = Vec::new();
        for row in pending_rows {
            if !row.deleted {
                live_inserts.push(RowRecord {
                    row_id: row.row_id,
                    vals: row.vals,
                });
            }
        }

        // Step 5: Try CoW-merging inserts into the right-most existing LWC page first.
        if !live_inserts.is_empty()
            && let Some(last_entry) = entries.last().copied()
        {
            let existing_tail_rows = self
                .decode_lwc_page_rows(last_entry.payload.block_id, metadata)
                .await?;
            if !existing_tail_rows.is_empty()
                && let Some((merged_tail_buf, consumed)) = self
                    .build_merged_tail_lwc_page(metadata, &existing_tail_rows, &live_inserts)
                    .await?
            {
                let new_tail_page_id = mutable.allocate_page_id()?;
                mutable
                    .write_page(new_tail_page_id, merged_tail_buf)
                    .await?;
                mutable.record_gc_page(last_entry.payload.block_id);
                let mut updated_payload = last_entry.payload;
                updated_payload.block_id = new_tail_page_id;
                let patches = [ColumnPagePayloadPatch {
                    start_row_id: last_entry.start_row_id,
                    payload: updated_payload,
                }];
                let column_index = ColumnBlockIndex::new_with_page_reader(
                    current_root_page_id,
                    current_end_row_id,
                    &reader,
                );
                current_root_page_id = column_index
                    .batch_replace_payloads(mutable, &patches, checkpoint_cts)
                    .await?;
                entries_changed = true;
                live_inserts.drain(0..consumed);
                if live_inserts.is_empty() {
                    current_end_row_id = next_row_id.max(root.pivot_row_id);
                }
            }
        }

        // Step 6: Persist any remaining inserts as new LWC pages and append index entries.
        if !live_inserts.is_empty() {
            let new_pages = self
                .build_lwc_pages_from_row_records(self.meta_pool, metadata, &live_inserts)
                .await?;
            let mut new_entries = Vec::with_capacity(new_pages.len());
            for page in new_pages {
                let page_id = mutable.allocate_page_id()?;
                mutable.write_page(page_id, page.buf).await?;
                new_entries.push((page.start_row_id, page_id));
            }
            if !new_entries.is_empty() {
                let new_end_row_id = next_row_id.max(root.pivot_row_id);
                let column_index = ColumnBlockIndex::new_with_page_reader(
                    current_root_page_id,
                    current_end_row_id,
                    &reader,
                );
                current_root_page_id = column_index
                    .batch_insert(mutable, &new_entries, new_end_row_id, checkpoint_cts)
                    .await?;
                current_end_row_id = new_end_row_id;
                entries_changed = true;
            }
        }

        // Step 7: Materialize deletion bitmap patches keyed by leaf start-row-id.
        let mut patch_storage: Vec<(RowID, Vec<u8>)> = Vec::new();
        for (start_row_id, pending) in &delete_deltas {
            let idx = entries
                .binary_search_by_key(start_row_id, |entry| entry.start_row_id)
                .map_err(|_| Error::InvalidState)?;
            let entry = entries[idx];
            let mut base = self
                .load_payload_deletion_deltas(&base_index, entry.payload)
                .await?;
            let old_len = base.len();
            base.extend(pending);
            if base.len() == old_len {
                continue;
            }
            patch_storage.push((*start_row_id, encode_deltas_to_bytes(&base)));
        }

        // Step 8: Apply deletion patches with CoW payload updates on the current root.
        if !patch_storage.is_empty() {
            let patches: Vec<OffloadedBitmapPatch<'_>> = patch_storage
                .iter()
                .map(|(start_row_id, bytes)| OffloadedBitmapPatch {
                    start_row_id: *start_row_id,
                    bitmap_bytes: bytes,
                })
                .collect();
            let column_index = ColumnBlockIndex::new_with_page_reader(
                current_root_page_id,
                current_end_row_id,
                &reader,
            );
            current_root_page_id = column_index
                .batch_update_offloaded_bitmaps(mutable, &patches, checkpoint_cts)
                .await?;
            entries_changed = true;
        }

        // Step 9: Publish final per-table root descriptor for this checkpoint batch.
        let root_page_id = if entries_changed {
            Some(NonZeroU64::new(current_root_page_id).ok_or(Error::InvalidState)?)
        } else {
            root.root_page_id
        };
        Ok(CatalogTableRootDesc {
            table_id,
            root_page_id,
            pivot_row_id: next_row_id.max(root.pivot_row_id),
        })
    }

    async fn collect_index_entries(&self, root_page_id: PageID) -> Result<Vec<CatalogIndexEntry>> {
        assert_ne!(root_page_id, 0, "root_page_id must be non-zero");
        let mut stack = vec![root_page_id];
        let mut entries = Vec::new();
        while let Some(page_id) = stack.pop() {
            let page_buf = self.read_catalog_mtb_page(page_id).await?;
            let page = page_buf.as_bytes();
            let header = parse_column_block_header(page)?;
            let count = header.count as usize;
            if header.height == 0 {
                if count > COLUMN_BLOCK_MAX_ENTRIES {
                    return Err(Error::InvalidFormat);
                }
                let row_ids_start = COLUMN_BLOCK_HEADER_SIZE;
                let row_ids_end = row_ids_start + count * mem::size_of::<RowID>();
                let payloads_end = row_ids_end + count * COLUMN_PAGE_PAYLOAD_SIZE;
                if payloads_end > COW_FILE_PAGE_SIZE {
                    return Err(Error::InvalidFormat);
                }
                let mut row_idx = row_ids_start;
                let mut payload_idx = row_ids_end;
                for _ in 0..count {
                    let start_row_id = read_u64(page, row_idx)?;
                    let payload_bytes = page
                        .get(payload_idx..payload_idx + COLUMN_PAGE_PAYLOAD_SIZE)
                        .ok_or(Error::InvalidFormat)?;
                    let payload = pod_read_unaligned::<ColumnPagePayload>(payload_bytes);
                    entries.push(CatalogIndexEntry {
                        start_row_id,
                        payload,
                    });
                    row_idx += mem::size_of::<u64>();
                    payload_idx += COLUMN_PAGE_PAYLOAD_SIZE;
                }
            } else {
                if count > COLUMN_BLOCK_MAX_BRANCH_ENTRIES {
                    return Err(Error::InvalidFormat);
                }
                let entries_start = COLUMN_BLOCK_HEADER_SIZE;
                let entries_end = entries_start + count * COLUMN_BRANCH_ENTRY_SIZE;
                if entries_end > COW_FILE_PAGE_SIZE {
                    return Err(Error::InvalidFormat);
                }
                let mut children = Vec::with_capacity(count);
                let mut idx = entries_start;
                for _ in 0..count {
                    let _start_row_id = read_u64(page, idx)?;
                    let child_page_id = read_u64(page, idx + mem::size_of::<u64>())?;
                    if child_page_id == 0 {
                        return Err(Error::InvalidFormat);
                    }
                    children.push(child_page_id);
                    idx += COLUMN_BRANCH_ENTRY_SIZE;
                }
                for child_page_id in children.into_iter().rev() {
                    stack.push(child_page_id);
                }
            }
        }
        Ok(entries)
    }

    async fn load_visible_rows_for_delete_lookup(
        &self,
        metadata: &TableMetadata,
        entries: &[CatalogIndexEntry],
        column_index: &ColumnBlockIndex<'_>,
    ) -> Result<Vec<ExistingVisibleRow>> {
        let mut rows = Vec::new();
        for entry in entries {
            let deleted = self
                .load_payload_deletion_deltas(column_index, entry.payload)
                .await?;
            let page_rows = self
                .decode_lwc_page_rows(entry.payload.block_id, metadata)
                .await?;
            for row in page_rows {
                let delta = row
                    .row_id
                    .checked_sub(entry.start_row_id)
                    .ok_or(Error::InvalidState)?;
                if delta > u32::MAX as u64 {
                    return Err(Error::InvalidState);
                }
                if deleted.contains(&(delta as u32)) {
                    continue;
                }
                rows.push(ExistingVisibleRow {
                    row_id: row.row_id,
                    start_row_id: entry.start_row_id,
                    vals: row.vals,
                    deleted: false,
                });
            }
        }
        Ok(rows)
    }

    async fn decode_lwc_page_rows(
        &self,
        page_id: u64,
        metadata: &TableMetadata,
    ) -> Result<Vec<RowRecord>> {
        let page_buf = self.read_catalog_mtb_page(page_id).await?;
        let lwc_page = LwcPage::try_from_bytes(page_buf.as_bytes())?;
        let row_count = lwc_page.header.row_count() as usize;
        let row_ids = decode_lwc_row_ids(lwc_page)?;
        if row_ids.len() != row_count {
            return Err(Error::InvalidCompressedData);
        }
        let mut rows = Vec::with_capacity(row_count);
        for (row_idx, row_id) in row_ids.into_iter().enumerate() {
            let mut vals = Vec::with_capacity(metadata.col_count());
            for col_idx in 0..metadata.col_count() {
                let column = lwc_page.column(metadata, col_idx)?;
                if column.is_null(row_idx) {
                    vals.push(Val::Null);
                } else {
                    let data = column.data()?;
                    let val = data.value(row_idx).ok_or(Error::InvalidCompressedData)?;
                    vals.push(val);
                }
            }
            rows.push(RowRecord { row_id, vals });
        }
        Ok(rows)
    }

    async fn build_lwc_pages_from_row_records(
        &self,
        meta_pool: &'static FixedBufferPool,
        metadata: &TableMetadata,
        rows: &[RowRecord],
    ) -> Result<Vec<PendingLwcPage>> {
        for row in rows {
            if row.vals.len() != metadata.col_count() {
                return Err(Error::InvalidFormat);
            }
        }
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let mut lwc_pages = Vec::new();
        let mut builder = LwcBuilder::new(metadata);
        let mut builder_start = None;
        let mut builder_end = 0u64;
        let mut temp_page = meta_pool.allocate_page::<RowPage>().await;

        for row in rows {
            if builder.is_empty() {
                builder_start = Some(row.row_id);
            }
            if !append_single_row_to_builder(metadata, &mut temp_page, &mut builder, row)? {
                let start_row_id = builder_start.take().ok_or(Error::InvalidState)?;
                if builder_end <= start_row_id {
                    return Err(Error::InvalidState);
                }
                let buf = builder.build()?;
                lwc_pages.push(PendingLwcPage { start_row_id, buf });

                builder = LwcBuilder::new(metadata);
                builder_start = Some(row.row_id);
                if !append_single_row_to_builder(metadata, &mut temp_page, &mut builder, row)? {
                    return Err(Error::InvalidState);
                }
            }
            builder_end = row.row_id.saturating_add(1);
        }

        meta_pool.deallocate_page(temp_page);

        if !builder.is_empty() {
            let start_row_id = builder_start.ok_or(Error::InvalidState)?;
            if builder_end <= start_row_id {
                return Err(Error::InvalidState);
            }
            let buf = builder.build()?;
            lwc_pages.push(PendingLwcPage { start_row_id, buf });
        }
        Ok(lwc_pages)
    }

    async fn build_merged_tail_lwc_page(
        &self,
        metadata: &TableMetadata,
        existing_tail_rows: &[RowRecord],
        inserts: &[RowRecord],
    ) -> Result<Option<(DirectBuf, usize)>> {
        if existing_tail_rows.is_empty() || inserts.is_empty() {
            return Ok(None);
        }
        for row in existing_tail_rows {
            if row.vals.len() != metadata.col_count() {
                return Err(Error::InvalidFormat);
            }
        }
        for row in inserts {
            if row.vals.len() != metadata.col_count() {
                return Err(Error::InvalidFormat);
            }
        }

        let mut builder = LwcBuilder::new(metadata);
        let mut temp_page = self.meta_pool.allocate_page::<RowPage>().await;

        for row in existing_tail_rows {
            if !append_single_row_to_builder(metadata, &mut temp_page, &mut builder, row)? {
                self.meta_pool.deallocate_page(temp_page);
                return Err(Error::InvalidState);
            }
        }

        let mut consumed = 0usize;
        for row in inserts {
            if !append_single_row_to_builder(metadata, &mut temp_page, &mut builder, row)? {
                break;
            }
            consumed += 1;
        }

        self.meta_pool.deallocate_page(temp_page);

        if consumed == 0 {
            return Ok(None);
        }
        let buf = builder.build()?;
        Ok(Some((buf, consumed)))
    }

    async fn load_payload_deletion_deltas(
        &self,
        column_index: &ColumnBlockIndex<'_>,
        mut payload: ColumnPagePayload,
    ) -> Result<BTreeSet<u32>> {
        let mut res = BTreeSet::new();
        if let Some(bytes) = column_index.read_offloaded_bitmap_bytes(&payload).await? {
            for delta in decode_deltas_from_bytes(&bytes)? {
                res.insert(delta);
            }
            return Ok(res);
        }
        let list = payload.deletion_list();
        res.extend(list.iter());
        Ok(res)
    }

    async fn read_catalog_mtb_page(&self, page_id: u64) -> Result<DirectBuf> {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        // SAFETY: `DirectBuf` memory is sector-aligned and valid until async read completes.
        unsafe {
            self.mtb
                .read_page_into_ptr(page_id, UnsafePtr(buf.as_bytes_mut().as_mut_ptr()))
                .await?;
        }
        Ok(buf)
    }
}

fn append_single_row_to_builder(
    metadata: &TableMetadata,
    temp_page: &mut crate::buffer::guard::PageExclusiveGuard<RowPage>,
    builder: &mut LwcBuilder<'_>,
    row: &RowRecord,
) -> Result<bool> {
    {
        let page = temp_page.page_mut();
        page.zero();
        page.init(row.row_id, 1, metadata);
        let insert = page.insert(metadata, &row.vals);
        if !matches!(insert, InsertRow::Ok(_)) {
            return Err(Error::InvalidState);
        }
    }
    builder.append_row_page(temp_page.page())
}

fn decode_lwc_row_ids(page: &LwcPage) -> Result<Vec<RowID>> {
    let row_count = page.header.row_count() as usize;
    let col_count = page.header.col_count() as usize;
    let start_idx = col_count * mem::size_of::<u16>();
    let end_idx = page.header.first_col_offset() as usize;
    if end_idx > page.body.len() || start_idx > end_idx {
        return Err(Error::InvalidCompressedData);
    }
    let row_id_data = LwcData::from_bytes(ValKind::U64, &page.body[start_idx..end_idx])?;
    let mut row_ids = Vec::with_capacity(row_count);
    for row_idx in 0..row_count {
        let row_id = row_id_data
            .value(row_idx)
            .and_then(|v| v.as_u64())
            .ok_or(Error::InvalidCompressedData)?;
        row_ids.push(row_id);
    }
    Ok(row_ids)
}

fn parse_column_block_header(page: &[u8]) -> Result<ColumnBlockNodeHeader> {
    if page.len() != COW_FILE_PAGE_SIZE {
        return Err(Error::InvalidFormat);
    }
    let header_slice = page
        .get(..COLUMN_BLOCK_HEADER_SIZE)
        .ok_or(Error::InvalidFormat)?;
    let header = pod_read_unaligned::<ColumnBlockNodeHeader>(header_slice);
    Ok(header)
}

fn read_u64(input: &[u8], idx: usize) -> Result<u64> {
    let end = idx + mem::size_of::<u64>();
    let bytes = input.get(idx..end).ok_or(Error::InvalidFormat)?;
    let arr: [u8; 8] = bytes.try_into()?;
    Ok(u64::from_le_bytes(arr))
}

fn row_matches_key(metadata: &TableMetadata, row: &[Val], key: &SelectKey) -> bool {
    let Some(index_spec) = metadata.index_specs.get(key.index_no) else {
        return false;
    };
    if index_spec.index_cols.len() != key.vals.len() {
        return false;
    }
    for (index_key, key_val) in index_spec.index_cols.iter().zip(&key.vals) {
        let col_idx = index_key.col_no as usize;
        if row.get(col_idx) != Some(key_val) {
            return false;
        }
    }
    true
}

fn encode_deltas_to_bytes(deltas: &BTreeSet<u32>) -> Vec<u8> {
    let mut out = Vec::with_capacity(deltas.len() * mem::size_of::<u32>());
    for delta in deltas {
        out.extend_from_slice(&delta.to_le_bytes());
    }
    out
}

fn decode_deltas_from_bytes(bytes: &[u8]) -> Result<Vec<u32>> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(mem::size_of::<u32>()) {
        return Err(Error::InvalidFormat);
    }
    let mut res = Vec::with_capacity(bytes.len() / mem::size_of::<u32>());
    for chunk in bytes.chunks_exact(mem::size_of::<u32>()) {
        res.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    res.sort_unstable();
    res.dedup();
    Ok(res)
}

#[cfg(test)]
mod tests {
    use crate::catalog::tests::{table1, table2};
    use crate::engine::EngineConfig;
    use crate::trx::sys_conf::TrxSysConfig;
    use tempfile::TempDir;

    #[test]
    fn test_catalog_checkpoint_tail_merge_rewrites_last_payload_without_new_entry() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("catalog-checkpoint-tail-merge")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(engine.trx_sys)
                .await
                .unwrap();

            let snap1 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let tables_root1 = snap1.meta.table_roots[0];
            assert!(tables_root1.root_page_id.is_some());
            let entries1 = engine
                .catalog()
                .storage
                .collect_index_entries(tables_root1.root_page_id.unwrap().get())
                .await
                .unwrap();
            assert!(!entries1.is_empty());

            let _ = table2(&engine).await;
            engine
                .catalog()
                .checkpoint_now(engine.trx_sys)
                .await
                .unwrap();

            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let tables_root2 = snap2.meta.table_roots[0];
            let entries2 = engine
                .catalog()
                .storage
                .collect_index_entries(tables_root2.root_page_id.unwrap().get())
                .await
                .unwrap();

            assert!(tables_root2.pivot_row_id > tables_root1.pivot_row_id);
            assert!(tables_root2.root_page_id != tables_root1.root_page_id);
            assert_eq!(
                entries2.len(),
                entries1.len(),
                "tail-page merge should reuse the existing last index entry when capacity allows"
            );

            let last1 = entries1.last().copied().unwrap();
            let last2 = entries2.last().copied().unwrap();
            assert_eq!(last2.start_row_id, last1.start_row_id);
            assert_ne!(last2.payload.block_id, last1.payload.block_id);
            assert_eq!(last2.payload.deletion_field, last1.payload.deletion_field);
        });
    }
}
