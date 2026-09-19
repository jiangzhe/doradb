//! Recovery-only whole-group decoding. Routing retains BTreeMap replacement
//! semantics; ordinary user rows own only ranges into flat group storage.

use super::packed::{PackedRow, PackedUpdate, PackedUpdates, PackedValue, ReplayKind, ReplayOp};
use crate::error::DataIntegrityResult;
use crate::id::{PageID, RowID, TableID, TrxID};
use crate::log::block_group::{read_trx_frame, validate_group_trx, validate_trx_frame_consumed};
use crate::log::redo::{
    DDLRedo, RedoHeader, RowRedo, RowRedoCode, RowRedoKind, TableDML, read_row_redo_code,
};
use crate::row::ops::UpdateCol;
use crate::serde::{Deser, MinBytesHint, combined_min_bytes, read_collection_len};
use crate::value::{Val, ValRef};
use std::collections::BTreeMap;
use std::ops::Range;
#[cfg(test)]
pub(super) use tests::{assert_group_matches_logs, decode_log};

/// Fully validated group, owning the reader's assembled allocation without copying it.
#[derive(Debug)]
pub(super) struct DecodedGroup {
    wire_bytes: Vec<u8>,
    /// Ordered transaction directory, movable separately from borrowed value storage.
    pub(super) transactions: Vec<DecodedTrx>,
    values: Vec<PackedValue>,
    updates: Vec<PackedUpdate>,
}

impl DecodedGroup {
    /// Decode every frame before exposing the group; failure drops all partial state.
    ///
    /// Implements the [group contract](crate::log::block_group::LogBlockGroup)
    /// and [transaction format contract](crate::log::block_group::TrxLog) using
    /// packed storage. Input contains only assembled logical payload bytes; the
    /// shared reader has already validated physical blocks and group metadata.
    /// Acceptance, decoded meaning, and integrity error kinds must match the
    /// owning `TrxLogIterator`/`TrxLog::deser` path. The differential tests below
    /// exercise both paths independently through the complete group.
    pub(super) fn decode(
        wire_bytes: Vec<u8>,
        min_cts: TrxID,
        max_cts: TrxID,
    ) -> DataIntegrityResult<Self> {
        let mut transactions = Vec::new();
        let mut values = Vec::new();
        let mut updates = Vec::new();
        let mut offset = 0;
        while offset < wire_bytes.len() {
            let (end, base, frame) = read_trx_frame(wire_bytes.as_slice(), offset)?;
            let mut reader = FrameReader {
                frame,
                base,
                offset: 0,
                values: &mut values,
                updates: &mut updates,
            };
            let header: RedoHeader = reader.read()?;
            let ddl: Option<Box<DDLRedo>> = reader.read()?;
            let kind = if let Some(ddl) = ddl {
                DecodedTrxKind::Ddl(ddl, reader.read()?)
            } else {
                DecodedTrxKind::Dml(reader.read_tables()?)
            };
            validate_trx_frame_consumed(frame.len(), reader.offset)?;
            validate_group_trx(offset, end, header.cts, min_cts, max_cts)?;
            transactions.push(DecodedTrx { header, kind });
            offset = end;
        }
        Ok(Self {
            wire_bytes,
            transactions,
            values,
            updates,
        })
    }

    /// Borrow one eligible row until admission has copied it into batch-owned storage.
    #[inline]
    pub(super) fn operation<'a>(&'a self, row: &'a DecodedRow, cts: TrxID) -> ReplayOp<'a> {
        let kind = match &row.kind {
            DecodedRowKind::Insert(_, range) => ReplayKind::Insert(PackedRow {
                values: &self.values[range.clone()],
                payload: &self.wire_bytes,
            }),
            DecodedRowKind::Update(_, range) => ReplayKind::Update(PackedUpdates {
                updates: &self.updates[range.clone()],
                payload: &self.wire_bytes,
            }),
            DecodedRowKind::Delete(_) => ReplayKind::Delete,
            DecodedRowKind::Keyed(_) => {
                unreachable!("keyed user redo must be rejected before page admission")
            }
        };
        ReplayOp {
            cts,
            row_id: row.row_id,
            kind,
            payload_bytes: row.payload_bytes,
        }
    }
}

/// Transaction header and its owned routing directory.
#[derive(Debug)]
pub(super) struct DecodedTrx {
    /// Original CTS and user/system kind.
    pub(super) header: RedoHeader,
    /// DDL retains the existing owning representation, including associated DML.
    pub(super) kind: DecodedTrxKind,
}

/// DDL keeps its established handlers; DML-only user rows use packed descriptors.
#[derive(Debug)]
pub(super) enum DecodedTrxKind {
    Ddl(Box<DDLRedo>, BTreeMap<TableID, TableDML>),
    Dml(BTreeMap<TableID, DecodedTable>),
}

/// Table directory entry, ordered by the encoded map key.
#[derive(Debug)]
pub(super) enum DecodedTable {
    Catalog(TableDML),
    User(BTreeMap<RowID, DecodedRow>),
}

impl DecodedTable {
    /// Count selected entries after duplicate-key replacement, as in owning replay.
    #[inline]
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Catalog(dml) => dml.rows.len(),
            Self::User(rows) => rows.len(),
        }
    }
}

/// One user row with no per-row vectors or byte buffers on the ordinary path.
#[derive(Debug)]
pub(super) struct DecodedRow {
    /// Payload identity, which can differ from its enclosing map key.
    pub(super) row_id: RowID,
    /// Page classification metadata and flat descriptor ranges.
    pub(super) kind: DecodedRowKind,
    payload_bytes: usize,
}

/// User redo routing; exceptional keyed forms are fully decoded before filtering.
#[derive(Debug)]
pub(super) enum DecodedRowKind {
    Insert(PageID, Range<usize>),
    Update(PageID, Range<usize>),
    Delete(Option<PageID>),
    Keyed(RowRedo),
}

struct FrameReader<'a> {
    frame: &'a [u8],
    base: usize,
    offset: usize,
    values: &'a mut Vec<PackedValue>,
    updates: &'a mut Vec<PackedUpdate>,
}

impl FrameReader<'_> {
    #[inline]
    fn read<T: Deser>(&mut self) -> DataIntegrityResult<T> {
        let (end, value) = T::deser(self.frame, self.offset)?;
        self.offset = end;
        Ok(value)
    }

    #[inline]
    fn read_count(
        &mut self,
        collection: &str,
        min_bytes: MinBytesHint,
    ) -> DataIntegrityResult<usize> {
        let (end, count) = read_collection_len(self.frame, self.offset, collection, min_bytes)?;
        self.offset = end;
        Ok(count)
    }

    fn read_tables(&mut self) -> DataIntegrityResult<BTreeMap<TableID, DecodedTable>> {
        let count = self.read_count(
            "BTreeMap",
            combined_min_bytes(TableID::MIN_BYTES_HINT, TableDML::MIN_BYTES_HINT),
        )?;
        let mut tables = BTreeMap::new();
        for _ in 0..count {
            let id: TableID = self.read()?;
            let table = if id.is_catalog() {
                DecodedTable::Catalog(self.read()?)
            } else {
                let count = self.read_count(
                    "BTreeMap",
                    combined_min_bytes(RowID::MIN_BYTES_HINT, RowRedo::MIN_BYTES_HINT),
                )?;
                let mut rows = BTreeMap::new();
                for _ in 0..count {
                    let key = self.read()?;
                    let row = self.read_row()?;
                    rows.insert(key, row);
                }
                DecodedTable::User(rows)
            };
            // Last encoded table replaces the entire preceding table, including
            // all its rows. Overwritten entries were still fully validated.
            tables.insert(id, table);
        }
        Ok(tables)
    }

    fn read_row(&mut self) -> DataIntegrityResult<DecodedRow> {
        let row_id = self.read()?;
        let kind_start = self.offset;
        let (end, code) = read_row_redo_code(self.frame, self.offset)?;
        self.offset = end;
        let mut payload_bytes = 0;
        let kind = match code {
            RowRedoCode::Insert => {
                let page = self.read()?;
                let count = self.read_count("Vec", Val::MIN_BYTES_HINT)?;
                let start = self.values.len();
                self.values.reserve(count);
                for _ in 0..count {
                    let value = self.read_value(&mut payload_bytes)?;
                    self.values.push(value);
                }
                DecodedRowKind::Insert(page, start..self.values.len())
            }
            RowRedoCode::Update => {
                let page = self.read()?;
                let count = self.read_count("Vec", UpdateCol::MIN_BYTES_HINT)?;
                let start = self.updates.len();
                self.updates.reserve(count);
                for _ in 0..count {
                    let idx = self.read::<u32>()? as usize;
                    let val = self.read_value(&mut payload_bytes)?;
                    self.updates.push(PackedUpdate { idx, val });
                }
                DecodedRowKind::Update(page, start..self.updates.len())
            }
            RowRedoCode::Delete => DecodedRowKind::Delete(self.read()?),
            RowRedoCode::DeleteByPrimaryKey | RowRedoCode::UpdateByPrimaryKey => {
                self.offset = kind_start;
                let kind: RowRedoKind = self.read()?;
                DecodedRowKind::Keyed(RowRedo { row_id, kind })
            }
        };
        Ok(DecodedRow {
            row_id,
            kind,
            payload_bytes,
        })
    }

    #[inline]
    fn read_value(&mut self, payload_bytes: &mut usize) -> DataIntegrityResult<PackedValue> {
        let (end, value) = ValRef::deser(self.frame, self.offset)?;
        self.offset = end;
        let len = match value {
            ValRef::VarByte(bytes) => bytes.len(),
            _ => 0,
        };
        // Values consume disjoint bytes within the checked frame, whose base and
        // end are already bounded by the owning group's allocation.
        *payload_bytes += len;
        let offset = self.base + end - len;
        Ok(PackedValue::from_wire(value, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::block_group::TrxLog;
    use crate::serde::Ser;

    use crate::catalog::{
        CATALOG_TABLE_ID_START, CatalogSelectKey, IndexID, IndexSlot, USER_TABLE_ID_START,
    };
    use crate::error::DataIntegrityError;
    use crate::log::redo::{RedoLogs, RedoTrxKind};
    use crate::recovery::packed::{PackedPageBatch, ReplayKind};
    use crate::recovery::stream::decode_owning_group;
    use crate::row::{RowValues, UpdateValues};
    use crate::serde::Serde;
    use ordered_float::OrderedFloat;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::slice::from_ref;

    #[derive(Debug, PartialEq, Eq)]
    enum ValueSnapshot {
        Null,
        I8(i8),
        U8(u8),
        I16(i16),
        U16(u16),
        I32(i32),
        U32(u32),
        F32(u32),
        I64(i64),
        U64(u64),
        F64(u64),
        Bytes(Vec<u8>),
    }

    #[derive(Debug, PartialEq, Eq)]
    enum DdlSnapshot {
        CreateTable(TableID),
        DropTable(TableID),
        CreateIndex(TableID, IndexID, IndexSlot),
        DropIndex(TableID, IndexID, IndexSlot),
        CreateRowPage(TableID, PageID, RowID, RowID),
        DataCheckpoint(TableID, RowID, TrxID),
        TableReplaySilentWatermark(TableID),
    }

    #[derive(Debug, PartialEq, Eq)]
    enum RowSnapshotKind {
        Insert(PageID, Vec<ValueSnapshot>),
        Update(PageID, Vec<(usize, ValueSnapshot)>),
        Delete(Option<PageID>),
        DeleteByPrimaryKey(IndexSlot, Vec<ValueSnapshot>),
        UpdateByPrimaryKey(IndexSlot, Vec<ValueSnapshot>, Vec<(usize, ValueSnapshot)>),
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RowSnapshot {
        row_id: RowID,
        kind: RowSnapshotKind,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct TrxSnapshot {
        cts: TrxID,
        trx_kind: RedoTrxKind,
        ddl: Option<DdlSnapshot>,
        tables: BTreeMap<TableID, BTreeMap<RowID, RowSnapshot>>,
    }

    /// Exercise the production packed decoder with an owning reference fixture.
    pub(in crate::recovery) fn decode_log(log: &TrxLog) -> DecodedGroup {
        let mut wire = vec![0; log.ser_len()];
        log.ser(wire.as_mut_slice(), 0);
        DecodedGroup::decode(wire, log.header.cts, log.header.cts).unwrap()
    }

    /// Compare all transactions and fields without using either decoder's output to drive the other.
    pub(in crate::recovery) fn assert_group_matches_logs(group: &DecodedGroup, logs: &[TrxLog]) {
        assert_eq!(group.transactions.len(), logs.len(), "transaction count");
        assert_eq!(packed_snapshots(group), owning_snapshots(logs));
    }

    fn snapshot_value(value: ValRef<'_>) -> ValueSnapshot {
        match value {
            ValRef::Null => ValueSnapshot::Null,
            ValRef::I8(v) => ValueSnapshot::I8(v),
            ValRef::U8(v) => ValueSnapshot::U8(v),
            ValRef::I16(v) => ValueSnapshot::I16(v),
            ValRef::U16(v) => ValueSnapshot::U16(v),
            ValRef::I32(v) => ValueSnapshot::I32(v),
            ValRef::U32(v) => ValueSnapshot::U32(v),
            ValRef::F32(v) => ValueSnapshot::F32(v.0.to_bits()),
            ValRef::I64(v) => ValueSnapshot::I64(v),
            ValRef::U64(v) => ValueSnapshot::U64(v),
            ValRef::F64(v) => ValueSnapshot::F64(v.0.to_bits()),
            ValRef::VarByte(v) => ValueSnapshot::Bytes(v.to_vec()),
        }
    }

    fn snapshot_values(values: &(impl RowValues + ?Sized)) -> Vec<ValueSnapshot> {
        (0..values.len())
            .map(|i| snapshot_value(values.value(i)))
            .collect()
    }

    fn snapshot_updates(updates: &(impl UpdateValues + ?Sized)) -> Vec<(usize, ValueSnapshot)> {
        (0..updates.len())
            .map(|i| {
                let (column, value) = updates.value(i);
                (column, snapshot_value(value))
            })
            .collect()
    }

    fn snapshot_ddl(ddl: &DDLRedo) -> DdlSnapshot {
        match ddl {
            DDLRedo::CreateTable(id) => DdlSnapshot::CreateTable(*id),
            DDLRedo::DropTable(id) => DdlSnapshot::DropTable(*id),
            DDLRedo::CreateIndex {
                table_id,
                index_id,
                index_slot,
            } => DdlSnapshot::CreateIndex(*table_id, *index_id, *index_slot),
            DDLRedo::DropIndex {
                table_id,
                index_id,
                index_slot,
            } => DdlSnapshot::DropIndex(*table_id, *index_id, *index_slot),
            DDLRedo::CreateRowPage {
                table_id,
                page_id,
                start_row_id,
                end_row_id,
            } => DdlSnapshot::CreateRowPage(*table_id, *page_id, *start_row_id, *end_row_id),
            DDLRedo::DataCheckpoint {
                table_id,
                pivor_row_id,
                checkpoint_ts,
            } => DdlSnapshot::DataCheckpoint(*table_id, *pivor_row_id, *checkpoint_ts),
            DDLRedo::TableReplaySilentWatermark { table_id } => {
                DdlSnapshot::TableReplaySilentWatermark(*table_id)
            }
        }
    }

    fn snapshot_row(row: &RowRedo) -> RowSnapshot {
        let kind = match &row.kind {
            RowRedoKind::Insert(page, values) => {
                RowSnapshotKind::Insert(*page, snapshot_values(values.as_slice()))
            }
            RowRedoKind::Update(page, updates) => {
                RowSnapshotKind::Update(*page, snapshot_updates(updates.as_slice()))
            }
            RowRedoKind::Delete(page) => RowSnapshotKind::Delete(*page),
            RowRedoKind::DeleteByPrimaryKey(key) => RowSnapshotKind::DeleteByPrimaryKey(
                key.index_slot,
                snapshot_values(key.vals.as_slice()),
            ),
            RowRedoKind::UpdateByPrimaryKey(key, updates) => RowSnapshotKind::UpdateByPrimaryKey(
                key.index_slot,
                snapshot_values(key.vals.as_slice()),
                snapshot_updates(updates.as_slice()),
            ),
        };
        RowSnapshot {
            row_id: row.row_id,
            kind,
        }
    }

    fn snapshot_tables(
        tables: &BTreeMap<TableID, TableDML>,
    ) -> BTreeMap<TableID, BTreeMap<RowID, RowSnapshot>> {
        tables
            .iter()
            .map(|(id, table)| {
                (
                    *id,
                    table
                        .rows
                        .iter()
                        .map(|(key, row)| (*key, snapshot_row(row)))
                        .collect(),
                )
            })
            .collect()
    }

    fn owning_snapshots(logs: &[TrxLog]) -> Vec<TrxSnapshot> {
        logs.iter()
            .map(|log| TrxSnapshot {
                cts: log.header.cts,
                trx_kind: log.header.trx_kind,
                ddl: log.payload.ddl.as_deref().map(snapshot_ddl),
                tables: snapshot_tables(&log.payload.dml),
            })
            .collect()
    }

    fn snapshot_decoded_row(group: &DecodedGroup, row: &DecodedRow) -> RowSnapshot {
        let kind = match &row.kind {
            DecodedRowKind::Insert(page, range) => RowSnapshotKind::Insert(
                *page,
                snapshot_values(&PackedRow {
                    values: &group.values[range.clone()],
                    payload: &group.wire_bytes,
                }),
            ),
            DecodedRowKind::Update(page, range) => RowSnapshotKind::Update(
                *page,
                snapshot_updates(&PackedUpdates {
                    updates: &group.updates[range.clone()],
                    payload: &group.wire_bytes,
                }),
            ),
            DecodedRowKind::Delete(page) => RowSnapshotKind::Delete(*page),
            DecodedRowKind::Keyed(owned) => {
                assert_eq!(row.row_id, owned.row_id);
                match &owned.kind {
                    RowRedoKind::DeleteByPrimaryKey(_) | RowRedoKind::UpdateByPrimaryKey(..) => {
                        snapshot_row(owned).kind
                    }
                    RowRedoKind::Insert(..) | RowRedoKind::Update(..) | RowRedoKind::Delete(_) => {
                        panic!("ordinary user row was classified as keyed")
                    }
                }
            }
        };
        RowSnapshot {
            row_id: row.row_id,
            kind,
        }
    }

    fn packed_snapshots(group: &DecodedGroup) -> Vec<TrxSnapshot> {
        group
            .transactions
            .iter()
            .map(|trx| {
                let (ddl, tables) = match &trx.kind {
                    DecodedTrxKind::Ddl(ddl, tables) => {
                        (Some(snapshot_ddl(ddl)), snapshot_tables(tables))
                    }
                    DecodedTrxKind::Dml(tables) => (
                        None,
                        tables
                            .iter()
                            .map(|(id, table)| {
                                let rows = match table {
                                    DecodedTable::Catalog(table) => {
                                        assert!(
                                            id.is_catalog(),
                                            "user table classified as catalog: {id}"
                                        );
                                        table
                                            .rows
                                            .iter()
                                            .map(|(key, row)| (*key, snapshot_row(row)))
                                            .collect()
                                    }
                                    DecodedTable::User(rows) => {
                                        assert!(
                                            id.is_user(),
                                            "catalog table classified as user: {id}"
                                        );
                                        rows.iter()
                                            .map(|(key, row)| {
                                                (*key, snapshot_decoded_row(group, row))
                                            })
                                            .collect()
                                    }
                                };
                                (*id, rows)
                            })
                            .collect(),
                    ),
                };
                TrxSnapshot {
                    cts: trx.header.cts,
                    trx_kind: trx.header.trx_kind,
                    ddl,
                    tables,
                }
            })
            .collect()
    }

    fn compare_decoders(
        wire: &[u8],
        min: u64,
        max: u64,
    ) -> Result<Vec<TrxSnapshot>, DataIntegrityError> {
        let owning = decode_owning_group(wire, TrxID::new(min), TrxID::new(max));
        let packed = DecodedGroup::decode(wire.to_vec(), TrxID::new(min), TrxID::new(max));
        match (owning, packed) {
            (Ok(logs), Ok(group)) => {
                assert_group_matches_logs(&group, &logs);
                Ok(packed_snapshots(&group))
            }
            (Err(owning), Err(packed)) => {
                assert_eq!(
                    owning.current_context(),
                    packed.current_context(),
                    "owning={owning:?}, packed={packed:?}"
                );
                Err(*owning.current_context())
            }
            (owning, packed) => panic!(
                "decoder acceptance differs: wire={wire:?}, owning={owning:?}, packed={packed:?}"
            ),
        }
    }

    fn encode_logs(logs: &[TrxLog]) -> Vec<u8> {
        let mut wire = vec![0; logs.iter().map(Ser::ser_len).sum()];
        let mut offset = 0;
        for log in logs {
            offset = log.ser(wire.as_mut_slice(), offset);
        }
        assert_eq!(offset, wire.len());
        wire
    }

    fn assert_value(actual: ValRef<'_>, expected: &Val) {
        match (actual, expected) {
            (ValRef::F32(a), Val::F32(b)) => assert_eq!(a.0.to_bits(), b.0.to_bits()),
            (ValRef::F64(a), Val::F64(b)) => assert_eq!(a.0.to_bits(), b.0.to_bits()),
            (a, b) => assert_eq!(a, b.view()),
        }
    }

    fn assert_operation(op: &ReplayOp<'_>, row: &RowRedo) {
        assert_eq!(op.row_id, row.row_id);
        match (&op.kind, &row.kind) {
            (ReplayKind::Insert(actual), RowRedoKind::Insert(_, expected)) => {
                assert_eq!(actual.len(), expected.len());
                for _ in 0..2 {
                    for (i, value) in expected.iter().enumerate() {
                        assert_value(actual.value(i), value);
                    }
                }
            }
            (ReplayKind::Update(actual), RowRedoKind::Update(_, expected)) => {
                assert_eq!(actual.len(), expected.len());
                for _ in 0..2 {
                    for (i, value) in expected.iter().enumerate() {
                        let (column, actual) = actual.value(i);
                        assert_eq!(column, value.idx);
                        assert_value(actual, &value.val);
                    }
                }
            }
            (ReplayKind::Delete, RowRedoKind::Delete(_)) => (),
            _ => panic!("different operation variants"),
        }
    }

    fn value_log(values: Vec<Val>, cts: u64) -> TrxLog {
        let mut payload = RedoLogs::default();
        let updates = values
            .iter()
            .cloned()
            .enumerate()
            .map(|(idx, val)| UpdateCol { idx, val })
            .collect();
        for (id, kind) in [
            (1, RowRedoKind::Insert(PageID::new(10), values)),
            (2, RowRedoKind::Update(PageID::new(10), updates)),
            (3, RowRedoKind::Delete(Some(PageID::new(10)))),
        ] {
            payload.insert_dml(
                USER_TABLE_ID_START,
                RowRedo {
                    row_id: RowID::new(id),
                    kind,
                },
            );
        }
        TrxLog::new(
            RedoHeader {
                cts: TrxID::new(cts),
                trx_kind: RedoTrxKind::System,
            },
            payload,
        )
    }

    fn differential(logs: &[TrxLog]) {
        let wire = encode_logs(logs);
        let owning = decode_owning_group(&wire, TrxID::new(0), TrxID::new(u64::MAX)).unwrap();
        let ptr = wire.as_ptr();
        let group = DecodedGroup::decode(wire, TrxID::new(0), TrxID::new(u64::MAX)).unwrap();
        assert_eq!(
            ptr,
            group.wire_bytes.as_ptr(),
            "group construction recopied bytes"
        );
        assert_group_matches_logs(&group, &owning);
        assert_eq!(packed_snapshots(&group), owning_snapshots(logs));
        let mut batch = PackedPageBatch::default();
        // A different source group seeds nonzero descriptor and byte offsets.
        let primer = decode_log(&value_log(vec![Val::from("primer")], 0));
        let DecodedTrxKind::Dml(tables) = &primer.transactions[0].kind else {
            panic!()
        };
        let DecodedTable::User(rows) = &tables[&USER_TABLE_ID_START] else {
            panic!()
        };
        batch.append(
            &primer.operation(&rows[&RowID::new(1)], TrxID::new(0)),
            usize::MAX,
        );
        drop(primer);
        let mut expected_rows = Vec::new();
        for (trx, owning) in group.transactions.iter().zip(owning) {
            let DecodedTrxKind::Dml(tables) = &trx.kind else {
                continue;
            };
            for (id, table) in tables {
                let DecodedTable::User(rows) = table else {
                    continue;
                };
                for (key, row) in rows {
                    if matches!(row.kind, DecodedRowKind::Keyed(_)) {
                        continue;
                    }
                    let op = group.operation(row, trx.header.cts);
                    let expected = &owning.payload.dml[id].rows[key];
                    assert_operation(&op, expected);
                    batch.append(&op, usize::MAX);
                    expected_rows.push(snapshot_row(expected));
                }
            }
        }
        drop(group); // Batch bytes must remain valid independently of every group.
        for (op, expected) in batch.operations().skip(1).zip(&expected_rows) {
            assert_eq!(op.row_id, expected.row_id);
            match (op.kind, &expected.kind) {
                (ReplayKind::Insert(values), RowSnapshotKind::Insert(_, expected)) => {
                    assert_eq!(snapshot_values(&values), *expected)
                }
                (ReplayKind::Update(updates), RowSnapshotKind::Update(_, expected)) => {
                    assert_eq!(snapshot_updates(&updates), *expected)
                }
                (ReplayKind::Delete, RowSnapshotKind::Delete(_)) => (),
                _ => panic!("packed batch changed operation kind"),
            }
        }
        assert_eq!(batch.len(), expected_rows.len() + 1);
    }

    fn boundary_values() -> Vec<Val> {
        let mut values = vec![Val::Null];
        macro_rules! signed {
            ($t:ty, $v:ident) => {
                for v in [<$t>::MIN, <$t>::MAX, 0, -1, 1, 0x35] {
                    values.push(Val::$v(v));
                }
            };
        }
        macro_rules! unsigned {
            ($t:ty, $v:ident) => {
                for v in [0, <$t>::MAX, 1, <$t>::MAX / 3] {
                    values.push(Val::$v(v));
                }
            };
        }
        signed!(i8, I8);
        signed!(i16, I16);
        signed!(i32, I32);
        signed!(i64, I64);
        unsigned!(u8, U8);
        unsigned!(u16, U16);
        unsigned!(u32, U32);
        unsigned!(u64, U64);
        for bits in [
            0, 0x80000000, 0x3f123456, 0x7f7fffff, 0xff7fffff, 0x00800000, 1, 0x807fffff,
            0x7f800000, 0xff800000, 0x7fc12345, 0xff812345,
        ] {
            values.push(Val::F32(OrderedFloat(f32::from_bits(bits))));
        }
        for bits in [
            0,
            0x8000000000000000,
            0x3ff123456789abcd,
            0x7fefffffffffffff,
            0xffefffffffffffff,
            0x0010000000000000,
            1,
            0x800fffffffffffff,
            0x7ff0000000000000,
            0xfff0000000000000,
            0x7ff8123456789abc,
            0xfff123456789abcd,
        ] {
            values.push(Val::F64(OrderedFloat(f64::from_bits(bits))));
        }
        for len in [0, 1, 6, 7, 14, 15, 255, 256, 4095, 4096, 4097, 65535] {
            let bytes: Vec<_> = (0..len).map(|i| (i * 37) as u8).collect();
            values.push(Val::from(bytes.as_slice()));
        }
        values
    }

    // Independent framing permits out-of-order and duplicate map keys.
    fn raw_frame(tables: Vec<(TableID, Vec<(RowID, RowRedo)>)>) -> Vec<u8> {
        let mut body = vec![
            0;
            9 + 1
                + 8
                + tables
                    .iter()
                    .map(|(_, rows)| 16
                        + rows.iter().map(|(_, row)| 8 + row.ser_len()).sum::<usize>())
                    .sum::<usize>()
        ];
        let mut i = body.ser_u64(0, 7);
        i = body.ser_u8(i, 1); // System transaction.
        i = body.ser_u8(i, 0); // No DDL.
        i = body.ser_u64(i, tables.len() as u64);
        for (table, rows) in tables {
            i = table.ser(body.as_mut_slice(), i);
            i = body.ser_u64(i, rows.len() as u64);
            for (key, row) in rows {
                i = key.ser(body.as_mut_slice(), i);
                i = row.ser(body.as_mut_slice(), i);
            }
        }
        assert_eq!(i, body.len());
        let mut wire = (body.len() as u64).to_le_bytes().to_vec();
        wire.extend(body);
        wire
    }

    fn raw_row(id: u64, value: &str) -> (RowID, RowRedo) {
        (
            RowID::new(id / 2),
            RowRedo {
                row_id: RowID::new(id),
                kind: RowRedoKind::Insert(PageID::new(10), vec![Val::from(value)]),
            },
        )
    }

    fn assert_corrupt(wire: &[u8]) {
        assert_eq!(
            compare_decoders(wire, 0, u64::MAX),
            Err(DataIntegrityError::InvalidPayload)
        );
    }

    fn contract_row_kinds() -> Vec<RowRedoKind> {
        vec![
            RowRedoKind::Insert(PageID::new(101), vec![]),
            RowRedoKind::Insert(
                PageID::new(u64::MAX),
                vec![
                    Val::Null,
                    Val::F32(OrderedFloat(f32::from_bits(0xffc12345))),
                    Val::from("outlined insert bytes"),
                ],
            ),
            RowRedoKind::Update(PageID::new(102), vec![]),
            RowRedoKind::Update(
                PageID::new(103),
                vec![
                    UpdateCol {
                        idx: u32::MAX as usize,
                        val: Val::U64(u64::MAX),
                    },
                    UpdateCol {
                        idx: 9,
                        val: Val::F64(OrderedFloat(f64::from_bits(0xfff8123456789abc))),
                    },
                    UpdateCol {
                        idx: 9,
                        val: Val::Null,
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("updated bytes"),
                    },
                ],
            ),
            RowRedoKind::Delete(None),
            RowRedoKind::Delete(Some(PageID::new(104))),
            RowRedoKind::DeleteByPrimaryKey(CatalogSelectKey::new(IndexSlot::new(0), vec![])),
            RowRedoKind::DeleteByPrimaryKey(CatalogSelectKey::new(
                IndexSlot::new(u16::MAX),
                vec![Val::from("key bytes"), Val::I16(-13)],
            )),
            RowRedoKind::UpdateByPrimaryKey(
                CatalogSelectKey::new(IndexSlot::new(3), vec![]),
                vec![],
            ),
            RowRedoKind::UpdateByPrimaryKey(
                CatalogSelectKey::new(IndexSlot::new(4), vec![Val::F32(OrderedFloat(-0.0))]),
                vec![UpdateCol {
                    idx: 2,
                    val: Val::from("new keyed bytes"),
                }],
            ),
        ]
    }

    fn contract_ddls() -> Vec<DDLRedo> {
        let table_id = USER_TABLE_ID_START + 5;
        vec![
            DDLRedo::CreateTable(table_id),
            DDLRedo::DropTable(table_id),
            DDLRedo::CreateIndex {
                table_id,
                index_id: IndexID::new(u32::MAX),
                index_slot: IndexSlot::new(17),
            },
            DDLRedo::DropIndex {
                table_id,
                index_id: IndexID::new(19),
                index_slot: IndexSlot::new(u16::MAX),
            },
            DDLRedo::CreateRowPage {
                table_id,
                page_id: PageID::new(23),
                start_row_id: RowID::new(100),
                end_row_id: RowID::new(200),
            },
            DDLRedo::DataCheckpoint {
                table_id,
                pivor_row_id: RowID::new(201),
                checkpoint_ts: TrxID::new(29),
            },
            DDLRedo::TableReplaySilentWatermark { table_id },
        ]
    }

    fn contract_logs() -> Vec<TrxLog> {
        let mut logs = Vec::new();
        for trx_kind in [RedoTrxKind::User, RedoTrxKind::System] {
            logs.push(TrxLog::new(
                RedoHeader {
                    cts: TrxID::new(7),
                    trx_kind,
                },
                RedoLogs::default(),
            ));
        }
        let mut mixed = RedoLogs::default();
        for table_id in [
            CATALOG_TABLE_ID_START,
            TableID::new(CATALOG_TABLE_ID_START.as_u64() - 1),
            USER_TABLE_ID_START,
            CATALOG_TABLE_ID_START + 1,
        ] {
            for (i, kind) in contract_row_kinds().into_iter().enumerate() {
                let key = RowID::new(i as u64 * 2);
                let row = RowRedo {
                    row_id: key + 1,
                    kind,
                };
                let table = TableDML {
                    rows: BTreeMap::from([(key, row)]),
                };
                let mut payload = RedoLogs::default();
                payload.dml.insert(table_id, table);
                logs.push(TrxLog::new(
                    RedoHeader {
                        cts: TrxID::new(7),
                        trx_kind: RedoTrxKind::User,
                    },
                    payload,
                ));
            }
            let rows = contract_row_kinds()
                .into_iter()
                .enumerate()
                .map(|(i, kind)| {
                    (
                        RowID::new(i as u64 * 2),
                        RowRedo {
                            row_id: RowID::new(i as u64 * 2 + 1),
                            kind,
                        },
                    )
                })
                .collect();
            mixed.dml.insert(table_id, TableDML { rows });
        }
        mixed.dml.insert(
            USER_TABLE_ID_START + 2,
            TableDML {
                rows: BTreeMap::new(),
            },
        );
        logs.push(TrxLog::new(
            RedoHeader {
                cts: TrxID::new(7),
                trx_kind: RedoTrxKind::System,
            },
            mixed,
        ));
        for ddl in contract_ddls() {
            let mut payload = RedoLogs {
                ddl: Some(Box::new(ddl)),
                dml: BTreeMap::new(),
            };
            for table in [CATALOG_TABLE_ID_START, USER_TABLE_ID_START] {
                payload.insert_dml(
                    table,
                    RowRedo {
                        row_id: RowID::new(5),
                        kind: RowRedoKind::UpdateByPrimaryKey(
                            CatalogSelectKey::new(IndexSlot::new(6), vec![Val::from("ddl key")]),
                            vec![UpdateCol {
                                idx: 7,
                                val: Val::F64(OrderedFloat(-0.0)),
                            }],
                        ),
                    },
                );
            }
            logs.push(TrxLog::new(
                RedoHeader {
                    cts: TrxID::new(7),
                    trx_kind: RedoTrxKind::System,
                },
                payload,
            ));
        }
        logs
    }

    fn fix_frame_length(wire: &mut [u8]) {
        if wire.len() >= 8 {
            let len = (wire.len() - 8) as u64;
            wire[..8].copy_from_slice(&len.to_le_bytes());
        }
    }

    #[test]
    fn complete_redo_contract_matches_for_all_variants_and_timestamp_bounds() {
        let logs = contract_logs();
        differential(&logs);
        for log in &logs {
            let wire = encode_logs(from_ref(log));
            let expected = owning_snapshots(from_ref(log));
            for (min, max) in [(7, 7), (0, 7), (7, u64::MAX), (0, u64::MAX)] {
                assert_eq!(compare_decoders(&wire, min, max).unwrap(), expected);
            }
            for (min, max) in [(8, 9), (0, 6), (8, 6)] {
                assert_eq!(
                    compare_decoders(&wire, min, max),
                    Err(DataIntegrityError::InvalidPayload)
                );
            }
        }
        for (min, max) in [(0, u64::MAX), (8, 6)] {
            assert!(compare_decoders(&[], min, max).unwrap().is_empty());
        }
        for cts in [0, u64::MAX] {
            let log = value_log(vec![], cts);
            let wire = encode_logs(from_ref(&log));
            assert_eq!(
                compare_decoders(&wire, cts, cts).unwrap(),
                owning_snapshots(&[log])
            );
        }
    }

    #[test]
    fn compact_contract_truncations_and_seeded_mutations_preserve_parity() {
        let mut rng = StdRng::seed_from_u64(0x311dec0de);
        for log in contract_logs() {
            let wire = encode_logs(&[log]);
            for len in 1..wire.len() {
                assert_corrupt(&wire[..len]);
                let mut truncated = wire[..len].to_vec();
                fix_frame_length(&mut truncated);
                assert_corrupt(&truncated);
            }
            for _ in 0..32 {
                let mut changed = wire.clone();
                for _ in 0..rng.random_range(1..=3) {
                    let idx = rng.random_range(0..changed.len());
                    changed[idx] = rng.random();
                }
                // A mutation may describe another valid record. Compare both outcomes.
                let _ = compare_decoders(&changed, 0, u64::MAX);
            }
            let mut trailing = wire.clone();
            trailing.push(0);
            assert_corrupt(&trailing);
            fix_frame_length(&mut trailing);
            assert_corrupt(&trailing);
        }
    }

    #[test]
    fn noncanonical_option_flags_and_independent_delete_wire_keep_their_meaning() {
        // Entire transaction assembled from documented field widths, without Ser.
        let mut wire = 60u64.to_le_bytes().to_vec();
        wire.extend(7u64.to_le_bytes()); // CTS.
        wire.extend([0, 0]); // User transaction, absent DDL.
        wire.extend(1u64.to_le_bytes()); // Table count.
        wire.extend(USER_TABLE_ID_START.as_u64().to_le_bytes());
        wire.extend(1u64.to_le_bytes()); // Row count.
        wire.extend(5u64.to_le_bytes()); // Map identity.
        wire.extend(9u64.to_le_bytes()); // Payload identity.
        wire.extend([2, 1]); // Delete, page present.
        wire.extend(123u64.to_le_bytes());
        let expected = vec![TrxSnapshot {
            cts: TrxID::new(7),
            trx_kind: RedoTrxKind::User,
            ddl: None,
            tables: BTreeMap::from([(
                USER_TABLE_ID_START,
                BTreeMap::from([(
                    RowID::new(5),
                    RowSnapshot {
                        row_id: RowID::new(9),
                        kind: RowSnapshotKind::Delete(Some(PageID::new(123))),
                    },
                )]),
            )]),
        }];
        for flag in [1, 2, 127, 255] {
            wire[59] = flag;
            assert_eq!(compare_decoders(&wire, 7, 7).unwrap(), expected);
        }
        wire.truncate(60);
        wire[59] = 0;
        fix_frame_length(&mut wire);
        let mut expected = expected;
        expected[0]
            .tables
            .get_mut(&USER_TABLE_ID_START)
            .unwrap()
            .get_mut(&RowID::new(5))
            .unwrap()
            .kind = RowSnapshotKind::Delete(None);
        assert_eq!(compare_decoders(&wire, 7, 7).unwrap(), expected);
        for ddl in contract_ddls() {
            let log = TrxLog::new(
                RedoHeader {
                    cts: TrxID::new(7),
                    trx_kind: RedoTrxKind::System,
                },
                RedoLogs {
                    ddl: Some(Box::new(ddl)),
                    dml: BTreeMap::new(),
                },
            );
            let expected = owning_snapshots(from_ref(&log));
            let mut wire = encode_logs(&[log]);
            for flag in [1, 2, 127, 255] {
                wire[17] = flag;
                assert_eq!(compare_decoders(&wire, 7, 7).unwrap(), expected);
            }
        }
    }

    #[test]
    fn overwritten_tables_and_later_transactions_are_still_validated() {
        let valid = raw_frame(vec![
            (USER_TABLE_ID_START, vec![raw_row(2, "first")]),
            (USER_TABLE_ID_START, vec![]),
        ]);
        let actual = compare_decoders(&valid, 7, 7).unwrap();
        assert!(actual[0].tables[&USER_TABLE_ID_START].is_empty());
        let mut bad = valid.clone();
        bad[58] = 255; // Bad row tag in a table replaced by the second table.
        assert_corrupt(&bad);
        let mut group = encode_logs(&[value_log(vec![], 7)]);
        group.extend(&bad);
        assert_corrupt(&group);
        let mut group = encode_logs(&[value_log(vec![], 7)]);
        group.extend(&valid);
        assert_eq!(compare_decoders(&group, 7, 7).unwrap().len(), 2);
    }

    #[test]
    fn all_value_boundaries_survive_decode_pack_and_repeated_borrows() {
        differential(&[
            value_log(vec![Val::from("leading transaction")], 1),
            value_log(boundary_values(), 2),
        ]);
    }

    #[test]
    fn seeded_mixed_values_match_owning_decode_bit_for_bit() {
        let mut rng = StdRng::seed_from_u64(311);
        let logs: Vec<_> = (0..64)
            .map(|cts| {
                let values = (0..16)
                    .map(|_| match rng.random_range(0..12) {
                        0 => Val::Null,
                        1 => Val::I8(rng.random()),
                        2 => Val::U8(rng.random()),
                        3 => Val::I16(rng.random()),
                        4 => Val::U16(rng.random()),
                        5 => Val::I32(rng.random()),
                        6 => Val::U32(rng.random()),
                        7 => Val::F32(OrderedFloat(f32::from_bits(rng.random()))),
                        8 => Val::I64(rng.random()),
                        9 => Val::U64(rng.random()),
                        10 => Val::F64(OrderedFloat(f64::from_bits(rng.random()))),
                        _ => {
                            let bytes: Vec<u8> = (0..rng.random_range(0..512))
                                .map(|_| rng.random())
                                .collect();
                            Val::from(bytes.as_slice())
                        }
                    })
                    .collect();
                value_log(values, cts)
            })
            .collect();
        differential(&logs);
    }

    #[test]
    fn map_order_replacement_and_payload_identity_match_owning() {
        let user = USER_TABLE_ID_START;
        let wire = raw_frame(vec![
            (user + 1, vec![raw_row(20, "replaced whole table")]),
            (
                user,
                vec![
                    raw_row(9, "replaced row"),
                    raw_row(2, "sorted first"),
                    raw_row(8, "last row wins"),
                ],
            ),
            (CATALOG_TABLE_ID_START, vec![raw_row(4, "catalog")]),
            (user + 1, vec![raw_row(10, "replacement table")]),
        ]);
        let actual = compare_decoders(&wire, 7, 7).unwrap();
        let expected = raw_frame(vec![
            (CATALOG_TABLE_ID_START, vec![raw_row(4, "catalog")]),
            (
                user,
                vec![raw_row(2, "sorted first"), raw_row(8, "last row wins")],
            ),
            (user + 1, vec![raw_row(10, "replacement table")]),
        ]);
        assert_eq!(actual, compare_decoders(&expected, 7, 7).unwrap());
    }

    #[test]
    fn truncation_tags_lengths_frames_and_overwritten_corruption_fail_closed() {
        for value in boundary_values() {
            let log = value_log(vec![value], 7);
            let mut wire = vec![0; log.ser_len()];
            log.ser(wire.as_mut_slice(), 0);
            for len in (1..wire.len().min(160)).chain([wire.len() - 1]) {
                let mut truncated = wire[..len].to_vec();
                if len >= 8 {
                    truncated[..8].copy_from_slice(&((len - 8) as u64).to_le_bytes());
                }
                assert_corrupt(&truncated);
            }
        }
        let large = encode_logs(&[value_log(vec![Val::from(vec![0x81; 65535])], 7)]);
        for len in [
            4095,
            4096,
            4097,
            65534,
            65535,
            65536,
            large.len() - 2,
            large.len() - 1,
        ] {
            assert_corrupt(&large[..len]);
            let mut truncated = large[..len].to_vec();
            fix_frame_length(&mut truncated);
            assert_corrupt(&truncated);
        }
        let wire = raw_frame(vec![(
            USER_TABLE_ID_START,
            vec![raw_row(2, "payload"), raw_row(3, "overwrites")],
        )]);
        // Frame prefix, transaction tag, table/row/value counts, row and value tags.
        for (offset, bytes) in [
            (0, u64::MAX.to_le_bytes().to_vec()),
            (16, vec![9]),
            (18, u64::MAX.to_le_bytes().to_vec()),
            (34, u64::MAX.to_le_bytes().to_vec()),
            (58, vec![255]),
            (67, u64::MAX.to_le_bytes().to_vec()),
            (75, 255u32.to_le_bytes().to_vec()),
        ] {
            let mut bad = wire.clone();
            bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
            assert_corrupt(&bad);
        }
        let mut trailing = wire.clone();
        trailing.push(0);
        let len = trailing.len() - 8;
        trailing[..8].copy_from_slice(&(len as u64).to_le_bytes());
        assert_corrupt(&trailing);
        for (min, max) in [(8, 9), (0, 6)] {
            assert_eq!(
                compare_decoders(&wire, min, max),
                Err(DataIntegrityError::InvalidPayload)
            );
        }
        // Sparse updates keep duplicate and descending ordinals for schema validation.
        let mut log = value_log(vec![Val::Null, Val::from("bytes"), Val::U64(1)], 7);
        let RowRedoKind::Update(_, updates) = &mut log
            .payload
            .dml
            .get_mut(&USER_TABLE_ID_START)
            .unwrap()
            .rows
            .get_mut(&RowID::new(2))
            .unwrap()
            .kind
        else {
            panic!()
        };
        updates[0].idx = 9;
        updates[1].idx = 9;
        updates[2].idx = 1;
        differential(&[TrxLog::new(log.header, log.payload)]);
    }

    #[test]
    fn independent_known_wire_tags_and_exact_scalar_bits() {
        let cases = [
            (0, vec![], Val::Null),
            (1, vec![0x81], Val::I8(-127)),
            (2, vec![0xfe], Val::U8(254)),
            (3, vec![0x34, 0x92], Val::I16(0x9234u16 as i16)),
            (4, vec![0x34, 0x92], Val::U16(0x9234)),
            (
                5,
                vec![0x78, 0x56, 0x34, 0x92],
                Val::I32(0x92345678u32 as i32),
            ),
            (6, vec![0x78, 0x56, 0x34, 0x92], Val::U32(0x92345678)),
            (
                7,
                vec![0x45, 0x23, 0xc1, 0xff],
                Val::F32(OrderedFloat(f32::from_bits(0xffc12345))),
            ),
            (
                8,
                vec![0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x81],
                Val::I64(0x8123456789abcdefu64 as i64),
            ),
            (
                9,
                vec![0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x81],
                Val::U64(0x8123456789abcdef),
            ),
            (
                10,
                vec![0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, 0xf8, 0xff],
                Val::F64(OrderedFloat(f64::from_bits(0xfff8123456789abc))),
            ),
            (
                11,
                vec![3, 0, 0xff, 0, 0x80],
                Val::from(&[0xff, 0, 0x80][..]),
            ),
        ];
        for (tag, bytes, expected) in cases {
            let mut wire = vec![tag, 0, 0, 0];
            wire.extend(bytes);
            let mut frame = ((67 + wire.len()) as u64).to_le_bytes().to_vec();
            frame.extend(7u64.to_le_bytes());
            frame.extend([1, 0]); // System transaction, no DDL.
            frame.extend(1u64.to_le_bytes()); // One table.
            frame.extend(USER_TABLE_ID_START.as_u64().to_le_bytes());
            frame.extend(1u64.to_le_bytes()); // One row.
            frame.extend(5u64.to_le_bytes()); // Map identity.
            frame.extend(9u64.to_le_bytes()); // Payload identity.
            frame.push(1); // Insert.
            frame.extend(123u64.to_le_bytes());
            frame.extend(1u64.to_le_bytes()); // One value.
            frame.extend(&wire);
            let expected_trx = TrxSnapshot {
                cts: TrxID::new(7),
                trx_kind: RedoTrxKind::System,
                ddl: None,
                tables: BTreeMap::from([(
                    USER_TABLE_ID_START,
                    BTreeMap::from([(
                        RowID::new(5),
                        RowSnapshot {
                            row_id: RowID::new(9),
                            kind: RowSnapshotKind::Insert(
                                PageID::new(123),
                                vec![snapshot_value(expected.view())],
                            ),
                        },
                    )]),
                )]),
            };
            assert_eq!(compare_decoders(&frame, 7, 7).unwrap(), vec![expected_trx]);
            let (end, value) = ValRef::deser(wire.as_slice(), 0).unwrap();
            assert_eq!(end, wire.len());
            assert_value(value, &expected);
            let packed = PackedValue::from_wire(
                value,
                end - if let ValRef::VarByte(v) = value {
                    v.len()
                } else {
                    0
                },
            );
            let descriptors = [packed];
            assert_value(
                PackedRow {
                    values: &descriptors,
                    payload: &wire,
                }
                .value(0),
                &expected,
            );
            for len in 0..wire.len() {
                assert!(ValRef::deser(&wire[..len], 0).is_err());
            }
        }
    }
}
