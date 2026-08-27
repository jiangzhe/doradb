use super::{
    LazyRow, LazyRowBuffer, RowPageDescriptor, Table, TableRuntimeLayout, TableScanColdPage,
    TableScanRuntime, TableScanUnit, TableScanWorklist,
};
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::error::{Result, RuntimeResult};
use crate::id::{BlockID, RowID};
use crate::index::ColumnLeafEntry;
use crate::row::RowPage;
use crate::row::ops::ScanRowDecision;
use crate::trx::MvccReadView;
use crate::value::Val;
use std::vec::IntoIter;

/// Source-agnostic ordered physical-unit advancement for a table scan.
pub(crate) trait TableScanUnitCursor {
    /// Return the next captured physical unit, if any.
    fn next_unit(&mut self) -> Option<TableScanUnit>;
}

/// Owned transaction worklist adapter that yields cold units before hot units.
pub(crate) struct TableScanWorklistCursor {
    cold_entries: IntoIter<ColumnLeafEntry>,
    hot_pages: IntoIter<RowPageDescriptor>,
}

impl TableScanWorklistCursor {
    /// Consume one captured worklist into its root scalars and unit cursor.
    #[inline]
    pub(crate) fn from_worklist(worklist: TableScanWorklist) -> (BlockID, RowID, Self) {
        let TableScanWorklist {
            column_root,
            pivot_row_id,
            cold_entries,
            hot_pages,
        } = worklist;
        (
            column_root,
            pivot_row_id,
            Self {
                cold_entries: cold_entries.into_iter(),
                hot_pages: hot_pages.into_iter(),
            },
        )
    }
}

impl TableScanUnitCursor for TableScanWorklistCursor {
    #[inline]
    fn next_unit(&mut self) -> Option<TableScanUnit> {
        self.cold_entries
            .next()
            .map(TableScanUnit::Cold)
            .or_else(|| self.hot_pages.next().map(TableScanUnit::Hot))
    }
}

/// Owned range adapter over an immutable physical-unit slice provider.
pub(crate) struct TableScanRangeCursor<S> {
    units: S,
    next: usize,
    end: usize,
}

impl<S> TableScanRangeCursor<S>
where
    S: AsRef<[TableScanUnit]>,
{
    /// Create a cursor over the validated half-open unit range.
    #[inline]
    pub(crate) fn new(units: S, start: usize, end: usize) -> Self {
        let unit_count = units.as_ref().len();
        assert!(
            start <= end && end <= unit_count,
            "table scan range invariant violated: start={start}, end={end}, unit_count={unit_count}"
        );
        Self {
            units,
            next: start,
            end,
        }
    }
}

impl<S> TableScanUnitCursor for TableScanRangeCursor<S>
where
    S: AsRef<[TableScanUnit]>,
{
    #[inline]
    fn next_unit(&mut self) -> Option<TableScanUnit> {
        if self.next == self.end {
            return None;
        }
        let unit = self.units.as_ref()[self.next];
        self.next += 1;
        Some(unit)
    }
}

enum TableScanUnitState {
    Idle,
    Pending(TableScanUnit),
    Cold {
        page: TableScanColdPage,
        next_row: usize,
    },
    Hot {
        page_guard: PageSharedGuard<RowPage>,
        next_row: usize,
    },
}

/// Result of advancing the row-oriented table-scan cursor without I/O.
pub(crate) enum TableScanCursorAdvance {
    /// One snapshot-visible projected row.
    Row(Vec<Val>),
    /// A pending descriptor must be loaded before row advancement can continue.
    NeedsLoad,
    /// The callback requested terminal stop.
    Stop,
    /// No captured physical units remain.
    Exhausted,
}

/// Shared bounded physical cursor for cold and hot MVCC table-scan rows.
pub(crate) struct TableScanCursor<C> {
    units: C,
    column_root: BlockID,
    pivot_row_id: RowID,
    current: TableScanUnitState,
    row_buffer: LazyRowBuffer,
}

impl<C> TableScanCursor<C>
where
    C: TableScanUnitCursor,
{
    /// Create a cursor over one captured root and ordered unit source.
    #[inline]
    pub(crate) fn new(
        units: C,
        column_root: BlockID,
        pivot_row_id: RowID,
        column_count: usize,
    ) -> Self {
        Self {
            units,
            column_root,
            pivot_row_id,
            current: TableScanUnitState::Idle,
            row_buffer: LazyRowBuffer::new(column_count),
        }
    }

    /// Load the persistent pending descriptor, retaining it if the future is cancelled.
    pub(crate) async fn load_pending(
        &mut self,
        runtime: TableScanRuntime<'_>,
        table: &Table,
        layout: &TableRuntimeLayout,
    ) -> RuntimeResult<()> {
        let unit = match &self.current {
            TableScanUnitState::Pending(unit) => *unit,
            _ => panic!("table scan cursor load requires a persistent pending descriptor"),
        };
        let accessor = table.accessor_with_layout(layout);
        let loaded = match unit {
            TableScanUnit::Cold(entry) => TableScanUnitState::Cold {
                page: accessor
                    .load_table_scan_cold_page(runtime, self.column_root, self.pivot_row_id, &entry)
                    .await?,
                next_row: 0,
            },
            TableScanUnit::Hot(descriptor) => TableScanUnitState::Hot {
                page_guard: accessor
                    .load_table_scan_hot_page(runtime, descriptor)
                    .await?,
                next_row: 0,
            },
        };
        assert!(
            matches!(
                &self.current,
                TableScanUnitState::Pending(pending) if *pending == unit
            ),
            "table scan cursor pending descriptor changed while its load completed"
        );
        self.current = loaded;
        Ok(())
    }

    /// Advance visible rows in the loaded unit until a row or boundary is reached.
    pub(crate) fn advance<F>(
        &mut self,
        table: &Table,
        layout: &TableRuntimeLayout,
        read_view: &MvccReadView,
        projection: &[usize],
        scan_row: &mut F,
    ) -> Result<TableScanCursorAdvance>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<ScanRowDecision>,
    {
        let accessor = table.accessor_with_layout(layout);
        loop {
            match &mut self.current {
                TableScanUnitState::Idle => {
                    let Some(unit) = self.units.next_unit() else {
                        return Ok(TableScanCursorAdvance::Exhausted);
                    };
                    self.current = TableScanUnitState::Pending(unit);
                    return Ok(TableScanCursorAdvance::NeedsLoad);
                }
                TableScanUnitState::Pending(_) => {
                    return Ok(TableScanCursorAdvance::NeedsLoad);
                }
                TableScanUnitState::Cold { page, next_row } => {
                    while *next_row < page.row_count() {
                        let row_idx = *next_row;
                        *next_row += 1;
                        let Some(lazy_row) = accessor.table_scan_cold_row(
                            read_view,
                            page,
                            row_idx,
                            &mut self.row_buffer,
                        ) else {
                            continue;
                        };
                        if let Some(advance) = apply_row(scan_row, projection, lazy_row)? {
                            return Ok(advance);
                        }
                    }
                }
                TableScanUnitState::Hot {
                    page_guard,
                    next_row,
                } => {
                    let row_count = page_guard.page().header.row_count();
                    while *next_row < row_count {
                        let row_idx = *next_row;
                        *next_row += 1;
                        let access = page_guard.read_row(row_idx);
                        let Some(lazy_row) =
                            accessor.table_scan_hot_row(read_view, access, &mut self.row_buffer)
                        else {
                            continue;
                        };
                        if let Some(advance) = apply_row(scan_row, projection, lazy_row)? {
                            return Ok(advance);
                        }
                    }
                }
            }

            // Destroy the exhausted block or page guard before exposing the
            // boundary or selecting another physical descriptor.
            self.current = TableScanUnitState::Idle;
        }
    }
}

#[inline]
fn apply_row<F>(
    scan_row: &mut F,
    projection: &[usize],
    mut lazy_row: LazyRow<'_>,
) -> Result<Option<TableScanCursorAdvance>>
where
    F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<ScanRowDecision>,
{
    let decision = scan_row(&mut lazy_row);
    match decision {
        Ok(ScanRowDecision::Include) => {
            let projected = lazy_row.project(projection);
            lazy_row.reset();
            projected.map(|row| Some(TableScanCursorAdvance::Row(row)))
        }
        Ok(ScanRowDecision::Skip) => {
            lazy_row.reset();
            Ok(None)
        }
        Ok(ScanRowDecision::Stop) => {
            lazy_row.reset();
            Ok(Some(TableScanCursorAdvance::Stop))
        }
        Err(error) => {
            lazy_row.reset();
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::PageID;
    use std::iter::from_fn;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;

    fn hot_unit(id: u64) -> TableScanUnit {
        TableScanUnit::Hot(RowPageDescriptor {
            page_id: PageID::new(id),
            start_row_id: RowID::new(id * 10),
            end_row_id: RowID::new(id * 10 + 10),
        })
    }

    fn collect(mut cursor: impl TableScanUnitCursor) -> Vec<TableScanUnit> {
        from_fn(|| cursor.next_unit()).collect()
    }

    #[test]
    fn range_cursor_covers_empty_singleton_interior_and_full_ranges() {
        let units: Arc<[TableScanUnit]> = Arc::from([hot_unit(1), hot_unit(2), hot_unit(3)]);
        assert!(collect(TableScanRangeCursor::new(Arc::clone(&units), 1, 1)).is_empty());
        assert_eq!(
            collect(TableScanRangeCursor::new(Arc::clone(&units), 1, 2)),
            [hot_unit(2)]
        );
        assert_eq!(
            collect(TableScanRangeCursor::new(Arc::clone(&units), 1, 3)),
            [hot_unit(2), hot_unit(3)]
        );
        assert_eq!(
            collect(TableScanRangeCursor::new(units, 0, 3)),
            [hot_unit(1), hot_unit(2), hot_unit(3)]
        );
    }

    #[test]
    fn range_cursor_rejects_invalid_ranges() {
        let units: Arc<[TableScanUnit]> = Arc::from([hot_unit(1)]);
        for (start, end) in [(1, 0), (0, 2)] {
            let result = catch_unwind(AssertUnwindSafe(|| {
                TableScanRangeCursor::new(Arc::clone(&units), start, end)
            }));
            assert!(result.is_err(), "start={start}, end={end}");
        }
    }

    #[test]
    fn worklist_and_arc_range_adapters_preserve_identical_unit_order() {
        let hot_pages = (1..=3)
            .map(|id| match hot_unit(id) {
                TableScanUnit::Hot(descriptor) => descriptor,
                TableScanUnit::Cold(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        let (_, _, worklist) = TableScanWorklistCursor::from_worklist(TableScanWorklist {
            column_root: BlockID::new(7),
            pivot_row_id: RowID::new(10),
            cold_entries: Vec::new(),
            hot_pages,
        });
        let expected = collect(worklist);
        let units: Arc<[TableScanUnit]> = Arc::from(expected.clone());
        let unit_count = units.len();
        assert_eq!(
            collect(TableScanRangeCursor::new(units, 0, unit_count)),
            expected
        );
    }
}
