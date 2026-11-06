//! Exact prefix admission and owned, single-block serialization.
//!
//! The cursor gathers source positions and statistics without copying values.
//! The planner combines those statistics with all accepted rows and computes
//! the complete block's encoded size. Only a fitting plan permits staging;
//! committing it keeps the staged rows paired with their exact encoding plan.
//! A builder can span several pages, and a cursor can resume across blocks.
//! Final encoding writes the committed plan into one owned output buffer.

use super::codec::{
    IntegerEncodingPlan, flat_bytes_len, flat_primitive_len, write_bytes, write_flat, write_integer,
};
use super::{
    LWC_BLOCK_BODY_SIZE, LWC_BLOCK_HEADER_SIZE, LWC_BLOCK_PAYLOAD_SIZE, LwcBlock, LwcBlockHeader,
    MAX_LWC_ROWS,
};
use crate::bitmap::Bitmap;
use crate::catalog::TableColumnLayout;
use crate::file::block_integrity::{LWC_BLOCK_SPEC, write_block_checksum, write_block_header};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::id::RowID;
use crate::index::identity_set::{IdentitySetSeed, IdentitySetSeeds};
use crate::io::DirectBuf;
use crate::primitive::{PageScalar, PrimitiveBuffer, primitive_types};
use crate::row::column_view::{PageVectorView, ValArrayRef};
use crate::serde::Serde;
use crate::value::{Val, ValKind};
use std::iter::from_fn;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

const SOURCE_BATCH_ROWS: usize = 1_024;

// Byte totals are bounded by MAX_LWC_ROWS values of at most u16::MAX bytes each.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ColumnSummary {
    Signed { extrema: Option<(i64, i64)> },
    Unsigned { extrema: Option<(u64, u64)> },
    Float,
    Bytes { non_null_bytes: usize },
}

impl ColumnSummary {
    #[inline]
    fn new(kind: ValKind) -> Self {
        match kind {
            ValKind::I8 | ValKind::I16 | ValKind::I32 | ValKind::I64 => {
                Self::Signed { extrema: None }
            }
            ValKind::U8 | ValKind::U16 | ValKind::U32 | ValKind::U64 => {
                Self::Unsigned { extrema: None }
            }
            ValKind::F32 | ValKind::F64 => Self::Float,
            ValKind::VarByte => Self::Bytes { non_null_bytes: 0 },
        }
    }

    #[inline]
    fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Self::Signed { extrema: left }, Self::Signed { extrema: right }) => {
                merge_extrema(left, *right)
            }
            (Self::Unsigned { extrema: left }, Self::Unsigned { extrema: right }) => {
                merge_extrema(left, *right)
            }
            (Self::Float, Self::Float) => {}
            (
                Self::Bytes {
                    non_null_bytes: left,
                },
                Self::Bytes {
                    non_null_bytes: right,
                },
            ) => *left += *right,
            _ => unreachable!("LWC summary kinds must match their layout"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct BlockSummary {
    rows: usize,
    columns: Vec<ColumnSummary>,
}

impl Clone for BlockSummary {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            rows: self.rows,
            columns: self.columns.clone(),
        }
    }

    /// Copies a summary while reusing the destination column allocation.
    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.rows = source.rows;
        self.columns.clone_from(&source.columns);
    }
}

impl BlockSummary {
    fn new(layout: &TableColumnLayout) -> Self {
        Self {
            rows: 0,
            columns: (0..layout.col_count())
                .map(|col| ColumnSummary::new(layout.val_kind(col)))
                .collect(),
        }
    }

    /// Replaces this summary with statistics for the selected visible positions.
    /// Counts, non-null extrema, and byte lengths determine current flat/FOR
    /// extents without copying source payloads into the builder.
    fn summarize(&mut self, view: &PageVectorView<'_, '_>, positions: &[u16]) {
        self.rows = positions.len();
        for (col, summary) in self.columns.iter_mut().enumerate() {
            let (nulls, values) = view.col(col);
            summary.summarize(values, nulls.as_deref(), positions);
        }
    }

    /// Merges compatible statistics into candidate scratch after the caller
    /// checks the combined row limit.
    fn merge(&mut self, other: &Self) {
        assert_eq!(
            self.columns.len(),
            other.columns.len(),
            "BlockSummary::merge requires summaries from the same column layout"
        );
        self.rows += other.rows;
        for (column, other) in self.columns.iter_mut().zip(&other.columns) {
            column.merge(other);
        }
    }

    /// Adds a complete validated row after the caller checks the row limit.
    fn append_row(&mut self, values: &[Val]) {
        assert_eq!(
            self.columns.len(),
            values.len(),
            "LWC direct append requires a complete validated row"
        );
        self.rows += 1;
        for (summary, value) in self.columns.iter_mut().zip(values) {
            summary.append_value(value);
        }
    }
}

macro_rules! impl_column_summary {
    (integers { $($int:ident: $it:ty, $category:ident, $wide:ty;)* }
     floats { $($float:ident: $ft:ty;)* }) => {
        impl ColumnSummary {
            fn summarize(&mut self, values: ValArrayRef<'_>, nulls: Option<&[u64]>, positions: &[u16]) {
                match (self, values) {
                    $((Self::$category { extrema }, ValArrayRef::$int(values)) => {
                        *extrema = summarize_extrema(values, positions, nulls, |value| <$it>::load(value) as $wide);
                    },)*
                    $((Self::Float, ValArrayRef::$float(_)) => {},)*
                    (Self::Bytes { non_null_bytes }, ValArrayRef::VarByte(values, _)) => {
                        *non_null_bytes = positions.iter().copied().map(usize::from)
                            .filter(|&pos| !is_null(nulls, pos)).map(|pos| values[pos].len()).sum();
                    }
                    _ => unreachable!("LWC summary and page column kinds must agree"),
                }
            }

            #[inline]
            fn append_value(&mut self, value: &Val) {
                match (self, value) {
                    (_, Val::Null) => {},
                    $((Self::$category { extrema }, Val::$int(value)) => {
                        let value = *value as $wide;
                        merge_extrema(extrema, Some((value, value)));
                    },)*
                    $((Self::Float, Val::$float(_)) => {},)*
                    (Self::Bytes { non_null_bytes }, Val::VarByte(value)) => {
                        *non_null_bytes += value.len();
                    }
                    _ => unreachable!("LWC direct append kinds must match the validated layout"),
                }
            }
        }
    };
}

primitive_types!(impl_column_summary);

struct BatchPart {
    selected: Range<usize>,
    summary: BlockSummary,
}

/// Owned source positions and summaries; no source values survive a guard scope.
pub(crate) struct LwcSourceCursor {
    start_row_id: RowID,
    source_rows: usize,
    source_range: Range<usize>,
    scan_pos: usize,
    consumed_to: usize,
    batch_rows: Vec<u16>,
    pending: Vec<BatchPart>,
    spare: Vec<BlockSummary>,
}

impl LwcSourceCursor {
    /// Captures the frozen source identity and source-coordinate bounds.
    ///
    /// # Panics
    /// Panics if the source row count violates the row page's u16 representation.
    pub(crate) fn new(view: &PageVectorView<'_, '_>) -> Self {
        let source_rows = view.source_rows();
        // RowPageHeader stores its row count in 16 bits; PageVectorView captures
        // that count unchanged, so every source position fits in a u16.
        assert!(
            source_rows <= usize::from(u16::MAX),
            "LwcSourceCursor::new source row count exceeds u16 positions: start_row_id={}, rows={source_rows}",
            view.start_row_id()
        );
        let source_range = view.source_range();
        Self {
            start_row_id: view.start_row_id(),
            source_rows,
            scan_pos: source_range.start,
            consumed_to: source_range.start,
            source_range,
            batch_rows: Vec::with_capacity(SOURCE_BATCH_ROWS),
            pending: Vec::new(),
            spare: Vec::new(),
        }
    }

    /// Returns the committed physical boundary, excluding unaccepted lookahead.
    #[inline]
    pub(crate) fn consumed_to(&self) -> usize {
        self.consumed_to
    }

    fn validate(&self, view: &PageVectorView<'_, '_>) {
        // A cursor belongs to one prepared page across block flushes. Checkpoint
        // reloads that same Transition page and range until the cursor is exhausted;
        // changing its source would invalidate the cached positions and summaries.
        assert!(
            self.start_row_id == view.start_row_id()
                && self.source_rows == view.source_rows()
                && self.source_range == view.source_range(),
            "LwcSourceCursor::validate source changed while resuming: start_row_id={}, rows={}, range={:?}, reloaded_start={}, reloaded_rows={}, reloaded_range={:?}",
            self.start_row_id,
            self.source_rows,
            self.source_range,
            view.start_row_id(),
            view.source_rows(),
            view.source_range()
        );
    }

    /// Prepares at most SOURCE_BATCH_ROWS visible positions after pending parts drain.
    /// Only positions and statistics are retained; source values remain in the page.
    /// `scan_pos` advances for lookahead, while `consumed_to` advances on acceptance.
    /// If no visible rows remain, the invisible tail is consumed immediately.
    fn prepare_batch(&mut self, view: &PageVectorView<'_, '_>, layout: &TableColumnLayout) {
        self.batch_rows.clear();
        for (start, end) in view.range_non_deleted_from(self.scan_pos) {
            let take = (end - start).min(SOURCE_BATCH_ROWS - self.batch_rows.len());
            self.batch_rows
                .extend((start..start + take).map(|pos| pos as u16));
            if self.batch_rows.len() == SOURCE_BATCH_ROWS {
                break;
            }
        }
        if let Some(&last) = self.batch_rows.last() {
            self.scan_pos = usize::from(last) + 1;
            let mut summary = self
                .spare
                .pop()
                .unwrap_or_else(|| BlockSummary::new(layout));
            summary.summarize(view, &self.batch_rows);
            self.pending.push(BatchPart {
                selected: 0..self.batch_rows.len(),
                summary,
            });
        } else {
            self.scan_pos = self.source_range.end;
            self.consumed_to = self.source_range.end;
        }
    }

    /// Refines a rejected part into two smaller candidates without consuming it.
    /// Each half needs fresh statistics because extrema cannot be subtracted.
    /// Push the right half first so the stack retries the left prefix first.
    fn split_front(
        &mut self,
        view: &PageVectorView<'_, '_>,
        layout: &TableColumnLayout,
        left_rows: usize,
    ) {
        let mut left = self
            .pending
            .pop()
            .expect("LWC refinement requires a pending part");
        let middle = left.selected.start + left_rows;
        let right_range = middle..left.selected.end;
        left.selected.end = middle;
        left.summary
            .summarize(view, &self.batch_rows[left.selected.clone()]);
        let mut right = self
            .spare
            .pop()
            .unwrap_or_else(|| BlockSummary::new(layout));
        right.summarize(view, &self.batch_rows[right_range.clone()]);
        self.pending.push(BatchPart {
            selected: right_range,
            summary: right,
        });
        self.pending.push(left);
    }

    /// Commits source progress after staging and the accepted-position callback.
    /// Recycle this part's summary; any unaccepted suffix remains pending.
    #[inline]
    fn consume_front(&mut self) {
        let part = self
            .pending
            .pop()
            .expect("LWC consumption requires an accepted part");
        self.consumed_to = usize::from(self.batch_rows[part.selected.end - 1]) + 1;
        self.spare.push(part.summary);
    }
}

macro_rules! define_column_plans {
    (integers { $($int:ident: $it:ty, $category:ident, $wide:ty;)* }
     floats { $($float:ident: $ft:ty;)* }) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        enum ColumnEncodingPlan {
            $($int(IntegerEncodingPlan<$it>),)*
            $($float { rows: usize },)*
            Bytes { rows: usize, data_len: usize },
        }

        impl ColumnEncodingPlan {
            /// Chooses an encoding for the complete candidate column. Its summary
            /// includes earlier accepted rows, whose integer width may grow too.
            fn new(kind: ValKind, summary: &ColumnSummary, rows: usize) -> Self {
                match (kind, summary) {
                    $((ValKind::$int, ColumnSummary::$category { extrema }) => {
                        let (min, max) = extrema.unwrap_or((0, 0));
                        Self::$int(IntegerEncodingPlan::from_extrema(rows, min as $it, max as $it))
                    },)*
                    $((ValKind::$float, ColumnSummary::Float) => Self::$float { rows },)*
                    (ValKind::VarByte, ColumnSummary::Bytes { non_null_bytes }) => Self::Bytes { rows, data_len: *non_null_bytes },
                    _ => unreachable!("LWC encoding plan kinds must match their summary"),
                }
            }

            /// Returns the exact value payload size including the codec tag.
            /// Block planning accounts for the null prefix and column offsets separately.
            #[inline]
            fn encoded_len(&self) -> usize {
                match self {
                    $(Self::$int(plan) => 1 + plan.ser_len(),)*
                    $(Self::$float { rows } => flat_primitive_len(*rows, mem::size_of::<$ft>()),)*
                    Self::Bytes { rows, data_len } => flat_bytes_len(*rows, *data_len),
                }
            }

            #[inline]
            fn rows(&self) -> usize {
                match self {
                    $(Self::$int(plan) => plan.len(),)*
                    $(Self::$float { rows } => *rows,)*
                    Self::Bytes { rows, .. } => *rows,
                }
            }

            fn write_values(&self, values: &mut LwcValues, nulls: Option<&[u64]>, output: &mut [u8]) -> usize {
                // Plans and staging are constructed together from the same column layout.
                match (self, values) {
                    $((Self::$int(plan), LwcValues::Primitive(PrimitiveBuffer::$int(values))) => write_integer(plan, values, nulls, output),)*
                    $((Self::$float { .. }, LwcValues::Primitive(PrimitiveBuffer::$float(values))) => write_flat(values, output),)*
                    (Self::Bytes { .. }, LwcValues::Bytes { offsets, data }) => write_bytes(offsets, data, output),
                    _ => unreachable!("ColumnEncodingPlan::write_values field=column_kind, frozen plan and values disagree"),
                }
            }
        }
    };
}

primitive_types!(define_column_plans);

impl ColumnEncodingPlan {
    fn write_into(&self, values: &mut LwcValues, nulls: Option<&[u64]>, output: &mut [u8]) {
        // Admission freezes the count and exact extent before staging values.
        assert_eq!(
            values.len(),
            self.rows(),
            "ColumnEncodingPlan::write_into field=row_count"
        );
        assert_eq!(
            output.len(),
            self.encoded_len(),
            "ColumnEncodingPlan::write_into field=column_length"
        );
        let written = self.write_values(values, nulls, output);
        assert_eq!(
            written,
            output.len(),
            "ColumnEncodingPlan::write_into field=written_length"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LwcColumnPlan {
    null_bitmap_len: Option<u16>,
    encoding: ColumnEncodingPlan,
    end_offset: u16,
}

/// Reusable column storage for incomplete block-planning attempts.
#[derive(Default)]
struct PlanScratch {
    columns: Vec<LwcColumnPlan>,
}

impl PlanScratch {
    /// Plans the entire candidate block and returns it only if both limits fit.
    /// The exact size includes the block header, column offset table, nullable
    /// prefixes, and each chosen encoding's payload. No source values are copied.
    /// An empty candidate or one exceeding the row/byte limit returns `None`.
    /// Success transfers column storage into the plan; rejection retains the
    /// allocation and partial columns in scratch, cleared on the next attempt.
    fn try_plan(
        &mut self,
        layout: &TableColumnLayout,
        summary: &BlockSummary,
    ) -> Option<LwcBlockPlan> {
        self.columns.clear();
        if summary.rows == 0 || summary.rows > MAX_LWC_ROWS {
            return None;
        }
        // TableColumnLayout bounds columns by the u16 ordinal domain.
        let mut offset = layout.col_count() * 2;
        for (col, summary_col) in summary.columns.iter().enumerate() {
            let null_bitmap_len = layout
                .nullable(col)
                .then_some(summary.rows.div_ceil(8) as u16);
            let encoding = ColumnEncodingPlan::new(layout.val_kind(col), summary_col, summary.rows);
            offset +=
                encoding.encoded_len() + null_bitmap_len.map_or(0, |len| 2 + usize::from(len));
            if offset > LWC_BLOCK_BODY_SIZE {
                return None;
            }
            self.columns.push(LwcColumnPlan {
                null_bitmap_len,
                encoding,
                end_offset: u16::try_from(offset).ok()?,
            });
        }
        let row_count = u16::try_from(summary.rows).ok()?;
        let payload_len = LWC_BLOCK_HEADER_SIZE + offset;
        if payload_len > LWC_BLOCK_PAYLOAD_SIZE {
            return None;
        }
        Some(LwcBlockPlan {
            row_count,
            columns: mem::take(&mut self.columns),
            payload_len,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LwcBlockPlan {
    row_count: u16,
    columns: Vec<LwcColumnPlan>,
    payload_len: usize,
}

struct LwcPlanner {
    accepted: BlockSummary,
    candidate: BlockSummary,
    accepted_plan: Option<LwcBlockPlan>,
    plan_scratch: PlanScratch,
}

impl LwcPlanner {
    fn new(layout: &TableColumnLayout) -> Self {
        Self {
            accepted: BlockSummary::new(layout),
            candidate: BlockSummary::new(layout),
            accepted_plan: None,
            plan_scratch: PlanScratch::default(),
        }
    }

    /// Tests accepted rows plus a source part, leaving accepted state unchanged.
    /// Rebuild the candidate from accepted state on every attempt so a rejected
    /// part cannot affect a later retry. Planning includes any widening of the
    /// existing columns. The returned plan must be committed after staging its
    /// rows and before another preview replaces the candidate summary.
    fn preview_append(
        &mut self,
        layout: &TableColumnLayout,
        delta: &BlockSummary,
    ) -> Option<LwcBlockPlan> {
        if delta.rows > MAX_LWC_ROWS - self.accepted.rows {
            return None;
        }
        self.candidate.clone_from(&self.accepted);
        self.candidate.merge(delta);
        self.plan_scratch.try_plan(layout, &self.candidate)
    }

    /// Applies the same complete-block preview to one validated catalog row.
    /// A rejected row leaves accepted statistics, staging, and identities intact.
    fn preview_row(&mut self, layout: &TableColumnLayout, values: &[Val]) -> Option<LwcBlockPlan> {
        if self.accepted.rows == MAX_LWC_ROWS {
            return None;
        }
        self.candidate.clone_from(&self.accepted);
        self.candidate.append_row(values);
        self.plan_scratch.try_plan(layout, &self.candidate)
    }

    /// Installs the most recent successful preview after its values are staged.
    /// The accepted summary and plan then describe exactly the owned staged rows.
    #[inline]
    fn commit_preview(&mut self, plan: LwcBlockPlan) {
        mem::swap(&mut self.accepted, &mut self.candidate);
        // Successful construction moved the scratch allocation into `plan`.
        // Recycle the previous accepted allocation for the next preview.
        if let Some(previous) = self.accepted_plan.replace(plan) {
            self.plan_scratch.columns = previous.columns;
        }
    }
}

struct LwcColumnBuffer {
    nulls: Option<Vec<u64>>,
    values: LwcValues,
}

enum LwcValues {
    Primitive(PrimitiveBuffer),
    Bytes { offsets: Vec<u32>, data: Vec<u8> },
}

impl LwcValues {
    #[inline]
    fn new(kind: ValKind) -> Self {
        if kind == ValKind::VarByte {
            Self::Bytes {
                offsets: vec![0],
                data: Vec::new(),
            }
        } else {
            Self::Primitive(PrimitiveBuffer::new(kind))
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Primitive(values) => values.len(),
            // Construction installs the origin offset; appends only extend it.
            Self::Bytes { offsets, .. } => offsets.len() - 1,
        }
    }

    #[inline]
    fn append_row(&mut self, value: &Val) {
        match (self, value) {
            (Self::Primitive(values), value) => values.append(value),
            (Self::Bytes { offsets, data }, Val::VarByte(value)) => {
                data.extend_from_slice(value.as_bytes());
                offsets.push(data.len() as u32);
            }
            (Self::Bytes { offsets, data }, Val::Null) => offsets.push(data.len() as u32),
            _ => unreachable!("LWC values must match the validated direct row"),
        }
    }

    /// Copies positions already admitted by the planner into owned column storage.
    /// The matching part summary supplies byte reservation sizes; null byte values
    /// append repeated offsets without reading their source descriptors.
    fn append_page(
        &mut self,
        values: ValArrayRef<'_>,
        nulls: Option<&[u64]>,
        positions: &[u16],
        summary: &ColumnSummary,
    ) {
        match (self, values) {
            (Self::Primitive(target), source) if !matches!(source, ValArrayRef::VarByte(..)) => {
                source.append_to(target, positions.len(), position_runs(positions));
            }
            (Self::Bytes { offsets, data }, ValArrayRef::VarByte(source, page)) => {
                offsets.reserve(positions.len());
                let ColumnSummary::Bytes { non_null_bytes } = summary else {
                    unreachable!("LWC byte staging requires a byte summary")
                };
                data.reserve(*non_null_bytes);
                for &pos in positions {
                    let pos = usize::from(pos);
                    // A null descriptor can contain stale or invalid physical bytes.
                    if !is_null(nulls, pos) {
                        data.extend_from_slice(source[pos].as_bytes(page));
                    }
                    offsets.push(data.len() as u32);
                }
            }
            _ => unreachable!("LWC staging kinds must match their page layout"),
        }
    }
}

/// Result of consuming the largest fitting visible prefix of a source page.
#[derive(Debug)]
pub(crate) struct AppendProgress {
    /// Committed physical boundary, including an exhausted invisible tail.
    pub(crate) consumed_to: usize,
    /// Visible rows accepted during this call.
    pub(crate) appended_rows: usize,
    /// Whether this source is exhausted or needs a fresh block.
    pub(crate) stop: AppendStop,
}

/// Reason prefix admission stopped.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AppendStop {
    PageExhausted,
    BlockFull,
}

/// Accumulates accepted rows and exact plans without copying rejected values.
pub(crate) struct LwcBuilder {
    layout: Arc<TableColumnLayout>,
    columns: Vec<LwcColumnBuffer>,
    planner: LwcPlanner,
    row_ids: Vec<RowID>,
    row_set_seeds: IdentitySetSeeds,
}

impl LwcBuilder {
    /// Creates an empty builder with lazily grown column staging.
    pub(crate) fn new(layout: Arc<TableColumnLayout>) -> Self {
        let columns = (0..layout.col_count())
            .map(|col| LwcColumnBuffer {
                nulls: layout.nullable(col).then(Vec::new),
                values: LwcValues::new(layout.val_kind(col)),
            })
            .collect();
        let planner = LwcPlanner::new(&layout);
        Self {
            layout,
            columns,
            planner,
            row_ids: Vec::new(),
            row_set_seeds: IdentitySetSeeds::default(),
        }
    }

    /// Returns whether no visible rows have been accepted.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }

    /// Returns the physical rows accepted in this block.
    #[inline]
    pub(crate) fn row_count(&self) -> usize {
        self.row_ids.len()
    }

    /// Returns the ordered accepted identities for final index planning.
    #[inline]
    pub(crate) fn row_ids(&self) -> &[RowID] {
        &self.row_ids
    }

    /// Returns page/window grouping hints for final identity planning.
    #[inline]
    pub(crate) fn row_set_seeds(&self) -> &[IdentitySetSeed] {
        self.row_set_seeds.as_slice()
    }

    /// Atomically admits a complete validated catalog row before copying values.
    /// Returns false when the full candidate block would exceed a row/byte limit,
    /// leaving the accepted block unchanged for finalization or another candidate.
    pub(crate) fn append_row_values(&mut self, row_id: RowID, values: &[Val]) -> bool {
        let Some(plan) = self.planner.preview_row(&self.layout, values) else {
            return false;
        };
        let ordinal = self.row_count();
        for (column, value) in self.columns.iter_mut().zip(values) {
            if let Some(nulls) = column.nulls.as_mut() {
                nulls.resize((ordinal + 1).div_ceil(64), 0);
                if value.is_null() {
                    nulls.bitmap_set(ordinal);
                }
            }
            column.values.append_row(value);
        }
        self.planner.commit_preview(plan);
        self.row_set_seeds.append_row(row_id);
        self.row_ids.push(row_id);
        true
    }

    /// Copies the largest fitting prepared prefix and reports accepted positions once.
    /// Each pending part is planned together with all rows already in this builder
    /// before its values are copied. Rejected parts split left-first until a prefix
    /// fits or the first remaining row cannot join the current block.
    /// Current flat/FOR costs never decrease as rows are added, so stopping at
    /// that row yields the largest fitting prefix.
    ///
    /// `BlockFull` leaves that row pending for a new builder using the same cursor.
    /// `PageExhausted` leaves accepted rows in this builder for the next page; it
    /// describes source exhaustion and does not imply that the block is full.
    ///
    /// # Panics
    /// Panics if the view differs from the cursor's original source identity,
    /// physical row count, or selected source range, or a row violates singleton admission.
    pub(crate) fn append_prefix(
        &mut self,
        view: &PageVectorView<'_, '_>,
        cursor: &mut LwcSourceCursor,
        mut on_accepted: impl FnMut(&[u16]),
    ) -> AppendProgress {
        cursor.validate(view);
        let initial_rows = self.row_count();
        let stop = loop {
            if cursor.pending.is_empty() {
                cursor.prepare_batch(view, &self.layout);
                if cursor.pending.is_empty() {
                    break AppendStop::PageExhausted;
                }
            }
            if self.row_count() == MAX_LWC_ROWS {
                break AppendStop::BlockFull;
            }
            let front = cursor
                .pending
                .last()
                .expect("LWC prepared batch has a front part");
            // This is the admission gate: the complete candidate block must fit
            // before copying values or reporting accepted positions.
            if let Some(plan) = self.planner.preview_append(&self.layout, &front.summary) {
                let positions = &cursor.batch_rows[front.selected.clone()];
                self.append_planned_part(view, positions, &front.summary, plan);
                on_accepted(positions);
                cursor.consume_front();
            } else if !self.refine_rejected_part(view, cursor) {
                break AppendStop::BlockFull;
            }
        };
        self.row_set_seeds
            .append_page(&self.row_ids[initial_rows..]);
        AppendProgress {
            consumed_to: cursor.consumed_to,
            appended_rows: self.row_count() - initial_rows,
            stop,
        }
    }

    /// Stages a fitting part, commits its plan, and records accepted identities.
    /// Positions and summary must describe the part used by the most recent
    /// successful preview; the plan includes that part and all earlier accepted rows.
    /// The caller validates the view against the cursor before planning, then
    /// reports accepted positions and consumes the cursor part after this returns.
    fn append_planned_part(
        &mut self,
        view: &PageVectorView<'_, '_>,
        positions: &[u16],
        summary: &BlockSummary,
        plan: LwcBlockPlan,
    ) {
        let ordinal = self.row_count();
        for (col, column) in self.columns.iter_mut().enumerate() {
            let (nulls, values) = view.col(col);
            let nulls = nulls.as_deref();
            if let Some(target) = column.nulls.as_mut() {
                target.resize((ordinal + positions.len()).div_ceil(64), 0);
                for (idx, &pos) in positions.iter().enumerate() {
                    if is_null(nulls, usize::from(pos)) {
                        target.bitmap_set(ordinal + idx);
                    }
                }
            }
            column
                .values
                .append_page(values, nulls, positions, &summary.columns[col]);
        }
        self.planner.commit_preview(plan);
        self.row_ids.extend(
            positions
                .iter()
                .map(|&pos| view.start_row_id() + u64::from(pos)),
        );
    }

    /// Refines a rejected part for a left-first retry without consuming rows.
    /// Returns true after splitting, or false when a singleton needs a fresh block.
    /// The caller checks the row cap before planning, so remaining capacity is positive.
    /// A rejected singleton in an empty builder violates row admission and panics.
    fn refine_rejected_part(
        &self,
        view: &PageVectorView<'_, '_>,
        cursor: &mut LwcSourceCursor,
    ) -> bool {
        let front = cursor
            .pending
            .last()
            .expect("LWC refinement requires a rejected front part");
        if front.selected.len() > 1 {
            let remaining = MAX_LWC_ROWS - self.row_count();
            let split = if remaining < front.selected.len() {
                remaining
            } else {
                front.selected.len() / 2
            };
            cursor.split_front(view, &self.layout, split);
            return true;
        }
        // DML and replay admit each complete row against singleton LWC capacity.
        let pos = cursor.batch_rows[front.selected.start];
        assert!(
            !self.is_empty(),
            "LwcBuilder::append_prefix source row violates singleton admission: row_id={}, range={}..{}, source_rows={}",
            cursor.start_row_id + u64::from(pos),
            pos,
            usize::from(pos) + 1,
            cursor.source_rows
        );
        false
    }

    /// Releases identity and planner scratch after the caller finalizes block binding.
    /// Transfers staged values and the last accepted plan to the encoding worker.
    /// Callers may finish a full block or the nonempty remainder after all pages.
    ///
    /// # Panics
    /// Panics if the caller has not accepted any rows.
    pub(crate) fn finish(self, binding: u64) -> LwcEncodeInput {
        // Callers finalize only a full block or a nonempty final remainder.
        assert!(
            !self.is_empty(),
            "LwcBuilder::finish requires accepted rows: binding={binding}"
        );
        LwcEncodeInput {
            binding,
            columns: self.columns,
            // Every accepted row commits its complete plan before admission returns.
            plan: self
                .planner
                .accepted_plan
                .expect("nonempty LwcBuilder must have a committed encoding plan"),
        }
    }
}

/// Owned worker input, containing only accepted staging and its frozen encoding plan.
pub(crate) struct LwcEncodeInput {
    binding: u64,
    columns: Vec<LwcColumnBuffer>,
    plan: LwcBlockPlan,
}

impl LwcEncodeInput {
    /// Writes each column directly into one zeroed final block and seals its checksum.
    /// Uses the codecs and destination extents frozen during admission. Assertions
    /// verify that staging still matches that plan; encoding does not choose a
    /// different compression scheme or admit additional rows.
    ///
    /// # Panics
    /// Panics if owned staging or the frozen plan violates the builder's admission contract.
    pub(crate) fn encode(mut self) -> DirectBuf {
        // Only LwcBuilder::finish constructs this input. Admission freezes exact
        // extents and copies the same accepted rows before committing the plan.
        let rows = usize::from(self.plan.row_count);
        assert!(
            (1..=MAX_LWC_ROWS).contains(&rows),
            "LwcEncodeInput::encode field=row_count, actual={rows}, maximum={MAX_LWC_ROWS}"
        );
        assert_eq!(
            self.columns.len(),
            self.plan.columns.len(),
            "LwcEncodeInput::encode field=column_count"
        );
        assert!(
            self.columns.len() <= usize::from(u16::MAX),
            "LwcEncodeInput::encode field=column_count, actual={}",
            self.columns.len()
        );
        let table_len = self.columns.len() * 2;
        assert!(
            table_len <= LWC_BLOCK_BODY_SIZE,
            "LwcEncodeInput::encode field=column_offset_table, actual={table_len}, capacity={LWC_BLOCK_BODY_SIZE}"
        );
        let mut end = table_len;
        // Validate every destination before indexing the fixed-size block.
        for (col, (column, plan)) in self.columns.iter().zip(&self.plan.columns).enumerate() {
            assert_eq!(
                column.values.len(),
                rows,
                "LwcEncodeInput::encode field=column_count, column_no={col}"
            );
            assert_eq!(
                plan.encoding.rows(),
                rows,
                "LwcEncodeInput::encode field=plan_count, column_no={col}"
            );
            assert_eq!(
                column.nulls.as_ref().map(Vec::len),
                plan.null_bitmap_len.map(|_| rows.div_ceil(64)),
                "LwcEncodeInput::encode field=null_bitmap_words, column_no={col}, rows={rows}"
            );
            assert!(
                plan.null_bitmap_len
                    .is_none_or(|len| usize::from(len) == rows.div_ceil(8)),
                "LwcEncodeInput::encode field=null_bitmap_bytes, column_no={col}, rows={rows}"
            );
            if let (ColumnEncodingPlan::Bytes { data_len, .. }, LwcValues::Bytes { data, .. }) =
                (&plan.encoding, &column.values)
            {
                assert_eq!(
                    data.len(),
                    *data_len,
                    "LwcEncodeInput::encode field=variable_data_length, column_no={col}"
                );
            }
            end += plan.encoding.encoded_len()
                + plan.null_bitmap_len.map_or(0, |len| 2 + usize::from(len));
            assert!(
                end <= LWC_BLOCK_BODY_SIZE && end == usize::from(plan.end_offset),
                "LwcEncodeInput::encode field=column_end_offset, column_no={col}, actual={end}, planned={}, capacity={LWC_BLOCK_BODY_SIZE}",
                plan.end_offset
            );
        }
        assert_eq!(
            end + LWC_BLOCK_HEADER_SIZE,
            self.plan.payload_len,
            "LwcEncodeInput::encode field=payload_length"
        );
        assert!(
            self.plan.payload_len <= LWC_BLOCK_PAYLOAD_SIZE,
            "LwcEncodeInput::encode field=payload_length, actual={}, capacity={LWC_BLOCK_PAYLOAD_SIZE}",
            self.plan.payload_len
        );
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_block_header(buf.data_mut(), LWC_BLOCK_SPEC);
        let page = LwcBlock::from_bytes_mut(
            &mut buf.data_mut()[payload_start..payload_start + LWC_BLOCK_PAYLOAD_SIZE],
        );
        page.header = LwcBlockHeader::new(
            self.binding,
            self.plan.row_count,
            self.columns.len() as u16,
            0,
        );
        let mut start = table_len;
        for (col, (column, plan)) in self.columns.iter_mut().zip(&self.plan.columns).enumerate() {
            page.body.ser_u16(col * 2, plan.end_offset);
            let output = &mut page.body[start..usize::from(plan.end_offset)];
            let prefix = if let Some(nulls) = column.nulls.as_ref() {
                let len = rows.div_ceil(8);
                output.ser_u16(0, len as u16);
                for (idx, byte) in output[2..2 + len].iter_mut().enumerate() {
                    *byte = (nulls[idx / 8] >> ((idx % 8) * 8)) as u8;
                }
                if rows % 8 != 0 {
                    output[1 + len] &= (1u8 << (rows % 8)) - 1;
                }
                2 + len
            } else {
                0
            };
            plan.encoding.write_into(
                &mut column.values,
                column.nulls.as_deref(),
                &mut output[prefix..],
            );
            start = usize::from(plan.end_offset);
        }
        write_block_checksum(buf.data_mut());
        buf
    }
}

#[inline]
fn merge_extrema<T: Copy + Ord>(left: &mut Option<(T, T)>, right: Option<(T, T)>) {
    if let Some((min, max)) = right {
        *left = Some(match *left {
            Some((lo, hi)) => (lo.min(min), hi.max(max)),
            None => (min, max),
        });
    }
}

#[inline]
fn is_null(nulls: Option<&[u64]>, pos: usize) -> bool {
    nulls.is_some_and(|nulls| nulls.bitmap_get(pos))
}

fn summarize_extrema<T: Copy, U: Copy + Ord>(
    values: &[T],
    positions: &[u16],
    nulls: Option<&[u64]>,
    decode: impl Fn(T) -> U,
) -> Option<(U, U)> {
    let mut result = None;
    for &pos in positions {
        let pos = usize::from(pos);
        if !is_null(nulls, pos) {
            let value = decode(values[pos]);
            merge_extrema(&mut result, Some((value, value)));
        }
    }
    result
}

#[inline]
fn position_runs(mut positions: &[u16]) -> impl Iterator<Item = Range<usize>> {
    from_fn(move || {
        let &first = positions.first()?;
        // Cursor positions are strictly increasing, so matching endpoints prove
        // a contiguous run without revisiting each position for every column.
        if usize::from(positions[positions.len() - 1]) == usize::from(first) + positions.len() - 1 {
            let end = usize::from(first) + positions.len();
            positions = &[];
            return Some(usize::from(first)..end);
        }
        let mut count = 1;
        while count < positions.len() && usize::from(positions[count]) == usize::from(first) + count
        {
            count += 1;
        }
        positions = &positions[count..];
        Some(usize::from(first)..usize::from(first) + count)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{StorageColumnFlags, StorageColumnSpec, TableMetadata};
    use crate::component::panic_payload_description;
    use crate::file::{FileKind, test_block_id};
    use crate::io::IOBuf;
    use crate::lwc::PreparedLwcBlock;
    use crate::row::column_view::bounded_view;
    use crate::row::{Delete, InsertRow, RowPage};
    use crate::value::MemVar;
    use ordered_float::OrderedFloat;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    struct EncodedSources {
        counts: Vec<usize>,
        used: Vec<usize>,
        seeds: Vec<usize>,
        ids: Vec<RowID>,
        sidecars: Vec<RowID>,
        rows: Vec<Vec<Val>>,
    }

    fn assert_lwc_invariant(operation: impl FnOnce(), expected: &[&str]) {
        let panic = catch_unwind(AssertUnwindSafe(operation))
            .expect_err("invalid internal LWC state must assert its contract");
        let message = panic_payload_description(panic.as_ref());
        for detail in expected {
            assert!(message.contains(detail), "missing {detail:?}: {message}");
        }
    }

    fn row_page_with_rows(
        metadata: &TableMetadata,
        first_row_id: RowID,
        rows: &[Vec<Val>],
    ) -> RowPage {
        let mut page = RowPage::new_test_page();
        page.init(first_row_id, rows.len(), metadata.col.as_ref());
        for (offset, row) in rows.iter().enumerate() {
            assert!(
                matches!(
                    page.insert(metadata.col.as_ref(), row),
                    InsertRow::Ok(row_id) if row_id == first_row_id + offset as u64
                ),
                "row={offset}"
            );
        }
        page
    }

    fn assert_persisted_rows(bytes: &[u8], col_layout: &TableColumnLayout, expected: &[Vec<Val>]) {
        let block =
            LwcBlock::try_from_persisted_bytes(bytes, FileKind::TableFile, test_block_id(1))
                .unwrap();
        assert_eq!(block.row_count(), expected.len());
        assert_eq!(block.header.col_count() as usize, col_layout.col_count());
        for col_idx in 0..col_layout.col_count() {
            let column = block.column(col_layout, col_idx).unwrap();
            assert_eq!(
                column.data().unwrap().len(),
                expected.len(),
                "column={col_idx}"
            );
        }
        let mut prepared = PreparedLwcBlock::new(block, col_layout).unwrap();
        for (row_idx, row) in expected.iter().enumerate() {
            let decoded = block.decode_full_row_values(col_layout, row_idx).unwrap();
            assert_eq!(&decoded, row, "row={row_idx}");
            for (col_idx, expected_value) in row.iter().enumerate() {
                assert_eq!(
                    decoded[col_idx].kind(),
                    expected_value.kind(),
                    "row={row_idx}, column={col_idx}"
                );
                assert_eq!(
                    &prepared
                        .decode_value(block, col_layout, row_idx, col_idx)
                        .unwrap(),
                    expected_value,
                    "prepared row={row_idx}, column={col_idx}"
                );
            }
        }
    }

    fn assert_built_rows(builder: LwcBuilder, expected: &[Vec<Val>]) {
        let col_layout = Arc::clone(&builder.layout);
        let buf = build_with_size_check(builder);
        assert_persisted_rows(buf.as_bytes(), col_layout.as_ref(), expected);
    }

    fn used_payload_size(buf: &DirectBuf) -> usize {
        let block = LwcBlock::try_from_persisted_bytes(
            buf.as_bytes(),
            FileKind::TableFile,
            test_block_id(1),
        )
        .unwrap();
        let end = block.header.col_count() as usize * 2;
        let used_body = u16::from_le_bytes(block.body[end - 2..end].try_into().unwrap()) as usize;
        assert!(block.body[used_body..].iter().all(|&byte| byte == 0));
        24 + used_body
    }

    fn build_with_size_check(builder: LwcBuilder) -> DirectBuf {
        let planned = builder.planner.accepted_plan.as_ref().unwrap().payload_len;
        let buf = builder.finish(0x0807_0605_0403_0201).encode();
        assert_eq!(used_payload_size(&buf), planned);
        assert!(planned <= LWC_BLOCK_PAYLOAD_SIZE);
        buf
    }

    fn append_stale_pages(
        builder: &mut LwcBuilder,
        metadata: &TableMetadata,
        rows: &[Vec<Val>],
        stale: &[Val],
    ) {
        for (chunk, rows) in rows.chunks(512).enumerate() {
            let physical: Vec<Vec<Val>> = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .enumerate()
                        .map(|(col, value)| {
                            if matches!(value, Val::Null) {
                                stale[col].clone()
                            } else {
                                value.clone()
                            }
                        })
                        .collect()
                })
                .collect();
            let mut page =
                row_page_with_rows(metadata, RowID::new((chunk * 512) as u64), &physical);
            for (row_idx, row) in rows.iter().enumerate() {
                for (col_idx, value) in row.iter().enumerate() {
                    if matches!(value, Val::Null) {
                        page.set_null_exclusive(&metadata.col, row_idx, col_idx, true);
                    }
                }
            }
            assert!(append_page(builder, page.vector_view(&metadata.col)));
        }
    }

    fn integer_stats(builder: &LwcBuilder) -> Vec<(bool, i64, i64, u64, u64)> {
        builder
            .planner
            .accepted
            .columns
            .iter()
            .map(|stat| match stat {
                ColumnSummary::Signed {
                    extrema: Some((min, max)),
                } => (true, *min, *max, 0, 0),
                ColumnSummary::Unsigned {
                    extrema: Some((min, max)),
                } => (true, 0, 0, *min, *max),
                _ => (false, 0, 0, 0, 0),
            })
            .collect()
    }

    fn append_page(builder: &mut LwcBuilder, view: PageVectorView<'_, '_>) -> bool {
        let mut cursor = LwcSourceCursor::new(&view);
        builder.append_prefix(&view, &mut cursor, |_| {}).stop == AppendStop::PageExhausted
    }

    fn encode_sources(metadata: &TableMetadata, sources: &[(RowPage, Vec<u64>)]) -> EncodedSources {
        let mut result = EncodedSources {
            counts: Vec::new(),
            used: Vec::new(),
            seeds: Vec::new(),
            ids: Vec::new(),
            sidecars: Vec::new(),
            rows: Vec::new(),
        };
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        for (page, bitmap) in sources {
            let view = page.vector_view_with_del_bitmap(&metadata.col, bitmap.as_slice());
            let mut cursor = LwcSourceCursor::new(&view);
            loop {
                let progress = builder.append_prefix(&view, &mut cursor, |positions| {
                    result
                        .sidecars
                        .extend(positions.iter().map(|&pos| page.row_id(usize::from(pos))));
                });
                assert!(cursor.batch_rows.len() <= SOURCE_BATCH_ROWS);
                assert!(cursor.pending.len() + cursor.spare.len() <= 12);
                assert_eq!(cursor.consumed_to(), progress.consumed_to);
                if progress.stop == AppendStop::PageExhausted {
                    assert_eq!(progress.consumed_to, page.header.row_count());
                    break;
                }
                assert!(!builder.is_empty());
                collect_encoded_source(
                    mem::replace(&mut builder, LwcBuilder::new(Arc::clone(&metadata.col))),
                    &mut result,
                );
            }
        }
        if !builder.is_empty() {
            collect_encoded_source(builder, &mut result);
        }
        assert_eq!(result.sidecars, result.ids);
        result
    }

    fn collect_encoded_source(builder: LwcBuilder, result: &mut EncodedSources) {
        let layout = Arc::clone(&builder.layout);
        result.counts.push(builder.row_count());
        result.seeds.push(builder.row_set_seeds().len());
        result.ids.extend_from_slice(builder.row_ids());
        let buf = build_with_size_check(builder);
        result.used.push(used_payload_size(&buf));
        let block = LwcBlock::try_from_persisted_bytes(
            buf.as_bytes(),
            FileKind::TableFile,
            test_block_id(1),
        )
        .unwrap();
        let mut prepared = PreparedLwcBlock::new(block, &layout).unwrap();
        for row in 0..block.row_count() {
            let values = block.decode_full_row_values(&layout, row).unwrap();
            for (col, value) in values.iter().enumerate() {
                assert_eq!(
                    prepared.decode_value(block, &layout, row, col).unwrap(),
                    *value
                );
            }
            result.rows.push(values);
        }
    }

    /// Purpose: Encode the formerly overflowing nullable U64 fixtures and all-null columns through both ingestion paths.
    /// Expected: Both paths produce identical canonical blocks, exact wire sizes, and all logical rows through the row cap.
    #[test]
    fn nullable_integer_capacity_regressions() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U64,
                StorageColumnFlags::NULLABLE,
            )],
            vec![],
        )
        .unwrap();
        for (count, all_null, expected_size) in [
            (8_060, false, 2_062),
            (9_000, false, 2_296),
            (MAX_LWC_ROWS, true, 3_886),
        ] {
            let rows: Vec<_> = (0..count)
                .map(|i| {
                    vec![if all_null || i % 2 == 1 {
                        Val::Null
                    } else {
                        Val::U64(1 << 40)
                    }]
                })
                .collect();
            let mut direct = LwcBuilder::new(Arc::clone(&metadata.col));
            for (idx, row) in rows.iter().enumerate() {
                assert!(
                    direct.append_row_values(RowID::new(idx as u64), row),
                    "row={idx}"
                );
            }
            if all_null {
                let seeds = direct.row_set_seeds().to_vec();
                assert!(!direct.append_row_values(RowID::new(count as u64), &[Val::Null]));
                assert_eq!(direct.row_set_seeds(), seeds);
                assert_eq!(integer_stats(&direct), vec![(false, 0, 0, 0, 0)]);
            }
            let mut pages = LwcBuilder::new(Arc::clone(&metadata.col));
            append_stale_pages(&mut pages, &metadata, &rows, &[Val::U64(u64::MAX)]);
            assert_eq!(pages.row_ids(), direct.row_ids());
            let direct = build_with_size_check(direct);
            let pages = build_with_size_check(pages);
            assert_eq!(used_payload_size(&direct), expected_size);
            assert_eq!(direct.as_bytes(), pages.as_bytes());
            assert_persisted_rows(direct.as_bytes(), &metadata.col, &rows);
        }
    }

    /// Purpose: Canonicalize every integer type against the final minimum for flat and packed nullable columns.
    /// Expected: Stale extremes and deleted rows do not affect canonical bytes, logical nulls, or row order.
    #[test]
    fn integer_null_slots_use_final_minimum() {
        let kinds = [
            ValKind::I8,
            ValKind::U8,
            ValKind::I16,
            ValKind::U16,
            ValKind::I32,
            ValKind::U32,
            ValKind::I64,
            ValKind::U64,
        ];
        let metadata = TableMetadata::try_new(
            kinds
                .into_iter()
                .map(|kind| StorageColumnSpec::new(kind, StorageColumnFlags::NULLABLE))
                .collect(),
            vec![],
        )
        .unwrap();
        let stale = vec![
            Val::I8(i8::MAX),
            Val::U8(u8::MAX),
            Val::I16(i16::MAX),
            Val::U16(u16::MAX),
            Val::I32(i32::MAX),
            Val::U32(u32::MAX),
            Val::I64(i64::MAX),
            Val::U64(u64::MAX),
        ];
        let low = vec![
            Val::I8(-100),
            Val::U8(100),
            Val::I16(-1000),
            Val::U16(1000),
            Val::I32(-100_000),
            Val::U32(100_000),
            Val::I64(-(1 << 41)),
            Val::U64(1 << 40),
        ];
        for count in [2, 19] {
            let mut rows = vec![vec![Val::Null; 8]; count];
            rows[count - 1] = low.clone();
            if count > 2 {
                rows[3] = vec![
                    Val::I8(-99),
                    Val::U8(101),
                    Val::I16(-999),
                    Val::U16(1001),
                    Val::I32(-99_999),
                    Val::U32(100_001),
                    Val::I64(-(1 << 41) + 1),
                    Val::U64((1 << 40) + 1),
                ];
            }
            let mut direct = LwcBuilder::new(Arc::clone(&metadata.col));
            for (idx, row) in rows.iter().enumerate() {
                assert!(direct.append_row_values(RowID::new(idx as u64), row));
            }
            let mut physical = vec![stale.clone(); count + 1];
            physical[count - 1] = low.clone();
            if count > 2 {
                physical[3] = rows[3].clone();
            }
            let mut page = row_page_with_rows(&metadata, RowID::new(0), &physical);
            for (row_idx, row) in rows.iter().enumerate().take(count - 1) {
                for (col_idx, value) in row.iter().enumerate() {
                    page.set_null_exclusive(
                        &metadata.col,
                        row_idx,
                        col_idx,
                        matches!(value, Val::Null),
                    );
                }
            }
            assert!(matches!(page.delete(RowID::new(count as u64)), Delete::Ok));
            let mut pages = LwcBuilder::new(Arc::clone(&metadata.col));
            assert!(append_page(&mut pages, page.vector_view(&metadata.col)));
            assert_eq!(pages.row_ids(), direct.row_ids());
            let direct = build_with_size_check(direct);
            let pages = build_with_size_check(pages);
            assert_eq!(direct.as_bytes(), pages.as_bytes());
            assert_persisted_rows(direct.as_bytes(), &metadata.col, &rows);
            let block = LwcBlock::try_from_persisted_bytes(
                direct.as_bytes(),
                FileKind::TableFile,
                test_block_id(1),
            )
            .unwrap();
            for (col_idx, expected) in low.iter().enumerate() {
                let column = block.column(&metadata.col, col_idx).unwrap();
                let data = column.data().unwrap();
                for (row_idx, row) in rows.iter().enumerate() {
                    let expected = if matches!(row[col_idx], Val::Null) {
                        expected
                    } else {
                        &row[col_idx]
                    };
                    assert_eq!(
                        data.value(row_idx).as_ref(),
                        Some(expected),
                        "physical row={row_idx}, col={col_idx}"
                    );
                }
            }
        }
    }

    /// Purpose: Admit an exact-fit mixed block while rejecting one extra byte and defending bypassed admission.
    /// Expected: The singleton uses flat, rejected appends preserve state, and both body and offset overflow assert the frozen-plan contract.
    #[test]
    fn integer_singleton_exact_fit_and_final_capacity_guard() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .unwrap();
        let row = vec![Val::U64(17), Val::try_from(vec![7; 65_442]).unwrap()];
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let too_large = vec![Val::U64(u64::MAX), Val::try_from(vec![7; 65_443]).unwrap()];
        assert!(!builder.append_row_values(RowID::new(0), &too_large));
        assert!(builder.is_empty());
        assert_eq!(builder.row_ids(), &[]);
        assert_eq!(builder.row_set_seeds(), &[]);
        assert!(integer_stats(&builder).iter().all(|stat| !stat.0));
        assert!(builder.append_row_values(RowID::new(0), &row));
        let buf = build_with_size_check(builder);
        assert_eq!(used_payload_size(&buf), 65_504);
        assert_persisted_rows(buf.as_bytes(), &metadata.col, &[row]);

        for length in [65_443, 65_500] {
            let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
            assert!(
                builder.append_row_values(
                    RowID::new(0),
                    &[Val::U64(17), Val::try_from("ok").unwrap()]
                )
            );
            let mut input = builder.finish(0);
            input.plan.columns[1].encoding = ColumnEncodingPlan::Bytes {
                rows: 1,
                data_len: length,
            };
            let LwcValues::Bytes { offsets, data } = &mut input.columns[1].values else {
                panic!("byte column");
            };
            data.resize(length, 7);
            offsets[1] = length as u32;
            assert_lwc_invariant(
                || {
                    input.encode();
                },
                &[
                    "LwcEncodeInput::encode",
                    "field=column_end_offset",
                    "column_no=1",
                    "capacity=65480",
                    "actual=",
                ],
            );
        }
    }

    /// Purpose: Preserve accepted state across direct and prefix capacity failures, bitmap growth and changed integer extrema.
    /// Expected: Identities, seeds, sizes, and statistics stay exact, and subsequent mixed-column rows round trip.
    #[test]
    fn mixed_column_capacity_rejection() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I64, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::F64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
            ],
            vec![],
        )
        .unwrap();
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let mut rows: Vec<_> = (0..8)
            .map(|i| {
                vec![
                    if i % 2 == 0 {
                        Val::Null
                    } else {
                        Val::I64(1 << 40)
                    },
                    Val::F64(OrderedFloat(i as f64)),
                    if i == 0 {
                        Val::try_from(vec![42; 64_900]).unwrap()
                    } else {
                        Val::Null
                    },
                ]
            })
            .collect();
        for (idx, row) in rows.iter().enumerate() {
            assert!(builder.append_row_values(RowID::new(idx as u64), row));
        }
        let ids = builder.row_ids().to_vec();
        let seeds = builder.row_set_seeds().to_vec();
        let stats = integer_stats(&builder);
        let size = builder.planner.accepted_plan.as_ref().unwrap().payload_len;
        for value in [Val::I64(i64::MIN), Val::Null] {
            let oversized = vec![
                value,
                Val::F64(OrderedFloat(-123.0)),
                Val::try_from(vec![5; 1_024]).unwrap(),
            ];
            assert!(!builder.append_row_values(RowID::new(8), &oversized));
            assert_eq!(builder.row_ids(), ids);
            assert_eq!(builder.row_set_seeds(), seeds);
            assert_eq!(integer_stats(&builder), stats);
            assert_eq!(
                builder.planner.accepted_plan.as_ref().unwrap().payload_len,
                size
            );
            let page = row_page_with_rows(&metadata, RowID::new(8), &[oversized]);
            assert!(!append_page(&mut builder, page.vector_view(&metadata.col)));
            assert_eq!(builder.row_ids(), ids);
            assert_eq!(builder.row_set_seeds(), seeds);
            assert_eq!(integer_stats(&builder), stats);
            assert_eq!(
                builder.planner.accepted_plan.as_ref().unwrap().payload_len,
                size
            );
        }
        let last = vec![
            Val::I64(-(1 << 40)),
            Val::F64(OrderedFloat(0.5)),
            Val::try_from("tail").unwrap(),
        ];
        assert!(builder.append_row_values(RowID::new(8), &last));
        rows.push(last);
        assert_built_rows(builder, &rows);
    }

    /// Purpose: Compare planned sizes with persisted offsets for reproducible mixed nullable values.
    /// Expected: Both ingestion paths encode identical logical rows and every accepted block has the exact planned used length.
    #[test]
    fn seeded_mixed_column_size_invariant() {
        use rand::rngs::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(337);
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I64, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::F32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .unwrap();
        for case in 0..32 {
            let count = rng.random_range(1..256);
            let rows: Vec<_> = (0..count)
                .map(|row| {
                    let signed = if case % 2 == 0 {
                        -(1 << 40) + rng.random_range(0..4)
                    } else {
                        rng.random::<i64>()
                    };
                    let unsigned = if case % 2 == 0 {
                        u64::MAX - rng.random_range(0..256)
                    } else {
                        rng.random::<u64>()
                    };
                    vec![
                        if case != 0 && rng.random_bool(0.3) {
                            Val::Null
                        } else {
                            Val::I64(signed)
                        },
                        if case != 0 && rng.random_bool(0.3) {
                            Val::Null
                        } else {
                            Val::U64(unsigned)
                        },
                        Val::F32(OrderedFloat(row as f32 / 4.0)),
                        Val::try_from(vec![row as u8; rng.random_range(0..32)]).unwrap(),
                    ]
                })
                .collect();
            let mut direct = LwcBuilder::new(Arc::clone(&metadata.col));
            for (idx, row) in rows.iter().enumerate() {
                assert!(
                    direct.append_row_values(RowID::new(idx as u64), row),
                    "seed=337 case={case} row={idx}"
                );
            }
            let mut pages = LwcBuilder::new(Arc::clone(&metadata.col));
            append_stale_pages(
                &mut pages,
                &metadata,
                &rows,
                &[
                    Val::I64(i64::MAX),
                    Val::U64(0),
                    Val::F32(OrderedFloat(0.0)),
                    Val::try_from("").unwrap(),
                ],
            );
            let direct = build_with_size_check(direct);
            let pages = build_with_size_check(pages);
            assert_eq!(direct.as_bytes(), pages.as_bytes(), "seed=337 case={case}");
            assert_persisted_rows(direct.as_bytes(), &metadata.col, &rows);
        }
    }

    /// Purpose: Keep LWC encoding inputs and results transferable to worker threads.
    /// Expected: Builder and result types satisfy the thread-transfer and lifetime bounds.
    #[test]
    fn lwc_encode_task_types_are_send_and_static() {
        fn assert_send_static<T: Send + 'static>() {}

        assert_send_static::<LwcEncodeInput>();
        assert_send_static::<LwcSourceCursor>();
        assert_send_static::<DirectBuf>();
    }

    /// Purpose: Build a persisted block from a row page containing nulls and deleted rows.
    /// Expected: Live rows retain their values and nulls with the supplied block binding value.
    #[test]
    fn test_lwc_builder_from_row_page() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U8, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I16, StorageColumnFlags::NULLABLE),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(100), 20, metadata.col.as_ref());

        let mut expected_rows = Vec::new();
        for offset in 0..10u64 {
            let row_id = 100 + offset;
            let c0 = Val::U8(offset as u8);
            let c1 = if offset % 2 == 0 {
                Val::Null
            } else {
                Val::I16(offset as i16)
            };
            let res = page.insert(metadata.col.as_ref(), &[c0, c1]);
            assert!(matches!(res, InsertRow::Ok(_)));
            expected_rows.push((
                row_id,
                offset as u8,
                if offset % 2 == 0 {
                    None
                } else {
                    Some(offset as i16)
                },
            ));
        }

        assert!(matches!(page.delete(RowID::new(102)), Delete::Ok));
        assert!(matches!(page.delete(RowID::new(105)), Delete::Ok));
        expected_rows.retain(|(row_id, _, _)| *row_id != 102 && *row_id != 105);

        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        assert!(builder.is_empty());
        assert_eq!(builder.row_count(), 0);
        let view = page.vector_view(metadata.col.as_ref());
        let appended = append_page(&mut builder, view);
        assert!(appended);
        let expected_ids: Vec<_> = expected_rows
            .iter()
            .map(|(row_id, _, _)| RowID::new(*row_id))
            .collect();
        assert_eq!(builder.row_ids(), expected_ids);
        let expected_binding_value = 0x0807_0605_0403_0201;
        let buf = builder.finish(expected_binding_value).encode();

        let lwc_block = LwcBlock::try_from_persisted_bytes(
            buf.as_bytes(),
            FileKind::TableFile,
            test_block_id(1),
        )
        .unwrap();
        assert_eq!(lwc_block.header.row_count() as usize, expected_rows.len());
        assert_eq!(
            lwc_block.header.block_binding_value(),
            expected_binding_value
        );

        let column0 = lwc_block.column(metadata.col.as_ref(), 0).unwrap();
        let column1 = lwc_block.column(metadata.col.as_ref(), 1).unwrap();
        let data0 = column0.data().unwrap();
        let data1 = column1.data().unwrap();
        assert_eq!(data0.len(), expected_rows.len());
        assert_eq!(data1.len(), expected_rows.len());
        for (idx, (_row_id, c0, c1)) in expected_rows.iter().enumerate() {
            assert_eq!(data0.value(idx).unwrap().as_u8().unwrap(), *c0);
            if let Some(value) = c1 {
                assert!(!column1.is_null(idx));
                assert_eq!(data1.value(idx).unwrap().as_i16().unwrap(), *value);
            } else {
                assert!(column1.is_null(idx));
            }
        }
    }

    /// Purpose: Compare direct row appends with row-page ingestion for mixed nullable values.
    /// Expected: Both ingestion paths produce the same row identities and persisted bytes.
    #[test]
    fn test_lwc_builder_append_row_values_matches_row_page() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I16, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::F32, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let rows = vec![
            vec![
                Val::Null,
                Val::U64(10),
                Val::try_from(Vec::from(&b"alpha"[..])).unwrap(),
                Val::F32(OrderedFloat(1.25)),
            ],
            vec![
                Val::I16(-5),
                Val::U64(11),
                Val::Null,
                Val::F32(OrderedFloat(2.5)),
            ],
            vec![
                Val::I16(9),
                Val::U64(12),
                Val::try_from(Vec::from(&b"beta"[..])).unwrap(),
                Val::F32(OrderedFloat(3.75)),
            ],
        ];
        let page = row_page_with_rows(&metadata, RowID::new(10), &rows);

        let mut page_builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let view = page.vector_view(metadata.col.as_ref());
        assert!(append_page(&mut page_builder, view));
        let mut direct_builder = LwcBuilder::new(Arc::clone(&metadata.col));
        for (offset, vals) in rows.iter().enumerate() {
            assert!(direct_builder.append_row_values(RowID::new(10 + offset as u64), vals));
        }

        let expected_ids = [RowID::new(10), RowID::new(11), RowID::new(12)];
        assert_eq!(direct_builder.row_ids(), expected_ids);
        assert_eq!(page_builder.row_ids(), expected_ids);
        let binding_value = 0x0807_0605_0403_0201;
        let page_buf = page_builder.finish(binding_value).encode();
        let direct_buf = direct_builder.finish(binding_value).encode();
        assert_eq!(direct_buf.as_bytes(), page_buf.as_bytes());
        assert_persisted_rows(direct_buf.as_bytes(), metadata.col.as_ref(), &rows);
    }

    /// Purpose: Recover a mixed nullable builder after a direct append exceeds block capacity.
    /// Expected: Rejection preserves identities, values, nulls, and statistics, and subsequent appends succeed.
    #[test]
    fn test_lwc_builder_append_row_values_preserves_state_on_rejection() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I16, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let first_row = vec![Val::I16(5), Val::try_from("ok").unwrap()];
        assert!(builder.append_row_values(RowID::new(1), &first_row));
        let expected_stats = vec![(true, 5, 5, 0, 0), (false, 0, 0, 0, 0)];
        assert_eq!(integer_stats(&builder), expected_stats);

        for value in [Val::I16(1000), Val::Null] {
            let oversized = Val::try_from(vec![0; LWC_BLOCK_PAYLOAD_SIZE]).unwrap();
            assert!(!builder.append_row_values(RowID::new(2), &[value, oversized]));
            assert_eq!(builder.row_count(), 1);
            assert_eq!(builder.row_ids(), &[RowID::new(1)]);
            assert_eq!(integer_stats(&builder), expected_stats);
        }

        let next_row = vec![Val::I16(8), Val::Null];
        assert!(builder.append_row_values(RowID::new(2), &next_row));
        assert_eq!(builder.row_ids(), &[RowID::new(1), RowID::new(2)]);
        assert_eq!(
            integer_stats(&builder),
            vec![(true, 5, 8, 0, 0), (false, 0, 0, 0, 0)]
        );
        assert_built_rows(builder, &[first_row, next_row]);
    }

    /// Purpose: Account for the complete persisted header when admitting rows near capacity.
    /// Expected: Admission stops before overflow and the accepted rows form a valid block.
    #[test]
    fn test_lwc_builder_capacity_includes_complete_persisted_header() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .expect("valid descriptor-like metadata");
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let mut expected_rows = Vec::new();
        for row_no in 0..100 {
            let vals = [
                Val::U64(row_no),
                Val::U64(0),
                Val::U64(0),
                Val::try_from(vec![0u8; 32]).unwrap(),
                Val::try_from(vec![row_no as u8; 6_718]).unwrap(),
            ];
            if !builder.append_row_values(RowID::new(row_no), &vals) {
                break;
            }
            expected_rows.push(vals.to_vec());
        }
        assert!(expected_rows.len() > 1);
        assert!(expected_rows.len() < 100);
        assert_built_rows(builder, &expected_rows);
    }

    /// Purpose: Admit the remaining row-cap prefix and reject subsequent direct appends without changing accepted state.
    /// Expected: The prefix consumes exactly the remaining row; a later rejection preserves identities, seeds, nullable values and extrema.
    #[test]
    fn row_cap_rejection_preserves_builder_state() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U8,
                StorageColumnFlags::NULLABLE,
            )],
            vec![],
        )
        .unwrap();
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let expected: Vec<_> = (0..MAX_LWC_ROWS)
            .map(|i| vec![if i % 3 == 0 { Val::Null } else { Val::U8(7) }])
            .collect();
        for (i, row) in expected[..MAX_LWC_ROWS - 1].iter().enumerate() {
            assert!(builder.append_row_values(RowID::new(i as u64), row));
        }
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(MAX_LWC_ROWS as u64 - 1), 2, &metadata.col);
        assert!(
            page.insert(&metadata.col, &expected[MAX_LWC_ROWS - 1])
                .is_ok()
        );
        assert!(page.insert(&metadata.col, &[Val::U8(255)]).is_ok());
        let view = page.vector_view(&metadata.col);
        let mut cursor = LwcSourceCursor::new(&view);
        let progress = builder.append_prefix(&view, &mut cursor, |_| {});
        assert_eq!(progress.stop, AppendStop::BlockFull);
        assert_eq!(progress.consumed_to, 1);
        assert_eq!(progress.appended_rows, 1);
        let stats = integer_stats(&builder);
        let seeds = builder.row_set_seeds().to_vec();
        assert!(!builder.append_row_values(RowID::new(MAX_LWC_ROWS as u64), &[Val::U8(255)]));
        assert_eq!(builder.row_count(), MAX_LWC_ROWS);
        assert_eq!(builder.row_set_seeds(), seeds);
        assert_eq!(integer_stats(&builder), stats);
        assert_built_rows(builder, &expected);
    }

    /// Purpose: Defend final serialization against internal state bypassing row-cap admission.
    /// Expected: Encoding asserts the row-count contract and identifies its field limit.
    #[test]
    fn test_lwc_builder_asserts_row_count_encoding_contract() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U8,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .expect("valid table metadata");
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));

        assert!(builder.append_row_values(RowID::new(0), &[Val::U8(0)]));
        let mut input = builder.finish(0);
        input.plan.row_count = (MAX_LWC_ROWS + 1) as u16;
        assert_lwc_invariant(
            || {
                input.encode();
            },
            &["LwcEncodeInput::encode", "field=row_count", "maximum=15360"],
        );
    }

    /// Purpose: Collect nullable integer statistics from live non-null row values.
    /// Expected: Nulls and deleted rows do not affect the recorded extrema.
    #[test]
    fn test_lwc_builder_nullable_integer_stats_skip_nulls_and_deleted_rows() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::I16,
                StorageColumnFlags::NULLABLE,
            )],
            vec![],
        )
        .expect("valid table metadata");
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(0), 8, metadata.col.as_ref());

        for (row_id, value) in [
            Val::Null,
            Val::I16(5),
            Val::I16(10),
            Val::Null,
            Val::I16(1000),
            Val::I16(8),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(matches!(
                page.insert(metadata.col.as_ref(), &[value]),
                InsertRow::Ok(inserted) if inserted == RowID::new(row_id as u64)
            ));
        }
        assert!(matches!(page.delete(RowID::new(4)), Delete::Ok));

        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let view = page.vector_view(metadata.col.as_ref());
        assert!(append_page(&mut builder, view));

        assert_eq!(integer_stats(&builder), vec![(true, 5, 10, 0, 0)]);
    }

    /// Purpose: Retain plan allocations through rejected previews and recycle them as accepted plans are replaced.
    /// Expected: Rejection preserves accepted state and scratch capacity; subsequent commits alternate the same two allocations and encode the accepted rows correctly.
    #[test]
    fn plan_columns_are_reused_across_rejection_and_commit() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::F32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
            ],
            vec![],
        )
        .unwrap();
        let rows = [
            vec![Val::from(1.25f32), Val::try_from("abc").unwrap()],
            vec![Val::from(2.5f32), Val::try_from("def").unwrap()],
            vec![Val::from(-0.0f32), Val::Null],
            vec![Val::from(4.0f32), Val::try_from("tail").unwrap()],
        ];
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        assert!(builder.planner.accepted_plan.is_none());
        assert!(builder.append_row_values(RowID::new(0), &rows[0]));
        let first = builder
            .planner
            .accepted_plan
            .as_ref()
            .unwrap()
            .columns
            .as_ptr();
        assert!(builder.append_row_values(RowID::new(1), &rows[1]));
        let second = builder
            .planner
            .accepted_plan
            .as_ref()
            .unwrap()
            .columns
            .as_ptr();
        assert_ne!(first, second, "both plan allocations remain live");
        assert_eq!(builder.planner.plan_scratch.columns.as_ptr(), first);
        let capacity = builder.planner.plan_scratch.columns.capacity();
        let accepted = builder.planner.accepted.clone();
        let plan = builder.planner.accepted_plan.clone();
        let oversized = [
            Val::from(99.0f32),
            Val::try_from(vec![7; LWC_BLOCK_PAYLOAD_SIZE]).unwrap(),
        ];
        for attempt in 0..3 {
            assert!(!builder.append_row_values(RowID::new(2), &oversized));
            assert_eq!(builder.planner.accepted, accepted, "attempt={attempt}");
            assert_eq!(builder.planner.accepted_plan, plan, "attempt={attempt}");
            assert_eq!(
                builder
                    .planner
                    .accepted_plan
                    .as_ref()
                    .unwrap()
                    .columns
                    .as_ptr(),
                second
            );
            assert_eq!(builder.planner.plan_scratch.columns.as_ptr(), first);
            assert_eq!(builder.planner.plan_scratch.columns.capacity(), capacity);
            assert_eq!(builder.row_ids(), [RowID::new(0), RowID::new(1)]);
        }
        for (pos, expected_plan, expected_scratch) in [(2, first, second), (3, second, first)] {
            assert!(builder.append_row_values(RowID::new(pos as u64), &rows[pos]));
            assert_eq!(
                builder
                    .planner
                    .accepted_plan
                    .as_ref()
                    .unwrap()
                    .columns
                    .as_ptr(),
                expected_plan
            );
            assert_eq!(
                builder.planner.plan_scratch.columns.as_ptr(),
                expected_scratch
            );
        }
        assert_built_rows(builder, &rows);
    }

    /// Purpose: Keep accepted plans and summaries immutable across rejected outlier previews.
    /// Expected: Repeated failure leaves identities, seeds and the frozen plan unchanged; later valid rows round trip.
    #[test]
    fn rejected_preview_preserves_accepted_plan() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I64, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .unwrap();
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let rows = [
            vec![Val::I64(17), Val::try_from(vec![3; 65_000]).unwrap()],
            vec![Val::Null, Val::try_from("tail").unwrap()],
        ];
        assert!(builder.append_row_values(RowID::new(0), &rows[0]));
        let summary = builder.planner.accepted.clone();
        let plan = builder.planner.accepted_plan.clone();
        let seeds = builder.row_set_seeds().to_vec();
        for _ in 0..3 {
            assert!(!builder.append_row_values(
                RowID::new(1),
                &[Val::I64(i64::MIN), Val::try_from(vec![5; 1_024]).unwrap()]
            ));
            assert_eq!(builder.planner.accepted, summary);
            assert_eq!(builder.planner.accepted_plan, plan);
            assert_eq!(builder.row_set_seeds(), seeds);
            assert_eq!(builder.row_ids(), &[RowID::new(0)]);
        }
        assert!(builder.append_row_values(RowID::new(1), &rows[1]));
        assert_built_rows(builder, &rows);
    }

    /// Purpose: Build and decode a row page containing every supported column value kind.
    /// Expected: Persisted columns retain their lengths, value kinds, and original values.
    #[test]
    fn test_lwc_builder_all_column_types() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I8, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U8, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I16, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U16, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::F32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::F64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let rows = vec![
            vec![
                Val::I8(-1),
                Val::U8(1),
                Val::I16(-10),
                Val::U16(10),
                Val::I32(-100),
                Val::U32(100),
                Val::I64(-1000),
                Val::U64(1000),
                Val::F32(OrderedFloat(1.25)),
                Val::F64(OrderedFloat(2.5)),
                Val::VarByte(MemVar::inline(b"alpha")),
            ],
            vec![
                Val::I8(2),
                Val::U8(3),
                Val::I16(20),
                Val::U16(30),
                Val::I32(200),
                Val::U32(300),
                Val::I64(2000),
                Val::U64(3000),
                Val::F32(OrderedFloat(-4.5)),
                Val::F64(OrderedFloat(6.75)),
                Val::VarByte(MemVar::inline(b"beta")),
            ],
            vec![
                Val::I8(4),
                Val::U8(5),
                Val::I16(40),
                Val::U16(50),
                Val::I32(400),
                Val::U32(500),
                Val::I64(4000),
                Val::U64(5000),
                Val::F32(OrderedFloat(7.75)),
                Val::F64(OrderedFloat(-8.125)),
                Val::VarByte(MemVar::inline(b"gamma")),
            ],
        ];

        let page = row_page_with_rows(&metadata, RowID::new(10), &rows);

        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let view = page.vector_view(metadata.col.as_ref());
        assert!(append_page(&mut builder, view));
        assert_eq!(
            builder.row_ids(),
            &[RowID::new(10), RowID::new(11), RowID::new(12)]
        );
        assert_built_rows(builder, &rows);
    }

    /// Purpose: Partition a valid inline-byte RowPage whose aggregate flat LWC payload exceeds one block.
    /// Expected: Exact wire capacity selects the maximal prefix, and both readers and callbacks preserve every row and identity once.
    #[test]
    fn oversized_inline_page_splits_at_exact_byte_boundary() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::VarByte,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let rows: Vec<_> = (0..8_000u64)
            .map(|row| vec![Val::try_from(row.to_le_bytes()[..6].to_vec()).unwrap()])
            .collect();
        let page = row_page_with_rows(&metadata, RowID::new(100), &rows);
        let bitmap = page.del_bitmap(rows.len());
        let result = encode_sources(&metadata, &[(page, bitmap)]);
        assert_eq!(result.counts, [6_546, 1_454]);
        assert_eq!(result.used, [39 + 10 * 6_546, 39 + 10 * 1_454]);
        assert_eq!(result.seeds, [1, 1]);
        assert_eq!(result.ids, (100..8_100).map(RowID::new).collect::<Vec<_>>());
        assert_eq!(result.rows, rows);
    }

    /// Purpose: Fill block remainders from subsequent outlined-byte pages instead of rejecting complete pages.
    /// Expected: Three page-sized selections occupy two blocks with exact flat payload lengths and page-level identity hints.
    #[test]
    fn prefix_packing_reduces_three_pages_to_two_blocks() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::VarByte,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let rows: Vec<_> = (0..300)
            .map(|row| vec![Val::try_from(vec![(row % 251) as u8; 400]).unwrap()])
            .collect();
        let sources: Vec<_> = rows
            .chunks(100)
            .enumerate()
            .map(|(chunk, rows)| {
                let page = row_page_with_rows(&metadata, RowID::new((chunk * 100) as u64), rows);
                let bitmap = page.del_bitmap(rows.len());
                (page, bitmap)
            })
            .collect();
        let result = encode_sources(&metadata, &sources);
        assert_eq!(result.counts, [162, 138]);
        assert_eq!(result.used, [39 + 404 * 162, 39 + 404 * 138]);
        assert_eq!(result.seeds, [2, 2]);
        assert_eq!(result.ids, (0..300).map(RowID::new).collect::<Vec<_>>());
        assert_eq!(result.rows, rows);
    }

    /// Purpose: Exclude stale physical descriptors beneath nulls from size planning and byte staging.
    /// Expected: Even out-of-bounds descriptors are never read, null offsets repeat, and hidden bytes occupy no encoded payload.
    #[test]
    fn null_bytes_skip_invalid_physical_descriptors() {
        use crate::value::PageVar;
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::VarByte,
                StorageColumnFlags::NULLABLE,
            )],
            vec![],
        )
        .unwrap();
        let physical = vec![vec![Val::try_from(vec![42; 200]).unwrap()]; 256];
        let mut page = row_page_with_rows(&metadata, RowID::new(0), &physical);
        for row in 0..256 {
            page.set_null_exclusive(&metadata.col, row, 0, true);
            page.update_var(row, 0, PageVar::outline(u16::MAX, u16::MAX, &[1, 2, 3, 4]));
        }
        let view = page.vector_view(&metadata.col);
        let mut cursor = LwcSourceCursor::new(&view);
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        assert_eq!(
            builder.append_prefix(&view, &mut cursor, |_| {}).stop,
            AppendStop::PageExhausted
        );
        let LwcValues::Bytes { offsets, data } = &builder.columns[0].values else {
            panic!("byte column")
        };
        assert_eq!(data.as_slice(), b"");
        assert_eq!(offsets, &vec![0; 257]);
        let buf = build_with_size_check(builder);
        assert_eq!(used_payload_size(&buf), 39 + 4 * 256 + 2 + 32);
        assert_persisted_rows(buf.as_bytes(), &metadata.col, &vec![vec![Val::Null]; 256]);
    }

    /// Purpose: Preserve a seeded prepared logical-row oracle across sparse source gaps, nulls and byte boundaries.
    /// Expected: Prefix retries use the prepared bitmap despite contradictory live deletion flags and preserve ordered callbacks, identities and values.
    #[test]
    fn seeded_sparse_prefixes_preserve_prepared_membership() {
        use rand::rngs::StdRng;
        use rand::{RngExt, SeedableRng};
        let mut rng = StdRng::seed_from_u64(339);
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I64, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::F64, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .unwrap();
        let mut sources = Vec::new();
        let mut expected_ids = Vec::new();
        let mut expected_rows = Vec::new();
        for chunk in 0..20 {
            let rows: Vec<_> = (0..100)
                .map(|_| {
                    vec![
                        if rng.random_ratio(1, 4) {
                            Val::Null
                        } else {
                            Val::I64(rng.random())
                        },
                        if rng.random_ratio(1, 3) {
                            Val::Null
                        } else {
                            Val::try_from(vec![rng.random(); rng.random_range(0..500)]).unwrap()
                        },
                        Val::F64(OrderedFloat(rng.random_range(-1000.0..1000.0))),
                    ]
                })
                .collect();
            let page = row_page_with_rows(&metadata, RowID::new(chunk * 300), &rows);
            let mut bitmap = page.del_bitmap(rows.len());
            for (pos, row) in rows.into_iter().enumerate() {
                if chunk == 0 || chunk == 19 || !(3..=95).contains(&pos) || rng.random_ratio(1, 3) {
                    bitmap.bitmap_set(pos);
                } else {
                    expected_ids.push(page.row_id(pos));
                    expected_rows.push(row);
                }
                if pos % 2 == 0 {
                    assert!(matches!(page.delete(page.row_id(pos)), Delete::Ok));
                }
            }
            sources.push((page, bitmap));
        }
        let result = encode_sources(&metadata, &sources);
        assert!(result.counts.len() > 2);
        assert_eq!(result.ids, expected_ids);
        assert_eq!(result.rows, expected_rows);
    }

    /// Purpose: Keep a nonfitting source row unconsumed without copying values or invoking sidecars.
    /// Expected: Repeated probes preserve owned staging and accepted plans, and a fresh block accepts that same source row.
    #[test]
    fn rejected_prefix_reuses_pending_summary_without_staging() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::VarByte,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        assert!(
            builder.append_row_values(RowID::new(0), &[Val::try_from(vec![1; 65_000]).unwrap()])
        );
        let page = row_page_with_rows(
            &metadata,
            RowID::new(1),
            &[vec![Val::try_from(vec![2; 1_000]).unwrap()]],
        );
        let view = page.vector_view(&metadata.col);
        let mut cursor = LwcSourceCursor::new(&view);
        let plan = builder.planner.accepted_plan.clone();
        for _ in 0..3 {
            let progress =
                builder.append_prefix(&view, &mut cursor, |_| panic!("rejected row callback"));
            assert_eq!(progress.stop, AppendStop::BlockFull);
            assert_eq!(progress.appended_rows, 0);
            assert_eq!(progress.consumed_to, 0);
            assert_eq!(builder.planner.accepted_plan, plan);
            assert_eq!(builder.row_ids(), &[RowID::new(0)]);
            let LwcValues::Bytes { offsets, data } = &builder.columns[0].values else {
                panic!("byte column")
            };
            assert_eq!(offsets, &[0, 65_000]);
            assert_eq!(data, &vec![1; 65_000]);
        }
        let mut next = LwcBuilder::new(Arc::clone(&metadata.col));
        let progress =
            next.append_prefix(&view, &mut cursor, |positions| assert_eq!(positions, &[0]));
        assert_eq!(progress.stop, AppendStop::PageExhausted);
        assert_eq!(next.row_ids(), &[RowID::new(1)]);
    }

    /// Purpose: Enforce cursor source identity, physical row count, and range invariants independently on reload.
    /// Expected: Each mismatch panics before callbacks, staging, or cursor progress; reloading the original selection still succeeds.
    #[test]
    fn source_reload_preserves_cursor_invariants() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U8,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let rows: Vec<_> = (0..4).map(|value| vec![Val::U8(value)]).collect();
        let page = row_page_with_rows(&metadata, RowID::new(10), &rows[..3]);
        let other_start = row_page_with_rows(&metadata, RowID::new(11), &rows[..3]);
        let other_count = row_page_with_rows(&metadata, RowID::new(10), &rows);
        let view = bounded_view(page.vector_view(&metadata.col), 1..2);
        let mut cursor = LwcSourceCursor::new(&view);
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let plan = builder.planner.accepted_plan.clone();
        for (case, reloaded) in [
            (
                "RowID base",
                bounded_view(other_start.vector_view(&metadata.col), 1..2),
            ),
            (
                "physical count",
                bounded_view(other_count.vector_view(&metadata.col), 1..2),
            ),
            (
                "range start",
                bounded_view(page.vector_view(&metadata.col), 0..2),
            ),
            (
                "range end",
                bounded_view(page.vector_view(&metadata.col), 1..3),
            ),
        ] {
            let mut called = false;
            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _ = builder.append_prefix(&reloaded, &mut cursor, |_| called = true);
            }))
            .expect_err("mismatched cursor source must panic");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .expect("source assertion includes a diagnostic");
            assert!(
                message.contains("LwcSourceCursor::validate source changed while resuming"),
                "{case}: {message}"
            );
            assert!(!called, "{case}: sidecar callback");
            assert!(builder.is_empty(), "{case}: staged rows");
            assert_eq!(builder.planner.accepted_plan, plan, "{case}: accepted plan");
            assert_eq!(cursor.consumed_to(), 1, "{case}: consumed source");
            assert_eq!(cursor.scan_pos, 1, "{case}: lookahead");
            assert!(cursor.batch_rows.is_empty(), "{case}: buffered positions");
            assert!(cursor.pending.is_empty(), "{case}: pending summaries");
        }
        let reloaded = bounded_view(page.vector_view(&metadata.col), 1..2);
        let mut accepted = Vec::new();
        let progress = builder.append_prefix(&reloaded, &mut cursor, |positions| {
            accepted.extend_from_slice(positions)
        });
        assert_eq!(accepted, [1]);
        assert_eq!(progress.appended_rows, 1);
        assert_eq!(progress.consumed_to, 2);
        assert_eq!(progress.stop, AppendStop::PageExhausted);
        assert_eq!(builder.row_ids(), [RowID::new(11)]);
    }

    /// Purpose: Prevent an empty builder from producing a worker encoding input.
    /// Expected: Finalization asserts the nonempty admission contract.
    #[test]
    fn empty_builder_cannot_finish() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U8,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let builder = LwcBuilder::new(Arc::clone(&metadata.col));
        assert_lwc_invariant(
            || {
                builder.finish(0);
            },
            &["LwcBuilder::finish requires accepted rows"],
        );
    }

    /// Purpose: Reject malformed frozen encoding inputs before publishing a block.
    /// Expected: Count, offset, null-shape, byte-length, and kind corruption assert the specific frozen-input contract.
    #[test]
    fn malformed_encode_contracts_are_asserted() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::VarByte,
                StorageColumnFlags::NULLABLE,
            )],
            vec![],
        )
        .unwrap();
        let rows = [vec![Val::try_from("first").unwrap()]];
        for (fault, field) in [
            (0, "field=column_end_offset"),
            (1, "field=column_count"),
            (2, "field=null_bitmap_words"),
            (3, "field=payload_length"),
            (4, "field=variable_data_length"),
            (5, "field=column_kind"),
        ] {
            let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
            assert!(builder.append_row_values(RowID::new(0), &rows[0]));
            let mut input = builder.finish(0);
            match fault {
                0 => input.plan.columns[0].end_offset += 1,
                1 => input.plan.row_count += 1,
                2 => input.columns[0].nulls.as_mut().unwrap().clear(),
                3 => input.plan.payload_len += 1,
                4 => {
                    let LwcValues::Bytes { data, .. } = &mut input.columns[0].values else {
                        panic!("byte column")
                    };
                    data.push(1);
                }
                _ => input.columns[0].values = LwcValues::Primitive(PrimitiveBuffer::U8(vec![1])),
            }
            let owner = if fault == 5 {
                "ColumnEncodingPlan::write_values"
            } else {
                "LwcEncodeInput::encode"
            };
            assert_lwc_invariant(
                || {
                    input.encode();
                },
                &[owner, field],
            );
        }
    }

    /// Purpose: Apply physical-row and encoded-byte limits together across multiple byte-column pages.
    /// Expected: Empty payloads stop at the row cap while one-byte payloads stop at their independently calculated byte capacity, preserving all rows.
    #[test]
    fn byte_columns_obey_both_count_and_payload_limits() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::VarByte,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        for (bytes, expected_counts) in [(0, vec![15_360, 2_640]), (1, vec![13_093, 4_907])] {
            let row = vec![Val::try_from(vec![7; bytes]).unwrap()];
            let rows = vec![row; 18_000];
            let sources: Vec<_> = rows
                .chunks(6_000)
                .enumerate()
                .map(|(chunk, rows)| {
                    let page =
                        row_page_with_rows(&metadata, RowID::new((chunk * 6_000) as u64), rows);
                    let bitmap = page.del_bitmap(rows.len());
                    (page, bitmap)
                })
                .collect();
            let result = encode_sources(&metadata, &sources);
            assert_eq!(result.counts, expected_counts);
            assert_eq!(
                result.used,
                result
                    .counts
                    .iter()
                    .map(|&count| 39 + (4 + bytes) * count)
                    .collect::<Vec<_>>()
            );
            assert_eq!(result.rows, rows);
            assert_eq!(result.ids, (0..18_000).map(RowID::new).collect::<Vec<_>>());
        }
    }

    /// Purpose: Start a prefix cursor inside a bitmap word while retaining original source coordinates and float payload bits.
    /// Expected: Selected rows across word and planning-batch boundaries preserve their original identities and non-null floating-point bit patterns.
    #[test]
    fn bounded_source_prefix_preserves_coordinates_and_float_bits() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::F64,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let bits = [
            0u64,
            1u64 << 63,
            0x7ff8_0000_0000_0123,
            0xfff0_0000_0000_0000,
        ];
        let rows: Vec<_> = (0..2_200)
            .map(|pos| {
                vec![Val::F64(OrderedFloat(f64::from_bits(
                    bits[pos % bits.len()],
                )))]
            })
            .collect();
        let page = row_page_with_rows(&metadata, RowID::new(500), &rows);
        let mut bitmap = page.del_bitmap(rows.len());
        for pos in (0..rows.len()).step_by(5) {
            bitmap.bitmap_set(pos);
        }
        let view = bounded_view(
            page.vector_view_with_del_bitmap(&metadata.col, bitmap.as_slice()),
            51..2_187,
        );
        let mut cursor = LwcSourceCursor::new(&view);
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        let mut positions = Vec::new();
        let progress = builder.append_prefix(&view, &mut cursor, |accepted| {
            positions.extend_from_slice(accepted)
        });
        let expected: Vec<_> = (51..2_187).filter(|pos| pos % 5 != 0).collect();
        assert_eq!(progress.stop, AppendStop::PageExhausted);
        assert_eq!(progress.consumed_to, 2_187);
        assert_eq!(
            positions
                .iter()
                .map(|&pos| usize::from(pos))
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            builder.row_ids(),
            expected
                .iter()
                .map(|&pos| RowID::new(500 + pos as u64))
                .collect::<Vec<_>>()
        );
        assert_eq!(builder.row_set_seeds().len(), 1);
        let buf = build_with_size_check(builder);
        let block = LwcBlock::try_from_persisted_bytes(
            buf.as_bytes(),
            FileKind::TableFile,
            test_block_id(1),
        )
        .unwrap();
        for (ordinal, &pos) in expected.iter().enumerate() {
            let values = block
                .decode_full_row_values(&metadata.col, ordinal)
                .unwrap();
            assert_eq!(
                values[0].as_f64().unwrap().to_bits(),
                bits[pos % bits.len()]
            );
        }
    }

    /// Purpose: Charge an outlier for widening every previously accepted integer, not only the new row.
    /// Expected: Prefix admission flushes the compressed constants before the outlier, and direct rejection retains the old plan for a later valid row.
    #[test]
    fn widening_outlier_replans_previously_accepted_rows() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U64,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let mut rows = vec![vec![Val::U64(0)]; 9_000];
        rows.extend([vec![Val::U64(u64::MAX)], vec![Val::U64(0)]]);
        let sources: Vec<_> = rows
            .chunks(6_000)
            .enumerate()
            .map(|(chunk, rows)| {
                let page = row_page_with_rows(&metadata, RowID::new((chunk * 6_000) as u64), rows);
                let bitmap = page.del_bitmap(rows.len());
                (page, bitmap)
            })
            .collect();
        let result = encode_sources(&metadata, &sources);
        assert_eq!(result.counts, [9_000, 2]);
        assert_eq!(
            result.used,
            [26 + 18 + 9_000_usize.div_ceil(8), 26 + 9 + 2 * 8]
        );
        assert_eq!(result.rows, rows);
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        for (pos, row) in rows[..9_000].iter().enumerate() {
            assert!(builder.append_row_values(RowID::new(pos as u64), row));
        }
        let plan = builder.planner.accepted_plan.clone();
        for _ in 0..2 {
            assert!(!builder.append_row_values(RowID::new(9_000), &rows[9_000]));
            assert_eq!(builder.planner.accepted_plan, plan);
            assert_eq!(builder.row_count(), 9_000);
        }
        assert!(builder.append_row_values(RowID::new(9_000), &rows[0]));
        assert_built_rows(builder, &vec![rows[0].clone(); 9_001]);
    }
}
