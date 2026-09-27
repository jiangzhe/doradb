use super::{
    BudgetedVec, DuplicateCheck, HotBuildSource, HotRunEntry, HotSortedRun, JobCompletion,
    LocalDuplicates, MemoryReservation,
};
use crate::buffer::guard::PageGuard;
use crate::error::{
    MultiDomainResultExt, RuntimeError, RuntimeOrFatalResult, RuntimeOrFatalResultExt,
};
use crate::memcmp::MEM_CMP_KEY_INLINE;
#[cfg(feature = "profiling")]
use crate::profiling::HotBuildWorkerProfile;
use crate::row::RowRead;
use crate::runtime::thread_pool::ThreadPool;
use crate::runtime::yield_now;
use crate::value::Val;
use error_stack::ResultExt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "profiling")]
use std::time::Instant;

/// Submit one finite extraction job with move-once completion.
pub(super) fn submit(
    pool: &ThreadPool,
    source: Arc<HotBuildSource>,
    stop: Arc<AtomicBool>,
    group: usize,
    pages: Range<usize>,
) -> JobCompletion {
    pool.submit_async(async move {
        extract_group(&source, &stop, group, pages)
            .await
            .attach_with(|| {
                format!(
                    "operation=hot_index_build, phase=worker, table_id={}, index={}, group={group}",
                    source.table.table_id(),
                    source.key.index
                )
            })
    })
}

/// Collect first adjacent equality only when the invocation requires checking.
pub(super) fn local_duplicates(
    entries: &[HotRunEntry],
    policy: DuplicateCheck,
    stop: &AtomicBool,
) -> LocalDuplicates {
    if policy == DuplicateCheck::Skip {
        return LocalDuplicates::Unchecked;
    }
    for position in 1..entries.len() {
        if entries[position - 1].key == entries[position].key {
            return LocalDuplicates::Checked {
                first_duplicate_position: Some(position),
            };
        }
        if position % 256 == 0 && stop.load(Ordering::Acquire) {
            return LocalDuplicates::Unchecked;
        }
    }
    LocalDuplicates::Checked {
        first_duplicate_position: None,
    }
}

async fn extract_group(
    source: &HotBuildSource,
    stop: &AtomicBool,
    group: usize,
    pages: Range<usize>,
) -> RuntimeOrFatalResult<Option<Arc<HotSortedRun>>> {
    #[cfg(feature = "profiling")]
    let started = Instant::now();
    #[cfg(test)]
    source.test.before(group).await?;
    if stop.load(Ordering::Acquire) {
        return Ok(None);
    }
    let budget = &source.budget;
    let mut run = HotSortedRun {
        group_id: group,
        entries: BudgetedVec::new(budget),
        duplicates: LocalDuplicates::Unchecked,
        #[cfg(feature = "profiling")]
        profile: HotBuildWorkerProfile::default(),
        payload: MemoryReservation::new(budget),
    };
    if !extract_rows(source, stop, pages, &mut run).await? {
        return Ok(None);
    }
    #[cfg(feature = "profiling")]
    let sort_started = Instant::now();
    if run.entries.len() > 1 {
        run.entries
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
    }
    if stop.load(Ordering::Acquire) {
        return Ok(None);
    }
    #[cfg(feature = "profiling")]
    let duplicate_started = Instant::now();
    run.duplicates = local_duplicates(&run.entries, source.key.duplicates, stop);
    if stop.load(Ordering::Acquire) {
        return Ok(None);
    }
    #[cfg(feature = "profiling")]
    {
        run.profile = HotBuildWorkerProfile::finish(
            started,
            sort_started,
            duplicate_started,
            Instant::now(),
            source.key.duplicates == DuplicateCheck::Collect,
        );
    }
    Ok(Some(Arc::new(run)))
}

async fn extract_rows(
    source: &HotBuildSource,
    stop: &AtomicBool,
    pages: Range<usize>,
    run: &mut HotSortedRun,
) -> RuntimeOrFatalResult<bool> {
    let group = run.group_id;
    let mut projection =
        Vec::with_capacity(source.key.columns.len() + usize::from(!source.key.unique));
    for (seq, page_idx) in pages.enumerate() {
        if stop.load(Ordering::Acquire) {
            return Ok(false);
        }
        let descriptor = source.pages[page_idx];
        let guard = source
            .table
            .row_store
            .get_captured_row_page_shared(&source.guards, descriptor)
            .await
            .change_runtime_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=hot_index_build, phase=reopen, group={group}, page={descriptor:?}"
                )
            })?;
        let page = guard.page();
        for row_idx in 0..page.header.row_count() {
            let row = page.row(row_idx);
            if row.is_deleted() {
                continue;
            }
            project(source, &row, &mut projection);
            let len = source
                .key
                .encoder
                .encoded_len(&projection)
                .change_context(RuntimeError::IndexAccess)?;
            let bytes = if len > MEM_CMP_KEY_INLINE { len } else { 0 };
            run.payload
                .grow(bytes, "outlined key")
                .change_context(RuntimeError::IndexAccess)?;
            let key = source.key.encoder.encode_with_len(&projection, len);
            run.entries
                .push(
                    HotRunEntry {
                        key,
                        row_id: row.row_id(),
                    },
                    "run entries",
                )
                .change_context(RuntimeError::IndexAccess)?;
            projection.clear();
        }
        drop(guard);
        // Process whole pages and release their guards before each 16-page yield.
        if seq % 16 == 15 {
            yield_now().await;
        }
    }
    Ok(!stop.load(Ordering::Acquire))
}

fn project(source: &HotBuildSource, row: &impl RowRead, projection: &mut Vec<Val>) {
    let columns = &source.layout.metadata().col;
    projection.extend(
        source
            .key
            .columns
            .iter()
            .map(|&column| row.val(columns, column)),
    );
    if !source.key.unique {
        projection.push(Val::from(row.row_id()));
    }
}
