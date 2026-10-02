//! Adaptive, directly searchable identity sets for cold rows and ordinal deletions.
//! All integers are packed LE; index sections own their headers.

use crate::error::{DataIntegrityError, DataIntegrityResult, ResourceError, ResourceResult};
use crate::id::RowID;
use error_stack::Report;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

// Persisted whole-entry tags. Tags 1 and 2 retain their original byte layout.
const DENSE: u8 = 1;
const LIST32: u8 = 2;
const LIST16: u8 = 3;
const RUNS16: u8 = 4;
const RUNS32: u8 = 5;
const BITMAP: u8 = 6;
const MISSING: u8 = 7;
const TRIMMED_MISSING: u8 = 8;
const SEGMENTED: u8 = 9;
const DIRECTORY_SIZE: usize = 16;
const WINDOW: u64 = 4096;

/// Statistics that can be combined without revisiting source rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IdentitySetStats {
    first: u64,
    last: u64,
    count: usize,
    runs: usize,
}

impl IdentitySetStats {
    #[inline]
    fn from_rows(rows: &[RowID]) -> Self {
        let mut stats = Self {
            first: rows[0].as_u64(),
            last: rows[0].as_u64(),
            count: 1,
            runs: 1,
        };
        for row in &rows[1..] {
            stats.push(*row);
        }
        stats
    }

    #[inline]
    fn push(&mut self, row: RowID) {
        self.runs += usize::from(row.as_u64() - self.last != 1);
        self.last = row.as_u64();
        self.count += 1;
    }

    #[inline]
    fn span(self) -> u64 {
        self.last - self.first + 1
    }

    #[inline]
    fn merge(self, right: Self) -> Self {
        Self {
            first: self.first,
            last: right.last,
            count: self.count + right.count,
            runs: self.runs + right.runs - usize::from(right.first - self.last == 1),
        }
    }
}

/// A transient packing hint; ordinals refer to the builder's retained RowIDs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IdentitySetSeed {
    start: usize,
    end: usize,
    stats: IdentitySetStats,
}

impl IdentitySetSeed {
    /// Records accepted rows from a page, splitting oversized ranges into windows.
    #[inline]
    pub(crate) fn append_page(seeds: &mut Vec<Self>, rows: &[RowID], ordinal: usize) {
        if rows.is_empty() {
            return;
        }
        if rows[rows.len() - 1].as_u64() - rows[0].as_u64() < u64::from(u16::MAX) {
            seeds.push(Self {
                start: ordinal,
                end: ordinal + rows.len(),
                stats: IdentitySetStats::from_rows(rows),
            });
        } else {
            for (idx, row) in rows.iter().enumerate() {
                Self::append_window(seeds, *row, ordinal + idx);
            }
        }
    }

    /// Records a direct-row append in its deterministic logical window.
    #[inline]
    pub(crate) fn append_window(seeds: &mut Vec<Self>, row: RowID, ordinal: usize) {
        if let Some(last) = seeds.last_mut()
            && last.end == ordinal
            && last.stats.first / WINDOW == row.as_u64() / WINDOW
        {
            last.end += 1;
            last.stats.push(row);
            return;
        }
        seeds.push(Self {
            start: ordinal,
            end: ordinal + 1,
            stats: IdentitySetStats::from_rows(&[row]),
        });
    }
}

#[derive(Clone, Copy, Debug)]
struct Candidate {
    codec: u8,
    trimmed: bool,
    len: usize,
    depth: u32,
    words: u8,
}

impl Candidate {
    #[inline]
    fn key(self) -> (usize, u32, u8, u8, bool) {
        (
            self.len,
            self.depth,
            self.words,
            codec_order(self.codec),
            self.trimmed,
        )
    }
}

#[derive(Clone, Debug)]
struct Segment {
    seed: IdentitySetSeed,
    candidate: Candidate,
    prev: Option<usize>,
    next: Option<usize>,
    generation: usize,
    active: bool,
}

/// A deterministic exact-size selection, retaining only O(seed count) metadata.
struct IdentitySetPlan {
    whole: Candidate,
    segments: Vec<Segment>,
    segmented: bool,
    len: usize,
}

impl IdentitySetPlan {
    fn new(rows: &[RowID], base: u64, span: u32, seeds: &[IdentitySetSeed]) -> Self {
        // Keep the cheapest whole-entry codec as a fallback. Counts, endpoints,
        // and run counts give exact byte costs without encoding each candidate.
        let stats = IdentitySetStats::from_rows(rows);
        let whole = whole_candidate(stats, base, span);
        let mut generated = Vec::new();
        // Prefer the builder's source-page/window seeds. Without supplied seeds,
        // group present rows into 4,096-position windows, skipping empty windows.
        let seeds = if seeds.is_empty() {
            for (idx, row) in rows.iter().enumerate() {
                IdentitySetSeed::append_window(&mut generated, *row, idx);
            }
            &generated
        } else {
            seeds
        };
        // Seeds are private builder-owned metadata, restored atomically on rollback.
        assert!(
            seeds.first().is_some_and(|s| s.start == 0)
                && seeds.last().is_some_and(|s| s.end == rows.len())
                && seeds.windows(2).all(|s| s[0].end == s[1].start),
            "column row-set seeds must partition accepted ordinals"
        );
        // Each seed starts with its cheapest local codec over its present
        // endpoints. Neighbor indices stay stable while merges retire entries.
        let mut segments: Vec<_> = seeds
            .iter()
            .enumerate()
            .map(|(idx, seed)| Segment {
                seed: *seed,
                candidate: local_candidate(seed.stats),
                prev: idx.checked_sub(1),
                next: (idx + 1 < seeds.len()).then_some(idx + 1),
                generation: 0,
                active: true,
            })
            .collect();
        // Merging removes one directory record, so the saving is
        // DIRECTORY_SIZE + left payload + right payload - merged payload.
        // offer_pair admits nonnegative savings within the 65,535-position limit.
        // The max-heap favors the largest saving, then the leftmost pair;
        // zero-saving merges are useful because they reduce the segment count.
        let mut heap = BinaryHeap::new();
        for idx in 0..segments.len() {
            offer_pair(&segments, idx, &mut heap);
        }
        while let Some((_, Reverse(left), left_gen, right_gen)) = heap.pop() {
            // Other merges may have invalidated this offer. Check generations
            // lazily instead of finding and removing stale entries in the heap.
            if !segments[left].active || segments[left].generation != left_gen {
                continue;
            }
            let Some(right) = segments[left].next else {
                continue;
            };
            if segments[right].generation != right_gen {
                continue;
            }
            // Combine statistics without rescanning rows. Adjacent boundary
            // rows join two runs; any gap becomes absent positions in the span.
            let combined = segments[left].seed.stats.merge(segments[right].seed.stats);
            segments[left].seed.stats = combined;
            segments[left].seed.end = segments[right].seed.end;
            segments[left].candidate = local_candidate(combined);
            segments[left].generation += 1;
            segments[left].next = segments[right].next;
            segments[right].active = false;
            if let Some(next) = segments[left].next {
                segments[next].prev = Some(left);
            }
            // Only the merged segment's two neighboring pairs need new offers.
            if let Some(prev) = segments[left].prev {
                offer_pair(&segments, prev, &mut heap);
            }
            offer_pair(&segments, left, &mut heap);
        }
        segments.retain(|s| s.active);
        // The segmented body has a four-byte count header, 16 bytes per directory
        // entry, and local payloads. Both alternatives omit the same index-owned
        // four-byte common header from their costs.
        let len = 4
            + segments.len() * DIRECTORY_SIZE
            + segments.iter().map(|s| s.candidate.len).sum::<usize>();
        let segment_depth = depth(segments.len())
            + segments
                .iter()
                .map(|s| s.candidate.depth)
                .max()
                .unwrap_or(0);
        let words = segments
            .iter()
            .map(|s| s.candidate.words)
            .max()
            .unwrap_or(0);
        // Compare bytes first, then segment count, estimated lookup work, and
        // stable codec order. A whole-entry codec counts as one segment.
        // Greedy merging need not find the globally smallest partition, but
        // this fallback guarantees a result no larger than the whole-entry body.
        let segmented = (
            len,
            segments.len(),
            segment_depth,
            words,
            codec_order(SEGMENTED),
        ) < (
            whole.len,
            1,
            whole.depth,
            whole.words,
            codec_order(whole.codec),
        );
        Self {
            whole,
            segments,
            segmented,
            len: if segmented { len } else { whole.len },
        }
    }

    fn encode(&self, rows: &[RowID], base: u64, span: u32) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len);
        if self.segmented {
            put16(&mut out, rows.len());
            put16(&mut out, self.segments.len());
            let mut offset = 4 + self.segments.len() * DIRECTORY_SIZE;
            for segment in &self.segments {
                let seed = segment.seed;
                put32(&mut out, (seed.stats.first - base) as u32);
                put16(&mut out, seed.stats.span() as usize);
                put16(&mut out, seed.stats.count);
                put16(&mut out, seed.start);
                put16(&mut out, offset);
                put16(&mut out, segment.candidate.len);
                out.extend_from_slice(&[segment.candidate.codec, 0]);
                offset += segment.candidate.len;
            }
            for segment in &self.segments {
                let seed = segment.seed;
                emit_local(
                    &mut out,
                    segment.candidate.codec,
                    &rows[seed.start..seed.end],
                    seed.stats.first,
                    seed.stats.span() as u32,
                );
            }
        } else {
            let c = self.whole;
            let stats = IdentitySetStats::from_rows(rows);
            let local_base = if c.trimmed { stats.first } else { base };
            let local_span = if c.trimmed { stats.span() as u32 } else { span };
            match c.codec {
                RUNS16 | RUNS32 => {
                    put16(&mut out, rows.len());
                    put16(&mut out, stats.runs);
                }
                BITMAP => {
                    put16(&mut out, rows.len());
                    out.extend_from_slice(&[u8::from(c.trimmed), 0]);
                    if c.trimmed {
                        put32(&mut out, (local_base - base) as u32);
                        put32(&mut out, local_span);
                    }
                }
                MISSING => {
                    put16(&mut out, rows.len());
                    put16(&mut out, 0);
                }
                TRIMMED_MISSING => {
                    put32(&mut out, (local_base - base) as u32);
                    put16(&mut out, local_span as usize);
                    put16(&mut out, rows.len());
                }
                _ => {}
            }
            emit_local(&mut out, c.codec, rows, local_base, local_span);
        }
        assert_eq!(
            out.len(),
            self.len,
            "column row-set estimator/encoder disagreement"
        );
        out
    }
}

type MergeHeap = BinaryHeap<(usize, Reverse<usize>, usize, usize)>;

/// Immutable encoded identity set shared by row identity and ordinal deletions.
/// Dense sets do not allocate a body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedIdentitySet {
    codec: u8,
    span: u32,
    body: Option<Arc<[u8]>>,
}

impl EncodedIdentitySet {
    /// Plans and encodes exactly once, checking capacity before allocating bytes.
    pub(crate) fn plan(
        start: RowID,
        end: RowID,
        rows: &[RowID],
        seeds: &[IdentitySetSeed],
        body_limit: usize,
    ) -> ResourceResult<Self> {
        let span = end
            .checked_sub(start)
            .and_then(|s| u32::try_from(s).ok())
            .filter(|s| *s != 0)
            .ok_or_else(|| Report::new(ResourceError::ColumnBlockEntryCapacityExceeded))?;
        if rows.is_empty() || rows.len() > u16::MAX as usize {
            return Err(Report::new(ResourceError::ColumnBlockEntryCapacityExceeded));
        }
        // Ordering and containment are caller contracts; capacity is a runtime outcome.
        assert!(
            rows[0] >= start && rows[rows.len() - 1] < end && rows.windows(2).all(|p| p[0] < p[1]),
            "column row-set input must be ordered within coverage"
        );
        let plan = IdentitySetPlan::new(rows, start.as_u64(), span, seeds);
        if plan.len > body_limit || plan.len > u16::MAX as usize {
            return Err(Report::new(ResourceError::ColumnBlockEntryCapacityExceeded));
        }
        let codec = if plan.segmented {
            SEGMENTED
        } else {
            plan.whole.codec
        };
        let body = (plan.len != 0).then(|| Arc::from(plan.encode(rows, start.as_u64(), span)));
        Ok(Self { codec, span, body })
    }

    /// Borrows the invariant established by encoding or cache admission.
    #[inline]
    pub(crate) fn as_ref(&self) -> IdentitySetRef<'_> {
        IdentitySetRef::from_admitted(self.codec, self.body(), self.span)
    }

    /// Returns the persisted codec tag.
    #[inline]
    pub(crate) fn codec(&self) -> u8 {
        self.codec
    }

    /// Returns identity bytes, excluding the index-owned common header.
    #[inline]
    pub(crate) fn body(&self) -> &[u8] {
        self.body.as_deref().unwrap_or(&[])
    }

    /// Returns the entry coverage, including absent tails.
    #[inline]
    pub(crate) fn row_id_span(&self) -> u32 {
        self.span
    }

    /// Returns the authoritative cardinality.
    #[inline]
    pub(crate) fn row_count(&self) -> usize {
        usize::from(self.as_ref().count)
    }

    /// Translates a present delta into its ordinal.
    #[inline]
    pub(crate) fn ordinal_for_delta(&self, delta: u32) -> Option<u32> {
        if self.codec == DENSE {
            return (delta < self.span).then_some(delta);
        }
        self.as_ref().ordinal_for_delta(delta).map(u32::from)
    }
}

/// Encoded identity set interpreted as cold row membership.
pub(crate) type EncodedRowSet = EncodedIdentitySet;

/// Borrowed identity set for row identity or ordinal deletions.
/// Usable only after validation or trusted encoding.
#[derive(Clone, Copy)]
pub(crate) struct IdentitySetRef<'a> {
    codec: u8,
    body: &'a [u8],
    span: u32,
    count: u16,
}

impl<'a> IdentitySetRef<'a> {
    /// Validates disk bytes completely before establishing a searchable view.
    pub(super) fn validate(codec: u8, body: &'a [u8], span: u32) -> DataIntegrityResult<Self> {
        let invalid = || {
            Report::new(DataIntegrityError::InvalidPayload).attach("invalid compact column row set")
        };
        if span == 0 {
            return Err(invalid());
        }
        let minimum = match codec {
            DENSE | LIST16 | LIST32 => 0,
            RUNS16 | RUNS32 | BITMAP | MISSING | SEGMENTED => 4,
            TRIMMED_MISSING => 8,
            _ => return Err(invalid()),
        };
        if body.len() < minimum {
            return Err(invalid());
        }
        let count = match codec {
            DENSE => u16::try_from(span).map_err(|_| invalid())?,
            LIST16 | LIST32 => {
                let width = if codec == LIST16 { 2 } else { 4 };
                if !body.len().is_multiple_of(width) {
                    return Err(invalid());
                }
                u16::try_from(body.len() / width).map_err(|_| invalid())?
            }
            TRIMMED_MISSING => read16(body, 6),
            _ => read16(body, 0),
        };
        if count == 0 || u32::from(count) > span {
            return Err(invalid());
        }
        let view = Self {
            codec,
            body,
            span,
            count,
        };
        if codec == SEGMENTED {
            let segments = usize::from(read16(body, 2));
            let mut offset = 4 + segments * DIRECTORY_SIZE;
            if segments == 0 || offset > body.len() {
                return Err(invalid());
            }
            let mut end = 0u64;
            let mut ordinal = 0usize;
            for idx in 0..segments {
                let dir = &body[4 + idx * DIRECTORY_SIZE..4 + (idx + 1) * DIRECTORY_SIZE];
                let start = read32(dir, 0);
                let local_span = read16(dir, 4);
                let local_count = read16(dir, 6);
                let len = usize::from(read16(dir, 12));
                if local_span == 0
                    || local_count == 0
                    || local_count > local_span
                    || u64::from(start) < end
                    || u64::from(start) + u64::from(local_span) > u64::from(span)
                    || usize::from(read16(dir, 8)) != ordinal
                    || usize::from(read16(dir, 10)) != offset
                    || offset + len > body.len()
                    || dir[15] != 0
                    || !matches!(dir[14], DENSE | LIST16 | MISSING | RUNS16 | BITMAP)
                {
                    return Err(invalid());
                }
                let local = LocalRef {
                    codec: dir[14],
                    body: &body[offset..offset + len],
                    span: u32::from(local_span),
                    count: local_count,
                };
                local.validate()?;
                end = u64::from(start) + u64::from(local_span);
                ordinal += usize::from(local_count);
                offset += len;
            }
            if ordinal != usize::from(count) || offset != body.len() {
                return Err(invalid());
            }
        } else {
            if codec == BITMAP && (body[2] > 1 || body[3] != 0 || (body[2] == 1 && body.len() < 12))
            {
                return Err(invalid());
            }
            if codec == MISSING && (read16(body, 2) != 0 || span > u32::from(u16::MAX)) {
                return Err(invalid());
            }
            let (base, local) = view.local();
            if local.span == 0 || u64::from(base) + u64::from(local.span) > u64::from(span) {
                return Err(invalid());
            }
            local.validate()?;
            if matches!(codec, RUNS16 | RUNS32)
                && usize::from(read16(body, 2)) != local.body.len() / local.run_width()
            {
                return Err(invalid());
            }
            if (codec == TRIMMED_MISSING || (codec == BITMAP && body[2] == 1))
                && (local.ordinal(0).is_none() || local.ordinal(local.span - 1).is_none())
            {
                return Err(invalid());
            }
        }
        Ok(view)
    }

    /// Reconstructs a view from immutable cache-admitted bytes. The index calls
    /// this only on a validated frame; arbitrary disk bytes must use `validate`.
    #[inline]
    pub(super) fn from_admitted(codec: u8, body: &'a [u8], span: u32) -> Self {
        let count = match codec {
            DENSE => span as u16,
            LIST32 => (body.len() / 4) as u16,
            LIST16 => (body.len() / 2) as u16,
            TRIMMED_MISSING => read16(body, 6),
            _ => read16(body, 0),
        };
        Self {
            codec,
            body,
            span,
            count,
        }
    }

    /// Copies compact bytes once for a shared scan descriptor or CoW rewrite.
    #[inline]
    pub(crate) fn to_owned(self) -> EncodedIdentitySet {
        EncodedIdentitySet {
            codec: self.codec,
            span: self.span,
            body: (!self.body.is_empty()).then(|| Arc::from(self.body)),
        }
    }

    /// Returns the validated cardinality.
    #[inline]
    pub(crate) fn row_count(self) -> u16 {
        self.count
    }

    /// Resolves membership and the zero-based ordinal without allocation.
    #[inline]
    pub(crate) fn ordinal_for_delta(self, delta: u32) -> Option<u16> {
        if delta >= self.span {
            return None;
        }
        if self.codec == SEGMENTED {
            let n = usize::from(read16(self.body, 2));
            let idx = upper_bound(n, |i| read32(self.body, 4 + i * DIRECTORY_SIZE) <= delta)
                .checked_sub(1)?;
            let (base, ordinal, local) = self.segment(idx);
            local.ordinal(delta - base).map(|k| ordinal + k)
        } else {
            let (base, local) = self.local();
            local.ordinal(delta.checked_sub(base)?)
        }
    }

    /// Selects a present delta by its zero-based ordinal without allocation.
    #[inline]
    pub(crate) fn delta_for_ordinal(self, ordinal: u16) -> Option<u32> {
        if ordinal >= self.count {
            return None;
        }
        if self.codec == SEGMENTED {
            let n = usize::from(read16(self.body, 2));
            let idx = upper_bound(n, |i| {
                read16(self.body, 4 + i * DIRECTORY_SIZE + 8) <= ordinal
            }) - 1;
            let (base, ordinal_base, local) = self.segment(idx);
            Some(base + local.select(ordinal - ordinal_base))
        } else {
            let (base, local) = self.local();
            Some(base + local.select(ordinal))
        }
    }

    /// Iterates in LWC ordinal order with a sequential codec cursor.
    #[inline]
    pub(crate) fn iter_deltas(self) -> IdentitySetIter<'a> {
        let (base, local) = if self.codec == SEGMENTED {
            let (base, _, local) = self.segment(0);
            (base, local)
        } else {
            self.local()
        };
        IdentitySetIter {
            view: self,
            segment: 0,
            base,
            cursor: LocalIter::new(local),
        }
    }

    #[inline]
    fn local(self) -> (u32, LocalRef<'a>) {
        let (base, span, offset, codec) = match self.codec {
            RUNS16 | RUNS32 | MISSING => (0, self.span, 4, self.codec),
            TRIMMED_MISSING => (
                read32(self.body, 0),
                u32::from(read16(self.body, 4)),
                8,
                MISSING,
            ),
            BITMAP if self.body[2] == 1 => (read32(self.body, 4), read32(self.body, 8), 12, BITMAP),
            BITMAP => (0, self.span, 4, BITMAP),
            _ => (0, self.span, 0, self.codec),
        };
        (
            base,
            LocalRef {
                codec,
                body: &self.body[offset..],
                span,
                count: self.count,
            },
        )
    }

    #[inline]
    fn segment(self, idx: usize) -> (u32, u16, LocalRef<'a>) {
        let dir = &self.body[4 + idx * DIRECTORY_SIZE..];
        let offset = usize::from(read16(dir, 10));
        let len = usize::from(read16(dir, 12));
        (
            read32(dir, 0),
            read16(dir, 8),
            LocalRef {
                codec: dir[14],
                body: &self.body[offset..offset + len],
                span: u32::from(read16(dir, 4)),
                count: read16(dir, 6),
            },
        )
    }
}

/// Borrowed identity set interpreted as cold row membership.
pub(crate) type RowSetRef<'a> = IdentitySetRef<'a>;

#[derive(Clone, Copy)]
struct LocalRef<'a> {
    codec: u8,
    body: &'a [u8],
    span: u32,
    count: u16,
}

impl LocalRef<'_> {
    #[inline]
    fn run_width(self) -> usize {
        if self.codec == RUNS32 { 8 } else { 6 }
    }

    #[inline]
    fn run(self, idx: usize) -> (u32, u16, u16) {
        let width = self.run_width();
        let b = &self.body[idx * width..];
        (
            if self.codec == RUNS32 {
                read32(b, 0)
            } else {
                u32::from(read16(b, 0))
            },
            read16(b, width - 4),
            read16(b, width - 2),
        )
    }

    #[inline]
    fn value(self, idx: usize) -> u32 {
        if self.codec == LIST32 {
            read32(self.body, idx * 4)
        } else {
            u32::from(read16(self.body, idx * 2))
        }
    }

    #[inline]
    fn word(self, idx: usize) -> u64 {
        let groups = u64::from(self.span).div_ceil(256) as usize;
        read64(self.body, groups * 2 + idx * 8)
    }

    fn validate(self) -> DataIntegrityResult<()> {
        let invalid = || {
            Report::new(DataIntegrityError::InvalidPayload).attach("invalid local column row set")
        };
        if self.count == 0 || u32::from(self.count) > self.span {
            return Err(invalid());
        }
        match self.codec {
            DENSE => {
                if !self.body.is_empty() || self.span != u32::from(self.count) {
                    return Err(invalid());
                }
            }
            LIST16 | LIST32 | MISSING => {
                let items = if self.codec == MISSING {
                    (self.span - u32::from(self.count)) as usize
                } else {
                    usize::from(self.count)
                };
                let width = if self.codec == LIST32 { 4 } else { 2 };
                if self.body.len() != items * width {
                    return Err(invalid());
                }
                let mut prev = None;
                for idx in 0..items {
                    let v = self.value(idx);
                    if v >= self.span || prev.is_some_and(|p| p >= v) {
                        return Err(invalid());
                    }
                    prev = Some(v);
                }
            }
            RUNS16 | RUNS32 => {
                if self.body.is_empty() || !self.body.len().is_multiple_of(self.run_width()) {
                    return Err(invalid());
                }
                let mut end = 0u64;
                let mut count = 0u32;
                for idx in 0..self.body.len() / self.run_width() {
                    let (start, len, prefix) = self.run(idx);
                    if len == 0
                        || (idx != 0 && u64::from(start) <= end)
                        || u32::from(prefix) != count
                    {
                        return Err(invalid());
                    }
                    end = u64::from(start) + u64::from(len);
                    if end > u64::from(self.span) || (self.codec == RUNS16 && end > 65536) {
                        return Err(invalid());
                    }
                    count += u32::from(len);
                }
                if count != u32::from(self.count) {
                    return Err(invalid());
                }
            }
            BITMAP => {
                if self.body.len() != bitmap_len(u64::from(self.span)) {
                    return Err(invalid());
                }
                let words = u64::from(self.span).div_ceil(64) as usize;
                let mut count = 0u32;
                for idx in 0..words {
                    if idx % 4 == 0 && u32::from(read16(self.body, idx / 4 * 2)) != count {
                        return Err(invalid());
                    }
                    let word = self.word(idx);
                    if idx + 1 == words
                        && !self.span.is_multiple_of(64)
                        && word >> (self.span % 64) != 0
                    {
                        return Err(invalid());
                    }
                    count += word.count_ones();
                }
                if count != u32::from(self.count) {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }

    #[inline]
    fn ordinal(self, delta: u32) -> Option<u16> {
        if delta >= self.span {
            return None;
        }
        match self.codec {
            DENSE => Some(delta as u16),
            LIST16 | LIST32 => {
                let idx = upper_bound(usize::from(self.count), |i| self.value(i) < delta);
                (idx < usize::from(self.count) && self.value(idx) == delta).then_some(idx as u16)
            }
            MISSING => {
                let n = (self.span - u32::from(self.count)) as usize;
                let idx = upper_bound(n, |i| self.value(i) < delta);
                if idx < n && self.value(idx) == delta {
                    None
                } else {
                    Some((delta - idx as u32) as u16)
                }
            }
            RUNS16 | RUNS32 => {
                let idx = upper_bound(self.body.len() / self.run_width(), |i| {
                    self.run(i).0 <= delta
                })
                .checked_sub(1)?;
                let (start, len, prefix) = self.run(idx);
                (delta - start < u32::from(len)).then(|| prefix + (delta - start) as u16)
            }
            BITMAP => {
                let idx = delta as usize / 64;
                let word = self.word(idx);
                let bit = delta % 64;
                if word & (1u64 << bit) == 0 {
                    return None;
                }
                let mut rank = u32::from(read16(self.body, idx / 4 * 2));
                for i in idx / 4 * 4..idx {
                    rank += self.word(i).count_ones();
                }
                rank += (word & ((1u64 << bit) - 1)).count_ones();
                Some(rank as u16)
            }
            _ => unreachable!("validated local column row-set codec"),
        }
    }

    #[inline]
    fn select(self, ordinal: u16) -> u32 {
        match self.codec {
            DENSE => u32::from(ordinal),
            LIST16 | LIST32 => self.value(usize::from(ordinal)),
            MISSING => {
                let n = (self.span - u32::from(self.count)) as usize;
                u32::from(ordinal)
                    + upper_bound(n, |i| self.value(i) - i as u32 <= u32::from(ordinal)) as u32
            }
            RUNS16 | RUNS32 => {
                let idx = upper_bound(self.body.len() / self.run_width(), |i| {
                    self.run(i).2 <= ordinal
                }) - 1;
                let (start, _, prefix) = self.run(idx);
                start + u32::from(ordinal - prefix)
            }
            BITMAP => {
                let groups = u64::from(self.span).div_ceil(256) as usize;
                let group = upper_bound(groups, |i| read16(self.body, i * 2) <= ordinal) - 1;
                let mut remaining = u32::from(ordinal - read16(self.body, group * 2));
                for idx in
                    group * 4..(group * 4 + 4).min(u64::from(self.span).div_ceil(64) as usize)
                {
                    let mut word = self.word(idx);
                    let count = word.count_ones();
                    if remaining < count {
                        // Bounded single-word select, at most six population partitions.
                        let mut shift = 0;
                        for half in [32, 16, 8, 4, 2, 1] {
                            let low = (word & ((1u64 << half) - 1)).count_ones();
                            if remaining >= low {
                                remaining -= low;
                                word >>= half;
                                shift += half;
                            }
                        }
                        return idx as u32 * 64 + shift;
                    }
                    remaining -= count;
                }
                unreachable!("validated bitmap ordinal")
            }
            _ => unreachable!("validated local column row-set codec"),
        }
    }
}

/// Sequential cursor over validated compact bytes, including segment boundaries.
pub(crate) struct IdentitySetIter<'a> {
    view: IdentitySetRef<'a>,
    segment: usize,
    base: u32,
    cursor: LocalIter<'a>,
}

impl Iterator for IdentitySetIter<'_> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        if let Some(delta) = self.cursor.next() {
            return Some(self.base + delta);
        }
        if self.view.codec != SEGMENTED {
            return None;
        }
        self.segment += 1;
        if self.segment >= usize::from(read16(self.view.body, 2)) {
            return None;
        }
        let (base, _, local) = self.view.segment(self.segment);
        self.base = base;
        self.cursor = LocalIter::new(local);
        self.cursor.next().map(|delta| base + delta)
    }
}

struct LocalIter<'a> {
    view: LocalRef<'a>,
    ordinal: u32,
    index: usize,
    delta: u32,
    word: u64,
}

impl<'a> LocalIter<'a> {
    #[inline]
    fn new(view: LocalRef<'a>) -> Self {
        Self {
            view,
            ordinal: 0,
            index: 0,
            delta: 0,
            word: if view.codec == BITMAP {
                view.word(0)
            } else {
                0
            },
        }
    }

    #[inline]
    fn next(&mut self) -> Option<u32> {
        if self.ordinal >= u32::from(self.view.count) {
            return None;
        }
        let delta = match self.view.codec {
            DENSE => self.ordinal,
            LIST16 | LIST32 => self.view.value(self.ordinal as usize),
            MISSING => {
                while self.index < self.view.body.len() / 2
                    && self.view.value(self.index) == self.delta
                {
                    self.index += 1;
                    self.delta += 1;
                }
                let delta = self.delta;
                self.delta += 1;
                delta
            }
            RUNS16 | RUNS32 => {
                let (mut start, len, mut prefix) = self.view.run(self.index);
                if self.ordinal == u32::from(prefix) + u32::from(len) {
                    self.index += 1;
                    (start, _, prefix) = self.view.run(self.index);
                }
                start + (self.ordinal - u32::from(prefix))
            }
            BITMAP => {
                while self.word == 0 {
                    self.index += 1;
                    self.word = self.view.word(self.index);
                }
                let delta = self.index as u32 * 64 + self.word.trailing_zeros();
                self.word &= self.word - 1;
                delta
            }
            _ => unreachable!("validated iterator codec"),
        };
        self.ordinal += 1;
        Some(delta)
    }
}

#[inline]
fn codec_order(codec: u8) -> u8 {
    match codec {
        DENSE => 0,
        LIST16 | LIST32 => 1,
        RUNS16 | RUNS32 => 2,
        MISSING | TRIMMED_MISSING => 3,
        BITMAP => 4,
        _ => 5,
    }
}

#[inline]
fn depth(n: usize) -> u32 {
    n.bit_width()
}

#[inline]
fn bitmap_len(span: u64) -> usize {
    let words = span.div_ceil(64) as usize;
    words * 8 + words.div_ceil(4) * 2
}

fn local_candidate(stats: IdentitySetStats) -> Candidate {
    let span = stats.span() as usize;
    let holes = span - stats.count;
    let mut best = Candidate {
        codec: LIST16,
        trimmed: false,
        len: stats.count * 2,
        depth: depth(stats.count),
        words: 0,
    };
    let candidates = [
        Candidate {
            codec: DENSE,
            trimmed: false,
            len: 0,
            depth: 0,
            words: 0,
        },
        Candidate {
            codec: RUNS16,
            trimmed: false,
            len: stats.runs * 6,
            depth: depth(stats.runs),
            words: 0,
        },
        Candidate {
            codec: MISSING,
            trimmed: false,
            len: holes * 2,
            depth: depth(holes),
            words: 0,
        },
        Candidate {
            codec: BITMAP,
            trimmed: false,
            len: bitmap_len(stats.span()),
            depth: depth(span.div_ceil(256)),
            words: 4,
        },
    ];
    for candidate in candidates {
        if candidate.codec == DENSE && holes != 0 {
            continue;
        }
        if candidate.key() < best.key() {
            best = candidate;
        }
    }
    best
}

fn whole_candidate(stats: IdentitySetStats, base: u64, span: u32) -> Candidate {
    let mut best = Candidate {
        codec: LIST32,
        trimmed: false,
        len: stats.count * 4,
        depth: depth(stats.count),
        words: 0,
    };
    let mut consider = |candidate: Candidate| {
        if candidate.key() < best.key() {
            best = candidate;
        }
    };
    if stats.count == span as usize {
        consider(Candidate {
            codec: DENSE,
            trimmed: false,
            len: 0,
            depth: 0,
            words: 0,
        });
    }
    let narrow = stats.last - base <= u64::from(u16::MAX);
    if narrow {
        consider(Candidate {
            codec: LIST16,
            trimmed: false,
            len: stats.count * 2,
            depth: depth(stats.count),
            words: 0,
        });
    }
    consider(Candidate {
        codec: if narrow { RUNS16 } else { RUNS32 },
        trimmed: false,
        len: 4 + stats.runs * if narrow { 6 } else { 8 },
        depth: depth(stats.runs),
        words: 0,
    });
    for (trimmed, bit_span) in [(false, u64::from(span)), (true, stats.span())] {
        consider(Candidate {
            codec: BITMAP,
            trimmed,
            len: if trimmed { 12 } else { 4 } + bitmap_len(bit_span),
            depth: depth(bit_span.div_ceil(256) as usize),
            words: 4,
        });
        if bit_span <= u64::from(u16::MAX) {
            let holes = bit_span as usize - stats.count;
            consider(Candidate {
                codec: if trimmed { TRIMMED_MISSING } else { MISSING },
                trimmed,
                len: if trimmed { 8 } else { 4 } + holes * 2,
                depth: depth(holes),
                words: 0,
            });
        }
    }
    best
}

#[inline]
fn offer_pair(segments: &[Segment], left: usize, heap: &mut MergeHeap) {
    let Some(right) = segments[left].next else {
        return;
    };
    let combined = segments[left].seed.stats.merge(segments[right].seed.stats);
    if combined.span() > u64::from(u16::MAX) {
        return;
    }
    let old = DIRECTORY_SIZE + segments[left].candidate.len + segments[right].candidate.len;
    let new = local_candidate(combined).len;
    if let Some(saving) = old.checked_sub(new) {
        heap.push((
            saving,
            Reverse(left),
            segments[left].generation,
            segments[right].generation,
        ));
    }
}

fn emit_local(out: &mut Vec<u8>, codec: u8, rows: &[RowID], base: u64, span: u32) {
    match codec {
        DENSE => {}
        LIST16 | LIST32 => {
            for row in rows {
                let delta = (row.as_u64() - base) as u32;
                if codec == LIST16 {
                    put16(out, delta as usize);
                } else {
                    put32(out, delta);
                }
            }
        }
        MISSING | TRIMMED_MISSING => {
            let mut next = 0;
            for row in rows {
                let delta = (row.as_u64() - base) as u32;
                for hole in next..delta {
                    put16(out, hole as usize);
                }
                next = delta + 1;
            }
            for hole in next..span {
                put16(out, hole as usize);
            }
        }
        RUNS16 | RUNS32 => {
            let mut idx = 0;
            while idx < rows.len() {
                let start = idx;
                idx += 1;
                while idx < rows.len() && rows[idx].as_u64() - rows[idx - 1].as_u64() == 1 {
                    idx += 1;
                }
                let delta = (rows[start].as_u64() - base) as u32;
                if codec == RUNS16 {
                    put16(out, delta as usize);
                } else {
                    put32(out, delta);
                }
                put16(out, idx - start);
                put16(out, start);
            }
        }
        BITMAP => {
            let begin = out.len();
            let words = u64::from(span).div_ceil(64) as usize;
            let groups = words.div_ceil(4);
            out.resize(begin + bitmap_len(u64::from(span)), 0);
            let word_start = begin + groups * 2;
            for row in rows {
                let delta = (row.as_u64() - base) as usize;
                out[word_start + delta / 8] |= 1 << (delta % 8);
            }
            let mut count = 0u16;
            for word in 0..words {
                if word % 4 == 0 {
                    out[begin + word / 4 * 2..begin + word / 4 * 2 + 2]
                        .copy_from_slice(&count.to_le_bytes());
                }
                count += read64(out, word_start + word * 8).count_ones() as u16;
            }
        }
        _ => unreachable!("selected column row-set codec"),
    }
}

#[inline]
fn upper_bound(n: usize, mut before: impl FnMut(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if before(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

#[inline]
fn put16(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u16).to_le_bytes());
}

#[inline]
fn put32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn read16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

#[inline]
fn read32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

#[inline]
fn read64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(deltas: impl IntoIterator<Item = u32>) -> Vec<RowID> {
        deltas
            .into_iter()
            .map(|d| RowID::new(u64::from(d)))
            .collect()
    }

    fn whole(codec: u8, span: u32, rows: &[RowID], trimmed: bool) -> Vec<u8> {
        let stats = IdentitySetStats::from_rows(rows);
        let base = if trimmed { stats.first } else { 0 };
        let local_span = if trimmed { stats.span() as u32 } else { span };
        let mut body = Vec::new();
        match codec {
            RUNS16 | RUNS32 => {
                put16(&mut body, rows.len());
                put16(&mut body, stats.runs);
            }
            MISSING => {
                put16(&mut body, rows.len());
                put16(&mut body, 0);
            }
            TRIMMED_MISSING => {
                put32(&mut body, base as u32);
                put16(&mut body, local_span as usize);
                put16(&mut body, rows.len());
            }
            BITMAP => {
                put16(&mut body, rows.len());
                body.extend_from_slice(&[u8::from(trimmed), 0]);
                if trimmed {
                    put32(&mut body, base as u32);
                    put32(&mut body, local_span);
                }
            }
            _ => {}
        }
        emit_local(&mut body, codec, rows, base, local_span);
        body
    }

    fn check(view: IdentitySetRef<'_>, expected: &[RowID]) {
        let deltas: Vec<_> = expected.iter().map(|r| r.as_u64() as u32).collect();
        assert_eq!(usize::from(view.row_count()), deltas.len());
        assert_eq!(
            view.iter_deltas().collect::<Vec<_>>(),
            deltas,
            "codec={}",
            view.codec
        );
        for (ordinal, delta) in deltas.iter().enumerate() {
            assert_eq!(
                view.ordinal_for_delta(*delta),
                Some(ordinal as u16),
                "codec={}, delta={delta}",
                view.codec
            );
            assert_eq!(
                view.delta_for_ordinal(ordinal as u16),
                Some(*delta),
                "codec={}, ordinal={ordinal}",
                view.codec
            );
            for probe in [delta.saturating_sub(1), delta.saturating_add(1)] {
                assert_eq!(
                    view.ordinal_for_delta(probe),
                    deltas.binary_search(&probe).ok().map(|i| i as u16),
                    "codec={}, probe={probe}",
                    view.codec
                );
            }
        }
        for probe in [0, view.span - 1, view.span, u32::MAX] {
            assert_eq!(
                view.ordinal_for_delta(probe),
                deltas.binary_search(&probe).ok().map(|i| i as u16)
            );
        }
        assert_eq!(view.delta_for_ordinal(view.count), None);
        assert_eq!(view.delta_for_ordinal(u16::MAX), None);
    }

    fn hash(mut v: u64) -> u64 {
        v = v.wrapping_add(0x9e3779b97f4a7c15);
        v = (v ^ (v >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        v = (v ^ (v >> 27)).wrapping_mul(0x94d049bb133111eb);
        v ^ (v >> 31)
    }

    /// Purpose: Protect every whole codec's membership, inverse mapping, and ordered cursor.
    /// Expected: Compact operations agree with an independent sorted oracle across narrow and wide boundaries.
    #[test]
    fn whole_codecs_match_oracle() {
        let fixtures = [
            rows([0]),
            rows(0..65535),
            rows([1, 2, 5, 255, 256, 1024, 1025, 2048]),
            rows((0..65535).filter(|i| i % 97 != 0)),
            rows([0, 65535, 65536, u32::MAX - 1]),
        ];
        for values in fixtures {
            let span = values.last().unwrap().as_u64() as u32 + 1;
            for codec in [
                DENSE,
                LIST32,
                LIST16,
                RUNS16,
                RUNS32,
                BITMAP,
                MISSING,
                TRIMMED_MISSING,
            ] {
                if codec == DENSE && values.len() != span as usize {
                    continue;
                }
                if matches!(codec, LIST16 | RUNS16) && span > 65536 {
                    continue;
                }
                if matches!(codec, MISSING | TRIMMED_MISSING) && span > 65535 {
                    continue;
                }
                if codec == BITMAP && span > 65536 {
                    continue;
                }
                for trimmed in [false, true] {
                    if trimmed && !matches!(codec, BITMAP | TRIMMED_MISSING) {
                        continue;
                    }
                    if codec == TRIMMED_MISSING && !trimmed {
                        continue;
                    }
                    let body = whole(codec, span, &values, trimmed);
                    let view = IdentitySetRef::validate(codec, &body, span).unwrap();
                    check(view, &values);
                }
            }
        }
    }

    /// Purpose: Protect the missing-offset inverse formula through leading, consecutive, and trailing holes.
    /// Expected: Every nonempty small-domain subset yields the exact ordinal and delta bijection.
    #[test]
    fn missing_inverse_exhaustive() {
        for span in 1..=10 {
            for mask in 1u32..1 << span {
                let values = rows((0..span).filter(|i| mask & (1 << i) != 0));
                for codec in [MISSING, TRIMMED_MISSING] {
                    let body = whole(codec, span, &values, codec == TRIMMED_MISSING);
                    check(
                        IdentitySetRef::validate(codec, &body, span).unwrap(),
                        &values,
                    );
                }
            }
        }
    }

    /// Purpose: Protect all local formats and directory searches across inter-segment gaps.
    /// Expected: Mixed segments preserve global ordinals, reject gaps, and iterate each present row once.
    #[test]
    fn mixed_segment_codecs() {
        let mut values = Vec::new();
        let mut segments = Vec::new();
        for (idx, codec) in [DENSE, LIST16, MISSING, RUNS16, BITMAP]
            .into_iter()
            .enumerate()
        {
            let base = idx as u32 * 70000;
            let local = match codec {
                DENSE => rows(base..base + 40),
                LIST16 => rows([base + 1, base + 30, base + 100]),
                MISSING => rows((base..base + 100).filter(|i| i % 17 != 0)),
                RUNS16 => rows((base..base + 100).filter(|i| i % 50 < 20)),
                _ => rows([base, base + 1, base + 1024, base + 2048]),
            };
            let seed = IdentitySetSeed {
                start: values.len(),
                end: values.len() + local.len(),
                stats: IdentitySetStats::from_rows(&local),
            };
            let mut payload = Vec::new();
            emit_local(
                &mut payload,
                codec,
                &local,
                seed.stats.first,
                seed.stats.span() as u32,
            );
            segments.push(Segment {
                seed,
                candidate: Candidate {
                    codec,
                    trimmed: false,
                    len: payload.len(),
                    depth: 0,
                    words: 0,
                },
                prev: None,
                next: None,
                generation: 0,
                active: true,
            });
            values.extend(local);
        }
        let len = 4
            + segments.len() * DIRECTORY_SIZE
            + segments.iter().map(|s| s.candidate.len).sum::<usize>();
        let plan = IdentitySetPlan {
            whole: whole_candidate(IdentitySetStats::from_rows(&values), 0, 400000),
            segments,
            segmented: true,
            len,
        };
        let body = plan.encode(&values, 0, 400000);
        check(
            IdentitySetRef::validate(SEGMENTED, &body, 400000).unwrap(),
            &values,
        );
        for len in 0..body.len() {
            assert!(IdentitySetRef::validate(SEGMENTED, &body[..len], 400000).is_err());
        }
        let mut overlapping = body.clone();
        overlapping[20..24].fill(0);
        assert!(IdentitySetRef::validate(SEGMENTED, &overlapping, 400000).is_err());
        for (offset, value) in [
            (2, 0),
            (4 + 15, 1),
            (4 + 14, 255),
            (4 + 10, 0),
            (4 + 16 + 8, 0),
        ] {
            let mut corrupt = body.clone();
            corrupt[offset] = value;
            assert!(
                IdentitySetRef::validate(SEGMENTED, &corrupt, 400000).is_err(),
                "offset={offset}"
            );
        }
    }

    /// Purpose: Exercise deterministic merging, stale heap records, sparse windows, and exact sizing.
    /// Expected: Repeated plans serialize identically, preserve the oracle, and never exceed the best whole candidate.
    #[test]
    fn generated_plans_and_sizes() {
        for seed in 0..120 {
            let span = 500 + seed * 193;
            let values = rows((0..span).filter(|i| {
                hash(u64::from(*i) ^ u64::from(seed)) % 100 < 1 + u64::from(seed % 100)
            }));
            if values.is_empty() {
                continue;
            }
            let mut seeds = Vec::new();
            for (idx, chunk) in values.chunks(23).enumerate() {
                IdentitySetSeed::append_page(&mut seeds, chunk, idx * 23);
            }
            let plan = IdentitySetPlan::new(&values, 0, span, &seeds);
            let encoded = EncodedIdentitySet::plan(
                RowID::new(0),
                RowID::new(u64::from(span)),
                &values,
                &seeds,
                65184,
            )
            .unwrap();
            assert_eq!(encoded.body().len(), plan.len);
            assert!(plan.len <= plan.whole.len);
            assert_eq!(
                encoded,
                EncodedIdentitySet::plan(
                    RowID::new(0),
                    RowID::new(u64::from(span)),
                    &values,
                    &seeds,
                    65184
                )
                .unwrap()
            );
            check(
                IdentitySetRef::validate(encoded.codec(), encoded.body(), span).unwrap(),
                &values,
            );
        }
        let values = rows([0, 1, 2, 1000000000, 1000000001, u32::MAX - 1]);
        let plan = IdentitySetPlan::new(&values, 0, u32::MAX, &[]);
        assert!(plan.segments.len() <= values.len());
        let encoded = EncodedIdentitySet::plan(
            RowID::new(0),
            RowID::new(u64::from(u32::MAX)),
            &values,
            &[],
            65184,
        )
        .unwrap();
        check(encoded.as_ref(), &values);
    }

    /// Purpose: Reject malformed compact metadata before constructing a searchable view.
    /// Expected: Truncation, bad prefixes, reserved fields, padding, and invalid directory records return integrity errors without panicking.
    #[test]
    fn malformed_compact_sections() {
        let values = rows([1, 2, 3, 100, 1024]);
        for codec in [
            LIST32,
            LIST16,
            RUNS16,
            RUNS32,
            BITMAP,
            MISSING,
            TRIMMED_MISSING,
        ] {
            let body = whole(codec, 1050, &values, codec == TRIMMED_MISSING);
            for len in 0..body.len() {
                // A shorter present list is independently valid when complete and sorted.
                if matches!(codec, LIST32 | LIST16) {
                    continue;
                }
                assert!(
                    IdentitySetRef::validate(codec, &body[..len], 1050).is_err(),
                    "codec={codec} len={len}"
                );
            }
            for seed in 0..200 {
                let mut corrupt = body.clone();
                let idx = hash(seed) as usize % corrupt.len();
                corrupt[idx] ^= (hash(seed + 1) as u8) | 1;
                if let Ok(view) = IdentitySetRef::validate(codec, &corrupt, 1050) {
                    let decoded = rows(view.iter_deltas());
                    assert!(decoded.windows(2).all(|p| p[0] < p[1]));
                    check(view, &decoded);
                }
            }
        }
        let mut body = whole(BITMAP, 1050, &values, false);
        body[3] = 1;
        assert!(IdentitySetRef::validate(BITMAP, &body, 1050).is_err());
        body[3] = 0;
        *body.last_mut().unwrap() |= 128;
        assert!(IdentitySetRef::validate(BITMAP, &body, 1050).is_err());
        let mut body = whole(RUNS16, 1050, &values, false);
        body[8] = 1;
        assert!(IdentitySetRef::validate(RUNS16, &body, 1050).is_err());
        assert!(IdentitySetRef::validate(255, &[], 1).is_err());
        assert!(IdentitySetRef::validate(DENSE, &[0], 1).is_err());
        assert!(IdentitySetRef::validate(DENSE, &[], 0).is_err());
        assert!(IdentitySetRef::validate(LIST16, &[1, 0, 1, 0], 10).is_err());
        assert!(IdentitySetRef::validate(LIST32, &[0; 3], 10).is_err());
    }

    /// Purpose: Enforce cardinality, coverage, and exact body budgets before encoding.
    /// Expected: Legal boundaries succeed and unsupported identities return the typed resource classification.
    #[test]
    fn capacity_and_row_id_boundaries() {
        let values = rows(0..65535);
        assert_eq!(
            EncodedIdentitySet::plan(RowID::new(0), RowID::new(65535), &values, &[], 0)
                .unwrap()
                .body(),
            []
        );
        for (end, values) in [
            (65536, rows(0..65536)),
            (u64::from(u32::MAX) + 1, rows([0])),
        ] {
            assert_eq!(
                EncodedIdentitySet::plan(RowID::new(0), RowID::new(end), &values, &[], 65184)
                    .unwrap_err()
                    .current_context(),
                &ResourceError::ColumnBlockEntryCapacityExceeded
            );
        }
        let values = rows((0..20000).map(|i| i * 100003));
        assert_eq!(
            EncodedIdentitySet::plan(RowID::new(0), RowID::new(2000000000), &values, &[], 65184)
                .unwrap_err()
                .current_context(),
            &ResourceError::ColumnBlockEntryCapacityExceeded
        );
        let values = rows([0, 100, 10000]);
        let exact = IdentitySetPlan::new(&values, 0, 10001, &[]).len;
        assert!(
            EncodedIdentitySet::plan(RowID::new(0), RowID::new(10001), &values, &[], exact).is_ok()
        );
        assert!(
            EncodedIdentitySet::plan(RowID::new(0), RowID::new(10001), &values, &[], exact - 1)
                .is_err()
        );
        let values = [RowID::new(u64::MAX - 3), RowID::new(u64::MAX - 1)];
        let encoded = EncodedIdentitySet::plan(
            RowID::new(u64::MAX - 4),
            RowID::new(u64::MAX),
            &values,
            &[],
            65184,
        )
        .unwrap();
        check(encoded.as_ref(), &rows([1, 3]));
    }
}
