use crate::error::{Error, Result};
use crate::eval::{Eval, EvalBuilder, EvalRef};

use doradb_catalog::{ColIndex, TableID};
use doradb_expr::{DataSourceID, ExprKind, QueryID, TypeInferer};
use doradb_storage::col::attr::Attr;
use doradb_storage::col::chunk::Chunk;
use doradb_storage::col::sel::Sel;
use std::mem;

pub type TableEvalPlan = EvalPlan<TableID>;
pub type QueryEvalPlan = EvalPlan<QueryID>;

/// Evaluation Plan.
///
/// It is designed to support scalar expression evaluation within chunk.
/// The plan evaluates all expressions in order, ensuring any common expression
/// be evaluated only once.
/// For example, if we have expressions: `abs(c0)`, `abs(c0)+1`, `abs(c0)+2`,
/// the common expression `abs(c0)` will be evaluated only once, and be reused
/// by others.
///
/// It also supports filter expression.
/// The filter expression will be evaluated first.
/// The result has two formats: Single, and Bitmap.
///
/// The plan maintains the input columns, evaluation cache instructions, and
/// output references to input and cache.
///
/// Currently, all expressions are considered as deterministic.
#[derive(Debug)]
pub struct EvalPlan<T> {
    pub(super) input: Vec<(T, ColIndex)>,
    pub(super) evals: Vec<(Eval, usize)>,
    pub(super) output: Vec<EvalRef>,
    // selection index
    pub(super) sel_idx: Option<usize>,
}

impl<T: DataSourceID> EvalPlan<T> {
    /// Create a new evaluation plan.
    #[inline]
    pub fn new<'a, E: IntoIterator<Item = &'a ExprKind>, I: TypeInferer>(
        exprs: E,
        inferer: &'a mut I,
    ) -> Result<Self> {
        EvalBuilder::new(inferer).build(exprs)
    }

    /// Create a new evaluation plan with filter expression.
    #[inline]
    pub fn with_filter<'a, E: IntoIterator<Item = &'a ExprKind>, I: TypeInferer>(
        exprs: E,
        cond_expr: &'a ExprKind,
        inferer: &'a mut I,
    ) -> Result<Self> {
        EvalBuilder::new(inferer).with_filter(exprs, cond_expr)
    }

    /// Evaluate one chunk.
    #[inline]
    pub fn eval(&self, chunk: &Chunk) -> Result<Vec<Attr>> {
        assert!(chunk.data.len() >= self.input.len());
        let mut cache: EvalCache = (0..self.evals.len())
            .map(|_| CacheEntry::default())
            .collect();
        let (sel, evals) = if let Some(sel_idx) = self.sel_idx {
            // Evaluate filter first
            for (e, idx) in &self.evals[..=sel_idx] {
                e.eval(chunk, &mut cache, *idx, None)?;
            }
            let sel = cache
                .get(sel_idx)
                .and_then(CacheEntry::attr)
                .map(Sel::try_from)
                .transpose()?;
            (sel, &self.evals[sel_idx + 1..])
        } else {
            (None, &self.evals[..])
        };
        // Evaluate all expressions
        for (e, idx) in evals {
            e.eval(chunk, &mut cache, *idx, sel.as_ref())?;
        }
        // fetch output from cache
        self.output(chunk, cache, sel)
    }

    #[inline]
    fn output(&self, chunk: &Chunk, mut cache: EvalCache, sel: Option<Sel>) -> Result<Vec<Attr>> {
        let mut output = Vec::with_capacity(self.output.len());
        for r in &self.output {
            match r {
                EvalRef::Input(idx) => {
                    let attr = chunk
                        .data
                        .get(*idx)
                        .map(|attr| {
                            if let Some(filter) = sel.as_ref() {
                                filter.apply(attr)
                            } else {
                                Ok(attr.to_owned())
                            }
                        })
                        .transpose()?
                        .ok_or(Error::FailToFetchAttr)?;
                    output.push(attr);
                }
                EvalRef::Cache(idx) => {
                    let ent = cache.get_mut(*idx).ok_or(Error::FailToFetchEvalCache)?;
                    match mem::take(ent) {
                        CacheEntry::Empty => return Err(Error::FailToFetchEvalCache),
                        CacheEntry::Some(attr) => {
                            let attr = if let Some(filter) = sel.as_ref() {
                                filter.apply(&attr)?
                            } else {
                                attr
                            };
                            let idx = output.len();
                            output.push(attr);
                            *ent = CacheEntry::Taken(idx); // update output index back
                        }
                        CacheEntry::Taken(out_idx) => {
                            let ext = output[out_idx].to_owned();
                            output.push(ext);
                            *ent = CacheEntry::Taken(out_idx); // update it back
                        }
                    }
                }
            }
        }
        Ok(output)
    }
}

/// Evaluation cache, backed by vector of codec.
/// It enables reusing intermediate computations.
/// e.g. SELECT abs(a)+1, abs(a)+2, abs(a)+3 from t
/// We compute abs(a) only once and store it in the cache,
/// and let other expressions to reuse it if possible.
pub type EvalCache = Vec<CacheEntry>;

#[derive(Debug)]
pub enum CacheEntry {
    Empty,
    Some(Attr),
    /// Taken is a special cases that the value is taken
    /// for output, but maybe other outputs also require
    /// it, so we leave the index and let latter to copy it
    /// from that index.
    Taken(usize),
}

impl CacheEntry {
    // Returns reference of attribute if exists.
    #[inline]
    pub fn attr(&self) -> Option<&Attr> {
        match self {
            CacheEntry::Some(attr) => Some(attr),
            _ => None,
        }
    }
}

impl Default for CacheEntry {
    #[inline]
    fn default() -> Self {
        CacheEntry::Empty
    }
}
