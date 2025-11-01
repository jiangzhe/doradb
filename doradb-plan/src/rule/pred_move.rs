use crate::error::{Error, Result};
use crate::join::{Join, JoinKind, JoinOp, QualifiedJoin};
use crate::lgc::{Op, OpKind, OpMutVisitor, ProjCol, QuerySet, SetopKind};
use crate::rule::op_id::assign_id;
use doradb_catalog::{Catalog, Key, TableID, TblCol};
use doradb_expr::controlflow::{Branch, ControlFlow, Unbranch};
use doradb_expr::{
    Col, ColIndex, ColKind, Const, ExprExt, ExprKind, ExprMutVisitor, ExprVisitor, FnlDep,
    GlobalID, Pred, PredFuncKind, QryCol, QueryID,
};
use smallvec::{smallvec, SmallVec};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem;

/// Predicate Movearound
///
/// This is a combination of prediate pushdown(PPD) and predicate pullup(PPU).
/// PPU pulls all predicates as high as possible, and store them into a separate
/// map, which can be pushed in future. Meanwhile, equal sets of columns will
/// be generated.
///
#[inline]
pub fn pred_move(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    assign_id(qry_set, qry_id)?;
    todo!()
}

/// Predicate pullup
///
/// Traverse the query plan and pull up predicates if possible,
/// returns the inner set of root operator
#[inline]
fn pred_pullup<C: Catalog>(
    catalog: &C,
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    pred_map: &mut PredMap,
    dep_cols: &mut DepCols,
) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut ppu = PredPullup {
            catalog,
            qry_set,
            pred_map,
            dep_cols,
        };
        op.walk_mut(&mut ppu).unbranch()
    })?
}

#[inline]
fn clear_filt(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut cf = ClearFilt(qry_set);
        op.walk_mut(&mut cf).unbranch()
    })?
}

/// PredMap stores inner set and outer set of each node.
/// The inner set is free to pull or push across nodes.
/// The outer set is only for join node itself.
#[derive(Debug, Default)]
pub struct PredMap {
    pub(super) inner: HashMap<GlobalID, InnerSet>,
    pub(super) outer: HashMap<GlobalID, OuterSet>,
    pub(super) qry_col_map: HashMap<QryCol, GlobalID>,
    // note that same table can be referred in a query multiple times.
    // so global id is stored in a set.
    pub(super) tbl_col_map: HashMap<TblCol, HashSet<GlobalID>>,
    pub(super) col_map: HashMap<GlobalID, Col>,
    pub(super) tbl_dep_map: HashMap<TblCol, Vec<Vec<TblCol>>>,
    // pub(super) tbl_occurrence: HashMap<TableID, usize>,
}

impl PredMap {
    /// remove and get a inner predicate set of given node.
    #[inline]
    pub(super) fn remove_inner(&mut self, op_id: GlobalID) -> InnerSet {
        self.inner.remove(&op_id).unwrap_or_default()
    }

    #[inline]
    pub(super) fn get_inner(&self, op_id: GlobalID) -> Option<&InnerSet> {
        self.inner.get(&op_id)
    }

    #[inline]
    pub(super) fn merge_inner(&mut self, op_id: GlobalID, mut inner: InnerSet) {
        if inner.is_empty() {
            return;
        }
        match self.inner.entry(op_id) {
            Entry::Occupied(occ) => {
                inner.merge(occ.remove());
                self.inner.insert(op_id, inner);
            }
            Entry::Vacant(vac) => {
                vac.insert(inner);
            }
        }
    }

    #[inline]
    pub(super) fn extract_and_merge_inner(
        &mut self,
        op_id: GlobalID,
        mut inner: InnerSet,
    ) -> InnerSet {
        if inner.is_empty() {
            return self.remove_inner(op_id);
        }
        match self.inner.entry(op_id) {
            Entry::Occupied(occ) => {
                inner.merge(occ.remove());
            }
            Entry::Vacant(_vac) => (),
        }
        inner
    }

    #[inline]
    pub(super) fn insert_inner(&mut self, op_id: GlobalID, inner_set: InnerSet) {
        self.inner.insert(op_id, inner_set);
    }

    #[inline]
    pub(super) fn insert_outer(&mut self, op_id: GlobalID, outer_set: OuterSet) {
        self.outer.insert(op_id, outer_set);
    }

    #[inline]
    pub(super) fn cols_eq_pred(&self, l_gid: GlobalID, r_gid: GlobalID) -> ExprKind {
        let l_col = self.col_map[&l_gid].clone();
        let r_col = self.col_map[&r_gid].clone();
        ExprKind::pred_func(
            PredFuncKind::Equal,
            vec![ExprKind::Col(l_col), ExprKind::Col(r_col)],
        )
    }

    #[inline]
    pub fn apply_inner(&self, inner: InnerSet, dst: &mut Vec<ExprKind>) {
        // equal set to expression
        for eq_set in inner.eq_sets {
            let mut eq_set_iter = eq_set.into_iter();
            let l_gid = eq_set_iter.next().unwrap();
            for r_gid in eq_set_iter {
                let e = self.cols_eq_pred(l_gid, r_gid);
                dst.push(e);
            }
        }
        // filter expression
        for p in inner.filt {
            let e = self.reflect_pred(p);
            dst.push(e);
        }
        // do nothing to functional dependencies
    }

    #[inline]
    pub(super) fn reflect_pred(&self, p: PredExpr) -> ExprKind {
        match p {
            PredExpr::OneCol(_, e) | PredExpr::Other(e) => e,
            PredExpr::ColEqConst(gid, cst) => {
                let col = self.col_map[&gid].clone();
                ExprKind::pred_func(
                    PredFuncKind::Equal,
                    vec![ExprKind::Col(col), ExprKind::Const(cst)],
                )
            }
            PredExpr::TwoColEq(gid1, gid2) => {
                let col1 = self.col_map[&gid1].clone();
                let col2 = self.col_map[&gid2].clone();
                ExprKind::pred_func(
                    PredFuncKind::Equal,
                    vec![ExprKind::Col(col1), ExprKind::Col(col2)],
                )
            }
        }
    }
}

/// InnerSet stores predicates and equal set of columns.
/// The columns source from inner join and filter operator.
///
/// e.g.1. t1 JOIN t2 ON t1.c1 = t2.c2 JOIN t3 ON t1.c1 = t3.c3
/// inner=[{t1.c1, t2.c2, t3.c3}], outer={}
///
/// e.g.2. t1 JOIN t2 ON t1.c1 = t2.c2 JOIN t3 ON t2.c5 = t3.c6
/// inner=[{t1.c1, t2.c2}, {t2.c5, t3.c6}], outer={}
///
/// e.g.3. t1 LEFT JOIN t2 ON t1.c1 = t2.c2 LEFT JOIN t3 ON t1.c1 = t3.c3
/// inner=[], outer={t1.c1 -> t2.c2, t1.c1 -> t3.c3}
///
/// e.g.4. t1 LEFT JOIN t2 ON t1.c1 = t2.c2 LEFT JOIN t3 ON t2.c2 = t3.c3
/// inner=[], outer={t1.c1 -> t2.c2, t2.c2 -> t3.c3}
///
/// e.g.5. t1 LEFT JOIN t2 ON t1.c1 = t2.c2 JOIN t3 ON t2.c2 = t3.c3
/// inner=[{t2.c2, t3.c3}], outer={t1.c1 -> t2.c2}
/// This case actually will not happen, because we run outerjoin reduce
/// rule before the predicate movearound. the first LEFT join will be
/// converted to INNER join as there is a null-rejected predicate on
/// join column of right table.
///
/// e.g.6. t1 JOIN t2 ON t1.c1 = t2.c2 LEFT JOIN t3 ON t2.c2 = t3.c3
/// inner=[{t1.c1, t2.c2}], outer={t2.c2 -> t3.c3, t1.c1 -> t3.c3}
#[derive(Debug, Default, Clone)]
pub struct InnerSet {
    /// equal set of inner join
    pub(super) eq_sets: Vec<HashSet<GlobalID>>,
    /// predicates other than equal columns.
    /// if node is join, it stores join conditions,
    /// otherwise filter expressions.
    pub(super) filt: Vec<PredExpr>,
    /// Functional dependency
    ///
    /// The meaning is one or more source columns can determine a target column value,
    /// we say the target column has functional dependency of source columns.
    ///
    /// There are two kinds of functional dependencies:
    /// 1. from projection.
    ///    e.g. SELECT c1, c1+1 as c2 FROM t1
    ///    Here c2's value is determined by c1. so there is one dependency c2 -> c1+1
    ///    Such dependency is not important, because if we have a predicate on top of the tree,
    ///    when we push it down through query blocks, it will be converted to suitable one.
    ///    If we have a predicate at leaf of the tree, we can pull multiple derived predicates
    ///    due to the functional dependency.
    ///    e.g. SELECT c1, c1+1 as c2 FROM t1 WHERE c1+1 > 0
    ///    There are two possible predicates: [c1+1>0, c2>0]. Once we push them down to leaf,
    ///    the duplicates can be eliminated and only one remains: c1+1>0.
    ///
    /// 2. from table constraints, such as primary key, unique key.
    ///    e.g. Table T has columns c1, c2, c3. c1 is primary key.
    ///    So values of c2, c3 are all determined by c1.
    ///    If there is a predicate c2 > 0, we can generate one dependency c2 -> col(T, 1, c1).
    ///
    /// Here is one example of predicate move-around by using functional dependency.
    ///
    /// select * from (select c1, c1+1 as c2 from t1) t1, t2 where t1.c1 = t2.c2 where t1.c2 > 0
    ///
    /// Once predicate are all pulled at root node, there are:
    /// 1. equal set: {t1.c1, t2.c2}
    /// 2. filter: [t1.c2 > 0]
    /// 3. functional dependency: t1.c2 -> t1.c1+1
    ///
    /// Now we can first derive t1.c1+1>0, from #2, #3.
    /// Then we can derive t2.c2+1>0 by using #1.
    pub(super) fnl_deps: HashMap<GlobalID, Vec<ExprKind>>,
}

impl InnerSet {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.eq_sets.is_empty() && self.filt.is_empty() && self.fnl_deps.is_empty()
    }

    /// Retain the differences from other inner set.
    /// This is used when the filters cannot be pushed down to child operator.
    #[inline]
    pub fn retain_diff(&mut self, other: &InnerSet) {
        // retain different eq set.
        let mut intersection: Vec<GlobalID> = vec![];
        for eq_set in &mut self.eq_sets {
            if eq_set.len() > 1 {
                // skip if eq set is not meaningless.
                for other_set in &other.eq_sets {
                    intersection.clear();
                    intersection.extend(eq_set.intersection(other_set));
                    if intersection.len() > 1 {
                        for gid in &intersection[1..] {
                            eq_set.remove(gid);
                        }
                    }
                }
            }
        }
        self.eq_sets.retain(|eq_set| eq_set.len() > 1);
        // retain different filter expression.
        self.filt.retain(|e| !other.filt.contains(e));
    }

    #[inline]
    pub fn merge(&mut self, new: InnerSet) {
        // merge inner equal sets
        for s in new.eq_sets {
            self.handle_eq_set(s);
        }
        // merge predicates
        for p in new.filt {
            self.handle_filt(p);
        }
        for (gid, deps) in new.fnl_deps {
            for dep in deps {
                self.handle_dep(gid, dep);
            }
        }
    }

    #[inline]
    pub fn handle_eq_set(&mut self, eq_set: HashSet<GlobalID>) {
        merge_eq_sets(&mut self.eq_sets, eq_set);
    }

    /// handle filter expressions.
    #[inline]
    pub fn handle_filt(&mut self, pred: PredExpr) {
        match &pred {
            PredExpr::OneCol(..) | PredExpr::Other(_) | PredExpr::ColEqConst(..) => {
                for p in &self.filt {
                    if p == &pred {
                        return;
                    }
                }
                self.filt.push(pred);
            }
            PredExpr::TwoColEq(gid1, gid2) => {
                let mut eq_set = HashSet::new();
                eq_set.insert(*gid1);
                eq_set.insert(*gid2);
                merge_eq_sets(&mut self.eq_sets, eq_set);
            }
        }
    }

    #[inline]
    pub fn handle_dep(&mut self, tgt_gid: GlobalID, expr: ExprKind) {
        if let ExprKind::Const(c) = expr {
            self.handle_filt(PredExpr::ColEqConst(tgt_gid, c));
            return;
        }
        self.fnl_deps.entry(tgt_gid).or_default().push(expr);
    }

    #[inline]
    pub fn total_fnl_deps(&self) -> usize {
        self.fnl_deps.values().map(|v| v.len()).sum()
    }
}

/// Outer set stores unidirectional equal set of columns and join
/// conditions for outer(left/full) join.
#[derive(Debug, Default)]
pub struct OuterSet {
    /// unidirectional equal column mapping of outer(left) join
    eq_map: HashMap<GlobalID, HashSet<GlobalID>>,
    /// join conditions
    cond: Vec<PredExpr>,
}

impl OuterSet {
    /// handle outer join condition
    #[inline]
    pub fn handle_cond(
        &mut self,
        cond: PredExpr,
        side: &JoinSide,
        col_map: &HashMap<GlobalID, Col>,
    ) {
        match &cond {
            PredExpr::OneCol(..) | PredExpr::Other(_) | PredExpr::ColEqConst(..) => {
                self.cond.push(cond);
            }
            PredExpr::TwoColEq(gid1, gid2) => {
                match side {
                    // full join
                    JoinSide::Full => {
                        self.cond.push(cond);
                    }
                    // left join
                    JoinSide::Right(_) => match (
                        side.left(col_map[gid1].kind.qry_id().unwrap()),
                        side.left(col_map[gid2].kind.qry_id().unwrap()),
                    ) {
                        (true, true) => {
                            // both left side
                            self.cond.push(cond);
                        }
                        (false, false) => {
                            // both right side, let predicate pushdown to push to right child.
                            self.cond.push(cond);
                        }
                        (true, false) => {
                            // gid1 from left side, gid2 from right side
                            self.eq_map.entry(*gid1).or_default().insert(*gid2);
                        }
                        (false, true) => {
                            // gid2 from left side, gid1 from right side
                            self.eq_map.entry(*gid2).or_default().insert(*gid1);
                        }
                    },
                }
            }
        }
    }
}

#[inline]
fn merge_eq_sets(dst: &mut Vec<HashSet<GlobalID>>, new: HashSet<GlobalID>) {
    let mut match_indexes: SmallVec<[_; 2]> = smallvec![];
    for (i, d_set) in dst.iter().enumerate() {
        if !d_set.is_disjoint(&new) {
            match_indexes.push(i);
        }
    }
    if match_indexes.is_empty() {
        dst.push(new);
    } else {
        for s in match_indexes[1..].iter().rev() {
            let set2 = dst.remove(*s);
            let set1 = &mut dst[match_indexes[0]];
            for gid in set2 {
                set1.insert(gid);
            }
        }
        let set1 = &mut dst[match_indexes[0]];
        for gid in new {
            set1.insert(gid);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredExpr {
    // predicate involves one column, e.g. c1 > 0, c1 = 1, c1 like 'abc'
    OneCol(GlobalID, ExprKind),
    // column equal const
    ColEqConst(GlobalID, Const),
    // two columns equal, e.g. c1 = c2.
    TwoColEq(GlobalID, GlobalID),
    Other(ExprKind),
}

impl PredExpr {
    #[inline]
    pub(super) fn include_col(&self, gid: GlobalID) -> bool {
        match self {
            PredExpr::OneCol(g, ..) | PredExpr::ColEqConst(g, _) => *g == gid,
            PredExpr::TwoColEq(g1, g2) => *g1 == gid || *g2 == gid,
            PredExpr::Other(e) => include_col(e, gid),
        }
    }

    #[inline]
    pub(super) fn find_fnl_dep(&self) -> HashSet<FnlDep> {
        match self {
            PredExpr::ColEqConst(..) | PredExpr::TwoColEq(..) => HashSet::new(),
            PredExpr::OneCol(_, e) | PredExpr::Other(e) => {
                let mut fd = FindFnlDep(HashSet::new());
                let _ = e.walk(&mut fd);
                fd.0
            }
        }
    }

    #[inline]
    pub(super) fn replace_col_with_expr(
        &self,
        gid: GlobalID,
        expr: &ExprKind,
        col_map: &HashMap<GlobalID, Col>,
    ) -> Self {
        match self {
            PredExpr::OneCol(g, e) => {
                if *g == gid {
                    let mut new_e = e.clone();
                    update_col_inplace(&mut new_e, gid, expr);
                    return new_e.into();
                }
                self.clone()
            }
            PredExpr::ColEqConst(g, cst) => {
                if *g == gid {
                    let new_e = ExprKind::pred_func(
                        PredFuncKind::Equal,
                        vec![expr.clone(), ExprKind::Const(cst.clone())],
                    );
                    return new_e.into();
                }
                self.clone()
            }
            PredExpr::TwoColEq(gid1, gid2) => {
                if *gid1 == gid {
                    let c2 = col_map[gid2].clone();
                    let new_e = ExprKind::pred_func(
                        PredFuncKind::Equal,
                        vec![ExprKind::Col(c2), expr.clone()],
                    );
                    return new_e.into();
                }
                if *gid2 == gid {
                    let c1 = col_map[gid1].clone();
                    let new_e = ExprKind::pred_func(
                        PredFuncKind::Equal,
                        vec![ExprKind::Col(c1), expr.clone()],
                    );
                    return new_e.into();
                }
                self.clone()
            }
            PredExpr::Other(e) => {
                let mut new_e = e.clone();
                update_col_inplace(&mut new_e, gid, expr);
                new_e.into()
            }
        }
    }

    /// replace one column with given expression.
    /// If the expression contains more columns, do nothing
    #[inline]
    pub(super) fn replace_cols(&self, col_map: &HashMap<GlobalID, Col>) -> Option<Self> {
        match self {
            PredExpr::OneCol(gid, e) => {
                if let Some(col) = col_map.get(gid) {
                    let mut new_e = e.clone();
                    update_cols_inplace(&mut new_e, col_map);
                    Some(PredExpr::OneCol(col.gid, new_e))
                } else {
                    None
                }
            }
            PredExpr::ColEqConst(gid, cst) => col_map
                .get(gid)
                .map(|c| PredExpr::ColEqConst(c.gid, cst.clone())),
            PredExpr::TwoColEq(gid1, gid2) => {
                if let Some(c1) = col_map.get(gid1)
                    && let Some(c2) = col_map.get(gid2) {
                        return Some(PredExpr::TwoColEq(c1.gid, c2.gid));
                    }
                None
            }
            PredExpr::Other(e) => {
                let mut new_e = e.clone();
                let mut rc = ReplaceCols(col_map);
                if new_e.walk_mut(&mut rc).is_continue() {
                    return Some(PredExpr::Other(new_e));
                }
                None
            }
        }
    }

    /// replace expressions with columns
    #[inline]
    pub(super) fn replace_exprs_with_cols(
        &self,
        e2c: &HashMap<ExprKind, Col>,
        c2c: &HashMap<GlobalID, Col>,
    ) -> Option<Self> {
        match self {
            PredExpr::OneCol(_, e) | PredExpr::Other(e) => {
                let mut new_e = e.clone();
                let mut ro = RewriteExprOut {
                    e2c,
                    c2c,
                    expr_replaced: false,
                };
                if new_e.walk_mut(&mut ro).is_break() {
                    return None;
                }
                Some(new_e.into())
            }
            PredExpr::ColEqConst(gid, cst) => {
                if let Some(c) = c2c.get(gid) {
                    let new_e = ExprKind::pred_func(
                        PredFuncKind::Equal,
                        vec![ExprKind::Col(c.clone()), ExprKind::Const(cst.clone())],
                    );
                    return Some(new_e.into());
                }
                None
            }
            PredExpr::TwoColEq(gid1, gid2) => {
                if let Some(c1) = c2c.get(gid1)
                    && let Some(c2) = c2c.get(gid2) {
                        let new_e = ExprKind::pred_func(
                            PredFuncKind::Equal,
                            vec![ExprKind::Col(c1.clone()), ExprKind::Col(c2.clone())],
                        );
                        return Some(new_e.into());
                    }
                None
            }
        }
    }

    #[inline]
    pub(super) fn replace_cols_with_exprs(
        &self,
        c2e: &HashMap<GlobalID, ExprKind>,
    ) -> Option<Self> {
        match self {
            PredExpr::OneCol(_, e) | PredExpr::Other(e) => {
                let mut new_e = e.clone();
                let mut ri = RewriteExprIn(c2e);
                if new_e.walk_mut(&mut ri).is_break() {
                    return None;
                }
                Some(new_e.into())
            }
            PredExpr::ColEqConst(gid, cst) => {
                if let Some(e) = c2e.get(gid) {
                    let new_e = ExprKind::pred_func(
                        PredFuncKind::Equal,
                        vec![e.clone(), ExprKind::Const(cst.clone())],
                    );
                    return Some(new_e.into());
                }
                None
            }
            PredExpr::TwoColEq(gid1, gid2) => {
                if let Some(e1) = c2e.get(gid1)
                    && let Some(e2) = c2e.get(gid2) {
                        let new_e =
                            ExprKind::pred_func(PredFuncKind::Equal, vec![e1.clone(), e2.clone()]);
                        return Some(new_e.into());
                    }
                None
            }
        }
    }

    /// check if all columns included in given set.
    #[inline]
    pub(super) fn all_cols_included(&self, col_set: &HashSet<GlobalID>) -> bool {
        match self {
            PredExpr::OneCol(gid, _) => col_set.contains(gid),
            PredExpr::TwoColEq(gid1, gid2) => col_set.contains(gid1) && col_set.contains(gid2),
            PredExpr::ColEqConst(gid, _) => col_set.contains(gid),
            PredExpr::Other(e) => {
                let mut aci = AllColsIncluded(col_set);
                e.walk(&mut aci).is_continue()
            }
        }
    }

    /// convert predicate to expression back
    #[inline]
    pub(super) fn into_expr(self, col_map: &HashMap<GlobalID, Col>) -> Option<ExprKind> {
        match self {
            PredExpr::OneCol(gid, e) => {
                if col_map.contains_key(&gid) {
                    let mut new_e = e.clone();
                    update_cols_inplace(&mut new_e, col_map);
                    Some(new_e)
                } else {
                    None
                }
            }
            PredExpr::ColEqConst(gid, cst) => col_map.get(&gid).map(|c| {
                ExprKind::pred_func(
                    PredFuncKind::Equal,
                    vec![ExprKind::Col(c.clone()), ExprKind::Const(cst)],
                )
            }),
            PredExpr::TwoColEq(gid1, gid2) => {
                if let Some(c1) = col_map.get(&gid1)
                    && let Some(c2) = col_map.get(&gid2) {
                        return Some(ExprKind::pred_func(
                            PredFuncKind::Equal,
                            vec![ExprKind::Col(c1.clone()), ExprKind::Col(c2.clone())],
                        ));
                    }
                None
            }
            PredExpr::Other(e) => {
                let mut new_e = e.clone();
                let mut rc = ReplaceCols(col_map);
                if new_e.walk_mut(&mut rc).is_continue() {
                    return Some(new_e);
                }
                None
            }
        }
    }

    /// Returns whether the predicate expression contains aggregation functions.
    #[inline]
    pub fn contains_aggr_func(&self) -> bool {
        match self {
            PredExpr::TwoColEq(..) | PredExpr::ColEqConst(..) => false,
            PredExpr::OneCol(_, e) | PredExpr::Other(e) => e.contains_aggr_func(),
        }
    }
}

impl From<ExprKind> for PredExpr {
    /// Classify an expression and convert to predicate expression,
    /// All used column info must be stored in the column mapping before this call.
    /// This is guaranteed by preorder processing.
    #[inline]
    fn from(expr: ExprKind) -> PredExpr {
        if let ExprKind::Pred(Pred::Func {
            kind: PredFuncKind::Equal,
            args,
        }) = &expr
        {
            match args.as_slice() {
                [ExprKind::Col(Col { gid: gid1, .. }), ExprKind::Col(Col { gid: gid2, .. })] => {
                    if gid1 != gid2 {
                        return PredExpr::TwoColEq(*gid1, *gid2);
                    }
                }
                [ExprKind::Col(Col { gid, .. }), ExprKind::Const(c)]
                | [ExprKind::Const(c), ExprKind::Col(Col { gid, .. })] => {
                    return PredExpr::ColEqConst(*gid, c.clone())
                }
                _ => (),
            }
        }
        let mut csc = CheckSinglePlainCol(None);
        if expr.walk(&mut csc).is_break() || csc.0.is_none() {
            PredExpr::Other(expr)
        } else {
            PredExpr::OneCol(csc.0.unwrap(), expr)
        }
    }
}
pub enum JoinSide {
    Right(HashSet<QueryID>), // only for left join
    Full,                    // only for full join
}

impl JoinSide {
    #[inline]
    fn left(&self, qid: QueryID) -> bool {
        if let JoinSide::Right(qids) = self {
            return !qids.contains(&qid);
        }
        false
    }
}

struct PredPullup<'a, C> {
    catalog: &'a C,
    qry_set: &'a mut QuerySet,
    pred_map: &'a mut PredMap,
    // columns of potential functional dependencies
    dep_cols: &'a mut DepCols,
}

impl<C: Catalog> PredPullup<'_, C> {
    #[inline]
    fn collect_dep_cols(&mut self, exprs: &[ExprKind]) {
        let mut cfd = CollectDepCols(self.dep_cols);
        for e in exprs {
            let _ = e.walk(&mut cfd);
        }
    }

    #[inline]
    fn pullup_query(&mut self, op_id: GlobalID, qid: QueryID) -> ControlFlow<Error, ()> {
        pred_pullup(
            self.catalog,
            self.qry_set,
            qid,
            self.pred_map,
            self.dep_cols,
        )
        .branch()?;
        let subq = self.qry_set.get(&qid).unwrap();
        let qry_root_id = subq.root.id;
        let mut inner = self.pred_map.remove_inner(qry_root_id);
        let mut new_inner = InnerSet::default();

        // translate inner set by output mapping.
        let mut col_out = HashMap::new();
        let mut expr_out = HashMap::new();
        for (i, e) in subq.out_cols().iter().enumerate() {
            let qry_col = QryCol(qid, ColIndex::from(i as u32));
            // query column which are not in out map is not used by parent nodes.
            // if column prune is executed, they are already removed.
            if let Some(out_gid) = self.pred_map.qry_col_map.get(&qry_col) {
                let col = Col {
                    gid: *out_gid,
                    kind: ColKind::Query(qry_col.0),
                    idx: qry_col.1,
                };
                if let ExprKind::Col(Col { gid, .. }) = &e.expr {
                    col_out.insert(*gid, col);
                } else {
                    // any other expressions can be put into expr output map
                    if let Entry::Vacant(vac) = expr_out.entry(e.expr.clone()) {
                        vac.insert(col);
                    }
                }
            }
        }
        // convert constant projections to equal predicates
        for (e, c) in &expr_out {
            if e.is_const() {
                let p = ExprKind::pred_func(
                    PredFuncKind::Equal,
                    vec![ExprKind::Col(c.clone()), e.clone()],
                )
                .into();
                new_inner.handle_filt(p);
            }
        }

        // translate functional dependencies
        // handle old dependencies
        for (tgt_gid, deps) in mem::take(&mut inner.fnl_deps) {
            // check if the target of functional dependency is in output,
            // if not, we try equal set.
            let gid_opt = if let Some(out_col) = col_out.get(&tgt_gid) {
                Some(out_col.gid)
            } else if let Some(eq_set) = inner.eq_sets.iter().find(|s| s.contains(&tgt_gid)) {
                // target not in output but has equal set.
                eq_set
                    .iter()
                    .find_map(|gid| col_out.get(gid))
                    .map(|out_col| out_col.gid)
            } else {
                None
            };
            if let Some(final_gid) = gid_opt {
                // check if src are all in output
                for mut dep in deps {
                    let mut rc = ReplaceCols(&col_out);
                    if dep.walk_mut(&mut rc).is_continue() {
                        // all source columns can be translated to output column
                        new_inner.handle_dep(final_gid, dep);
                    }
                }
            } else {
                // the target of dependency is not exported, try to convert predicates that is involved.
                // e.g. SELECT c0 FROM t2 WHERE c2 > 0
                // even though c2 is not exported, we can convert and export predicate:
                // TableDepFunc(t2, c2_idx, c0) > 0
                for p in &inner.filt {
                    if p.include_col(tgt_gid) {
                        // try to convert filter according to dependency.
                        for dep in &deps {
                            let new_p =
                                p.replace_col_with_expr(tgt_gid, dep, &self.pred_map.col_map);
                            new_inner.handle_filt(new_p);
                        }
                    }
                }
            }
        }
        // translate equal set
        for eq_set in &mut inner.eq_sets {
            let new_set: HashSet<_> = eq_set
                .iter()
                .filter_map(|k| col_out.get(k).map(|c| c.gid))
                .collect();
            if new_set.len() > 1 {
                // output at least two columns of this equal set, we need to remove new_set.len() - 1
                // columns of original set.
                let mut new_set_iter = new_set.iter();
                for _ in 0..new_set.len() - 1 {
                    if let Some(k) = new_set_iter.next() {
                        eq_set.remove(k);
                    }
                }
                new_inner.eq_sets.push(new_set);
            } // otherwise, keep original equal set as is
        }
        // translate filter
        let mut old_filt = vec![];
        for p in inner.filt {
            if let Some(new_p) = p.replace_exprs_with_cols(&expr_out, &col_out) {
                new_inner.handle_filt(new_p);
                // duplicate the predicate by only use out columns
                if let Some(new_p) = p.replace_cols(&col_out) {
                    new_inner.handle_filt(new_p);
                }
            } else {
                old_filt.push(p);
            }
        }
        inner.filt = old_filt;

        println!(
            "query fnl_deps={:?}, keys={:?}",
            new_inner.total_fnl_deps(),
            Vec::from_iter(new_inner.fnl_deps.keys())
        );
        // update back
        self.pred_map.insert_inner(qry_root_id, inner);
        // update new set
        self.pred_map.insert_inner(op_id, new_inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_cross_join(
        &mut self,
        op_id: GlobalID,
        children: &[JoinOp],
    ) -> ControlFlow<Error, ()> {
        // pull up inner predicates of all children and merge
        let (first, others) = children.split_first().unwrap();
        let mut inner = self.pred_map.remove_inner(first.id);
        for r in others {
            let r_inner = self.pred_map.remove_inner(r.id);
            inner.merge(r_inner);
        }
        // store inner set
        self.pred_map.insert_inner(op_id, inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_inner_join(
        &mut self,
        op_id: GlobalID,
        left_id: GlobalID,
        right_id: GlobalID,
        cond: Vec<ExprKind>,
        filt: Vec<ExprKind>,
    ) -> ControlFlow<Error, ()> {
        // pull up inner predicates from left child
        let mut inner = self.pred_map.remove_inner(left_id);
        // pull up inner predicates from right child
        let r = self.pred_map.remove_inner(right_id);
        // merge right
        inner.merge(r);
        // merge conditions and filters
        for e in cond {
            inner.handle_filt(e.into());
        }
        for e in filt {
            inner.handle_filt(e.into());
        }
        println!(
            "inner fnl_deps={:?}, keys={:?}",
            inner.total_fnl_deps(),
            Vec::from_iter(inner.fnl_deps.keys())
        );
        // store inner set
        self.pred_map.insert_inner(op_id, inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_left_join(
        &mut self,
        op_id: GlobalID,
        left_id: GlobalID,
        right: &JoinOp,
        cond: Vec<ExprKind>,
        filt: Vec<ExprKind>,
    ) -> ControlFlow<Error, ()> {
        // pull up inner predicates of left child,
        // and reject right child
        let mut inner = self.pred_map.remove_inner(left_id);
        // merge filters
        for e in filt {
            inner.handle_filt(e.into());
        }
        // handle outer join conditions
        let mut right_qids = HashSet::new();
        right.collect_qry_ids(&mut right_qids);
        let side = JoinSide::Right(right_qids);
        let mut outer = OuterSet::default();
        for e in cond {
            outer.handle_cond(e.into(), &side, &self.pred_map.col_map);
        }
        // store inner set
        self.pred_map.insert_inner(op_id, inner);
        // store outer set
        self.pred_map.insert_outer(op_id, outer);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_full_join(
        &mut self,
        op_id: GlobalID,
        cond: Vec<ExprKind>,
        filt: Vec<ExprKind>,
    ) -> ControlFlow<Error, ()> {
        // reject either left or right child
        if !filt.is_empty() {
            let mut inner = InnerSet::default();
            for e in filt {
                inner.handle_filt(e.into());
            }
            // store inner set
            self.pred_map.insert_inner(op_id, inner);
        }
        let mut outer = OuterSet::default();
        for e in cond {
            outer.handle_cond(e.into(), &JoinSide::Full, &self.pred_map.col_map);
        }
        // store outer set
        self.pred_map.insert_outer(op_id, outer);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_filt(
        &mut self,
        op_id: GlobalID,
        input_id: GlobalID,
        pred: Vec<ExprKind>,
    ) -> ControlFlow<Error, ()> {
        // pull up all preds and equal sets of child.
        let mut inner = self.pred_map.remove_inner(input_id);
        for e in pred {
            inner.handle_filt(e.into());
        }
        println!(
            "filter fnl_deps={:?}, keys={:?}",
            inner.total_fnl_deps(),
            Vec::from_iter(inner.fnl_deps.keys())
        );
        self.pred_map.insert_inner(op_id, inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_union(
        &mut self,
        op_id: GlobalID,
        left_id: GlobalID,
        right_id: GlobalID,
        cols: &[ProjCol],
    ) -> ControlFlow<Error, ()> {
        let mut left_inner = self.pred_map.remove_inner(left_id);
        let mut right_inner = self.pred_map.remove_inner(right_id);

        let mut new_inner = InnerSet::default();
        // intersect equal sets of both side
        let (l_map, r_map) = setop_out_map(cols);
        for l_eq_set in &mut left_inner.eq_sets {
            let l_out: HashSet<_> = l_eq_set.iter().map(|gid| l_map[gid].gid).collect();
            for r_eq_set in &right_inner.eq_sets {
                let r_out: HashSet<_> = r_eq_set.iter().map(|gid| r_map[gid].gid).collect();
                let new_set: HashSet<GlobalID> = l_out.intersection(&r_out).cloned().collect();
                if new_set.len() > 1 {
                    new_inner.eq_sets.push(new_set);
                } // otherwise, no change
            }
        }
        // intersect filter expressions, current only support exactly the same expression.
        // old filt of left child
        let mut old_l_filt = vec![];
        // old filt of right child
        let mut old_r_filt = vec![];
        let mut l_out_filt: Vec<_> = left_inner
            .filt
            .iter()
            .map(|p| {
                let new_p = p.replace_cols(&l_map).unwrap();
                (new_p, false)
            })
            .collect();
        let mut r_out_filt: Vec<_> = right_inner
            .filt
            .iter()
            .map(|p| {
                let new_p = p.replace_cols(&r_map).unwrap();
                (new_p, false)
            })
            .collect();
        for (l_out, l_flag) in &mut l_out_filt {
            for (r_out, r_flag) in &mut r_out_filt {
                if l_out == r_out {
                    *l_flag = true;
                    *r_flag = true;
                }
            }
        }
        for (old, (new, pullable)) in left_inner.filt.into_iter().zip(l_out_filt) {
            if pullable {
                new_inner.handle_filt(new);
            } else {
                old_l_filt.push(old);
            }
        }
        left_inner.filt = old_l_filt;
        for (old, (_, pullable)) in right_inner.filt.into_iter().zip(r_out_filt) {
            if !pullable {
                old_r_filt.push(old);
            }
        }
        right_inner.filt = old_r_filt;
        // update right back
        self.pred_map.insert_inner(right_id, right_inner);
        // update left back
        self.pred_map.insert_inner(left_id, left_inner);
        // update new set
        self.pred_map.insert_inner(op_id, new_inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_intersect(
        &mut self,
        op_id: GlobalID,
        left_id: GlobalID,
        right_id: GlobalID,
        cols: &[ProjCol],
    ) -> ControlFlow<Error, ()> {
        let mut left_inner = self.pred_map.remove_inner(left_id);
        let mut right_inner = self.pred_map.remove_inner(right_id);
        // union equal sets of both side
        let (l_map, r_map) = setop_out_map(cols);
        let mut new_inner = InnerSet::default();
        for eq_set in &left_inner.eq_sets {
            let new_set = eq_set.iter().map(|gid| l_map[gid].gid).collect();
            new_inner.handle_eq_set(new_set);
        }
        for eq_set in &right_inner.eq_sets {
            let new_set = eq_set.iter().map(|gid| r_map[gid].gid).collect();
            new_inner.handle_eq_set(new_set);
        }
        // translate and union filt of both side
        for p in mem::take(&mut left_inner.filt) {
            let new_p = p.replace_cols(&l_map).unwrap();
            new_inner.handle_filt(new_p);
        }
        for p in mem::take(&mut right_inner.filt) {
            let new_p = p.replace_cols(&r_map).unwrap();
            new_inner.handle_filt(new_p);
        }
        // translate functional dependency
        if !left_inner.fnl_deps.is_empty() {
            let fnl_deps = mem::take(&mut left_inner.fnl_deps);
            for (gid, deps) in fnl_deps {
                for mut dep in deps {
                    let new_gid = l_map[&gid].gid;
                    update_cols_inplace(&mut dep, &l_map);
                    new_inner.handle_dep(new_gid, dep);
                }
            }
        }
        if !right_inner.fnl_deps.is_empty() {
            let fnl_deps = mem::take(&mut right_inner.fnl_deps);
            for (gid, deps) in fnl_deps {
                for mut dep in deps {
                    let new_gid = r_map[&gid].gid;
                    update_cols_inplace(&mut dep, &r_map);
                    new_inner.handle_dep(new_gid, dep);
                }
            }
        }
        // update right back
        self.pred_map.insert_inner(right_id, right_inner);
        // update left back
        self.pred_map.insert_inner(left_id, left_inner);
        // update new set
        self.pred_map.insert_inner(op_id, new_inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_except(
        &mut self,
        op_id: GlobalID,
        left_id: GlobalID,
        cols: &[ProjCol],
    ) -> ControlFlow<Error, ()> {
        let mut inner = self.pred_map.remove_inner(left_id);
        // only left map is required
        let mut l_map = HashMap::with_capacity(cols.len());
        for pc in cols {
            if let ExprKind::Col(
                c @ Col {
                    kind: ColKind::Setop(args),
                    ..
                },
            ) = &pc.expr
            {
                l_map.insert(args[0].col_gid().unwrap(), c.clone());
            }
        }
        // translate equal set
        for eq_set in &mut inner.eq_sets {
            let new_set: HashSet<_> = eq_set.iter().map(|gid| l_map[gid].gid).collect();
            *eq_set = new_set;
        }
        // translate filter
        for p in &mut inner.filt {
            let new_p = p.replace_cols(&l_map).unwrap();
            *p = new_p;
        }
        // translate functional dependency
        if !inner.fnl_deps.is_empty() {
            let fnl_deps = mem::take(&mut inner.fnl_deps);
            for (gid, deps) in fnl_deps {
                for mut dep in deps {
                    let new_gid = l_map[&gid].gid;
                    update_cols_inplace(&mut dep, &l_map);
                    inner.handle_dep(new_gid, dep);
                }
            }
        }
        self.pred_map.insert_inner(op_id, inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_aggr(
        &mut self,
        op_id: GlobalID,
        input_id: GlobalID,
        groups: &[ExprKind],
        proj: &[ProjCol],
        filt: Vec<ExprKind>,
    ) -> ControlFlow<Error, ()> {
        let mut inner = self.pred_map.remove_inner(input_id);
        let group_out_cols = collect_group_out_cols(groups, proj);
        let mut new_inner = InnerSet::default();
        // collect equal set
        for eq_set in &mut inner.eq_sets {
            let new_set: HashSet<GlobalID> = eq_set
                .iter()
                .filter(|k| group_out_cols.contains(*k))
                .cloned()
                .collect();
            if new_set.len() > 1 {
                // output at least two columns of this equal set, we need to remove new_set.len() - 1
                // columns of original set.
                let mut new_set_iter = new_set.iter();
                for _ in 0..new_set.len() - 1 {
                    if let Some(k) = new_set_iter.next() {
                        eq_set.remove(k);
                    }
                }
                new_inner.handle_eq_set(new_set);
            } // otherwise, keep original equal set as is
        }
        // collect filter
        let mut old_filt = vec![];
        for p in inner.filt {
            if p.all_cols_included(&group_out_cols) {
                new_inner.handle_filt(p);
            } else {
                old_filt.push(p);
            }
        }
        inner.filt = old_filt;
        // update back
        self.pred_map.insert_inner(input_id, inner);
        // aggr may have filters
        for e in filt {
            new_inner.handle_filt(e.into());
        }
        // update new set
        self.pred_map.insert_inner(op_id, new_inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn collect_tbl_fnl_deps(&mut self, inner: &mut InnerSet, table_id: TableID, cols: &[ProjCol]) {
        // note: table may be referred multiple times in one query,
        // so we remove the table from dep_tbl_cols map when collecting its dependencies.
        if let Some(col_indexes) = self.dep_cols.tbl.remove(&table_id) {
            let keys = self.catalog.find_keys(table_id);
            if keys.is_empty() {
                return; // no key in current table
            }
            let exported_cols: HashMap<ColIndex, Col> = cols
                .iter()
                .map(|c| {
                    if let ExprKind::Col(c @ Col { idx, .. }) = &c.expr {
                        (*idx, c.clone())
                    } else {
                        unreachable!()
                    }
                })
                .collect();

            let keys: Vec<_> = keys
                .iter()
                .filter_map(|k| match k {
                    Key::PrimaryKey(cols) | Key::UniqueKey(cols) => {
                        if cols.iter().any(|c| !exported_cols.contains_key(&c.idx)) {
                            None
                        } else {
                            let mut idx_set = HashSet::new();
                            let mut exprs = vec![];
                            let mut tbl_cols = vec![];
                            for c in cols {
                                idx_set.insert(c.idx);
                                exprs.push(ExprKind::Col(exported_cols[&c.idx].clone()));
                                tbl_cols.push(TblCol(c.table_id, c.idx));
                            }
                            Some((idx_set, exprs, tbl_cols))
                        }
                    }
                })
                .collect();
            if keys.is_empty() {
                return; // no key exported
            }
            // inject functional dependencies to scan node.
            for col_idx in col_indexes {
                let gid = exported_cols[&col_idx].gid;
                for (k_idx, k_exprs, k_cols) in &keys {
                    if k_idx.contains(&col_idx) {
                        continue; // skip if this column is part of key
                    }
                    let dep = ExprKind::tbl_fnl_dep(table_id, col_idx, k_exprs.clone());
                    inner.handle_dep(gid, dep);
                    let tbl_col = TblCol(table_id, col_idx);
                    self.pred_map
                        .tbl_dep_map
                        .entry(tbl_col)
                        .or_default()
                        .push(k_cols.clone());
                }
            }
        }
    }

    #[inline]
    fn pullup_scan(
        &mut self,
        op_id: GlobalID,
        table_id: TableID,
        cols: &[ProjCol],
        filt: Vec<ExprKind>,
    ) -> ControlFlow<Error, ()> {
        let mut inner = InnerSet::default();
        // handle filter expressions
        for e in filt {
            inner.handle_filt(e.into());
        }
        // collect dependencies in table.
        self.collect_tbl_fnl_deps(&mut inner, table_id, cols);
        println!(
            "tbl fnl_deps={:?}, keys={:?}",
            inner.total_fnl_deps(),
            Vec::from_iter(inner.fnl_deps.keys())
        );
        self.pred_map.insert_inner(op_id, inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_limit(&mut self, op_id: GlobalID, input_id: GlobalID) -> ControlFlow<Error, ()> {
        // we need to copy inner set because the predicate pushdown can not cross limit.
        let inner = self.pred_map.remove_inner(input_id);
        self.pred_map.insert_inner(op_id, inner.clone());
        self.pred_map.insert_inner(input_id, inner);
        ControlFlow::Continue(())
    }

    #[inline]
    fn pullup_base(&mut self, op_id: GlobalID, input_id: GlobalID) -> ControlFlow<Error, ()> {
        let inner = self.pred_map.remove_inner(input_id);
        self.pred_map.insert_inner(op_id, inner);
        ControlFlow::Continue(())
    }
}

impl<C: Catalog> OpMutVisitor for PredPullup<'_, C> {
    type Cont = ();
    type Break = Error;

    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error, ()> {
        // collect column mapping
        for e in op.kind.exprs() {
            let mut cm = CollectColMapping {
                qry_col_map: &mut self.pred_map.qry_col_map,
                tbl_col_map: &mut self.pred_map.tbl_col_map,
                col_map: &mut self.pred_map.col_map,
            };
            let _ = e.walk(&mut cm);
        }
        // collect potential functional dependencies of table
        match &op.kind {
            OpKind::Filt { pred, .. } => self.collect_dep_cols(pred),
            OpKind::Join(join) => {
                if let Join::Qualified(QualifiedJoin { cond, filt, .. }) = &**join {
                    self.collect_dep_cols(cond);
                    self.collect_dep_cols(filt);
                }
            }
            OpKind::Aggr(aggr) => self.collect_dep_cols(&aggr.filt),
            OpKind::Query(qid) => {
                if let Some(subq) = self.qry_set.get(qid) {
                    let out_cols = subq.out_cols();
                    if let Some(indexes) = self.dep_cols.qry.get(qid).cloned() {
                        for idx in indexes {
                            let e = &out_cols[idx.value() as usize].expr;
                            let mut cfd = CollectDepCols(self.dep_cols);
                            let _ = e.walk(&mut cfd);
                        }
                    }
                }
            }
            _ => (),
        }
        ControlFlow::Continue(())
    }

    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error, ()> {
        match &mut op.kind {
            OpKind::Query(qid) => self.pullup_query(op.id, *qid),
            OpKind::Join(join) => match join.as_mut() {
                Join::Cross(children) => self.pullup_cross_join(op.id, children),
                Join::Qualified(QualifiedJoin {
                    kind,
                    left,
                    right,
                    cond,
                    filt,
                }) => match kind {
                    JoinKind::Inner => self.pullup_inner_join(
                        op.id,
                        left.id,
                        right.id,
                        mem::take(cond),
                        mem::take(filt),
                    ),
                    JoinKind::Left => self.pullup_left_join(
                        op.id,
                        left.id,
                        right,
                        mem::take(cond),
                        mem::take(filt),
                    ),
                    JoinKind::Full => {
                        self.pullup_full_join(op.id, mem::take(cond), mem::take(filt))
                    }
                    _ => todo!("unexpected join type: {:?}", kind),
                },
            },
            OpKind::Filt { pred, input } => self.pullup_filt(op.id, input.id, mem::take(pred)),
            // Scan is the only operator in a query with location equals to disk.
            // The original plan won't have predicates on scan.
            OpKind::Scan(scan) => {
                self.pullup_scan(op.id, scan.table_id, &scan.cols, mem::take(&mut scan.filt))
            }
            OpKind::Row(_) => ControlFlow::Continue(()),
            OpKind::Setop(setop) => match setop.kind {
                SetopKind::Union => {
                    self.pullup_union(op.id, setop.left.id, setop.right.id, &setop.cols)
                }
                SetopKind::Intersect => {
                    self.pullup_intersect(op.id, setop.left.id, setop.right.id, &setop.cols)
                }
                SetopKind::Except => self.pullup_except(op.id, setop.left.id, &setop.cols),
            },
            OpKind::Aggr(aggr) => self.pullup_aggr(
                op.id,
                aggr.input.id,
                &aggr.groups,
                &aggr.proj,
                mem::take(&mut aggr.filt),
            ),
            OpKind::Proj { input, .. }
            | OpKind::Sort {
                input, limit: None, ..
            } => self.pullup_base(op.id, input.id),
            OpKind::Limit { input, .. }
            | OpKind::Sort {
                input,
                limit: Some(_),
                ..
            } => self.pullup_limit(op.id, input.id),
            OpKind::JoinGraph(_) => unreachable!(),
            OpKind::Attach(..) | OpKind::Empty => todo!(),
        }
    }
}

struct ReplaceCols<'a>(&'a HashMap<GlobalID, Col>);

impl ExprMutVisitor for ReplaceCols<'_> {
    type Cont = ();
    type Break = ();

    #[inline]
    fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<(), ()> {
        match e {
            ExprKind::Col(c @ Col { .. }) => {
                if let Some(new_col) = self.0.get(&c.gid) {
                    *c = new_col.clone();
                } else {
                    // cannot find column to replace
                    return ControlFlow::Break(());
                }
            }
            ExprKind::Aggf { .. } => return ControlFlow::Break(()),
            _ => (),
        }
        ControlFlow::Continue(())
    }
}

#[inline]
fn update_cols_inplace(e: &mut ExprKind, col_out: &HashMap<GlobalID, Col>) {
    let mut rc = ReplaceCols(col_out);
    assert!(e.walk_mut(&mut rc).is_continue());
}

struct ReplaceCol<'a>(GlobalID, &'a ExprKind);

impl ExprMutVisitor for ReplaceCol<'_> {
    type Cont = ();
    type Break = ();

    #[inline]
    fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<(), ()> {
        if let ExprKind::Col(Col { gid, .. }) = e
            && *gid == self.0 {
                *e = self.1.clone();
            }
        ControlFlow::Continue(())
    }
}

#[inline]
fn update_col_inplace(e: &mut ExprKind, gid: GlobalID, expr: &ExprKind) {
    let mut rc: ReplaceCol<'_> = ReplaceCol(gid, expr);
    let _ = e.walk_mut(&mut rc);
}

struct IncludeCol(GlobalID);

impl ExprVisitor<'_> for IncludeCol {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<(), ()> {
        if let ExprKind::Col(Col { gid, .. }) = e
            && *gid == self.0 {
                return ControlFlow::Break(());
            }
        ControlFlow::Continue(())
    }
}

#[inline]
fn include_col(e: &ExprKind, gid: GlobalID) -> bool {
    let mut ic = IncludeCol(gid);
    e.walk(&mut ic).is_break()
}

// Rewrite predicate expressions to query output
// e.g.1. SELECT * FROM (SELECT c1+1 as c2 FROM t1 WHERE c1+1 > 0) t
// the top predicate should be c2 > 0.
// e.g.2. SELECT * FROM (SELECT c1 FROM t1 WHERE c1+1 > 0) t
// the top predicate should be c1+1 > 0.
struct RewriteExprOut<'a> {
    e2c: &'a HashMap<ExprKind, Col>,
    c2c: &'a HashMap<GlobalID, Col>,
    expr_replaced: bool,
}

impl ExprMutVisitor for RewriteExprOut<'_> {
    type Cont = ();
    type Break = ();

    #[inline]
    fn enter(&mut self, e: &mut ExprKind) -> ControlFlow<(), ()> {
        if let Some(c) = self.e2c.get(e) {
            *e = ExprKind::Col(c.clone());
            self.expr_replaced = true;
        }
        ControlFlow::Continue(())
    }

    #[inline]
    fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<(), ()> {
        if self.expr_replaced {
            // just replaced with column, do not check it
            self.expr_replaced = false;
            return ControlFlow::Continue(());
        }
        match e {
            ExprKind::Col(c @ Col { .. }) => {
                if let Some(new_col) = self.c2c.get(&c.gid) {
                    *c = new_col.clone();
                } else {
                    // cannot find column to replace
                    return ControlFlow::Break(());
                }
            }
            // predicates contain aggregation function and cannot
            // be translated by expression output, fail.
            ExprKind::Aggf { .. } => return ControlFlow::Break(()),
            _ => (),
        }
        ControlFlow::Continue(())
    }
}

/// Rewrite predicate expressions to query input
pub(super) struct RewriteExprIn<'a>(pub(super) &'a HashMap<GlobalID, ExprKind>);

impl ExprMutVisitor for RewriteExprIn<'_> {
    type Cont = ();
    type Break = ();

    #[inline]
    fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<(), ()> {
        if let ExprKind::Col(Col { gid, .. }) = e
            && let Some(new_e) = self.0.get(gid) {
                *e = new_e.clone();
            }
        ControlFlow::Continue(())
    }
}

struct AllColsIncluded<'a>(&'a HashSet<GlobalID>);

impl<'a> ExprVisitor<'a> for AllColsIncluded<'a> {
    type Cont = ();
    type Break = ();

    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<(), ()> {
        if let ExprKind::Col(Col { gid, .. }) = e
            && !self.0.contains(gid) {
                return ControlFlow::Break(());
            }
        ControlFlow::Continue(())
    }
}

pub(super) struct CollectColMapping<'a> {
    pub(super) qry_col_map: &'a mut HashMap<QryCol, GlobalID>,
    pub(super) tbl_col_map: &'a mut HashMap<TblCol, HashSet<GlobalID>>,
    pub(super) col_map: &'a mut HashMap<GlobalID, Col>,
}

impl<'a> ExprVisitor<'a> for CollectColMapping<'a> {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<(), ()> {
        if let ExprKind::Col(c @ Col { gid, idx, kind }) = e {
            self.col_map.insert(*gid, c.clone());
            match kind {
                ColKind::Query(qid) | ColKind::Correlated(qid) => {
                    self.qry_col_map.insert(QryCol(*qid, *idx), *gid);
                }
                ColKind::Table(table_id, ..) => {
                    self.tbl_col_map
                        .entry(TblCol(*table_id, *idx))
                        .or_default()
                        .insert(*gid);
                }
                _ => (),
            }
        }
        ControlFlow::Continue(())
    }
}

struct CheckSinglePlainCol(Option<GlobalID>);

impl ExprVisitor<'_> for CheckSinglePlainCol {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<(), ()> {
        match e {
            ExprKind::Col(Col { gid, .. }) => {
                if let Some(g) = &mut self.0 {
                    if g == gid {
                        return ControlFlow::Continue(());
                    }
                    ControlFlow::Break(())
                } else {
                    self.0 = Some(*gid);
                    ControlFlow::Continue(())
                }
            }
            ExprKind::Aggf { .. } => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    }
}

struct CollectDepCols<'a>(&'a mut DepCols);

impl<'a> ExprVisitor<'a> for CollectDepCols<'a> {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<(), ()> {
        if let ExprKind::Col(Col { idx, kind, .. }) = e {
            match kind {
                ColKind::Query(qid) | ColKind::Correlated(qid) => {
                    self.0.qry.entry(*qid).or_default().insert(*idx);
                }
                ColKind::Table(table_id, ..) => {
                    self.0.tbl.entry(*table_id).or_default().insert(*idx);
                }
                _ => (),
            }
        }
        ControlFlow::Continue(())
    }
}

struct ClearFilt<'a>(&'a mut QuerySet);

impl OpMutVisitor for ClearFilt<'_> {
    type Cont = ();
    type Break = Error;
    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error, ()> {
        match &mut op.kind {
            OpKind::Filt { pred, input } => {
                if pred.is_empty() {
                    *op = mem::take(&mut *input);
                }
            }
            OpKind::Query(qid) => {
                clear_filt(self.0, *qid).branch()?;
            }
            _ => (),
        }
        ControlFlow::Continue(())
    }
}

#[inline]
fn collect_group_out_cols(group: &[ExprKind], proj: &[ProjCol]) -> HashSet<GlobalID> {
    let mut in_group = HashSet::new();
    for g in group {
        if let ExprKind::Col(Col { gid, .. }) = g {
            in_group.insert(*gid);
        }
    }
    let mut res = HashSet::new();
    for p in proj {
        if let ExprKind::Col(Col { gid, .. }) = &p.expr
            && in_group.contains(gid) {
                res.insert(*gid);
            }
    }
    res
}

struct FindFnlDep(HashSet<FnlDep>);

impl ExprVisitor<'_> for FindFnlDep {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<(), ()> {
        if let ExprKind::FnlDep(dep) = e {
            self.0.insert(dep.clone());
        }
        ControlFlow::Continue(())
    }
}

#[inline]
fn setop_out_map(cols: &[ProjCol]) -> (HashMap<GlobalID, Col>, HashMap<GlobalID, Col>) {
    let mut l_map = HashMap::with_capacity(cols.len());
    let mut r_map = HashMap::with_capacity(cols.len());
    for pc in cols {
        if let ExprKind::Col(
            c @ Col {
                kind: ColKind::Setop(args),
                ..
            },
        ) = &pc.expr
        {
            l_map.insert(args[0].col_gid().unwrap(), c.clone());
            r_map.insert(args[1].col_gid().unwrap(), c.clone());
        }
    }
    (l_map, r_map)
}

#[derive(Default)]
struct DepCols {
    qry: HashMap<QueryID, HashSet<ColIndex>>,
    tbl: HashMap<TableID, HashSet<ColIndex>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{assert_j_plan1, j_catalog, print_plan};
    use crate::lgc::OpVisitor;
    use doradb_catalog::Catalog;

    #[test]
    fn test_pred_pullup_single_table() {
        let cat = j_catalog();
        assert_inner_set(&cat, "select c1 from t1", |inner| {
            assert!(inner.eq_sets.is_empty());
            assert!(inner.filt.is_empty());
        });
        assert_inner_set(&cat, "select * from (select c1 from t1) x1", |inner| {
            assert!(inner.eq_sets.is_empty());
            assert!(inner.filt.is_empty());
        });
        assert_inner_set(
            &cat,
            "select c1 from (select c1 from t1 where c1 = 0) x1",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert_eq!(inner.filt.len(), 1);
            },
        );
        assert_inner_set(&cat, "select c1 from t1 where t1.c1 = t1.c0", |inner| {
            assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
            assert!(inner.filt.is_empty());
        });
        assert_inner_set(
            &cat,
            "select c1 from t1 where t1.c1 = t1.c0 and t1.c1 = 0",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c1 from t1 where c0 + c1 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c1, c0 from t1 where c0 + c1 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, 1 as c1 from t1) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1); // project a constant is converted to a equal predicate
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, c0+1 as c1 from t1) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(inner.total_fnl_deps() == 0); // ignore projection dependency.
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, c1 from t1 where c1 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
                assert!(inner.total_fnl_deps() == 1); // pk dependency.
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, c1 from t3 where c1 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
                assert!(inner.total_fnl_deps() == 0); // filter contains pk.
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, c2 from t3 where c2 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
                assert!(inner.total_fnl_deps() == 0); // part of pk not exported.
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, c1, c2 from t4 where c2 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
                assert!(inner.total_fnl_deps() == 2); // pk and uk
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0 from t2 where c2 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1); // table dependency enables predicate conversion
                assert!(inner.total_fnl_deps() == 0);
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c1 from t2 where c2 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty()); // no key exported, dependency and predicates is dropped
                assert!(inner.total_fnl_deps() == 0);
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c1+1 as c3 from t2 where c1+1 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1); // expression rewrite
                assert!(inner.total_fnl_deps() == 0);
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c1, c1+1 as c2 from t2 where c1+1 > 0) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 2); // predicates of both c1 and c2 are kept.
                assert!(inner.total_fnl_deps() == 0);
            },
        );
    }

    #[test]
    fn test_pred_pullup_cross_join() {
        let cat = j_catalog();
        assert_inner_set(&cat, "select t1.c1 from t1, t2", |inner| {
            assert!(inner.eq_sets.is_empty());
            assert!(inner.filt.is_empty());
        });
        assert_inner_set(
            &cat,
            "select t1.c1 from t1, t2 where t1.c1 = t2.c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1);
                assert!(inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select t1.c1 from (select c1 from t1 where c1 = 0) t1, t2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select t1.c1 from (select c1 from t1 where c1 = 0) t1, (select c2 from t2 where c2 = 0) t2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c2 from (select c1 from t1 where c1 = 0) t1, (select c2 from t2 where c2 = 0) t2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 2);
            },
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c2 from (select c1 from t1 where c1 = 0) t1, (select c2 from t2 where c2 = 0) t2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 2);
            },
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c2 from (select c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, c1 from t1 where c0 = c1) t1, t2 where t1.c1 = t2.c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, c1 from t1 where c0 = c1) t1, (select c2, c3 from t3 where c2 = c3) t3",
            |inner| {
                assert!(inner.eq_sets.len() == 2 && inner.eq_sets[0].len() == 2 && inner.eq_sets[1].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c0, c1 from t1 where c0 = c1) t1, (select c2, c3 from t3 where c2 = c3) t3 where t1.c1 = t3.c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 4);
                assert!(inner.filt.is_empty());
            },
        );
    }

    #[test]
    fn test_pred_pullup_outer_join() {
        let cat = j_catalog();
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from (select c1 from t1 where c1 > 0) t1 left join t2 on t1.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
                assert!(outer.eq_map.len() == 1);
                assert!(outer.cond.is_empty());
            }
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from (select c1 from t1 where c1 > 0) t1 right join t2 on t1.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.len() == 1);
                assert!(outer.cond.is_empty());
            }
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from t1 left join (select c2 from t2 where c2 > 0) t2 on t1.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.len() == 1);
                assert!(outer.cond.is_empty());
            }
        );
        assert_inner_outer(
            &cat,
            "select t1.c0, t1.c1, t2.c2 from (select c0, c1 from t1 where c0 = c1) t1 left join t2 on t1.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.len() == 1);
                assert!(outer.cond.is_empty());
            }
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c0, t2.c2 from t1 left join (select c0, c2 from t2 where c0 = c2) t2 on t1.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.len() == 1);
                assert!(outer.cond.is_empty());
            }
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from (select c1 from t1 where c1 > 0) t1 full join t2 on t1.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.is_empty());
                assert!(outer.cond.len() == 1);
            }
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from t1 full join (select c2 from t2 where c2 > 0) t2 on t1.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.is_empty());
                assert!(outer.cond.len() == 1);
            }
        );
        assert_inner_outer(
            &cat,
            "select t1.c0, t1.c1, t2.c2 from (select c0, c1 from t1 where c0 = c1) t1 full join t2 on t1.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.is_empty());
                assert!(outer.cond.len() == 1);
            }
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from t1 left join t2 on t1.c1 = t2.c2 and t2.c2 > 0",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.len() == 1);
                assert!(outer.cond.len() == 1);
            },
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from t1 full join t2 on t1.c1 = t2.c2 and t2.c2 > 0",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.is_empty());
                assert!(outer.cond.len() == 2);
            },
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from t1 left join t2 on t1.c0 = t1.c1",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.is_empty());
                assert!(outer.cond.len() == 1);
            },
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from t1 left join t2 on t2.c1 = t2.c2",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.is_empty());
                assert!(outer.cond.len() == 1);
            },
        );
        assert_inner_outer(
            &cat,
            "select t1.c1, t2.c2 from t1 left join t2 on t2.c2 = t1.c1",
            |inner, outer| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(outer.eq_map.len() == 1);
                assert!(outer.cond.is_empty());
            },
        );
    }

    #[test]
    fn test_pred_pullup_aggr() {
        let cat = j_catalog();
        assert_inner_set(&cat, "select count(*) from t1", |inner| {
            assert!(inner.eq_sets.is_empty());
            assert!(inner.filt.is_empty());
        });
        assert_inner_set(&cat, "select count(*) from t3 where c1 > 0", |inner| {
            assert!(inner.eq_sets.is_empty());
            assert!(inner.filt.is_empty());
        });
        assert_inner_set(
            &cat,
            "select c1, count(*) from t3 where c1 > 0 group by c1",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, count(*) from t3 where c2 > 0 group by c1",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, count(*) from t3 where c1 = c2 group by c1",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, count(*) from t3 where c1 = c2 group by c1, c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, count(*) from t3 group by c1 having count(*) > 1",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, count(*) from t3 where c1 + c2 > 0 group by c1, c2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, count(*) from t3 where c1 + c2 > 0 group by c1, c2 having count(*) - 10 > 0",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 2);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from (select c1, c2, c3 from t3 where c1 = c2) t3  group by c1, c2, c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from (select c1, c2, c3 from t3 where c1 = c2) t3  group by c1, c2, c3 having c2 = c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, count(c3) from (select * from t3 where c3 > 1) t3 group by c1, c2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select * from (select c1, c2, count(c1)+1 from t3 group by c1, c2) t",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
                assert!(inner.fnl_deps.is_empty());
            },
        );
    }

    #[test]
    fn test_pred_pullup_union() {
        let cat = j_catalog();
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 union select c1, c2 from t4",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 where c1 = c2 union select c1, c2 from t4",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 union select c1, c2 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 where c1 = c2 union select c1, c2 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 and c1 = c3 union select c1, c2, c3 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 union select c1, c2, c3 from t4 where c1 = c2 and c1 = c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 and c1 = c3 union select c1, c2, c0 from t4 where c0 = c2 and c2 = c1",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 union select c1, c2, c0 from t4 where c0 = c2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 union select c0, c2, c1 from t4",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 union select c0, c2, c1 from t4 where c0 > 0",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 union select c0, c2, c1 from t4 where c0 > 0",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 union select c0, c2, c1 from t4 where c0 > 3",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 and c1 < 10 union select c0, c2, c1 from t4 where c0 > 3 and c2 < 10",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 and c1 < 10 union select c0, c2, c1 from t4 where c0 > 4 and c2 < 10",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
    }

    #[test]
    fn test_pred_pullup_except() {
        let cat = j_catalog();
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 except select c1, c2 from t4",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 where c1 = c2 except select c1, c2 from t4",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 except select c1, c2 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 where c1 = c2 except select c1, c2 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 and c1 = c3 except select c1, c2, c3 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 except select c1, c2, c3 from t4 where c1 = c2 and c1 = c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 and c1 = c3 except select c1, c2, c0 from t4 where c0 = c2 and c2 = c1",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 except select c1, c2, c0 from t4 where c0 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 except select c0, c2, c1 from t4",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 except select c0, c2, c1 from t4 where c0 > 0",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 except select c0, c2, c1 from t4 where c0 > 0",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 except select c0, c2, c1 from t4 where c0 > 3",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 and c1 < 10 except select c0, c2, c1 from t4 where c0 > 3 and c2 < 10",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 2);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 and c1 < 10 except select c0, c2, c1 from t4 where c0 > 4 and c2 < 10",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 2);
            },
        );
    }

    #[test]
    fn test_pred_pullup_intersect() {
        let cat = j_catalog();
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 intersect select c1, c2 from t4",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 where c1 = c2 intersect select c1, c2 from t4",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 intersect select c1, c2 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2 from t3 where c1 = c2 intersect select c1, c2 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 2);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 and c1 = c3 intersect select c1, c2, c3 from t4 where c1 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 intersect select c1, c2, c3 from t4 where c1 = c2 and c1 = c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 and c1 = c3 intersect select c1, c2, c0 from t4 where c0 = c2 and c2 = c1",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 = c2 intersect select c1, c2, c0 from t4 where c0 = c2",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 intersect select c0, c2, c1 from t4",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 intersect select c0, c2, c1 from t4 where c0 > 0",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 intersect select c0, c2, c1 from t4 where c0 > 0",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 2);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 intersect select c0, c2, c1 from t4 where c0 > 3",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 1);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 and c1 < 10 intersect select c0, c2, c1 from t4 where c0 > 3 and c2 < 10",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 3);
            },
        );
        assert_inner_set(
            &cat,
            "select c1, c2, c3 from t3 where c1 > 3 and c1 < 10 intersect select c0, c2, c1 from t4 where c0 > 4 and c2 < 10",
            |inner| {
                assert!(inner.eq_sets.is_empty());
                assert!(inner.filt.len() == 4);
            },
        );
    }

    #[test]
    fn test_pred_pullup_multi_tables() {
        let cat = j_catalog();
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c2 from t1, t2, t3 where t1.c1 = t2.c2 and t2.c2 = t3.c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c2 from t1, t2, t3 where t1.c1 = t2.c2 and t2.c2 = t3.c3 and t3.c3 > 0",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.len() == 1);
            }
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c0 from t1 join t2 on t1.c1 = t2.c2 join t3 on t1.c1 = t3.c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.is_empty());
            },
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c0 from t1 join t2 on t1.c1 = t2.c2 join t3 on t1.c1 = t3.c3 where t3.c3 > 0",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.len() == 1);
            }
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c0 from t1 join t2 on t1.c1 = t2.c2 join (select * from t3 where c3 > 0) t3 on t1.c1 = t3.c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.len() == 1);
            }
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c0 from t1 join (select * from t2 where c1 > 1) t2 on t1.c1 = t2.c2 join (select * from t3 where c3 > 3) t3 on t1.c1 = t3.c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.len() == 2);
                assert!(inner.total_fnl_deps() == 1);
            }
        );
        assert_inner_set(
            &cat,
            "select t1.c1, t2.c0 from t1 join (select * from t2 where c0 > 0) t2 on t1.c1 = t2.c2 join (select * from t3 where c3 > 0) t3 on t1.c1 = t3.c3",
            |inner| {
                assert!(inner.eq_sets.len() == 1 && inner.eq_sets[0].len() == 3);
                assert!(inner.filt.len() == 2);
            }
        );
    }

    fn assert_inner_set<C: Catalog, F: Fn(&InnerSet)>(cat: &C, sql: &str, f: F) {
        assert_j_plan1(cat, sql, |s1, mut q1| {
            assign_id(&mut q1.qry_set, q1.root).unwrap();
            print_plan(s1, &q1);
            let mut pm = PredMap::default();
            let mut dep_cols = DepCols::default();
            pred_pullup(cat, &mut q1.qry_set, q1.root, &mut pm, &mut dep_cols).unwrap();
            print_plan(s1, &q1);
            let subq = q1.qry_set.get(&q1.root).unwrap();
            let inner_set = pm.inner.get(&subq.root.id).unwrap();
            f(inner_set);
        })
    }

    fn assert_inner_outer<C: Catalog, F: Fn(&InnerSet, &OuterSet)>(cat: &C, sql: &str, f: F) {
        assert_j_plan1(cat, sql, |s1, mut q1| {
            assign_id(&mut q1.qry_set, q1.root).unwrap();
            let mut pm = PredMap::default();
            let mut dep_cols = DepCols::default();
            pred_pullup(cat, &mut q1.qry_set, q1.root, &mut pm, &mut dep_cols).unwrap();
            // clear_filt(&mut q1.qry_set, q1.root).unwrap();
            print_plan(s1, &q1);
            // let subq = q1.qry_set.get(&q1.root).unwrap();
            let subq = q1.qry_set.get(&q1.root).unwrap();
            let inner_set = pm.inner.get(&subq.root.id).unwrap();
            let outer_id = find_first_outer_join_id(&q1.qry_set, q1.root).unwrap();
            let outer_set = pm.outer.get(&outer_id).unwrap();
            f(inner_set, outer_set);
        })
    }

    fn find_first_outer_join_id(qry_set: &QuerySet, root: QueryID) -> Option<GlobalID> {
        struct FindFirstOuterJoinID<'a>(&'a QuerySet);
        impl OpVisitor for FindFirstOuterJoinID<'_> {
            type Cont = ();
            type Break = GlobalID;
            #[inline]
            fn enter(&mut self, op: &Op) -> ControlFlow<GlobalID, ()> {
                match &op.kind {
                    OpKind::Query(qid) => {
                        if let Some(gid) = find_first_outer_join_id(self.0, *qid) {
                            return ControlFlow::Break(gid);
                        }
                        ControlFlow::Continue(())
                    }
                    OpKind::Join(join) => match join.as_ref() {
                        Join::Qualified(QualifiedJoin {
                            kind: JoinKind::Left | JoinKind::Full,
                            ..
                        }) => {
                            return ControlFlow::Break(op.id);
                        }
                        _ => ControlFlow::Continue(()),
                    },
                    _ => ControlFlow::Continue(()),
                }
            }
        }
        let mut f = FindFirstOuterJoinID(qry_set);
        if let Some(subq) = qry_set.get(&root) {
            if let ControlFlow::Break(gid) = subq.root.walk(&mut f) {
                return Some(gid);
            }
        }
        None
    }
}
