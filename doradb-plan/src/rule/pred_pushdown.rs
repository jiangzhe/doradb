use crate::error::{Error, Result};
use crate::join::{Join, QualifiedJoin};
use crate::lgc::{Aggr, Op, OpKind, OpVisitor, ProjCol, QryIDs, QuerySet};
use crate::rule::op_id::assign_id;
use crate::rule::pred_move::{CollectColMapping, InnerSet, PredMap, RewriteExprIn};
use doradb_catalog::{TableID, TblCol};
use doradb_expr::controlflow::{Branch, ControlFlow, Unbranch};
use doradb_expr::{
    Col, ColIndex, ColKind, ExprExt, ExprKind, ExprVisitor, FnlDep, GlobalID, PredFuncKind, QryCol,
    QueryID,
};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::mem;

#[inline]
pub fn pred_pushdown(qry_set: &mut QuerySet, qry_id: QueryID, col_id: &mut GlobalID) -> Result<()> {
    let mut pred_map = PredMap::default();
    let inner = InnerSet::default();
    let mut max_op_id = assign_id(qry_set, qry_id)?;
    collect_cols(qry_set, qry_id, &mut pred_map)?;
    pushdown_pred(
        qry_set,
        qry_id,
        &mut pred_map,
        &mut max_op_id,
        col_id,
        inner,
    )?;
    Ok(())
}

#[inline]
fn collect_cols(qry_set: &mut QuerySet, qry_id: QueryID, pred_map: &mut PredMap) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut cc = CollectCols { qry_set, pred_map };
        op.walk(&mut cc).unbranch()
    })?
}

#[inline]
fn pushdown_pred(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    pred_map: &mut PredMap,
    op_id: &mut GlobalID,
    col_id: &mut GlobalID,
    inner: InnerSet,
) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut ppd = PredPushdown {
            qry_set,
            pred_map,
            op_id,
            col_id,
        };
        ppd.push(op, inner)
    })?
}

struct PredPushdown<'a> {
    qry_set: &'a mut QuerySet,
    pred_map: &'a mut PredMap,
    op_id: &'a mut GlobalID,
    col_id: &'a mut GlobalID,
}

impl PredPushdown<'_> {
    /// push inner set to node.
    #[inline]
    pub fn push(&mut self, op: &mut Op, mut inner: InnerSet) -> Result<()> {
        inner = self.pred_map.extract_and_merge_inner(op.id, inner);
        match &mut op.kind {
            OpKind::Limit { input, .. }
            | OpKind::Sort {
                limit: Some(_),
                input,
                ..
            } => {
                // predicates should not be pushed down to limit or sort operator,
                // except it originates from downside.
                let inner = self.push_retain_diff(input, inner)?;
                // create new filter if some predicates remained above limit operator.
                if !inner.is_empty() {
                    let filter = self.create_new_filt(mem::take(op), inner);
                    *op = filter;
                }
                Ok(())
            }
            OpKind::Proj { input, .. } | OpKind::Sort { input, .. } | OpKind::Attach(input, _) => {
                self.push_base(input, inner)
            }
            OpKind::Filt { input, pred } => {
                if !pred.is_empty() {
                    for e in mem::take(pred) {
                        inner.handle_filt(e.into());
                    }
                }
                self.push(input, inner)?;
                let mut input = mem::take(input);
                mem::swap(op, &mut *input);
                Ok(())
            }
            OpKind::Query(qry_id) => self.push_query(*qry_id, inner),
            OpKind::Scan(scan) => self.apply_scan(scan.table_id, &scan.cols, &mut scan.filt, inner),
            OpKind::Aggr(aggr) => {
                if let Some(new_inner) = self.push_aggr(aggr, inner)? {
                    if !new_inner.is_empty() {
                        let orig_op = mem::take(op);
                        *op = self.create_new_filt(orig_op, new_inner);
                    }
                }
                Ok(())
            }
            OpKind::Setop(so) => {
                // push to both side and won't fail
                let left = so.left.as_mut();
                let inner_copy = inner.clone();
                self.push(left, inner_copy)?;
                let right = so.right.as_mut();
                self.push(right, inner)?;
                Ok(())
            }
            OpKind::Empty => Ok(()),
            OpKind::Row(_) => todo!(), // todo: evaluate immediately
            OpKind::JoinGraph(_) => {
                unreachable!("Predicates pushdown to join graph is not supported")
            }
            OpKind::Join(_join) => todo!(),
        }
    }

    /// Push predicates down across LIMIT operator, we need to
    /// retain the differences between current node and its child.
    /// So there might be some expression that can not be pushed down.
    /// the caller will create a filter upon the LIMIT operator.
    #[inline]
    fn push_retain_diff(&mut self, input: &mut Box<Op>, mut inner: InnerSet) -> Result<InnerSet> {
        // we should return the difference of input inner set and child's inner set.
        if let Some(child_inner) = self.pred_map.get_inner(input.id) {
            inner.retain_diff(child_inner);
        }
        // we should reject all predicates pushed
        self.push(input.as_mut(), InnerSet::default())?;
        Ok(inner)
    }

    /// Push predicates to base operators such as PROJECTION and SORT.
    #[inline]
    fn push_base(&mut self, input: &mut Box<Op>, inner: InnerSet) -> Result<()> {
        self.push(&mut *input, inner)?;
        Ok(())
    }

    /// Push predicates across AGGREGATE.
    #[inline]
    fn push_aggr(&mut self, aggr: &mut Box<Aggr>, mut inner: InnerSet) -> Result<Option<InnerSet>> {
        if !aggr.filt.is_empty() {
            for e in mem::take(&mut aggr.filt) {
                inner.handle_filt(e.into());
            }
        }
        // equal set here can always be pushed to child, because
        // columns must be in group items.
        debug_assert!(inner
            .eq_sets
            .iter()
            .all(|eq_set| eq_set_all_included_in_groups(eq_set, &aggr.groups)));
        let mut new_inner = InnerSet {
            eq_sets: mem::take(&mut inner.eq_sets),
            ..InnerSet::default()
        };
        if inner.filt.is_empty() {
            self.push(&mut aggr.input, new_inner)?;
            return Ok(None);
        }
        // handle filter
        // expressions with aggregate functions can not be pushed down to child.
        let (curr_filt, child_filt): (Vec<_>, Vec<_>) =
            inner.filt.into_iter().partition(|e| e.contains_aggr_func());
        inner.filt = curr_filt;
        new_inner.filt = child_filt;
        self.push(&mut aggr.input, new_inner)?;
        if inner.is_empty() {
            return Ok(None);
        }
        Ok(Some(inner))
    }

    // create new filter node and return its child
    #[inline]
    fn create_new_filt(&mut self, input: Op, new_inner: InnerSet) -> Op {
        let orig_in = input;
        let mut pred = vec![];
        self.pred_map.apply_inner(new_inner, &mut pred);
        let filter = OpKind::Filt {
            pred,
            input: Box::new(orig_in),
        };
        let op_id = self.op_id.inc_fetch();
        Op {
            id: op_id,
            kind: filter,
        }
    }

    #[inline]
    fn translate_query_inner_set(&mut self, qry_id: QueryID, inner: &InnerSet) -> InnerSet {
        // collect mapping from output columns to input expressions
        let in_map: HashMap<GlobalID, ExprKind> = {
            let subq = self.qry_set.get(&qry_id).unwrap();
            let out_cols = subq.out_cols();
            out_cols
                .iter()
                .enumerate()
                .filter_map(|(i, pc)| {
                    let qry_col = QryCol(qry_id, ColIndex::from(i as u32));
                    self.pred_map
                        .qry_col_map
                        .get(&qry_col)
                        .map(|gid| (*gid, pc.expr.clone()))
                })
                .collect()
        };
        let mut new_inner = InnerSet::default();
        // translate inner set
        for eq_set in &inner.eq_sets {
            let mut new_gid_set = HashSet::new();
            let mut new_expr_set = HashSet::new();
            for gid in eq_set {
                match &in_map[gid] {
                    ExprKind::Col(Col { gid, .. }) => {
                        new_gid_set.insert(*gid);
                    }
                    e => {
                        new_expr_set.insert(e.clone());
                    }
                }
            }
            match (new_gid_set.len(), new_expr_set.len()) {
                (0, 0) => unreachable!(),
                // e.g. SELECT c1, c2 FROM (SELECT c1+1 as c1, c2+2 as c2 FROM t1) t WHERE c1 = c2
                (0, _) => {
                    let mut expr_iter = new_expr_set.into_iter();
                    let e1 = expr_iter.next().unwrap();
                    for e2 in expr_iter {
                        let e = ExprKind::pred_func(PredFuncKind::Equal, vec![e1.clone(), e2]);
                        new_inner.handle_filt(e.into());
                    }
                }
                // e.g. SELECT c1, c2 FROM (SELECT c1, c2 FROM t1) t WHERE c1 = c2
                (_, 0) => {
                    let gid = new_gid_set.into_iter().next().unwrap();
                    let col = self.pred_map.col_map[&gid].clone();
                    for expr in new_expr_set {
                        let e = ExprKind::pred_func(
                            PredFuncKind::Equal,
                            vec![ExprKind::Col(col.clone()), expr],
                        );
                        new_inner.handle_filt(e.into());
                    }
                }
                // e.g. SELECT c1, c2 FROM (SELECT c1, 1 as c2 FROM t1) t WHERE c1 = c2
                // e.g. SELECT c1, c2, c3 FROM (SELECT c1, c2, 1 as c3 FROM t1) t WHERE c1 = c2 AND c2 = c3
                (_, _) => {
                    let gid = new_gid_set.iter().next().cloned().unwrap();
                    let col = self.pred_map.col_map[&gid].clone();
                    for expr in new_expr_set {
                        let e = ExprKind::pred_func(
                            PredFuncKind::Equal,
                            vec![ExprKind::Col(col.clone()), expr],
                        );
                        new_inner.handle_filt(e.into());
                    }
                    if new_gid_set.len() > 1 {
                        new_inner.handle_eq_set(new_gid_set);
                    }
                }
            }
        }
        // translate filter expression
        for p in &inner.filt {
            let new_p = p.replace_cols_with_exprs(&in_map).unwrap();
            new_inner.handle_filt(new_p);
        }
        // translate functional dependencies
        for (gid, deps) in &inner.fnl_deps {
            for dep in deps {
                let e = &in_map[gid];
                if e == dep {
                    // e.g. SELECT 1 as c1 FROM t1.
                    // functional dependency is c1 -> 1.
                    // If we translate functional dependency back, we got
                    // predicate 1 = 1.
                    // So we check if this can be skipped.
                    continue;
                }
                if let ExprKind::Col(c) = e {
                    // still an column, just keep the dependency.
                    // e.g.
                    let new_gid = c.gid;
                    new_inner.handle_dep(new_gid, dep.clone());
                } else {
                    let mut ri = RewriteExprIn(&in_map);
                    let mut new_dep = dep.clone();
                    assert!(new_dep.walk_mut(&mut ri).is_continue());
                    let new_e =
                        ExprKind::pred_func(PredFuncKind::Equal, vec![e.clone(), new_dep.clone()]);
                    new_inner.handle_filt(new_e.into());
                }
            }
        }
        new_inner
    }

    #[inline]
    fn push_query(&mut self, qry_id: QueryID, inner: InnerSet) -> Result<()> {
        if !inner.is_empty() {
            let inner = self.translate_query_inner_set(qry_id, &inner);
            // after translating inner set of subquery, we try to push it.
            // if we cannot push, create a new filter to hold all the predicates.
            // e.g. SELECT * FORM (SELECT c1 FROM t1 LIMIT 1) WHERE c1 > 0
            // PPD will first remove the outer filter and try to push to subquery.
            // but root operator of subquery is limit, which can not be pushed.
            // so the predicate will fall back to outer query, and then we create
            // a new filter to hold it.
            return pushdown_pred(
                self.qry_set,
                qry_id,
                self.pred_map,
                self.op_id,
                self.col_id,
                inner,
            );
        }
        pushdown_pred(
            self.qry_set,
            qry_id,
            self.pred_map,
            self.op_id,
            self.col_id,
            inner,
        )
    }

    #[inline]
    fn resolve_tbl_dep(
        &mut self,
        dep: FnlDep,
        exported_cols: &HashSet<GlobalID>,
        eq_sets: &[HashSet<TblCol>],
    ) -> Option<(ExprKind, Col)> {
        let mut key_cols = vec![];
        for key in &dep.keys[..] {
            if let ExprKind::Col(Col {
                idx,
                kind: ColKind::Table(table_id, ..),
                ..
            }) = key
            {
                let key_tbl_col = TblCol(*table_id, *idx);
                key_cols.push(key_tbl_col);
            } else {
                // key is not column, fail.
                return None;
            }
        }
        // check if key match original dependency
        let orig_keys: Vec<&Vec<TblCol>> = self.pred_map.tbl_dep_map[&dep.tbl_col]
            .iter()
            .filter(|key| key.len() == key_cols.len())
            .collect();
        if orig_keys.is_empty() {
            // key number mismatch
            return None;
        }
        for orig_key in orig_keys {
            if orig_key.len() != key_cols.len() {
                continue;
            }
            let mut perm_keys = permute_keys(orig_key, eq_sets);
            let mut test_key = Vec::with_capacity(orig_key.len());
            while perm_keys.next(&mut test_key) {
                if test_key == key_cols {
                    // dependency can convert to predicate
                    let gids = &self.pred_map.tbl_col_map[&dep.tbl_col]; // at least one
                    if let Some(gid) = gids.iter().find(|gid| exported_cols.contains(gid)) {
                        let c = self.pred_map.col_map[gid].clone();
                        return Some((ExprKind::FnlDep(dep), c));
                    } else {
                        // generate table column with new gid
                        let mut c = self.pred_map.col_map[gids.iter().next().unwrap()].clone();
                        c.gid = self.col_id.inc_fetch();
                        return Some((ExprKind::FnlDep(dep), c));
                    }
                }
            }
        }
        None
    }

    #[inline]
    fn apply_scan(
        &mut self,
        table_id: TableID,
        cols: &[ProjCol],
        filt: &mut Vec<ExprKind>,
        inner: InnerSet,
    ) -> Result<()> {
        assert!(filt.is_empty());
        // convert equal set to filter expression
        for eq_set in &inner.eq_sets {
            let mut eq_set_iter = eq_set.iter();
            let gid1 = eq_set_iter.next().unwrap();
            let c1 = self.pred_map.col_map[gid1].clone();
            for gid2 in eq_set_iter {
                let c2 = self.pred_map.col_map[gid2].clone();
                let e = ExprKind::pred_func(
                    PredFuncKind::Equal,
                    vec![ExprKind::Col(c1.clone()), ExprKind::Col(c2)],
                );
                filt.push(e);
            }
        }
        let exported_cols: HashSet<_> = cols.iter().map(|pc| pc.expr.col_gid().unwrap()).collect();
        let eq_sets = inner
            .eq_sets
            .iter()
            .map(|eq_set| {
                eq_set
                    .iter()
                    .map(|gid| {
                        let col = &self.pred_map.col_map[gid]; // must be table column
                        TblCol(table_id, col.idx)
                    })
                    .collect::<HashSet<_>>()
            })
            .collect::<Vec<_>>();
        // apply filter expression, we need to check if any functional dependency exists.
        'FILT_LOOP: for p in inner.filt {
            let fnl_deps = p.find_fnl_dep();
            if fnl_deps.is_empty() {
                let e = p.into_expr(&self.pred_map.col_map).unwrap(); // won't fail
                filt.push(e)
            } else {
                // try to resolve dependency to column
                let mut expr_map = HashMap::new();
                for dep in fnl_deps {
                    if let Some((dep, col)) = self.resolve_tbl_dep(dep, &exported_cols, &eq_sets) {
                        expr_map.insert(dep, col);
                    } else {
                        // drop the predicate if dependency is not resolvable.
                        continue 'FILT_LOOP;
                    }
                }
                let e = p
                    .replace_exprs_with_cols(&expr_map, &self.pred_map.col_map)
                    .and_then(|p| p.into_expr(&self.pred_map.col_map))
                    .unwrap();
                filt.push(e);
            }
        }
        Ok(())
    }

    #[inline]
    fn extract_inner(&mut self, op: &mut Op) -> InnerSet {
        let mut inner = self.pred_map.remove_inner(op.id);
        match &mut op.kind {
            OpKind::Filt { pred, .. } => {
                for e in mem::take(pred) {
                    inner.handle_filt(e.into());
                }
            }
            OpKind::Aggr(aggr) => {
                for e in mem::take(&mut aggr.filt) {
                    inner.handle_filt(e.into());
                }
            }
            OpKind::Join(join) => {
                if let Join::Qualified(QualifiedJoin { filt, .. }) = &mut **join {
                    for e in mem::take(filt) {
                        inner.handle_filt(e.into());
                    }
                }
            }
            OpKind::Scan(scan) => {
                for e in mem::take(&mut scan.filt) {
                    inner.handle_filt(e.into());
                }
            }
            _ => (),
        }
        inner
    }

    // try to push inner set to child node.
    #[inline]
    fn try_push(&mut self, op: &mut Op, inner: InnerSet) -> Option<InnerSet> {
        if inner.is_empty() {
            return None;
        }
        match &op.kind {
            OpKind::Limit { .. } | OpKind::Sort { limit: Some(_), .. } => {
                // predicate can not be pushed down across limit operator.
                Some(inner)
            }
            _ => {
                self.pred_map.merge_inner(op.id, inner);
                None
            }
        }
    }
}

#[inline]
fn permute_keys<'a, T: Clone + Hash + PartialEq + Eq>(
    key: &'a [T],
    eq_sets: &'a [HashSet<T>],
) -> Permutation<'a, T> {
    let mut key_sets = vec![];
    for k in key {
        if let Some(eq_set) = eq_sets.iter().find(|s| s.contains(k)) {
            let key_set: Vec<&T> = eq_set.iter().collect();
            key_sets.push(key_set);
        } else {
            key_sets.push(vec![k]);
        }
    }
    Permutation::new(key_sets)
}

struct Permutation<'a, T> {
    key_sets: Vec<Vec<&'a T>>,
    div: Vec<usize>,
    start_idx: usize,
    end_idx: usize,
}

impl<'a, T: Clone> Permutation<'a, T> {
    #[inline]
    fn new(key_sets: Vec<Vec<&'a T>>) -> Self {
        let mut end_idx = 1;
        for key_set in &key_sets {
            end_idx *= key_set.len();
        }
        let mut div = vec![0usize; key_sets.len() - 1];
        let mut n = 1;
        let mut i = key_sets.len() - 1;
        while i > 0 {
            n *= key_sets[i].len();
            i -= 1;
            div[i] = n;
        }
        Permutation {
            key_sets,
            div,
            start_idx: 0,
            end_idx,
        }
    }

    #[inline]
    fn next(&mut self, buf: &mut Vec<T>) -> bool {
        if self.start_idx == self.end_idx {
            return false;
        }
        buf.clear();
        let mut idx = self.start_idx;
        for (key_set, div) in self.key_sets.iter().zip(self.div.iter()) {
            let k_idx = idx / div;
            buf.push(key_set[k_idx].clone());
            idx -= k_idx * div;
        }
        buf.push(self.key_sets[self.key_sets.len() - 1][idx].clone());
        self.start_idx += 1;
        true
    }
}

struct CollectCols<'a> {
    qry_set: &'a mut QuerySet,
    pred_map: &'a mut PredMap,
}

impl OpVisitor for CollectCols<'_> {
    type Cont = ();
    type Break = Error;

    #[inline]
    fn enter(&mut self, op: &Op) -> ControlFlow<Error, ()> {
        match &op.kind {
            OpKind::Query(qid) => {
                collect_cols(self.qry_set, *qid, self.pred_map).branch()?;
            }
            _ => {
                // collect column mapping
                for e in op.kind.exprs() {
                    let mut cm = CollectColMapping {
                        qry_col_map: &mut self.pred_map.qry_col_map,
                        tbl_col_map: &mut self.pred_map.tbl_col_map,
                        col_map: &mut self.pred_map.col_map,
                    };
                    let _ = e.walk(&mut cm);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

#[inline]
fn eq_set_all_included_in_groups(eq_set: &HashSet<GlobalID>, groups: &[ExprKind]) -> bool {
    eq_set.iter().all(|g| {
        groups.iter().any(|e| {
            if let ExprKind::Col(Col { gid, .. }) = e {
                gid == g
            } else {
                false
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{
        assert_j_plan1, get_subq_by_location, get_table_filt_expr, j_catalog, print_plan,
    };
    use crate::lgc::{LgcPlan, Location};

    #[test]
    fn test_permutation() {
        let eq_sets: Vec<HashSet<i32>> = vec![(1..4).collect(), (4..5).collect(), (5..7).collect()];
        let key = vec![1, 4, 5];
        let mut perm = permute_keys(&key, &eq_sets);
        let mut res = vec![];
        let mut buf = vec![];
        while perm.next(&mut buf) {
            res.push(buf.clone());
        }
        assert!(res.len() == 6);
    }

    #[test]
    fn test_pred_pushdown_single_table() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 where c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 where c0 + c1 = c1 + c0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 having c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 > 0) x1",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select c1, count(*) from t1 group by c1 having c1 > 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 order by c0) x1 where c1 > 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 order by c0 limit 10) x1 where c1 > 0",
            assert_no_filt_on_disk_table,
        );

        // todo: as we identify the predicate is always true or false,
        // we can eliminate it or the whole operator subtree.
        //
        // assert_j_plan1(
        //     &cat,
        //     "select 1 from (select 1 as c1 from t1) x1 where c1 > 0",
        //     assert_no_filt_on_disk_table,
        // );
        // assert_j_plan1(
        //     &cat,
        //     "select 1 from (select 1 as c1 from t1) x1 where c1 = 0",
        //     |s1, mut q1| {
        //         pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
        //         print_plan(s1, &q1);
        //         let subq = q1.root_query().unwrap();
        //         if let OpKind::Proj { input, .. } = &subq.root.kind {
        //             assert!(input.is_empty());
        //         }
        //     },
        // );
        // assert_j_plan1(
        //     &cat,
        //     "select 1 from (select null as c1 from t1) x1 where c1 = 0",
        //     |s1, mut q1| {
        //         pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
        //         print_plan(s1, &q1);
        //         let subq = q1.root_query().unwrap();
        //         if let OpKind::Proj { input, .. } = &subq.root.kind {
        //             assert!(input.is_empty());
        //         }
        //     },
        // )
    }

    // #[test]
    // fn test_pred_pushdown_cross_join() {
    //     let cat = j_catalog();
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1, t2 where t1.c1 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select t1.c1 from t1, t2 having t1.c1 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from (select t1.c1 from t1, t2) x1 where x1.c1 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select t1.c1, count(*) from t1, t2 group by t1.c1 having c1 > 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1, t2 where t1.c1 = t2.c1",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             assert_eq!(vec!["inner"], jks);
    //         },
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1, t2, t3 where t1.c1 = t2.c1 and t1.c1 = t3.c1",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             assert_eq!(vec!["inner", "inner"], jks);
    //         },
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1, t2, t3 where t1.c1 = t2.c1 and t1.c1 = t3.c1 and t2.c1 = t3.c1",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             assert_eq!(vec!["inner", "inner"], jks);
    //         },
    //     )
    // }

    // #[test]
    // fn test_pred_pushdown_inner_join() {
    //     let cat = j_catalog();
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 join t2 where t1.c1 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 join t2 where c2 = 0",
    //         assert_filt_on_disk_table1r,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select t1.c1 from t1 join t2 having t1.c1 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from (select t1.c1 from t1 join t2) x1 where x1.c1 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select t1.c1, c2, count(*) from t1 join t2 where t1.c1 = 0 group by t1.c1, t2.c2 having c2 > 100",
    //         assert_filt_on_disk_table2,
    //     );
    // }

    // #[test]
    // fn test_pred_pushdown_left_join() {
    //     let cat = j_catalog();
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 left join t2 where t1.c1 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 left join t2 where c2 = 0",
    //         assert_filt_on_disk_table1r,
    //     );
    //     // filter expression NOT rejects null, so cannot be pushed
    //     // to table scan.
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 left join t2 where c2 is null",
    //         assert_no_filt_on_disk_table,
    //     );
    //     // involve both sides, cannot be pushed to table scan,
    //     // join type will be converted to inner join.
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 left join t2 where t1.c1 = c2",
    //         assert_no_filt_on_disk_table,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select t1.c1 from t1 left join t2 having t1.c1 = 0 order by c1",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from (select t1.c1 from t1 left join t2) x1 where x1.c1 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select t1.c1, c2, count(*) from t1 left join t2 where t1.c1 = 0 group by t1.c1, t2.c2 having c2 > 100",
    //         assert_filt_on_disk_table2,
    //     );
    //     // one left join converted to inner join
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 left join t2 left join t3 on t1.c1 = t3.c3 where t1.c1 = t2.c2",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             // the predicate pushdown only change the topmost join type.
    //             assert_eq!(vec!["left", "inner"], jks);
    //         },
    //     );
    //     // both left joins converted to inner joins, and one more inner join added.
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 left join t2 left join t3 on t1.c1 = t2.c2 and t1.c1 = t3.c3 where t2.c2 = t3.c3",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             // in this case, the predicate pushdown stops at the first join
    //             // so second join will not be converted to inner join
    //             // but will be fixed by predicate propagation rule.
    //             assert_eq!(vec!["inner", "left"], jks);
    //         }
    //     );
    //     // both left joins converted to inner joins, and remove as no join condition,
    //     // one more inner join added.
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 left join t2 left join t3 where t2.c2 = t3.c3",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             // stops at first join, second will not be converted to inner join
    //             assert_eq!(vec!["inner", "left"], jks);
    //         },
    //     );
    //     // one is pushed as join condition, one is pushed as filter
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 join t2 left join t3 left join t4 where t1.c1 = t2.c2 and t3.c3 is null",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             assert_eq!(vec!["left", "left", "inner"], jks);
    //         }
    //     );
    // }

    // #[test]
    // fn test_pred_pushdown_right_join() {
    //     let cat = j_catalog();
    //     // right join is replaced by left join, so right table 2 is t1.
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 right join t2 where t1.c1 = 0",
    //         assert_filt_on_disk_table1r,
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 right join t2 where t2.c2 = 0",
    //         assert_filt_on_disk_table1,
    //     );
    // }

    // #[test]
    // fn test_pred_pushdown_full_join() {
    //     let cat = j_catalog();
    //     // full join converted to left join
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 full join t2 where t1.c1 = 0",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq1 = get_subq_by_location(&q1, Location::Disk);
    //             // converted to right table, then left table
    //             // the underlying query postion is changed.
    //             assert!(!get_table_filt_expr(&subq1[1]).is_empty());
    //         },
    //     );
    //     // full join converted to right join, then left join
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 full join t2 where t2.c2 = 0",
    //         assert_filt_on_disk_table1r,
    //     );
    //     // full join converted to inner join
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 full join t2 where t1.c1 = t2.c2",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             assert_eq!(vec!["inner"], jks);
    //         },
    //     );
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 full join t2 on t1.c1 = t2.c2 where t1.c1 is null and t2.c2 is null",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             assert_eq!(vec!["full"], jks);
    //         },
    //     );
    //     // convert to left join and add one filt
    //     assert_j_plan1(
    //         &cat,
    //         "select 1 from t1 full join t2 on t1.c1 = t2.c2 where t1.c0 > 0 and t2.c2 is null",
    //         |s1, mut q1| {
    //             pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
    //             print_plan(s1, &q1);
    //             let subq = q1.root_query().unwrap();
    //             let jks = extract_join_kinds(&subq.root);
    //             assert_eq!(vec!["left"], jks);
    //         },
    //     );
    // }

    fn assert_filt_on_disk_table1(s1: &str, mut q1: LgcPlan) {
        print_plan(s1, &q1);
        pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_table_filt_expr(&subq1[0]).is_empty());
    }

    fn assert_filt_on_disk_table1r(s1: &str, mut q1: LgcPlan) {
        pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_table_filt_expr(&subq1[1]).is_empty());
    }

    fn assert_filt_on_disk_table2(s1: &str, mut q1: LgcPlan) {
        pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_table_filt_expr(&subq1[0]).is_empty());
        assert!(!get_table_filt_expr(&subq1[1]).is_empty());
    }

    fn assert_no_filt_on_disk_table(s1: &str, mut q1: LgcPlan) {
        pred_pushdown(&mut q1.qry_set, q1.root, &mut q1.max_cid).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(subq1
            .into_iter()
            .all(|subq| get_table_filt_expr(subq).is_empty()));
    }
}

#[derive(Default, Clone)]
struct ExprAttr {
    qry_ids: QryIDs,
    has_aggf: bool,
    // whether the predicate contains subquery that cannot be pushed down.
    has_subq: bool,
}

impl ExprVisitor<'_> for ExprAttr {
    type Cont = ();
    type Break = ();
    #[inline]
    fn enter(&mut self, e: &ExprKind) -> ControlFlow<()> {
        match e {
            ExprKind::Aggf { .. } => self.has_aggf = true,
            ExprKind::Col(Col {
                kind: ColKind::Query(qry_id),
                ..
            }) => match &mut self.qry_ids {
                QryIDs::Empty => {
                    self.qry_ids = QryIDs::Single(*qry_id);
                }
                QryIDs::Single(qid) => {
                    if qid != qry_id {
                        let mut hs = HashSet::new();
                        hs.insert(*qid);
                        hs.insert(*qry_id);
                        self.qry_ids = QryIDs::Multi(hs);
                    }
                }
                QryIDs::Multi(hs) => {
                    hs.insert(*qry_id);
                }
            },
            ExprKind::Subq(..) | ExprKind::Attval(_) => {
                self.has_subq = true;
            }
            _ => (),
        }
        ControlFlow::Continue(())
    }
}

/* below is deprecated */

// #[derive(Clone)]
// struct CachedExpr {
//     e: ExprKind,
//     // lazy field
//     attr: Option<ExprAttr>,
//     reject_nulls: Option<HashMap<QueryID, bool>>,
// }

// impl CachedExpr {
//     #[inline]
//     fn new(e: ExprKind) -> Self {
//         CachedExpr {
//             e,
//             attr: None,
//             reject_nulls: None,
//         }
//     }

//     #[inline]
//     fn load_attr(&self) -> &ExprAttr {
//         if self.attr.is_none() {
//             let mut attr = ExprAttr::default();
//             let _ = self.e.walk(&mut attr);
//             self.attr = Some(attr);
//         }
//         self.attr.as_ref().unwrap() // won't fail
//     }

//     #[inline]
//     fn load_reject_null(&mut self, qry_id: QueryID) -> Result<bool> {
//         if let Some(reject_nulls) = &mut self.reject_nulls {
//             let res = match reject_nulls.entry(qry_id) {
//                 Entry::Occupied(occ) => *occ.get(),
//                 Entry::Vacant(vac) => {
//                     let rn = self.e.clone().reject_null(|e| match e {
//                         ExprKind::Col(Col {
//                             kind: ColKind::Query(qid),
//                             ..
//                         }) if *qid == qry_id => {
//                             *e = ExprKind::const_null();
//                         }
//                         _ => (),
//                     })?;
//                     vac.insert(rn);
//                     rn
//                 }
//             };
//             Ok(res)
//         } else {
//             let mut reject_nulls = HashMap::new();
//             let rn = self.e.clone().reject_null(|e| match e {
//                 ExprKind::Col(Col {
//                     kind: ColKind::Query(qid),
//                     ..
//                 }) if *qid == qry_id => {
//                     *e = ExprKind::const_null();
//                 }
//                 _ => (),
//             })?;
//             reject_nulls.insert(qry_id, rn);
//             self.reject_nulls = Some(reject_nulls);
//             Ok(rn)
//         }
//     }

//     #[inline]
//     fn rewrite(&mut self, qry_id: QueryID, out: &[ProjCol]) {
//         let mut roe = RewriteOutExpr { qry_id, out };
//         let _ = self.e.walk_mut(&mut roe);
//         // must reset lazy field as the expression changed
//         self.attr = None;
//         self.reject_nulls = None;
//     }
// }

// #[inline]
// fn push_single(
//     qry_set: &mut QuerySet,
//     op: &mut Op,
//     mut p: ExprItem,
// ) -> Result<(RuleEffect, Option<ExprItem>)> {
//     let mut eff = RuleEffect::empty();
//     let res = match &mut op.kind {
//         OpKind::Query(qry_id) => {
//             if let Some(subq) = qry_set.get(qry_id) {
//                 p.rewrite(*qry_id, subq.out_cols());
//                 // after rewriting, Simplify it before pushing
//                 simplify_nested(&mut p.e, NullCoalesce::Null)?;
//                 match &p.e {
//                     ExprKind::Const(Const::Null) => {
//                         *op = Op::empty();
//                         eff |= RuleEffect::OP;
//                         return Ok((eff, None));
//                     }
//                     ExprKind::Const(c) => {
//                         if c.is_zero().unwrap_or_default() {
//                             *op = Op::empty();
//                             eff |= RuleEffect::OP;
//                             return Ok((eff, None));
//                         } else {
//                             return Ok((eff, None));
//                         }
//                     }
//                     _ => (),
//                 }
//                 let e = qry_set.transform_op(*qry_id, |qry_set, _, op| {
//                     let (e, pred) = push_single(qry_set, op, p)?;
//                     assert!(pred.is_none()); // this push must succeed
//                     eff |= e;
//                     eff |= RuleEffect::EXPR;
//                     Ok::<_, Error>(eff)
//                 })??;
//                 eff |= e;
//                 None
//             } else {
//                 Some(p)
//             }
//         }
//         // Table always accept
//         OpKind::Scan(scan) => {
//             scan.filt.push(p.e);
//             None
//         }
//         // Empty just ignores
//         OpKind::Empty => None,
//         OpKind::Row(_) => todo!(), // todo: evaluate immediately
//         // Proj/Sort/Limit/Attach will try pushing pred, and if fails just accept.
//         // todo: pushdown to limit should be forbidden
//         OpKind::Proj { .. } | OpKind::Sort { .. } | OpKind::Limit { .. } | OpKind::Attach(..) => {
//             let (e, item) = push_or_accept(qry_set, op, p)?;
//             eff |= e;
//             item
//         }
//         OpKind::Aggr(aggr) => {
//             if p.load_attr().has_aggf {
//                 Some(p)
//             } else {
//                 // after the validation, all expressions containing no aggregate
//                 // functions can be pushed down through Aggr operator, as they can
//                 // only be composite of group columns, constants and functions.
//                 let (e, item) = push_single(qry_set, &mut aggr.input, p)?;
//                 assert!(item.is_none()); // just succeed
//                 eff |= e;
//                 None
//             }
//         }
//         OpKind::Filt { pred, input } => match push_single(qry_set, input, p)? {
//             (e, Some(p)) => {
//                 eff |= e;
//                 let mut old = mem::take(pred);
//                 old.push(p.e);
//                 let mut new = ExprKind::pred_conj(old);
//                 eff |= simplify_nested(&mut new, NullCoalesce::False)?;
//                 *pred = new.into_conj();
//                 None
//             }
//             (e, None) => {
//                 eff |= e;
//                 eff |= RuleEffect::EXPR;
//                 None
//             }
//         },
//         OpKind::Setop(so) => {
//             // push to both side and won't fail
//             let (e, item) = push_single(qry_set, so.left.as_mut(), p.clone())?;
//             assert!(item.is_none());
//             eff |= e;
//             eff |= RuleEffect::EXPR;
//             let (e, item) = push_single(qry_set, so.right.as_mut(), p)?;
//             eff |= e;
//             eff |= RuleEffect::EXPR;
//             assert!(item.is_none());
//             None
//         }
//         OpKind::JoinGraph(_) => unreachable!("Predicates pushdown to join graph is not supported"),
//         OpKind::Join(join) => match join.as_mut() {
//             Join::Cross(jos) => {
//                 if p.load_attr().has_aggf {
//                     // do not push predicates with aggregate functions
//                     Some(p)
//                 } else {
//                     let qry_ids = &p.load_attr().qry_ids;
//                     match qry_ids {
//                         QryIDs::Empty => unreachable!(), // Currently marked as unreachable
//                         QryIDs::Single(qry_id) => {
//                             // predicate of single table
//                             let mut jo_qids = HashSet::new(); // reused container
//                             for jo in jos {
//                                 jo_qids.clear();
//                                 jo.collect_qry_ids(&mut jo_qids);
//                                 if jo_qids.contains(qry_id) {
//                                     let (e, item) = push_single(qry_set, jo.as_mut(), p)?;
//                                     assert!(item.is_none());
//                                     eff |= e;
//                                     return Ok((eff, None));
//                                 }
//                             }
//                             Some(p)
//                         }
//                         QryIDs::Multi(qry_ids) => {
//                             // if involved multiple tables, we convert cross join into join tree
//                             // currently only two-way join is supported.
//                             // once cross join are converted as a join tree, these rejected predicates
//                             // can be pushed further.
//                             if qry_ids.len() > 2 {
//                                 Some(p)
//                             } else {
//                                 let (qid1, qid2) = {
//                                     let mut iter = qry_ids.iter();
//                                     let q1 = iter.next().cloned().unwrap();
//                                     let q2 = iter.next().cloned().unwrap();
//                                     (q1, q2)
//                                 };
//                                 let mut join_ops = mem::take(jos);
//                                 if let Some((idx1, jo)) = join_ops
//                                     .iter()
//                                     .enumerate()
//                                     .find(|(_, jo)| jo.contains_qry_id(qid1))
//                                 {
//                                     if jo.contains_qry_id(qid2) {
//                                         // belong to single join op, push to it
//                                         *jos = join_ops;
//                                         let (e, item) =
//                                             push_single(qry_set, jos[idx1].as_mut(), p.clone())?;
//                                         assert!(item.is_none());
//                                         eff |= e;
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                     if let Some(idx2) =
//                                         join_ops.iter().position(|jo| jo.contains_qry_id(qid2))
//                                     {
//                                         let (jo1, jo2) = if idx1 < idx2 {
//                                             // get larger one first
//                                             let jo2 = join_ops.swap_remove(idx2);
//                                             let jo1 = join_ops.swap_remove(idx1);
//                                             (jo1, jo2)
//                                         } else {
//                                             let jo1 = join_ops.swap_remove(idx1);
//                                             let jo2 = join_ops.swap_remove(idx2);
//                                             (jo2, jo1)
//                                         };
//                                         if join_ops.is_empty() {
//                                             // entire cross join is converted to inner join tree.
//                                             let new_join = Join::Qualified(QualifiedJoin {
//                                                 kind: JoinKind::Inner,
//                                                 left: jo1,
//                                                 right: jo2,
//                                                 cond: vec![p.e],
//                                                 filt: vec![],
//                                             });
//                                             *join.as_mut() = new_join;
//                                             eff |= RuleEffect::OPEXPR;
//                                             return Ok((eff, None));
//                                         } else {
//                                             let new_join = JoinOp::qualified(
//                                                 JoinKind::Inner,
//                                                 jo1,
//                                                 jo2,
//                                                 vec![p.e],
//                                                 vec![],
//                                             );
//                                             join_ops.push(new_join);
//                                             *jos = join_ops;
//                                             eff |= RuleEffect::OPEXPR;
//                                             return Ok((eff, None));
//                                         }
//                                     } else {
//                                         return Err(Error::InvalidJoinCondition);
//                                     }
//                                 } else {
//                                     return Err(Error::InvalidJoinCondition);
//                                 }
//                             }
//                         }
//                     }
//                 }
//             }
//             Join::Qualified(QualifiedJoin {
//                 kind,
//                 left,
//                 right,
//                 cond,
//                 filt,
//             }) => {
//                 let qry_ids = &p.load_attr().qry_ids;
//                 match qry_ids {
//                     QryIDs::Empty => unreachable!(), // Currently marked as unreachable
//                     QryIDs::Single(qry_id) => {
//                         let qry_id = *qry_id;
//                         if left.contains_qry_id(qry_id) {
//                             match kind {
//                                 JoinKind::Inner | JoinKind::Left => {
//                                     let (e, item) = push_single(qry_set, left.as_mut(), p)?;
//                                     assert!(item.is_none());
//                                     eff |= e;
//                                     eff |= RuleEffect::EXPR;
//                                     return Ok((eff, None));
//                                 }
//                                 JoinKind::Full => {
//                                     if p.load_reject_null(qry_id)? {
//                                         // reject null
//                                         // convert full join to right join, then to left join
//                                         *kind = JoinKind::Left;
//                                         mem::swap(left, right);
//                                         // push to (original) left side
//                                         let (e, item) = push_single(qry_set, right.as_mut(), p)?;
//                                         assert!(item.is_none());
//                                         eff |= e;
//                                         eff |= RuleEffect::OPEXPR;
//                                         return Ok((eff, None));
//                                     } else {
//                                         // not reject null
//                                         filt.push(p.e);
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                 }
//                                 _ => todo!(),
//                             }
//                         } else if right.contains_qry_id(qry_id) {
//                             match kind {
//                                 JoinKind::Inner => {
//                                     let (e, item) = push_single(qry_set, right.as_mut(), p)?;
//                                     assert!(item.is_none());
//                                     eff |= e;
//                                     return Ok((eff, None));
//                                 }
//                                 JoinKind::Left => {
//                                     if p.load_reject_null(qry_id)? {
//                                         // reject null
//                                         // convert left join to inner join
//                                         *kind = JoinKind::Inner;
//                                         if !filt.is_empty() {
//                                             cond.extend(mem::take(filt)) // put filters into condition
//                                         }
//                                         let (e, item) = push_single(qry_set, right.as_mut(), p)?;
//                                         assert!(item.is_none());
//                                         eff |= e;
//                                         eff |= RuleEffect::OPEXPR;
//                                         return Ok((eff, None));
//                                     } else {
//                                         // not reject null
//                                         filt.push(p.e);
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                 }
//                                 JoinKind::Full => {
//                                     if p.load_reject_null(qry_id)? {
//                                         // reject null
//                                         // convert full join to left join
//                                         *kind = JoinKind::Left;
//                                         let (e, item) = push_single(qry_set, right.as_mut(), p)?;
//                                         assert!(item.is_none());
//                                         eff |= e;
//                                         eff |= RuleEffect::OPEXPR;
//                                         return Ok((eff, None));
//                                     } else {
//                                         // not reject null
//                                         filt.push(p.e);
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                 }
//                                 _ => todo!(),
//                             }
//                         } else {
//                             // this should not happen, the predicate must belong to either side
//                             unreachable!()
//                         }
//                     }
//                     QryIDs::Multi(qry_ids) => {
//                         let mut left_qids = HashSet::new();
//                         left.collect_qry_ids(&mut left_qids);
//                         let left_qids: HashSet<QueryID> =
//                             qry_ids.intersection(&left_qids).cloned().collect();
//                         let mut right_qids = HashSet::new();
//                         right.collect_qry_ids(&mut right_qids);
//                         let right_qids: HashSet<QueryID> =
//                             qry_ids.intersection(&right_qids).cloned().collect();
//                         match (left_qids.is_empty(), right_qids.is_empty()) {
//                             (false, true) => {
//                                 // handle on left side
//                                 match kind {
//                                     JoinKind::Inner | JoinKind::Left => {
//                                         let (e, item) = push_single(qry_set, left.as_mut(), p)?;
//                                         assert!(item.is_none());
//                                         eff |= e;
//                                         return Ok((eff, None));
//                                     }
//                                     JoinKind::Full => {
//                                         for qry_id in left_qids {
//                                             if p.load_reject_null(qry_id)? {
//                                                 // convert full join to right join, then to left join
//                                                 *kind = JoinKind::Left;
//                                                 mem::swap(left, right);
//                                                 // push to (original) left side
//                                                 let (e, item) =
//                                                     push_single(qry_set, right.as_mut(), p)?;
//                                                 assert!(item.is_none());
//                                                 eff |= e;
//                                                 eff |= RuleEffect::OPEXPR;
//                                                 return Ok((eff, None));
//                                             }
//                                         }
//                                         // not reject null
//                                         filt.push(p.e);
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                     _ => todo!(),
//                                 }
//                             }
//                             (true, false) => {
//                                 // handle on right side
//                                 match kind {
//                                     JoinKind::Inner => {
//                                         let (e, item) = push_single(qry_set, right.as_mut(), p)?;
//                                         assert!(item.is_none());
//                                         eff |= e;
//                                         return Ok((eff, None));
//                                     }
//                                     JoinKind::Left => {
//                                         for qry_id in right_qids {
//                                             if p.load_reject_null(qry_id)? {
//                                                 // convert left join to inner join
//                                                 *kind = JoinKind::Inner;
//                                                 if !filt.is_empty() {
//                                                     // put filters into condition
//                                                     cond.extend(mem::take(filt));
//                                                 }
//                                                 let (e, item) =
//                                                     push_single(qry_set, right.as_mut(), p)?;
//                                                 assert!(item.is_none());
//                                                 eff |= e;
//                                                 eff |= RuleEffect::OPEXPR;
//                                                 return Ok((e, None));
//                                             }
//                                         }
//                                         // not reject null
//                                         filt.push(p.e);
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                     JoinKind::Full => {
//                                         for qry_id in right_qids {
//                                             if p.load_reject_null(qry_id)? {
//                                                 // convert full join to left join
//                                                 *kind = JoinKind::Left;
//                                                 let (e, item) =
//                                                     push_single(qry_set, right.as_mut(), p)?;
//                                                 assert!(item.is_none());
//                                                 eff |= e;
//                                                 eff |= RuleEffect::OP;
//                                                 return Ok((eff, None));
//                                             }
//                                         }
//                                         // not reject null
//                                         filt.push(p.e);
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                     _ => todo!(),
//                                 }
//                             }
//                             (false, false) => {
//                                 // handle on both sides
//                                 match kind {
//                                     JoinKind::Inner => {
//                                         cond.push(p.e);
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                     JoinKind::Left => {
//                                         for qry_id in right_qids {
//                                             if p.load_reject_null(qry_id)? {
//                                                 // convert left join to inner join
//                                                 *kind = JoinKind::Inner;
//                                                 if !filt.is_empty() {
//                                                     cond.extend(mem::take(filt))
//                                                     // put filters into condition
//                                                 }
//                                                 cond.push(p.e);
//                                                 eff |= RuleEffect::OPEXPR;
//                                                 return Ok((eff, None));
//                                             }
//                                         }
//                                         // not reject null on right side
//                                         filt.push(p.e);
//                                         eff |= RuleEffect::EXPR;
//                                         return Ok((eff, None));
//                                     }
//                                     JoinKind::Full => {
//                                         let mut left_reject_null = false;
//                                         let mut right_reject_null = false;
//                                         for qry_id in left_qids {
//                                             if p.load_reject_null(qry_id)? {
//                                                 left_reject_null = true;
//                                                 break;
//                                             }
//                                         }
//                                         for qry_id in right_qids {
//                                             if p.load_reject_null(qry_id)? {
//                                                 right_reject_null = true;
//                                                 break;
//                                             }
//                                         }
//                                         match (left_reject_null, right_reject_null) {
//                                             (true, true) => {
//                                                 // convert to inner join
//                                                 *kind = JoinKind::Inner;
//                                                 if !filt.is_empty() {
//                                                     cond.extend(mem::take(filt))
//                                                 }
//                                                 cond.push(p.e);
//                                                 eff |= RuleEffect::OPEXPR;
//                                                 return Ok((eff, None));
//                                             }
//                                             (true, false) => {
//                                                 // convert to right join then left join
//                                                 *kind = JoinKind::Left;
//                                                 mem::swap(left, right);
//                                                 filt.push(p.e);
//                                                 eff |= RuleEffect::OPEXPR;
//                                                 return Ok((eff, None));
//                                             }
//                                             (false, true) => {
//                                                 // convert to left join
//                                                 *kind = JoinKind::Left;
//                                                 filt.push(p.e);
//                                                 eff |= RuleEffect::OPEXPR;
//                                                 return Ok((eff, None));
//                                             }
//                                             (false, false) => {
//                                                 filt.push(p.e);
//                                                 eff |= RuleEffect::EXPR;
//                                                 return Ok((eff, None));
//                                             }
//                                         }
//                                     }
//                                     _ => todo!(),
//                                 }
//                             }
//                             (true, true) => unreachable!(),
//                         }
//                     }
//                 }
//             }
//         },
//     };
//     Ok((eff, res))
// }

// #[inline]
// fn push_or_accept(
//     qry_set: &mut QuerySet,
//     op: &mut Op,
//     pred: ExprItem,
// ) -> Result<(RuleEffect, Option<ExprItem>)> {
//     let input = op.kind.input_mut().unwrap(); // won't fail
//     match push_single(qry_set, input, pred)? {
//         (eff, Some(pred)) => {
//             let child = mem::take(input);
//             let new_filt = Op::new(OpKind::filt(vec![pred.e], child));
//             *input = new_filt;
//             // as child reject it, we do not merge effect, as parent will update it
//             Ok((eff, None))
//         }
//         (eff, None) => Ok((eff, None)),
//     }
// }

// struct RewriteOutExpr<'a> {
//     qry_id: QueryID,
//     out: &'a [ProjCol],
// }

// impl ExprMutVisitor for RewriteOutExpr<'_> {
//     type Cont = ();
//     type Break = ();
//     #[inline]
//     fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<()> {
//         if let ExprKind::Col(Col {
//             kind: ColKind::Query(qry_id),
//             idx,
//             ..
//         }) = e
//         {
//             if *qry_id == self.qry_id {
//                 let new_c = &self.out[idx.value() as usize];
//                 *e = new_c.expr.clone();
//             }
//         }
//         ControlFlow::Continue(())
//     }
// }
