use crate::error::{Error, Result};
use crate::join::{Join, JoinKind, JoinOp, QualifiedJoin};
use crate::lgc::{Op, OpKind, OpMutVisitor, ProjCol, QuerySet};
use crate::rule::expr_simplify::{update_simplify_nested, NullCoalesce, PartialExpr};
use doradb_expr::controlflow::{Branch, ControlFlow, Unbranch};
use doradb_expr::{
    Col, ColIndex, ColKind, ExprExt, ExprKind, ExprVisitor, FuncKind, GlobalID, Pred, PredFuncKind,
    QryCol, QueryID,
};
use std::collections::{HashMap, HashSet};
use std::mem;

/// Pullup predicates.
/// The canonical optimizations is pushing down predicates, not pulling up,
/// in order to filter data as early as possible.
/// But in some scenarios, the pullup with propagation enables generate many
/// more predicates that can be pushed down other tables.
/// This is an interesting rationale: "pull to push more".
///
/// Pullup should be performed only when necessary:
/// 1. Join exists.
/// 2. Only columns present in join condition should be pulled up.
/// 3. full join is discarded as propagation is not available.
/// 4. left join has limitation that predicates can be propagated
///    only from left side to right side.
///
/// This rule must be used with(before) predicate pushdown, because it move
/// predicates into a separate map, and rely on predicate pushdown to move
/// them back and push down.
#[inline]
pub fn pred_pullup(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    let mut p_preds = HashMap::new();
    pullup_pred(qry_set, qry_id, HashMap::new(), &mut p_preds) // pass empty parent columns, so pulled preds must be empty
}

#[inline]
fn pullup_pred(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    p_cols: HashMap<QryCol, GlobalID>,
    p_preds: &mut HashMap<QryCol, PartialExprSet>,
) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut ppu = PredPullup::new(qry_set, p_cols, p_preds);
        op.walk_mut(&mut ppu).unbranch()?;
        Ok(())
    })?
}

struct PredPullup<'a> {
    qry_set: &'a mut QuerySet,
    // parent columns that predicates target
    p_cols: HashMap<QryCol, GlobalID>,
    // predicates converted based on mapping, this field is passed by
    // parent oprator
    p_preds: &'a mut HashMap<QryCol, PartialExprSet>,
    // mapping current cols to parent cols
    mapping: HashMap<QryCol, ExprKind>,
    // whether the parent columns have been translated.
    translated: bool,
    // current columns involved in current predicates.
    c_cols: HashMap<QryCol, GlobalID>,
    // current preds, that will be passed to child query.
    c_preds: HashMap<QryCol, PartialExprSet>,
    // store join op that are temporarily removed, in order to restore back
    stack: Vec<JoinOp>,
}

impl<'a> PredPullup<'a> {
    #[inline]
    fn new(
        qry_set: &'a mut QuerySet,
        p_cols: HashMap<QryCol, GlobalID>,
        p_preds: &'a mut HashMap<QryCol, PartialExprSet>,
    ) -> Self {
        PredPullup {
            qry_set,
            p_cols,
            p_preds,
            mapping: HashMap::new(),
            translated: false,
            c_cols: HashMap::new(),
            c_preds: HashMap::new(),
            stack: vec![],
        }
    }

    // Translate columns based on the parent column reference and output sequence.
    // We have to build mapping between current column and parent column.
    // e.g. "SELECT c1 FROM (SELECT c0 + 1 as c1 FROM t1) t"
    // The outer query has column "c1", the inner query outputs "c0 + 1", so we
    // can build equation "c1 = c0 + 1", then we try to convert the expression
    // to "c0 = c1 - 1", and store it in mapping. Suppose we find a predicate
    // "c0 > 1" later, then we can replace "c0" based on the mapping and get
    // new predicate "c1 - 1 > 1" or simplified version "c1 > 2" which could be
    // propagated in parent scope.
    #[inline]
    fn translate_p_cols(&mut self, out_cols: &[ProjCol]) {
        if self.translated {
            // translate at most once for each query block
            return;
        }
        for (QryCol(p_qid, p_idx), p_gid) in &self.p_cols {
            let c = &out_cols[p_idx.value() as usize];
            if let Some((c_col, new_e)) = translate_col(*p_gid, *p_qid, *p_idx, &c.expr) {
                self.mapping.insert(c_col, new_e);
            }
        }
        self.translated = true;
    }

    #[inline]
    fn collect_p_preds(&mut self, c_preds: &[ExprKind]) -> Result<()> {
        if self.mapping.is_empty() || c_preds.is_empty() {
            return Ok(());
        }
        for p in c_preds {
            if let Some((gid, qid, idx, e)) = translate_pred(p, &self.mapping)? {
                let peset = self
                    .p_preds
                    .entry(QryCol(qid, idx))
                    .or_insert_with(|| PartialExprSet(gid, HashSet::new()));
                peset.1.insert(e);
            }
        }
        Ok(())
    }

    #[inline]
    fn collect_c_cols(&mut self, c_preds: &[ExprKind]) {
        if c_preds.is_empty() {
            return;
        }
        for p in c_preds {
            collect_non_aggr_qry_cols(p, &mut self.c_cols);
        }
    }

    #[inline]
    fn pull_inner_side(&mut self, jo: &mut JoinOp) -> ControlFlow<Error> {
        let p_cols = HashMap::new();
        let mut p_preds = HashMap::new();
        let mut ppu = PredPullup::new(self.qry_set, p_cols, &mut p_preds);
        jo.as_mut().walk_mut(&mut ppu)
    }

    // currently we only support propagate based on equation join condition.
    #[inline]
    fn propagate_preds(
        &mut self,
        conds: &[ExprKind],
    ) -> Vec<(GlobalID, QueryID, ColIndex, PartialExpr)> {
        let mut res = vec![];
        for c in conds {
            if let ExprKind::Pred(Pred::Func { kind, args }) = c
                && let (
                    PredFuncKind::Equal,
                    [ExprKind::Col(Col {
                        gid: l_gid,
                        kind: ColKind::Query(l_qid),
                        idx: l_idx,
                    }), ExprKind::Col(Col {
                        gid: r_gid,
                        kind: ColKind::Query(r_qid),
                        idx: r_idx,
                    })],
                ) = (kind, &args[..])
                {
                    let l_qry_col = QryCol(*l_qid, *l_idx);
                    if let Some(PartialExprSet(_, pes)) = self.c_preds.get(&l_qry_col) {
                        for pe in pes {
                            res.push((*r_gid, *r_qid, *r_idx, pe.clone()));
                        }
                    }
                    let r_qry_col = QryCol(*r_qid, *r_idx);
                    if let Some(PartialExprSet(_, pes)) = self.c_preds.get(&r_qry_col) {
                        for pe in pes {
                            res.push((*l_gid, *l_qid, *l_idx, pe.clone()));
                        }
                    }
                }
        }
        res
    }
}

impl OpMutVisitor for PredPullup<'_> {
    type Cont = ();
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match &mut op.kind {
            // top down, translate parent cols to current cols, for first proj or aggr
            OpKind::Proj { cols, .. } => {
                self.translate_p_cols(cols);
            }
            // top down , translate parent cols
            OpKind::Aggr(aggr) => {
                self.translate_p_cols(&aggr.proj);
                // Here we do not collect c_cols because all predicates that
                // can be pushed should have been pushed by rule pred pushdown.
                // So remaining filters should contain aggf that can not be
                // handled by current rule.
            }
            OpKind::Filt { pred, .. } => {
                // collect columns in filter predicates, which can be passed to
                // child query to pull predicates
                self.collect_c_cols(pred);
            }
            OpKind::Join(join) => match join.as_mut() {
                Join::Cross(_) => (),
                Join::Qualified(QualifiedJoin {
                    kind,
                    left,
                    right,
                    cond,
                    ..
                }) => match kind {
                    JoinKind::Inner => {
                        // for inner join, from top down, we'd like to collect columns that we care about
                        // and let child to collect related predicates
                        self.collect_c_cols(cond);
                    }
                    JoinKind::Left => {
                        // for left join, from top down, we only need to collect columns of left side,
                        // because predicates from right side is not allowed to propagated to left side.
                        // Here we remove and process right side, when bottom up, we add it back.
                        let mut right = mem::take(right);
                        self.pull_inner_side(&mut right)?;
                        self.stack.push(right);
                        // still collect columns for pulling left side
                        self.collect_c_cols(cond);
                    }
                    JoinKind::Full => {
                        // for full join, from top down, we don't want any predicates because they can
                        // not be propagate to the other side.
                        let mut right = mem::take(right);
                        self.pull_inner_side(&mut right)?;
                        self.stack.push(right);
                        let mut left = mem::take(left);
                        self.pull_inner_side(&mut left)?;
                        self.stack.push(left);
                    }
                    _ => todo!(),
                },
            },
            OpKind::Query(qry_id) => {
                // Here we collect all p_cols according to the child query id,
                // and recursively call the predicate pullup.
                let p_cols: HashMap<_, _> = self
                    .c_cols
                    .iter()
                    .filter_map(|(QryCol(qid, idx), gid)| {
                        if qid == qry_id {
                            Some((QryCol(*qid, *idx), *gid))
                        } else {
                            None
                        }
                    })
                    .collect();
                pullup_pred(self.qry_set, *qry_id, p_cols, &mut self.c_preds).branch()?;
            }
            OpKind::Sort { .. }
            | OpKind::Limit { .. }
            | OpKind::Setop(_)
            | OpKind::Attach(..)
            | OpKind::Scan(..)
            | OpKind::JoinGraph(_)
            | OpKind::Row(_)
            | OpKind::Empty => (),
        }
        ControlFlow::Continue(())
    }

    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match &mut op.kind {
            // bottom up to Proj means all predicates in current tree are pulled up
            // but no Filt operator to gather them, so add one.
            OpKind::Proj { input, .. } => {
                if !self.c_preds.is_empty() {
                    let mut pred = vec![];
                    for (QryCol(qid, idx), PartialExprSet(gid, pes)) in self.c_preds.drain() {
                        for pe in pes {
                            let new_e = ExprKind::pred_func(
                                pe.kind,
                                vec![
                                    ExprKind::query_col(gid, qid, idx),
                                    ExprKind::Const(pe.r_arg),
                                ],
                            );
                            pred.push(new_e);
                        }
                    }
                    let old = mem::take(input);
                    let filt = Op::new(OpKind::Filt { pred, input: old });
                    *input = Box::new(filt);
                }
            }
            OpKind::Aggr(aggr) => {
                if !self.c_preds.is_empty() {
                    for (QryCol(qid, idx), PartialExprSet(gid, pes)) in self.c_preds.drain() {
                        for pe in pes {
                            let new_e = ExprKind::pred_func(
                                pe.kind,
                                vec![
                                    ExprKind::query_col(gid, qid, idx),
                                    ExprKind::Const(pe.r_arg),
                                ],
                            );
                            aggr.filt.push(new_e);
                        }
                    }
                }
            }
            // bottom up and append all predicates to Filt
            OpKind::Filt { pred, .. } => {
                if !pred.is_empty() && !self.c_preds.is_empty() {
                    let new_preds = self.propagate_preds(pred);
                    for (QryCol(qid, idx), PartialExprSet(gid, pes)) in self.c_preds.drain() {
                        for pe in pes {
                            let new_e = ExprKind::pred_func(
                                pe.kind,
                                vec![
                                    ExprKind::query_col(gid, qid, idx),
                                    ExprKind::Const(pe.r_arg),
                                ],
                            );
                            pred.push(new_e);
                        }
                    }
                    for (gid, qid, idx, pe) in new_preds {
                        let new_e = ExprKind::pred_func(
                            pe.kind,
                            vec![
                                ExprKind::query_col(gid, qid, idx),
                                ExprKind::Const(pe.r_arg),
                            ],
                        );
                        pred.push(new_e);
                    }
                }
                self.collect_p_preds(pred).branch()?;
            }
            OpKind::Join(join) => match join.as_mut() {
                Join::Cross(_) => (), // bypass cross join
                // filt field in join is not processed, maybe enhance in future
                Join::Qualified(QualifiedJoin {
                    kind,
                    left,
                    right,
                    cond,
                    ..
                }) => match kind {
                    JoinKind::Inner => {
                        // Here we propagate predicates based on transitivity.
                        // e.g. join condition: a1=b1, predicate: a1>0, propagated: b1 > 0
                        let new_preds = self.propagate_preds(cond);
                        for (gid, qid, idx, pe) in new_preds {
                            let peset = self
                                .c_preds
                                .entry(QryCol(qid, idx))
                                .or_insert_with(|| PartialExprSet(gid, HashSet::new()));
                            peset.1.insert(pe);
                        }
                    }
                    JoinKind::Left => {
                        let new_preds = self.propagate_preds(cond);
                        let r_jo = self.stack.pop().unwrap();
                        if !new_preds.is_empty() {
                            // we need to specify which one belongs to right side
                            let mut r_qids = HashSet::new();
                            r_jo.collect_qry_ids(&mut r_qids);
                            // add right predicates to join condition, add others to c_preds
                            for (gid, qid, idx, pe) in new_preds {
                                if r_qids.contains(&qid) {
                                    let new_e = ExprKind::pred_func(
                                        pe.kind,
                                        vec![
                                            ExprKind::query_col(gid, qid, idx),
                                            ExprKind::Const(pe.r_arg),
                                        ],
                                    );
                                    cond.push(new_e);
                                } else {
                                    // all others belong to left side and can be pulled up
                                    let peset = self
                                        .c_preds
                                        .entry(QryCol(qid, idx))
                                        .or_insert_with(|| PartialExprSet(gid, HashSet::new()));
                                    peset.1.insert(pe);
                                }
                            }
                        }
                        *right = r_jo;
                    }
                    JoinKind::Full => {
                        let l_jo = self.stack.pop().unwrap();
                        let r_jo = self.stack.pop().unwrap();
                        *left = l_jo;
                        *right = r_jo;
                    }
                    _ => todo!(),
                },
            },
            OpKind::Sort { .. }
            | OpKind::Limit { .. }
            | OpKind::Setop(_)
            | OpKind::Attach(..)
            | OpKind::Scan(..)
            | OpKind::JoinGraph(_)
            | OpKind::Query(_)
            | OpKind::Row(_)
            | OpKind::Empty => (),
        }
        ControlFlow::Continue(())
    }
}

/// collect query columns in expression.
#[inline]
fn collect_non_aggr_qry_cols(e: &ExprKind, hs: &mut HashMap<QryCol, GlobalID>) {
    let mut c = CollectQryCols(hs);
    let _ = e.walk(&mut c);
}
struct CollectQryCols<'a>(&'a mut HashMap<QryCol, GlobalID>);

impl ExprVisitor<'_> for CollectQryCols<'_> {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<()> {
        match e {
            ExprKind::Col(Col {
                gid,
                kind: ColKind::Query(qry_id),
                idx,
            }) => {
                let qry_col = QryCol(*qry_id, *idx);
                self.0.insert(qry_col, *gid);
                ControlFlow::Continue(())
            }
            ExprKind::Aggf { .. } => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    }
}

// currently, we only support out column 4 patterns:
// col, col + const, col - const, const - col
#[inline]
fn translate_col(
    p_gid: GlobalID,
    p_qid: QueryID,
    p_idx: ColIndex,
    e: &ExprKind,
) -> Option<(QryCol, ExprKind)> {
    let res = match e {
        ExprKind::Col(Col {
            kind: ColKind::Query(c_qid),
            idx: c_idx,
            ..
        }) => {
            // direct mapping between columns: c_col -> p_col
            let new_e = ExprKind::query_col(p_gid, p_qid, p_idx);
            (QryCol(*c_qid, *c_idx), new_e)
        }
        ExprKind::Func { kind, args, .. } => match (kind, &args[..]) {
            (
                FuncKind::Add,
                [ExprKind::Col(Col {
                    kind: ColKind::Query(c_qid),
                    idx: c_idx,
                    ..
                }), c @ ExprKind::Const(_)],
            ) => {
                // "p_col = c_col + const" => "c_col = p_col - const"
                let new_e = ExprKind::func(
                    FuncKind::Sub,
                    vec![ExprKind::query_col(p_gid, p_qid, p_idx), c.clone()],
                );
                (QryCol(*c_qid, *c_idx), new_e)
            }
            (
                FuncKind::Sub,
                [ExprKind::Col(Col {
                    kind: ColKind::Query(c_qid),
                    idx: c_idx,
                    ..
                }), c @ ExprKind::Const(_)],
            ) => {
                // "p_col = c_col - const" => "c_col = p_col + const"
                let new_e = ExprKind::func(
                    FuncKind::Add,
                    vec![ExprKind::query_col(p_gid, p_qid, p_idx), c.clone()],
                );
                (QryCol(*c_qid, *c_idx), new_e)
            }
            (
                FuncKind::Sub,
                [c @ ExprKind::Const(_), ExprKind::Col(Col {
                    kind: ColKind::Query(c_qid),
                    idx: c_idx,
                    ..
                })],
            ) => {
                // "p_col = const - c_col" => "c_col = const - p_col"
                let new_e = ExprKind::func(
                    FuncKind::Sub,
                    vec![c.clone(), ExprKind::query_col(p_gid, p_qid, p_idx)],
                );
                (QryCol(*c_qid, *c_idx), new_e)
            }
            _ => return None,
        },
        _ => return None,
    };
    Some(res)
}

// currently, we only support out predicates with: =, !=, >, >=, <, <=
// todo: add in, between, like, regexp, etc.
#[inline]
fn translate_pred(
    c_pred: &ExprKind,
    mapping: &HashMap<QryCol, ExprKind>,
) -> Result<Option<(GlobalID, QueryID, ColIndex, PartialExpr)>> {
    let mut new_p = c_pred.clone();
    let res = update_simplify_nested(&mut new_p, NullCoalesce::False, |e| match e {
        ExprKind::Col(Col {
            kind: ColKind::Query(qry_id),
            idx,
            ..
        }) => {
            let qry_col = QryCol(*qry_id, *idx);
            if let Some(new_e) = mapping.get(&qry_col) {
                *e = new_e.clone();
                Ok(())
            } else {
                Err(Error::Break)
            }
        }
        _ => Ok(()),
    });
    match res {
        Ok(_) => match new_p {
            ExprKind::Pred(Pred::Func { kind, args }) => match (kind, &args[..]) {
                (
                    PredFuncKind::Equal
                    | PredFuncKind::Greater
                    | PredFuncKind::GreaterEqual
                    | PredFuncKind::Less
                    | PredFuncKind::LessEqual
                    | PredFuncKind::NotEqual,
                    [ExprKind::Col(Col {
                        gid,
                        kind: ColKind::Query(qry_id),
                        idx,
                    }), ExprKind::Const(c)],
                ) => {
                    let partial_e = PartialExpr {
                        kind,
                        r_arg: c.clone(),
                    };
                    Ok(Some((*gid, *qry_id, *idx, partial_e)))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        },
        Err(Error::Break) => Ok(None),
        Err(e) => Err(e),
    }
}

struct PartialExprSet(GlobalID, HashSet<PartialExpr>);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{assert_j_plan1, get_subq_filt_expr, j_catalog, print_plan};

    #[test]
    fn test_pred_pullup_cross_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(3, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0 and c0 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(3, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1+1 as c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(3, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1-1 as c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(3, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select 1-c1 as c1 from t1 where c1 = 0) t1, t2 where t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(3, filt.len());
            },
        );
    }

    #[test]
    fn test_pred_pullup_inner_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1 join t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(2, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 join (select c1 from t2 where c1 = 0) t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(2, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c0, c1 from t1 where c1 = 0 and c0 > 5) t1 join t2 on t1.c1 = t2.c1 and t1.c0 = t2.c0",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(4, filt.len());
            },
        );
    }

    #[test]
    fn test_pred_pullup_left_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1 left join t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(1, filt.len());
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join (select c1 from t2 where c1 = 0) t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert!(filt.is_empty())
            },
        );
    }

    #[test]
    fn test_pred_pullup_full_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1 full join t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert!(filt.is_empty())
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c1 from t1 where c1 = 0) t1 full join (select c1 from t2 where c1 > 0) t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert!(filt.is_empty())
            },
        );
    }

    #[test]
    fn test_pred_pullup_aggr() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from (select c1, count(*) as c2 from t1 group by c1 having count(*) > 0 and c1 > 1) t1 join t2 on t1.c1 = t2.c1",
            |sql, mut plan| {
                pred_pullup(&mut plan.qry_set, plan.root).unwrap();
                print_plan(&sql, &plan);
                let filt = get_subq_filt_expr(plan.root_query().unwrap());
                assert_eq!(2, filt.len())
            },
        );
    }
}
