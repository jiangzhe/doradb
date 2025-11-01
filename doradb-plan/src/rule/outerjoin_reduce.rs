use crate::error::{Error, Result};
use crate::join::{Join, JoinKind, QualifiedJoin};
use crate::lgc::{Location, Op, OpKind, OpMutVisitor, ProjCol, QuerySet};
use crate::rule::expr_simplify::{simplify_single, NullCoalesce};
use crate::rule::RuleEffect;
use doradb_expr::controlflow::{Branch, ControlFlow, Unbranch};
use doradb_expr::fold::Fold;
use doradb_expr::{Col, ColKind, ExprExt, ExprKind, ExprMutVisitor, QueryID};
use std::collections::{HashMap, HashSet};
use std::mem;

/// Reduce outer join based on predicate analysis.
/// This rule recognize null rejecting predicates in advance
/// and converting corresponding join types.
/// The fully predicates pushdown will be applied after
/// join graph initialization.
#[inline]
pub fn outerjoin_reduce(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<RuleEffect> {
    reduce_outerjoin(qry_set, qry_id, None)
}

#[inline]
fn reduce_outerjoin(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    rn_map: Option<&HashMap<QueryID, Vec<ExprKind>>>,
) -> Result<RuleEffect> {
    qry_set.transform_op(qry_id, |qry_set, loc, op| {
        if loc != Location::Intermediate {
            return Ok(RuleEffect::empty());
        }
        // as entering a new query, original reject-null exprs should be translated in current scope
        // and check whether it still rejects null, and initialize new rn_map.
        let mut rn_map = if let Some(exprs) = rn_map.and_then(|m| m.get(&qry_id)) {
            let out_cols = op.out_cols().unwrap(); // won't fail
            translate_rn_exprs(qry_id, exprs, out_cols)?
        } else {
            HashMap::new()
        };
        let mut r = Reduce {
            qry_set,
            rn_map: &mut rn_map,
        };
        op.walk_mut(&mut r).unbranch()
    })?
}

struct Reduce<'a> {
    qry_set: &'a mut QuerySet,
    rn_map: &'a mut HashMap<QueryID, Vec<ExprKind>>,
}

impl OpMutVisitor for Reduce<'_> {
    type Cont = RuleEffect;
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error, RuleEffect> {
        let mut eff = RuleEffect::empty();
        match &mut op.kind {
            OpKind::Filt { pred, .. } => analyze_conj_preds(pred, self.rn_map).branch()?,
            OpKind::Aggr(aggr) => analyze_conj_preds(&aggr.filt, self.rn_map).branch()?,
            OpKind::Query(qry_id) => {
                eff |= reduce_outerjoin(self.qry_set, *qry_id, Some(self.rn_map)).branch()?;
            }
            OpKind::Join(join) => match join.as_mut() {
                Join::Cross(_) => (), // do not operate on cross join
                Join::Qualified(QualifiedJoin {
                    kind,
                    left,
                    right,
                    cond,
                    filt,
                    ..
                }) => {
                    // always analyze post filter at first
                    analyze_conj_preds(filt, self.rn_map).branch()?;
                    match kind {
                        JoinKind::Left => {
                            let mut right_qids = HashSet::new();
                            right.as_ref().collect_qry_ids(&mut right_qids);
                            if right_qids.iter().any(|qid| self.rn_map.contains_key(qid)) {
                                // null rejecting predicates on right table, convert join type to inner join
                                *kind = JoinKind::Inner;
                                eff |= RuleEffect::OP;
                                analyze_conj_preds(cond, self.rn_map).branch()?;
                                // merge filters into conditions
                                if !filt.is_empty() {
                                    cond.extend(mem::take(filt));
                                    eff |= RuleEffect::EXPR;
                                }
                            } else {
                                // no such condition, keep join type as is.
                                // condition that involves right qids should be analyzed
                                // "a LEFT JOIN (SELECT c.c2 FROM B LEFT JOIN c) x ON a.c1 = x.c2"
                                for qid in &right_qids {
                                    for e in &*cond {
                                        if reject_null_single(e, *qid).branch()? {
                                            self.rn_map.entry(*qid).or_default().push(e.clone())
                                        }
                                    }
                                }
                            }
                        }
                        JoinKind::Full => {
                            let mut left_qids = HashSet::new();
                            left.as_ref().collect_qry_ids(&mut left_qids);
                            let mut right_qids = HashSet::new();
                            right.as_ref().collect_qry_ids(&mut right_qids);
                            let l_rn = left_qids.iter().any(|qid| self.rn_map.contains_key(qid));
                            let r_rn = right_qids.iter().any(|qid| self.rn_map.contains_key(qid));
                            match (l_rn, r_rn) {
                                (true, true) => {
                                    // both sides reject null, convert to inner join
                                    *kind = JoinKind::Inner;
                                    eff |= RuleEffect::OP;
                                    analyze_conj_preds(cond, self.rn_map).branch()?;
                                    if !filt.is_empty() {
                                        cond.extend(mem::take(filt));
                                        eff |= RuleEffect::EXPR;
                                    }
                                }
                                (false, true) => {
                                    // right side rejects null, convert to left join
                                    *kind = JoinKind::Left;
                                    eff |= RuleEffect::OP;
                                    // analyze conditions of right side
                                    for qid in &right_qids {
                                        for e in &*cond {
                                            if reject_null_single(e, *qid).branch()? {
                                                self.rn_map.entry(*qid).or_default().push(e.clone())
                                            }
                                        }
                                    }
                                }
                                (true, false) => {
                                    // left side reject null, convert to right join.
                                    // we do not support right join, swap both side and change to left join
                                    *kind = JoinKind::Left;
                                    mem::swap(left, right);
                                    eff |= RuleEffect::OP;
                                    // analyze conditions of (original) left side
                                    for qid in &left_qids {
                                        for e in &*cond {
                                            if reject_null_single(e, *qid).branch()? {
                                                self.rn_map.entry(*qid).or_default().push(e.clone())
                                            }
                                        }
                                    }
                                }
                                (false, false) => (), // cannot change anything
                            }
                        }
                        JoinKind::Inner => {
                            analyze_conj_preds(cond, self.rn_map).branch()?;
                            if !filt.is_empty() {
                                cond.extend(mem::take(filt));
                                eff |= RuleEffect::EXPR;
                            }
                        }
                        _ => todo!(),
                    }
                }
            },
            OpKind::JoinGraph(_) => {
                unreachable!("Outerjoin reduce should be applied before initializing join graph")
            }
            OpKind::Proj { .. }
            | OpKind::Sort { .. }
            | OpKind::Limit { .. }
            | OpKind::Empty
            | OpKind::Setop(_)
            | OpKind::Attach(..) => (),
            OpKind::Scan(..) | OpKind::Row(_) => unreachable!(),
        }
        ControlFlow::Continue(eff)
    }
}

#[inline]
fn analyze_conj_preds(
    exprs: &[ExprKind],
    rn_map: &mut HashMap<QueryID, Vec<ExprKind>>,
) -> Result<()> {
    if exprs.is_empty() {
        return Ok(());
    }
    let mut tmp = HashSet::new();
    for e in exprs {
        tmp.clear();
        e.collect_qry_ids(&mut tmp);
        for qid in &tmp {
            if reject_null_single(e, *qid)? {
                rn_map.entry(*qid).or_default().push(e.clone());
            }
        }
    }
    Ok(())
}

// translate reject null expressions,
// returns the expression set that still rejects null
#[inline]
fn translate_rn_exprs(
    qry_id: QueryID,
    exprs: &[ExprKind],
    mapping: &[ProjCol],
) -> Result<HashMap<QueryID, Vec<ExprKind>>> {
    let mut res: HashMap<QueryID, Vec<ExprKind>> = HashMap::new();
    let mut tmp = HashSet::new();
    for e in exprs {
        let mut new_e = e.clone();
        tmp.clear(); // clear and reuse the hash set
        let mut tcs = TransformCollectSimplify {
            old: qry_id,
            new_ids: &mut tmp,
            mapping,
        };
        let _ = new_e.walk_mut(&mut tcs);
        // skip constant expression or expression without column
        if new_e.is_const() || tmp.is_empty() {
            continue;
        }
        for new_qid in &tmp {
            if reject_null_single(&new_e, *new_qid)? {
                res.entry(*new_qid).or_default().push(new_e.clone());
            }
        }
    }
    Ok(res)
}

#[inline]
pub(crate) fn reject_null_single(expr: &ExprKind, qry_id: QueryID) -> Result<bool> {
    expr.clone()
        .reject_null(|e| {
            if let ExprKind::Col(Col {
                kind: ColKind::Query(qid),
                ..
            }) = e
                && *qid == qry_id {
                    *e = ExprKind::const_null();
                }
        })
        .map_err(Into::into)
}

// transform, collect and simplify.
// The order is importatnt: we should perform transformation in preorder,
// so that further collecting can be performed on new expression.
// finally perform simplification.
struct TransformCollectSimplify<'a> {
    old: QueryID,
    new_ids: &'a mut HashSet<QueryID>,
    mapping: &'a [ProjCol],
}

impl ExprMutVisitor for TransformCollectSimplify<'_> {
    // rewrite on cloned expression, no effect
    type Cont = ();
    type Break = Error;
    #[inline]
    fn enter(&mut self, e: &mut ExprKind) -> ControlFlow<Error> {
        if let ExprKind::Col(Col {
            kind: ColKind::Query(qry_id),
            idx,
            ..
        }) = e
            && *qry_id == self.old {
                let mut new_e = self.mapping[idx.value() as usize].expr.clone();
                simplify_single(&mut new_e, NullCoalesce::Null).branch()?;
                *e = new_e;
            }
        ControlFlow::Continue(())
    }

    #[inline]
    fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<Error> {
        match e {
            ExprKind::Col(Col {
                kind: ColKind::Query(qry_id),
                ..
            }) => {
                self.new_ids.insert(*qry_id);
            }
            _ => {
                let _ = simplify_single(e, NullCoalesce::Null).branch()?;
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{
        assert_j_plan1, extract_join_kinds, get_lvl_queries, j_catalog, print_plan,
    };
    use crate::lgc::LgcPlan;

    #[test]
    fn test_outerjoin_reduce_left_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where t2.c2 = 0",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 inner join t3 where t2.c2 = 0",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 inner join t3 on t2.c2 = t3.c2",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 left join t3 where t2.c2 = t3.c2",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 left join t3 where t1.c1 = t3.c3",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 left join t3 group by t3.c3 having t3.c3 > 0",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where t1.c1 > 0",
            assert_left,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select c2 from t1 left join t2) tt where tt.c2 > 0",
            assert_subq_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select t1.c1, c2 from t1 left join t2) tt where tt.c1 > c2 + 1",
            assert_subq_inner,
        );
    }

    #[test]
    fn test_outerjoin_reduce_right_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 where t1.c1 = 0",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 right join t3 where t1.c1 = 0",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 inner join t3 where t1.c1 = 0",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 inner join t3 where t1.c1 = t3.c3",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 where t2.c2 = 0",
            assert_left,
        );
    }

    #[test]
    fn test_outerjoin_reduce_full_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 > t2.c2",
            assert_inner,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 full join t3 where t1.c1 > t2.c2 and t2.c2 > t3.c3",
            assert_inner,
        );
        // not reject null, join type not change
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 is null or t2.c2 is null",
            assert_full,
        );
        // right side rejects null, convert to left join
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t2.c2 > 0",
            assert_left,
        );
        // left side rejects null, convert to right join, then to left join
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 > 0",
            assert_left,
        );
        assert_j_plan1(
            &cat,
            "select c1, c2 from (select t1.c1, t2.c2 from t1 full join t2 where t1.c1 is null or t2.c2 is null) tt where c1 = c2",
            assert_subq_inner,
        );
    }

    #[test]
    fn test_outerjoin_reduce_cross_join() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select 1 from t1, t2 where t1.c1 = 0", assert_cross);
    }

    fn assert_inner(sql: &str, mut plan: LgcPlan) {
        outerjoin_reduce(&mut plan.qry_set, plan.root).unwrap();
        print_plan(sql, &plan);
        let subq = plan.root_query().unwrap();
        let joins = extract_join_kinds(&subq.root);
        assert!(joins.into_iter().all(|k| k == "inner"));
    }

    fn assert_subq_inner(sql: &str, mut plan: LgcPlan) {
        outerjoin_reduce(&mut plan.qry_set, plan.root).unwrap();
        print_plan(sql, &plan);
        // let subq = plan.root_query().unwrap();
        let subq = get_lvl_queries(&plan, 1);
        let joins = extract_join_kinds(&subq[0].root);
        assert!(joins.into_iter().all(|k| k == "inner"));
    }

    fn assert_left(sql: &str, mut plan: LgcPlan) {
        outerjoin_reduce(&mut plan.qry_set, plan.root).unwrap();
        print_plan(sql, &plan);
        let subq = plan.root_query().unwrap();
        let joins = extract_join_kinds(&subq.root);
        assert!(joins.into_iter().all(|k| k == "left"));
    }

    fn assert_full(sql: &str, mut plan: LgcPlan) {
        outerjoin_reduce(&mut plan.qry_set, plan.root).unwrap();
        print_plan(sql, &plan);
        let subq = plan.root_query().unwrap();
        let joins = extract_join_kinds(&subq.root);
        assert!(joins.into_iter().all(|k| k == "full"));
    }

    fn assert_cross(sql: &str, mut plan: LgcPlan) {
        outerjoin_reduce(&mut plan.qry_set, plan.root).unwrap();
        print_plan(sql, &plan);
        let subq = plan.root_query().unwrap();
        let joins = extract_join_kinds(&subq.root);
        assert!(joins.into_iter().all(|k| k == "cross"));
    }
}
