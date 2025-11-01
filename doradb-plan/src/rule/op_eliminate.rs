use crate::error::{Error, Result};
use crate::join::{Join, JoinKind, JoinOp, QualifiedJoin};
use crate::lgc::{Op, OpKind, OpMutVisitor, QuerySet, Setop, SetopKind};
use crate::rule::RuleEffect;
use crate::rule::expr_simplify::{NullCoalesce, update_simplify_nested};
use doradb_expr::controlflow::{Branch, ControlFlow, Unbranch};
use doradb_expr::{Col, ColKind, Const, ExprKind, QueryID, Setq};
use std::collections::HashSet;
use std::mem;

/// Eliminate redundant operators.
/// 1. Filter with true predicate can be removed.
/// 2. Join with some empty child can be rewritten.
/// 3. Setop with some empty child can be rewritten.
/// 4. Aggr with empty child can be directly evaluated. (todo)
/// 5. Any other operators except Join, Setop, Aggr with empty child can be replaced with Empty.
/// 6. LIMIT 0 can eliminiate entire tree.
/// 7. ORDER BY in subquery without LIMIT can be removed.
/// 8. join with false/null condition can be removed.
#[inline]
pub fn op_eliminate(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<RuleEffect> {
    eliminate_op(qry_set, qry_id, false)
}

#[inline]
fn eliminate_op(qry_set: &mut QuerySet, qry_id: QueryID, is_subq: bool) -> Result<RuleEffect> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut eo = EliminateOp::new(qry_set, is_subq);
        op.walk_mut(&mut eo).unbranch()
    })?
}

struct EliminateOp<'a> {
    qry_set: &'a mut QuerySet,
    is_subq: bool,
    has_limit: bool,
    empty_qs: HashSet<QueryID>,
}

impl<'a> EliminateOp<'a> {
    #[inline]
    fn new(qry_set: &'a mut QuerySet, is_subq: bool) -> Self {
        EliminateOp {
            qry_set,
            is_subq,
            has_limit: false,
            empty_qs: HashSet::new(),
        }
    }

    #[inline]
    fn bottom_up(&mut self, op: &mut Op) -> ControlFlow<Error, RuleEffect> {
        let mut eff = RuleEffect::empty();
        match &mut op.kind {
            OpKind::Join(j) => match j.as_mut() {
                Join::Cross(tbls) => {
                    if tbls.iter().any(|t| t.is_empty()) {
                        // any child in cross join results in empty rows
                        *op = Op::empty();
                        eff |= RuleEffect::OP;
                    }
                }
                Join::Qualified(QualifiedJoin {
                    kind,
                    left:
                        JoinOp(Op {
                            kind: OpKind::Empty,
                            ..
                        }),
                    right: JoinOp(right_op),
                    ..
                }) => match kind {
                    JoinKind::Inner
                    | JoinKind::Left
                    | JoinKind::Semi
                    | JoinKind::AntiSemi
                    | JoinKind::Mark
                    | JoinKind::Single => *op = Op::empty(),
                    JoinKind::Full => {
                        if right_op.is_empty() {
                            *op = Op::empty();
                            eff |= RuleEffect::OP;
                        } else {
                            // As left table is empty, we can convert full join to single table,
                            // when bottom up, rewrite all columns derived from right table to null
                            let new = mem::take(right_op);
                            *op = new;
                            eff |= RuleEffect::OP;
                            return ControlFlow::Continue(eff); // skip the cleansing because left table won't contain right columns.
                        }
                    }
                },
                Join::Qualified(QualifiedJoin {
                    kind,
                    left: JoinOp(left_op),
                    right:
                        JoinOp(Op {
                            kind: OpKind::Empty,
                            ..
                        }),
                    ..
                }) => match kind {
                    JoinKind::Inner
                    | JoinKind::Semi
                    | JoinKind::AntiSemi
                    | JoinKind::Mark
                    | JoinKind::Single => *op = Op::empty(),
                    JoinKind::Left | JoinKind::Full => {
                        // similar to left table case, we replace current op with the other child,
                        // and clean up when bottom up.
                        let new = mem::take(left_op);
                        *op = new;
                        eff |= RuleEffect::OP;
                        return ControlFlow::Continue(eff);
                    }
                },
                _ => (),
            },
            OpKind::Setop(so) => {
                let Setop {
                    kind,
                    q,
                    left,
                    right,
                    ..
                } = so.as_mut();
                match (&mut left.kind, &mut right.kind) {
                    (OpKind::Empty, OpKind::Empty) => {
                        *op = Op::empty();
                        eff |= RuleEffect::OP;
                    }
                    (OpKind::Empty, right) => match (kind, q) {
                        (SetopKind::Union, Setq::All) => {
                            *op = Op::new(mem::take(right));
                            eff |= RuleEffect::OP;
                        }
                        (SetopKind::Except | SetopKind::Intersect, Setq::All) => {
                            *op = Op::empty();
                            eff |= RuleEffect::OP;
                        }
                        _ => (),
                    },
                    (left, OpKind::Empty) => {
                        if *q == Setq::All {
                            *op = Op::new(mem::take(left));
                            eff |= RuleEffect::OP;
                        }
                    }
                    _ => (),
                }
            }
            OpKind::Limit { input, .. } => {
                if input.is_empty() {
                    *op = Op::empty();
                    eff |= RuleEffect::OP;
                }
            }
            OpKind::Sort { input, .. } => {
                if input.is_empty() {
                    *op = Op::empty();
                    eff |= RuleEffect::OP;
                }
            }
            OpKind::Aggr(_) => (), // todo: leave aggr as is, and optimize later
            OpKind::Proj { input, .. } => {
                if input.is_empty() {
                    *op = Op::empty();
                    eff |= RuleEffect::OP;
                }
            }
            OpKind::Filt { input, .. } => {
                if input.is_empty() {
                    *op = Op::empty();
                    eff |= RuleEffect::OP;
                }
            }
            OpKind::Attach(input, _) => {
                if input.is_empty() {
                    *op = Op::empty();
                    eff |= RuleEffect::OP;
                }
            }
            OpKind::Query(_) | OpKind::Scan(..) | OpKind::Row(_) | OpKind::JoinGraph(_) => {
                unreachable!()
            }
            OpKind::Empty => (),
        }
        if !op.is_empty() && !self.empty_qs.is_empty() {
            // current operator is not eliminated but we have some child query set to empty,
            // all column references to it must be set to null
            let qs = &self.empty_qs;
            for e in op.kind.exprs_mut() {
                eff |= update_simplify_nested(e, NullCoalesce::Null, |e| {
                    if let ExprKind::Col(Col {
                        kind: ColKind::Query(qry_id),
                        ..
                    }) = e
                        && qs.contains(qry_id) {
                            *e = ExprKind::const_null();
                        }
                    Ok(())
                })
                .branch()?;
            }
        }
        ControlFlow::Continue(eff)
    }
}

impl OpMutVisitor for EliminateOp<'_> {
    type Cont = RuleEffect;
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error, RuleEffect> {
        let mut eff = RuleEffect::empty();
        match &mut op.kind {
            OpKind::Filt { pred, input } => {
                if pred.is_empty() {
                    // no predicates, remove current filter
                    *op = Op::empty();
                    eff |= RuleEffect::OP;
                } else {
                    match pair_const_false(pred) {
                        (false, _) => (),
                        (true, true) => {
                            *op = Op::empty();
                            eff |= RuleEffect::OP;
                        }
                        (true, false) => {
                            let input = mem::take(input.as_mut());
                            *op = input;
                            eff |= RuleEffect::OP;
                            eff |= self.enter(op)?;
                            return ControlFlow::Continue(eff);
                        }
                    }
                }
            }
            OpKind::Join(join) => match join.as_mut() {
                Join::Cross(_) => (),
                Join::Qualified(QualifiedJoin {
                    kind: JoinKind::Inner,
                    cond,
                    ..
                }) => match pair_const_false(cond) {
                    (false, _) => (),
                    (true, true) => {
                        *op = Op::empty();
                        eff |= RuleEffect::OP;
                    }
                    (true, false) => {
                        cond.clear();
                    }
                },
                Join::Qualified(QualifiedJoin {
                    kind: JoinKind::Left | JoinKind::Full,
                    filt,
                    ..
                }) => match pair_const_false(filt) {
                    (false, _) => (),
                    (true, true) => {
                        *op = Op::empty();
                        eff |= RuleEffect::OP;
                    }
                    (true, false) => {
                        filt.clear();
                    }
                },
                _ => todo!(),
            },
            OpKind::Limit { start, end, .. } => {
                if *start == *end {
                    *op = Op::empty();
                    eff |= RuleEffect::OP;
                } else {
                    self.has_limit = true;
                }
            }
            OpKind::Sort { input, .. } => {
                if self.is_subq && !self.has_limit {
                    // in case subquery that does not have limit upon sort,
                    // sort can be eliminated.
                    let input = mem::take(input.as_mut());
                    *op = input;
                    eff |= RuleEffect::OP;
                    eff |= self.enter(op)?;
                    return ControlFlow::Continue(eff);
                }
            }
            OpKind::Query(query_id) => {
                eff |= eliminate_op(self.qry_set, *query_id, true).branch()?;
            }
            _ => (),
        };
        // always returns true, even if current node is updated in-place, we still
        // need to traverse back to eliminate entire tree if possible.
        ControlFlow::Continue(eff)
    }

    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error, RuleEffect> {
        let mut eff = RuleEffect::empty();
        match &mut op.kind {
            OpKind::Query(qry_id) => {
                if let Some(subq) = self.qry_set.get(qry_id)
                    && subq.root.is_empty() {
                        self.empty_qs.insert(*qry_id);
                        *op = Op::empty();
                        eff |= RuleEffect::OP;
                    }
            }
            OpKind::Scan(..) | OpKind::Row(_) => (),
            _ => {
                eff |= self.bottom_up(op)?;
            }
        }
        ControlFlow::Continue(eff)
    }
}

// treat null as false
#[inline]
fn pair_const_false(es: &[ExprKind]) -> (bool, bool) {
    match es {
        [ExprKind::Const(Const::Null)] => (true, true),
        [ExprKind::Const(c)] => (true, c.is_zero().unwrap_or_default()),
        _ => (false, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{assert_j_plan1, get_lvl_queries, j_catalog, print_plan};
    use crate::lgc::{LgcPlan, OpTy, preorder};

    #[test]
    fn test_op_eliminate_false_pred() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select c1 from t1 where null", assert_empty_root);
        assert_j_plan1(&cat, "select c1 from t1 where false", assert_empty_root);
    }

    #[test]
    fn test_op_eliminate_true_pred() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select c1 from t1 where true", |s, mut q| {
            op_eliminate(&mut q.qry_set, q.root).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            if let OpKind::Proj { input, .. } = &subq.root.kind {
                assert_eq!(input.ty(), OpTy::Query);
            } else {
                panic!("fail")
            }
        })
    }

    #[test]
    fn test_op_eliminate_derived_table() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c1 from (select c1 as c1 from t1 where null) x1",
            assert_empty_root,
        );
        assert_j_plan1(
            &cat,
            "select c1 from (select c1 as c1 from (select c1 from t2 limit 0) x2) x1",
            assert_empty_root,
        );
        assert_j_plan1(
            &cat,
            "select c1 from (select c1 from t1 where null) x1 where c1 > 0 order by c1 having c1 > 2 limit 1",
            assert_empty_root,
        );
    }

    #[test]
    fn test_op_eliminate_order_by() {
        let cat = j_catalog();
        // remove ORDER BY in subquery
        assert_j_plan1(
            &cat,
            "select c1 from (select c1 from t1 order by c0) x1",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subqs = get_lvl_queries(&q, 1);
                assert_eq!(subqs.len(), 1);
                assert_eq!(subqs[0].root.ty(), OpTy::Proj);
            },
        );
        // do NOT remove ORDER BY because of LIMIT exists
        assert_j_plan1(
            &cat,
            "select c1 from (select c1 from t1 order by c0 limit 1) x1",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subqs = get_lvl_queries(&q, 1);
                assert_eq!(subqs.len(), 1);
                if let OpKind::Limit { input, .. } = &subqs[0].root.kind {
                    assert_eq!(input.ty(), OpTy::Sort);
                } else {
                    panic!("fail")
                }
            },
        );
    }

    #[test]
    fn test_op_eliminate_limit() {
        let cat = j_catalog();
        // eliminate entire tree if LIMIT 0
        assert_j_plan1(&cat, "select c1 from t1 limit 0", |s, mut q| {
            op_eliminate(&mut q.qry_set, q.root).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            assert!(subq.root.is_empty());
        });
        assert_j_plan1(&cat, "select c1 from t1 limit 0 offset 3", |s, mut q| {
            op_eliminate(&mut q.qry_set, q.root).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            assert!(subq.root.is_empty());
        });
        assert_j_plan1(
            &cat,
            "select c1 from t1 where null order by c1 limit 10",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                assert!(subq.root.is_empty());
            },
        );
        // do NOT eliminate if LIMIT non-zero
        assert_j_plan1(&cat, "select c1 from t1 limit 1", |s, mut q| {
            op_eliminate(&mut q.qry_set, q.root).unwrap();
            print_plan(s, &q);
            let subq = q.root_query().unwrap();
            assert_eq!(subq.root.ty(), OpTy::Limit);
        });
    }

    #[test]
    fn test_op_eliminate_union_all() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c1 from t1 where false union all select c1 from t1 limit 0",
            assert_empty_root,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where false union all select c1 from t1",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                let _ = subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 union all select c1 from t1 limit 0",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                let _ = subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
    }

    #[test]
    fn test_op_eliminate_except_all() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c1 from t1 where false except all select c1 from t1",
            assert_empty_root,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 except all select c1 from t1 where null",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                let _ = subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
    }

    #[test]
    fn test_op_eliminate_intersect_all() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c1 from t1 where false intersect all select c1 from t1",
            assert_empty_root,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 intersect all select c1 from t1 where null",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                let _ = subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Setop(_) => panic!("fail to eliminate setop"),
                    _ => (),
                }));
            },
        );
    }

    // cross join
    #[test]
    fn test_op_eliminate_cross_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1 where null) x1, (select * from t2 where false) x2",
            assert_empty_root,
        );
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1) x1, (select * from t2 limit 0) x2",
            assert_empty_root,
        );
    }

    // inner join
    #[test]
    fn test_op_eliminate_inner_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1 where null) x1 join (select * from t2) x2",
            assert_empty_root,
        );
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1) x1 join (select * from t2 limit 0) x2",
            assert_empty_root,
        );
    }

    // left join
    #[test]
    fn test_op_eliminate_left_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1 where null) x1 left join (select * from t2) x2",
            assert_empty_root,
        );
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1) x1 left join (select * from t2 limit 0) x2",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                let _ = subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Join(_) => panic!("fail to eliminate op"),
                    _ => (),
                }));
                if let OpKind::Proj { cols, .. } = &subq.root.kind {
                    assert_eq!(&cols[0].expr, &ExprKind::const_null());
                } else {
                    panic!("fail")
                }
            },
        );
    }

    // right join
    #[test]
    fn test_op_eliminate_right_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1 where null) x1 right join (select * from t2) x2",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                let _ = subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Join(_) => panic!("fail to eliminate op"),
                    _ => (),
                }));
                if let OpKind::Proj { cols, .. } = &subq.root.kind {
                    assert_ne!(&cols[0].expr, &ExprKind::const_null());
                } else {
                    panic!("fail")
                }
            },
        );
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1) x1 right join (select * from t2 where false) x2",
            assert_empty_root,
        );
    }

    // full join
    #[test]
    fn test_op_eliminate_full_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select x1.c1, c2 from (select c1 from t1 where null) x1 full join (select * from t2) x2",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                let _ = subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Join(_) => panic!("fail to eliminate op"),
                    _ => (),
                }));
                if let OpKind::Proj { cols, .. } = &subq.root.kind {
                    assert_eq!(&cols[0].expr, &ExprKind::const_null());
                    assert_ne!(&cols[1].expr, &ExprKind::const_null());
                } else {
                    panic!("fail")
                }
            },
        );
        assert_j_plan1(
            &cat,
            "select x1.c1, c2 from (select c1 from t1) x1 full join (select * from t2 where false) x2",
            |s, mut q| {
                op_eliminate(&mut q.qry_set, q.root).unwrap();
                print_plan(s, &q);
                let subq = q.root_query().unwrap();
                let _ = subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Join(_) => panic!("fail to eliminate op"),
                    _ => (),
                }));
                if let OpKind::Proj { cols, .. } = &subq.root.kind {
                    assert_ne!(&cols[0].expr, &ExprKind::const_null());
                    assert_eq!(&cols[1].expr, &ExprKind::const_null());
                } else {
                    panic!("fail")
                }
            },
        );
        assert_j_plan1(
            &cat,
            "select c2 from (select c1 from t1 limit 0) x1 full join (select * from t2 where false) x2",
            assert_empty_root,
        );
    }

    fn assert_empty_root(s1: &str, mut q1: LgcPlan) {
        op_eliminate(&mut q1.qry_set, q1.root).unwrap();
        print_plan(s1, &q1);
        let root = &q1.root_query().unwrap().root;
        assert!(root.is_empty());
    }
}
