use crate::error::{Error, Result};
use crate::join::{Join, JoinKind, JoinOp, QualifiedJoin};
use crate::lgc::{Op, OpKind, OpMutVisitor, ProjCol, QryIDs, QuerySet};
use crate::rule::expr_simplify::{simplify_nested, NullCoalesce};
use crate::rule::RuleEffect;
use doradb_expr::controlflow::{Branch, ControlFlow, Unbranch};
use doradb_expr::fold::Fold;
use doradb_expr::{Col, ColKind, Const, ExprExt, ExprKind, ExprMutVisitor, ExprVisitor, QueryID};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem;

/// Pushdown predicates.
#[inline]
pub fn pred_pushdown(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<RuleEffect> {
    pushdown_pred(qry_set, qry_id)
}

#[inline]
fn pushdown_pred(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<RuleEffect> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut ppd = PredPushdown { qry_set };
        op.walk_mut(&mut ppd).unbranch()
    })?
}

struct PredPushdown<'a> {
    qry_set: &'a mut QuerySet,
}

impl OpMutVisitor for PredPushdown<'_> {
    type Cont = RuleEffect;
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error, RuleEffect> {
        let mut eff = RuleEffect::empty();
        match &mut op.kind {
            OpKind::Filt { pred, input } => {
                let mut fallback = vec![];
                let mut exprs = mem::take(pred);
                loop {
                    let orig_count = exprs.len();
                    for e in exprs {
                        // todo: we do not support subquery push, in future we will
                        // always unnest subqueries, this will not be a problem.
                        let mut item = ExprItem::new(e);
                        if item.load_attr().has_subq {
                            fallback.push(item.e);
                            continue;
                        }
                        match push_single(self.qry_set, input, item).branch()? {
                            (e, None) => {
                                // push succeeds
                                eff |= e;
                                eff |= RuleEffect::EXPR;
                            }
                            (e, Some(p)) => {
                                // child rejects the predicate, add it to fallback
                                eff |= e;
                                fallback.push(p.e);
                            }
                        }
                    }
                    if fallback.is_empty() {
                        // all predicates are pushed down, current operator can be removed.
                        let input = mem::take(input.as_mut());
                        *op = input;
                        eff |= RuleEffect::OP;
                        eff |= self.enter(op)?;
                        break;
                    } else if fallback.len() == orig_count {
                        // still have certain predicates can not be pushed down,
                        // and no further progress could be made.
                        *pred = fallback;
                        break;
                    } else {
                        // can still make progress, retry
                        exprs = mem::take(&mut fallback);
                    }
                }
            }
            OpKind::Query(qry_id) => {
                eff |= pushdown_pred(self.qry_set, *qry_id).branch()?;
            }
            _ => (),
        }
        ControlFlow::Continue(eff)
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

#[derive(Clone)]
struct ExprItem {
    e: ExprKind,
    // lazy field
    attr: Option<ExprAttr>,
    reject_nulls: Option<HashMap<QueryID, bool>>,
}

impl ExprItem {
    #[inline]
    fn new(e: ExprKind) -> Self {
        ExprItem {
            e,
            attr: None,
            reject_nulls: None,
        }
    }

    #[inline]
    fn load_attr(&mut self) -> &ExprAttr {
        if self.attr.is_none() {
            let mut attr = ExprAttr::default();
            let _ = self.e.walk(&mut attr);
            self.attr = Some(attr);
        }
        self.attr.as_ref().unwrap() // won't fail
    }

    #[inline]
    fn load_reject_null(&mut self, qry_id: QueryID) -> Result<bool> {
        if let Some(reject_nulls) = &mut self.reject_nulls {
            let res = match reject_nulls.entry(qry_id) {
                Entry::Occupied(occ) => *occ.get(),
                Entry::Vacant(vac) => {
                    let rn = self.e.clone().reject_null(|e| match e {
                        ExprKind::Col(Col {
                            kind: ColKind::Query(qid),
                            ..
                        }) if *qid == qry_id => {
                            *e = ExprKind::const_null();
                        }
                        _ => (),
                    })?;
                    vac.insert(rn);
                    rn
                }
            };
            Ok(res)
        } else {
            let mut reject_nulls = HashMap::new();
            let rn = self.e.clone().reject_null(|e| match e {
                ExprKind::Col(Col {
                    kind: ColKind::Query(qid),
                    ..
                }) if *qid == qry_id => {
                    *e = ExprKind::const_null();
                }
                _ => (),
            })?;
            reject_nulls.insert(qry_id, rn);
            self.reject_nulls = Some(reject_nulls);
            Ok(rn)
        }
    }

    #[inline]
    fn rewrite(&mut self, qry_id: QueryID, out: &[ProjCol]) {
        let mut roe = RewriteOutExpr { qry_id, out };
        let _ = self.e.walk_mut(&mut roe);
        // must reset lazy field as the expression changed
        self.attr = None;
        self.reject_nulls = None;
    }
}

#[inline]
fn push_single(
    qry_set: &mut QuerySet,
    op: &mut Op,
    mut p: ExprItem,
) -> Result<(RuleEffect, Option<ExprItem>)> {
    let mut eff = RuleEffect::empty();
    let res = match &mut op.kind {
        OpKind::Query(qry_id) => {
            if let Some(subq) = qry_set.get(qry_id) {
                p.rewrite(*qry_id, subq.out_cols());
                // after rewriting, Simplify it before pushing
                simplify_nested(&mut p.e, NullCoalesce::Null)?;
                match &p.e {
                    ExprKind::Const(Const::Null) => {
                        *op = Op::empty();
                        eff |= RuleEffect::OP;
                        return Ok((eff, None));
                    }
                    ExprKind::Const(c) => {
                        if c.is_zero().unwrap_or_default() {
                            *op = Op::empty();
                            eff |= RuleEffect::OP;
                            return Ok((eff, None));
                        } else {
                            return Ok((eff, None));
                        }
                    }
                    _ => (),
                }
                let e = qry_set.transform_op(*qry_id, |qry_set, _, op| {
                    let (e, pred) = push_single(qry_set, op, p)?;
                    assert!(pred.is_none()); // this push must succeed
                    eff |= e;
                    eff |= RuleEffect::EXPR;
                    Ok::<_, Error>(eff)
                })??;
                eff |= e;
                None
            } else {
                Some(p)
            }
        }
        // Table always accept
        OpKind::Scan(scan) => {
            scan.filt.push(p.e);
            None
        }
        // Empty just ignores
        OpKind::Empty => None,
        OpKind::Row(_) => todo!(), // todo: evaluate immediately
        // Proj/Sort/Limit/Attach will try pushing pred, and if fails just accept.
        // todo: pushdown to limit should be forbidden
        OpKind::Proj { .. } | OpKind::Sort { .. } | OpKind::Limit { .. } | OpKind::Attach(..) => {
            let (e, item) = push_or_accept(qry_set, op, p)?;
            eff |= e;
            item
        }
        OpKind::Aggr(aggr) => {
            if p.load_attr().has_aggf {
                Some(p)
            } else {
                // after the validation, all expressions containing no aggregate
                // functions can be pushed down through Aggr operator, as they can
                // only be composite of group columns, constants and functions.
                let (e, item) = push_single(qry_set, &mut aggr.input, p)?;
                assert!(item.is_none()); // just succeed
                eff |= e;
                None
            }
        }
        OpKind::Filt { pred, input } => match push_single(qry_set, input, p)? {
            (e, Some(p)) => {
                eff |= e;
                let mut old = mem::take(pred);
                old.push(p.e);
                let mut new = ExprKind::pred_conj(old);
                eff |= simplify_nested(&mut new, NullCoalesce::False)?;
                *pred = new.into_conj();
                None
            }
            (e, None) => {
                eff |= e;
                eff |= RuleEffect::EXPR;
                None
            }
        },
        OpKind::Setop(so) => {
            // push to both side and won't fail
            let (e, item) = push_single(qry_set, so.left.as_mut(), p.clone())?;
            assert!(item.is_none());
            eff |= e;
            eff |= RuleEffect::EXPR;
            let (e, item) = push_single(qry_set, so.right.as_mut(), p)?;
            eff |= e;
            eff |= RuleEffect::EXPR;
            assert!(item.is_none());
            None
        }
        OpKind::JoinGraph(_) => unreachable!("Predicates pushdown to join graph is not supported"),
        OpKind::Join(join) => match join.as_mut() {
            Join::Cross(jos) => {
                if p.load_attr().has_aggf {
                    // do not push predicates with aggregate functions
                    Some(p)
                } else {
                    let qry_ids = &p.load_attr().qry_ids;
                    match qry_ids {
                        QryIDs::Empty => unreachable!(), // Currently marked as unreachable
                        QryIDs::Single(qry_id) => {
                            // predicate of single table
                            let mut jo_qids = HashSet::new(); // reused container
                            for jo in jos {
                                jo_qids.clear();
                                jo.collect_qry_ids(&mut jo_qids);
                                if jo_qids.contains(qry_id) {
                                    let (e, item) = push_single(qry_set, jo.as_mut(), p)?;
                                    assert!(item.is_none());
                                    eff |= e;
                                    return Ok((eff, None));
                                }
                            }
                            Some(p)
                        }
                        QryIDs::Multi(qry_ids) => {
                            // if involved multiple tables, we convert cross join into join tree
                            // currently only two-way join is supported.
                            // once cross join are converted as a join tree, these rejected predicates
                            // can be pushed further.
                            if qry_ids.len() > 2 {
                                Some(p)
                            } else {
                                let (qid1, qid2) = {
                                    let mut iter = qry_ids.iter();
                                    let q1 = iter.next().cloned().unwrap();
                                    let q2 = iter.next().cloned().unwrap();
                                    (q1, q2)
                                };
                                let mut join_ops = mem::take(jos);
                                if let Some((idx1, jo)) = join_ops
                                    .iter()
                                    .enumerate()
                                    .find(|(_, jo)| jo.contains_qry_id(qid1))
                                {
                                    if jo.contains_qry_id(qid2) {
                                        // belong to single join op, push to it
                                        *jos = join_ops;
                                        let (e, item) =
                                            push_single(qry_set, jos[idx1].as_mut(), p.clone())?;
                                        assert!(item.is_none());
                                        eff |= e;
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                    if let Some(idx2) =
                                        join_ops.iter().position(|jo| jo.contains_qry_id(qid2))
                                    {
                                        let (jo1, jo2) = if idx1 < idx2 {
                                            // get larger one first
                                            let jo2 = join_ops.swap_remove(idx2);
                                            let jo1 = join_ops.swap_remove(idx1);
                                            (jo1, jo2)
                                        } else {
                                            let jo1 = join_ops.swap_remove(idx1);
                                            let jo2 = join_ops.swap_remove(idx2);
                                            (jo2, jo1)
                                        };
                                        if join_ops.is_empty() {
                                            // entire cross join is converted to inner join tree.
                                            let new_join = Join::Qualified(QualifiedJoin {
                                                kind: JoinKind::Inner,
                                                left: jo1,
                                                right: jo2,
                                                cond: vec![p.e],
                                                filt: vec![],
                                            });
                                            *join.as_mut() = new_join;
                                            eff |= RuleEffect::OPEXPR;
                                            return Ok((eff, None));
                                        } else {
                                            let new_join = JoinOp::qualified(
                                                JoinKind::Inner,
                                                jo1,
                                                jo2,
                                                vec![p.e],
                                                vec![],
                                            );
                                            join_ops.push(new_join);
                                            *jos = join_ops;
                                            eff |= RuleEffect::OPEXPR;
                                            return Ok((eff, None));
                                        }
                                    } else {
                                        return Err(Error::InvalidJoinCondition);
                                    }
                                } else {
                                    return Err(Error::InvalidJoinCondition);
                                }
                            }
                        }
                    }
                }
            }
            Join::Qualified(QualifiedJoin {
                kind,
                left,
                right,
                cond,
                filt,
            }) => {
                let qry_ids = &p.load_attr().qry_ids;
                match qry_ids {
                    QryIDs::Empty => unreachable!(), // Currently marked as unreachable
                    QryIDs::Single(qry_id) => {
                        let qry_id = *qry_id;
                        if left.contains_qry_id(qry_id) {
                            match kind {
                                JoinKind::Inner | JoinKind::Left => {
                                    let (e, item) = push_single(qry_set, left.as_mut(), p)?;
                                    assert!(item.is_none());
                                    eff |= e;
                                    eff |= RuleEffect::EXPR;
                                    return Ok((eff, None));
                                }
                                JoinKind::Full => {
                                    if p.load_reject_null(qry_id)? {
                                        // reject null
                                        // convert full join to right join, then to left join
                                        *kind = JoinKind::Left;
                                        mem::swap(left, right);
                                        // push to (original) left side
                                        let (e, item) = push_single(qry_set, right.as_mut(), p)?;
                                        assert!(item.is_none());
                                        eff |= e;
                                        eff |= RuleEffect::OPEXPR;
                                        return Ok((eff, None));
                                    } else {
                                        // not reject null
                                        filt.push(p.e);
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                }
                                _ => todo!(),
                            }
                        } else if right.contains_qry_id(qry_id) {
                            match kind {
                                JoinKind::Inner => {
                                    let (e, item) = push_single(qry_set, right.as_mut(), p)?;
                                    assert!(item.is_none());
                                    eff |= e;
                                    return Ok((eff, None));
                                }
                                JoinKind::Left => {
                                    if p.load_reject_null(qry_id)? {
                                        // reject null
                                        // convert left join to inner join
                                        *kind = JoinKind::Inner;
                                        if !filt.is_empty() {
                                            cond.extend(mem::take(filt)) // put filters into condition
                                        }
                                        let (e, item) = push_single(qry_set, right.as_mut(), p)?;
                                        assert!(item.is_none());
                                        eff |= e;
                                        eff |= RuleEffect::OPEXPR;
                                        return Ok((eff, None));
                                    } else {
                                        // not reject null
                                        filt.push(p.e);
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                }
                                JoinKind::Full => {
                                    if p.load_reject_null(qry_id)? {
                                        // reject null
                                        // convert full join to left join
                                        *kind = JoinKind::Left;
                                        let (e, item) = push_single(qry_set, right.as_mut(), p)?;
                                        assert!(item.is_none());
                                        eff |= e;
                                        eff |= RuleEffect::OPEXPR;
                                        return Ok((eff, None));
                                    } else {
                                        // not reject null
                                        filt.push(p.e);
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                }
                                _ => todo!(),
                            }
                        } else {
                            // this should not happen, the predicate must belong to either side
                            unreachable!()
                        }
                    }
                    QryIDs::Multi(qry_ids) => {
                        let mut left_qids = HashSet::new();
                        left.collect_qry_ids(&mut left_qids);
                        let left_qids: HashSet<QueryID> =
                            qry_ids.intersection(&left_qids).cloned().collect();
                        let mut right_qids = HashSet::new();
                        right.collect_qry_ids(&mut right_qids);
                        let right_qids: HashSet<QueryID> =
                            qry_ids.intersection(&right_qids).cloned().collect();
                        match (left_qids.is_empty(), right_qids.is_empty()) {
                            (false, true) => {
                                // handle on left side
                                match kind {
                                    JoinKind::Inner | JoinKind::Left => {
                                        let (e, item) = push_single(qry_set, left.as_mut(), p)?;
                                        assert!(item.is_none());
                                        eff |= e;
                                        return Ok((eff, None));
                                    }
                                    JoinKind::Full => {
                                        for qry_id in left_qids {
                                            if p.load_reject_null(qry_id)? {
                                                // convert full join to right join, then to left join
                                                *kind = JoinKind::Left;
                                                mem::swap(left, right);
                                                // push to (original) left side
                                                let (e, item) =
                                                    push_single(qry_set, right.as_mut(), p)?;
                                                assert!(item.is_none());
                                                eff |= e;
                                                eff |= RuleEffect::OPEXPR;
                                                return Ok((eff, None));
                                            }
                                        }
                                        // not reject null
                                        filt.push(p.e);
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                    _ => todo!(),
                                }
                            }
                            (true, false) => {
                                // handle on right side
                                match kind {
                                    JoinKind::Inner => {
                                        let (e, item) = push_single(qry_set, right.as_mut(), p)?;
                                        assert!(item.is_none());
                                        eff |= e;
                                        return Ok((eff, None));
                                    }
                                    JoinKind::Left => {
                                        for qry_id in right_qids {
                                            if p.load_reject_null(qry_id)? {
                                                // convert left join to inner join
                                                *kind = JoinKind::Inner;
                                                if !filt.is_empty() {
                                                    // put filters into condition
                                                    cond.extend(mem::take(filt));
                                                }
                                                let (e, item) =
                                                    push_single(qry_set, right.as_mut(), p)?;
                                                assert!(item.is_none());
                                                eff |= e;
                                                eff |= RuleEffect::OPEXPR;
                                                return Ok((e, None));
                                            }
                                        }
                                        // not reject null
                                        filt.push(p.e);
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                    JoinKind::Full => {
                                        for qry_id in right_qids {
                                            if p.load_reject_null(qry_id)? {
                                                // convert full join to left join
                                                *kind = JoinKind::Left;
                                                let (e, item) =
                                                    push_single(qry_set, right.as_mut(), p)?;
                                                assert!(item.is_none());
                                                eff |= e;
                                                eff |= RuleEffect::OP;
                                                return Ok((eff, None));
                                            }
                                        }
                                        // not reject null
                                        filt.push(p.e);
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                    _ => todo!(),
                                }
                            }
                            (false, false) => {
                                // handle on both sides
                                match kind {
                                    JoinKind::Inner => {
                                        cond.push(p.e);
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                    JoinKind::Left => {
                                        for qry_id in right_qids {
                                            if p.load_reject_null(qry_id)? {
                                                // convert left join to inner join
                                                *kind = JoinKind::Inner;
                                                if !filt.is_empty() {
                                                    cond.extend(mem::take(filt))
                                                    // put filters into condition
                                                }
                                                cond.push(p.e);
                                                eff |= RuleEffect::OPEXPR;
                                                return Ok((eff, None));
                                            }
                                        }
                                        // not reject null on right side
                                        filt.push(p.e);
                                        eff |= RuleEffect::EXPR;
                                        return Ok((eff, None));
                                    }
                                    JoinKind::Full => {
                                        let mut left_reject_null = false;
                                        let mut right_reject_null = false;
                                        for qry_id in left_qids {
                                            if p.load_reject_null(qry_id)? {
                                                left_reject_null = true;
                                                break;
                                            }
                                        }
                                        for qry_id in right_qids {
                                            if p.load_reject_null(qry_id)? {
                                                right_reject_null = true;
                                                break;
                                            }
                                        }
                                        match (left_reject_null, right_reject_null) {
                                            (true, true) => {
                                                // convert to inner join
                                                *kind = JoinKind::Inner;
                                                if !filt.is_empty() {
                                                    cond.extend(mem::take(filt))
                                                }
                                                cond.push(p.e);
                                                eff |= RuleEffect::OPEXPR;
                                                return Ok((eff, None));
                                            }
                                            (true, false) => {
                                                // convert to right join then left join
                                                *kind = JoinKind::Left;
                                                mem::swap(left, right);
                                                filt.push(p.e);
                                                eff |= RuleEffect::OPEXPR;
                                                return Ok((eff, None));
                                            }
                                            (false, true) => {
                                                // convert to left join
                                                *kind = JoinKind::Left;
                                                filt.push(p.e);
                                                eff |= RuleEffect::OPEXPR;
                                                return Ok((eff, None));
                                            }
                                            (false, false) => {
                                                filt.push(p.e);
                                                eff |= RuleEffect::EXPR;
                                                return Ok((eff, None));
                                            }
                                        }
                                    }
                                    _ => todo!(),
                                }
                            }
                            (true, true) => unreachable!(),
                        }
                    }
                }
            }
        },
    };
    Ok((eff, res))
}

#[inline]
fn push_or_accept(
    qry_set: &mut QuerySet,
    op: &mut Op,
    pred: ExprItem,
) -> Result<(RuleEffect, Option<ExprItem>)> {
    let input = op.kind.input_mut().unwrap(); // won't fail
    match push_single(qry_set, input, pred)? {
        (eff, Some(pred)) => {
            let child = mem::take(input);
            let new_filt = Op::new(OpKind::filt(vec![pred.e], child));
            *input = new_filt;
            // as child reject it, we do not merge effect, as parent will update it
            Ok((eff, None))
        }
        (eff, None) => Ok((eff, None)),
    }
}

struct RewriteOutExpr<'a> {
    qry_id: QueryID,
    out: &'a [ProjCol],
}

impl ExprMutVisitor for RewriteOutExpr<'_> {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<()> {
        if let ExprKind::Col(Col {
            kind: ColKind::Query(qry_id),
            idx,
            ..
        }) = e
            && *qry_id == self.qry_id {
                let new_c = &self.out[idx.value() as usize];
                *e = new_c.expr.clone();
            }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{
        assert_j_plan1, extract_join_kinds, get_subq_by_location, get_table_filt_expr, j_catalog,
        print_plan,
    };
    use crate::lgc::{LgcPlan, Location};

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
            "select 1 from (select c1 from t1 order by c0 limit 10) x1 where c1 > 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select 1 as c1 from t1) x1 where c1 > 0",
            assert_no_filt_on_disk_table,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select 1 as c1 from t1) x1 where c1 = 0",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                if let OpKind::Proj { input, .. } = &subq.root.kind {
                    assert!(input.is_empty());
                }
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select null as c1 from t1) x1 where c1 = 0",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                if let OpKind::Proj { input, .. } = &subq.root.kind {
                    assert!(input.is_empty());
                }
            },
        )
    }

    #[test]
    fn test_pred_pushdown_cross_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1, t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1 from t1, t2 having t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select t1.c1 from t1, t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1, count(*) from t1, t2 group by t1.c1 having c1 > 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1, t2 where t1.c1 = t2.c1",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["inner"], jks);
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1, t2, t3 where t1.c1 = t2.c1 and t1.c1 = t3.c1",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["inner", "inner"], jks);
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1, t2, t3 where t1.c1 = t2.c1 and t1.c1 = t3.c1 and t2.c1 = t3.c1",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["inner", "inner"], jks);
            },
        )
    }

    #[test]
    fn test_pred_pushdown_inner_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 where c2 = 0",
            assert_filt_on_disk_table1r,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1 from t1 join t2 having t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select t1.c1 from t1 join t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1, c2, count(*) from t1 join t2 where t1.c1 = 0 group by t1.c1, t2.c2 having c2 > 100",
            assert_filt_on_disk_table2,
        );
    }

    #[test]
    fn test_pred_pushdown_left_join() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where c2 = 0",
            assert_filt_on_disk_table1r,
        );
        // filter expression NOT rejects null, so cannot be pushed
        // to table scan.
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where c2 is null",
            assert_no_filt_on_disk_table,
        );
        // involve both sides, cannot be pushed to table scan,
        // join type will be converted to inner join.
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 where t1.c1 = c2",
            assert_no_filt_on_disk_table,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1 from t1 left join t2 having t1.c1 = 0 order by c1",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select 1 from (select t1.c1 from t1 left join t2) x1 where x1.c1 = 0",
            assert_filt_on_disk_table1,
        );
        assert_j_plan1(
            &cat,
            "select t1.c1, c2, count(*) from t1 left join t2 where t1.c1 = 0 group by t1.c1, t2.c2 having c2 > 100",
            assert_filt_on_disk_table2,
        );
        // one left join converted to inner join
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 left join t3 on t1.c1 = t3.c3 where t1.c1 = t2.c2",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                // the predicate pushdown only change the topmost join type.
                assert_eq!(vec!["left", "inner"], jks);
            },
        );
        // both left joins converted to inner joins, and one more inner join added.
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 left join t3 on t1.c1 = t2.c2 and t1.c1 = t3.c3 where t2.c2 = t3.c3",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                // in this case, the predicate pushdown stops at the first join
                // so second join will not be converted to inner join
                // but will be fixed by predicate propagation rule.
                assert_eq!(vec!["inner", "left"], jks);
            }
        );
        // both left joins converted to inner joins, and remove as no join condition,
        // one more inner join added.
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2 left join t3 where t2.c2 = t3.c3",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                // stops at first join, second will not be converted to inner join
                assert_eq!(vec!["inner", "left"], jks);
            },
        );
        // one is pushed as join condition, one is pushed as filter
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 left join t3 left join t4 where t1.c1 = t2.c2 and t3.c3 is null",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["left", "left", "inner"], jks);
            }
        );
    }

    #[test]
    fn test_pred_pushdown_right_join() {
        let cat = j_catalog();
        // right join is replaced by left join, so right table 2 is t1.
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 where t1.c1 = 0",
            assert_filt_on_disk_table1r,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2 where t2.c2 = 0",
            assert_filt_on_disk_table1,
        );
    }

    #[test]
    fn test_pred_pushdown_full_join() {
        let cat = j_catalog();
        // full join converted to left join
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 = 0",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq1 = get_subq_by_location(&q1, Location::Disk);
                // converted to right table, then left table
                // the underlying query postion is changed.
                assert!(!get_table_filt_expr(&subq1[1]).is_empty());
            },
        );
        // full join converted to right join, then left join
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t2.c2 = 0",
            assert_filt_on_disk_table1r,
        );
        // full join converted to inner join
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 where t1.c1 = t2.c2",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["inner"], jks);
            },
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 on t1.c1 = t2.c2 where t1.c1 is null and t2.c2 is null",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["full"], jks);
            },
        );
        // convert to left join and add one filt
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2 on t1.c1 = t2.c2 where t1.c0 > 0 and t2.c2 is null",
            |s1, mut q1| {
                pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                let subq = q1.root_query().unwrap();
                let jks = extract_join_kinds(&subq.root);
                assert_eq!(vec!["left"], jks);
            },
        );
    }

    fn assert_filt_on_disk_table1(s1: &str, mut q1: LgcPlan) {
        pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_table_filt_expr(&subq1[0]).is_empty());
    }

    fn assert_filt_on_disk_table1r(s1: &str, mut q1: LgcPlan) {
        pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_table_filt_expr(&subq1[1]).is_empty());
    }

    fn assert_filt_on_disk_table2(s1: &str, mut q1: LgcPlan) {
        pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(!get_table_filt_expr(&subq1[0]).is_empty());
        assert!(!get_table_filt_expr(&subq1[1]).is_empty());
    }

    fn assert_no_filt_on_disk_table(s1: &str, mut q1: LgcPlan) {
        pred_pushdown(&mut q1.qry_set, q1.root).unwrap();
        print_plan(s1, &q1);
        let subq1 = get_subq_by_location(&q1, Location::Disk);
        assert!(subq1
            .into_iter()
            .all(|subq| get_table_filt_expr(subq).is_empty()));
    }
}
