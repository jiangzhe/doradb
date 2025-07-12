use crate::error::{Error, Result};
use crate::join::{Join, QualifiedJoin};
use crate::lgc::{Op, OpKind, OpMutVisitor, QuerySet};
use crate::rule::RuleEffect;
use doradb_datatype::AlignPartialOrd;
use doradb_expr::controlflow::{Branch, ControlFlow, Unbranch};
use doradb_expr::fold::*;
use doradb_expr::{
    Col, ColIndex, ColKind, Const, ExprExt, ExprKind, ExprMutVisitor, FuncKind, GlobalID, Pred,
    PredFuncKind, QryCol, QueryID,
};
use indexmap::{IndexMap, IndexSet};
use std::cmp::Ordering;
use std::mem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullCoalesce {
    Null,
    False,
    True,
}

impl NullCoalesce {
    #[inline]
    fn flip(&mut self) {
        match self {
            NullCoalesce::Null => (),
            NullCoalesce::False => *self = NullCoalesce::True,
            NullCoalesce::True => *self = NullCoalesce::False,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PartialExpr {
    // allow operator =, !=, >, >=, <, <=
    pub(crate) kind: PredFuncKind,
    // argument on right side, only allow constant
    pub(crate) r_arg: Const,
}

/// Simplify expressions.
#[inline]
pub fn expr_simplify(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<RuleEffect> {
    simplify_expr(qry_set, qry_id)
}

#[inline]
fn simplify_expr(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<RuleEffect> {
    qry_set.transform_op(qry_id, |qry_set, _, op| {
        let mut es = ExprSimplify { qry_set };
        op.walk_mut(&mut es).unbranch()
    })?
}

struct ExprSimplify<'a> {
    qry_set: &'a mut QuerySet,
}

impl OpMutVisitor for ExprSimplify<'_> {
    type Cont = RuleEffect;
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error, RuleEffect> {
        match &mut op.kind {
            OpKind::Query(qry_id) => simplify_expr(self.qry_set, *qry_id).branch(),
            OpKind::Filt { pred, .. } => {
                let mut eff = RuleEffect::empty();
                for p in pred.iter_mut() {
                    eff |= simplify_nested(p, NullCoalesce::False).branch()?;
                    normalize_single(p);
                }
                eff |= simplify_conj(pred, NullCoalesce::False).branch()?;
                ControlFlow::Continue(eff)
            }
            OpKind::Join(join) => match join.as_mut() {
                Join::Cross(_) => ControlFlow::Continue(RuleEffect::empty()),
                Join::Qualified(QualifiedJoin { cond, filt, .. }) => {
                    let mut eff = RuleEffect::empty();
                    for c in cond.iter_mut() {
                        eff |= simplify_nested(c, NullCoalesce::False).branch()?;
                        normalize_single(c);
                    }
                    eff |= simplify_conj(cond, NullCoalesce::False).branch()?;
                    if !filt.is_empty() {
                        for f in filt.iter_mut() {
                            eff |= simplify_nested(f, NullCoalesce::False).branch()?;
                            normalize_single(f);
                        }
                        eff |= simplify_conj(filt, NullCoalesce::False).branch()?;
                    }
                    ControlFlow::Continue(eff)
                }
            },
            _ => {
                let mut eff = RuleEffect::empty();
                for e in op.kind.exprs_mut() {
                    eff |= simplify_nested(e, NullCoalesce::Null).branch()?;
                    normalize_single(e);
                }
                ControlFlow::Continue(eff)
            }
        }
    }
}

#[inline]
pub(crate) fn simplify_nested(e: &mut ExprKind, null_coalesce: NullCoalesce) -> Result<RuleEffect> {
    update_simplify_nested(e, null_coalesce, |_| Ok(()))
}

#[inline]
pub(crate) fn update_simplify_nested<F: FnMut(&mut ExprKind) -> Result<()>>(
    e: &mut ExprKind,
    null_coalesce: NullCoalesce,
    f: F,
) -> Result<RuleEffect> {
    struct SimplifyNested<F>(F, NullCoalesce);
    impl<F: FnMut(&mut ExprKind) -> Result<()>> ExprMutVisitor for SimplifyNested<F> {
        type Cont = RuleEffect;
        type Break = Error;
        #[inline]
        fn enter(&mut self, e: &mut ExprKind) -> ControlFlow<Error, RuleEffect> {
            if let ExprKind::Pred(Pred::Not(_)) = e {
                self.1.flip();
            }
            ControlFlow::Continue(RuleEffect::empty())
        }

        #[inline]
        fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<Error, RuleEffect> {
            // we should not change not in enter method, otherwise, we may miss the flip() call
            // on NullCoalesce.
            let not = matches!(e, ExprKind::Pred(Pred::Not(_)));
            let cf = update_simplify_single(e, self.1, &mut self.0).branch();
            if not {
                self.1.flip()
            }
            cf
        }
    }
    let mut sn = SimplifyNested(f, null_coalesce);
    e.walk_mut(&mut sn).unbranch()
}

// normalize considers
#[inline]
pub(crate) fn normalize_single(e: &mut ExprKind) {
    if let ExprKind::Pred(Pred::Func { kind, args }) = e {
        if let Some(flipped_kind) = kind.pos_flip() {
            match &mut args[..] {
                [e1 @ ExprKind::Const(_), e2] => {
                    // "const cmp expr" => "expr cmp const"
                    mem::swap(e1, e2);
                    *kind = flipped_kind;
                }
                [ExprKind::Col(c1), ExprKind::Col(c2)] => {
                    // if e1 and e2 are columns, fixed the order so that
                    // "e1 cmp e2" always has InternalOrder(e1) < InternalOrder(e2)
                    if c1.gid > c2.gid {
                        mem::swap(c1, c2);
                        *kind = flipped_kind;
                    }
                }
                _ => (),
            }
        }
    }
}

/// Simplify single expression.
/// This method will only simplify the top level of the expression.
#[inline]
pub(crate) fn simplify_single(e: &mut ExprKind, null_coalesce: NullCoalesce) -> Result<RuleEffect> {
    update_simplify_single(e, null_coalesce, |_| Ok(()))
}

enum ConjCmpEffect {
    None,
    Reject, // entire conjunctive expression rejected
    Reduce,
}

fn append_conj_cmp(
    cmps: &mut Vec<(PredFuncKind, Const)>,
    (gid, qry_id, idx): (GlobalID, QueryID, ColIndex),
    new_k: PredFuncKind,
    new_c: Const,
    null_coalesce: NullCoalesce,
    conv: &mut IndexSet<ExprKind>,
) -> ConjCmpEffect {
    let mut to_remove = vec![];
    for (i, (old_k, old_c)) in cmps.iter_mut().enumerate() {
        // Here we reduce exprs based on kinds
        match (*old_k, new_k) {
            (PredFuncKind::Greater, PredFuncKind::Greater)
            | (PredFuncKind::GreaterEqual, PredFuncKind::GreaterEqual) => {
                // a > const0 and a > const1, a >= const0 and a >= const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord == Ordering::Less {
                    *old_c = new_c;
                }
                return ConjCmpEffect::Reduce;
            }
            (PredFuncKind::Greater, PredFuncKind::GreaterEqual) => {
                // a > const0 and a >= const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord == Ordering::Less {
                    *old_c = new_c;
                    *old_k = new_k;
                }
                return ConjCmpEffect::Reduce;
            }
            (PredFuncKind::GreaterEqual, PredFuncKind::Greater) => {
                // a >= const0 and a > const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord != Ordering::Greater {
                    *old_c = new_c;
                    *old_k = new_k;
                }
                return ConjCmpEffect::Reduce;
            }
            (PredFuncKind::Greater, PredFuncKind::Less | PredFuncKind::LessEqual)
            | (PredFuncKind::GreaterEqual, PredFuncKind::Less) => {
                // a > const0 and a < const1, a > const0 and a <= const1, a >= const0 and a < const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord != Ordering::Less {
                    // impossible predicate
                    match null_coalesce {
                        NullCoalesce::Null => (),
                        NullCoalesce::False => return ConjCmpEffect::Reject,
                        NullCoalesce::True => {
                            // if column is not null, returns false, if column is null, coalesce to true,
                            // so we can replace it with IsNull(col)
                            let e = ExprKind::pred_func(
                                PredFuncKind::IsNull,
                                vec![ExprKind::query_col(gid, qry_id, idx)],
                            );
                            conv.insert(e);
                            to_remove.push(i);
                        }
                    }
                } // otherwise, do nothing
            }
            (PredFuncKind::GreaterEqual, PredFuncKind::LessEqual) => {
                // a >= const0 and a <= const1
                match old_c.align_partial_cmp(&new_c).unwrap() {
                    Ordering::Equal => {
                        // only equal is possible
                        *old_k = PredFuncKind::Equal;
                        return ConjCmpEffect::Reduce;
                    }
                    Ordering::Greater => {
                        // impossible predicate
                        match null_coalesce {
                            NullCoalesce::Null => (),
                            NullCoalesce::False => return ConjCmpEffect::Reject,
                            NullCoalesce::True => {
                                // if column is not null, returns false, if column is null, coalesce to true,
                                // so we can replace it with IsNull(col)
                                let e = ExprKind::pred_func(
                                    PredFuncKind::IsNull,
                                    vec![ExprKind::query_col(gid, qry_id, idx)],
                                );
                                conv.insert(e);
                                to_remove.push(i);
                            }
                        }
                    }
                    Ordering::Less => (),
                } // otherwise, do nothing
            }
            (PredFuncKind::Less, PredFuncKind::Less)
            | (PredFuncKind::LessEqual, PredFuncKind::LessEqual) => {
                // a < const0 and a < const1, a <= const0 and a <= const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord == Ordering::Greater {
                    *old_c = new_c;
                }
                return ConjCmpEffect::Reduce;
            }
            (PredFuncKind::Less, PredFuncKind::LessEqual) => {
                // a < const0 and a <= const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord == Ordering::Greater {
                    *old_c = new_c;
                    *old_k = new_k;
                }
                return ConjCmpEffect::Reduce;
            }
            (PredFuncKind::LessEqual, PredFuncKind::Less) => {
                // a <= const0 and a < const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord != Ordering::Less {
                    *old_c = new_c;
                    *old_k = new_k;
                }
                return ConjCmpEffect::Reduce;
            }
            (PredFuncKind::Less, PredFuncKind::Greater | PredFuncKind::GreaterEqual)
            | (PredFuncKind::LessEqual, PredFuncKind::Greater) => {
                // a < const0 and a > const1, a < const0 and a >= const1, a <= const0 and a > const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord != Ordering::Greater {
                    // impossible predicates
                    match null_coalesce {
                        NullCoalesce::Null => (),
                        NullCoalesce::False => return ConjCmpEffect::Reject,
                        NullCoalesce::True => {
                            // if column is not null, returns false, if column is null, coalesce to true,
                            // so we can replace it with IsNull(col)
                            let e = ExprKind::pred_func(
                                PredFuncKind::IsNull,
                                vec![ExprKind::query_col(gid, qry_id, idx)],
                            );
                            conv.insert(e);
                            to_remove.push(i);
                        }
                    }
                } // otherwise, do nothing
            }
            (PredFuncKind::LessEqual, PredFuncKind::GreaterEqual) => {
                // a <= const0 and a >= const1
                match old_c.align_partial_cmp(&new_c).unwrap() {
                    Ordering::Equal => {
                        // only equal is possible
                        *old_k = PredFuncKind::Equal;
                        return ConjCmpEffect::Reduce;
                    }
                    Ordering::Less => {
                        // impossible predicate
                        match null_coalesce {
                            NullCoalesce::Null => (),
                            NullCoalesce::False => return ConjCmpEffect::Reject,
                            NullCoalesce::True => {
                                // if column is not null, returns false, if column is null, coalesce to true,
                                // so we can replace it with IsNull(col)
                                let e = ExprKind::pred_func(
                                    PredFuncKind::IsNull,
                                    vec![ExprKind::query_col(gid, qry_id, idx)],
                                );
                                conv.insert(e);
                                to_remove.push(i);
                            }
                        }
                    }
                    Ordering::Greater => (),
                } // otherwise, do nothing
            }
            (
                PredFuncKind::Equal,
                PredFuncKind::Equal
                | PredFuncKind::Greater
                | PredFuncKind::GreaterEqual
                | PredFuncKind::Less
                | PredFuncKind::LessEqual
                | PredFuncKind::NotEqual,
            ) => {
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if !check_ord_left_eq(ord, new_k) {
                    // impossible predicate
                    match null_coalesce {
                        NullCoalesce::Null => (),
                        NullCoalesce::False => return ConjCmpEffect::Reject,
                        NullCoalesce::True => {
                            // if column is not null, returns false, if column is null, coalesce to true,
                            // so we can replace it with IsNull(col)
                            let e = ExprKind::pred_func(
                                PredFuncKind::IsNull,
                                vec![ExprKind::query_col(gid, qry_id, idx)],
                            );
                            conv.insert(e);
                            to_remove.push(i);
                        }
                    }
                } else {
                    // if old equals, new is always true, so keep old and remove new
                    return ConjCmpEffect::Reduce;
                }
            }
            (
                PredFuncKind::Greater
                | PredFuncKind::GreaterEqual
                | PredFuncKind::Less
                | PredFuncKind::LessEqual
                | PredFuncKind::NotEqual,
                PredFuncKind::Equal,
            ) => {
                // flip left and right
                let ord = new_c.align_partial_cmp(old_c).unwrap();
                if !check_ord_left_eq(ord, *old_k) {
                    match null_coalesce {
                        NullCoalesce::Null => (),
                        NullCoalesce::False => return ConjCmpEffect::Reject,
                        NullCoalesce::True => {
                            // if column is not null, returns false, if column is null, coalesce to true,
                            // so we can replace it with IsNull(col)
                            let e = ExprKind::pred_func(
                                PredFuncKind::IsNull,
                                vec![ExprKind::query_col(gid, qry_id, idx)],
                            );
                            conv.insert(e);
                            to_remove.push(i);
                        }
                    }
                } else {
                    // if new equals, old is always true, so replace old with new
                    *old_k = new_k;
                    *old_c = new_c;
                    return ConjCmpEffect::Reduce;
                }
            }
            (PredFuncKind::NotEqual, PredFuncKind::Greater) => {
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord != Ordering::Greater {
                    *old_k = new_k;
                    *old_c = new_c;
                    return ConjCmpEffect::Reduce;
                } // otherwise, nothing
            }
            (PredFuncKind::Greater, PredFuncKind::NotEqual) => {
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord != Ordering::Less {
                    return ConjCmpEffect::Reduce;
                } // otherwise, nothing
            }
            (PredFuncKind::NotEqual, PredFuncKind::GreaterEqual) => {
                match old_c.align_partial_cmp(&new_c).unwrap() {
                    Ordering::Equal => {
                        *old_k = PredFuncKind::Greater;
                        return ConjCmpEffect::Reduce;
                    }
                    Ordering::Less => {
                        *old_k = new_k;
                        *old_c = new_c;
                        return ConjCmpEffect::Reduce;
                    }
                    _ => (),
                }
            }
            (PredFuncKind::GreaterEqual, PredFuncKind::NotEqual) => {
                // a >= const0 and a != const1
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                match ord {
                    Ordering::Equal => {
                        *old_k = PredFuncKind::Greater;
                        return ConjCmpEffect::Reduce;
                    }
                    Ordering::Greater => return ConjCmpEffect::Reduce,
                    _ => (),
                }
            }
            (PredFuncKind::NotEqual, PredFuncKind::Less) => {
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord != Ordering::Less {
                    *old_k = new_k;
                    *old_c = new_c;
                    return ConjCmpEffect::Reduce;
                } // otherwise, nothing
            }
            (PredFuncKind::Less, PredFuncKind::NotEqual) => {
                let ord = old_c.align_partial_cmp(&new_c).unwrap();
                if ord != Ordering::Greater {
                    return ConjCmpEffect::Reduce;
                } // otherwise, nothing
            }
            (PredFuncKind::NotEqual, PredFuncKind::LessEqual) => {
                match old_c.align_partial_cmp(&new_c).unwrap() {
                    Ordering::Equal => {
                        *old_k = PredFuncKind::Less;
                        return ConjCmpEffect::Reduce;
                    }
                    Ordering::Greater => {
                        *old_k = new_k;
                        *old_c = new_c;
                        return ConjCmpEffect::Reduce;
                    }
                    _ => (),
                }
            }
            (PredFuncKind::LessEqual, PredFuncKind::NotEqual) => {
                match old_c.align_partial_cmp(&new_c).unwrap() {
                    Ordering::Equal => {
                        *old_k = PredFuncKind::Less;
                        return ConjCmpEffect::Reduce;
                    }
                    Ordering::Less => {
                        return ConjCmpEffect::Reduce;
                    }
                    _ => (),
                }
            }
            _ => (),
        }
    }
    if to_remove.is_empty() {
        cmps.push((new_k, new_c));
        return ConjCmpEffect::None;
    }
    while let Some(i) = to_remove.pop() {
        cmps.remove(i);
    }
    ConjCmpEffect::Reduce
}

// Simplify conjunctive expression with short circuit.
// If returned value is not empty, use it to replace the original expression list,
// this is called short circuit.
fn simplify_conj_short_circuit(
    es: &mut Vec<ExprKind>,
    null_coalesce: NullCoalesce,
    eff: &mut RuleEffect,
) -> Option<ExprKind> {
    let mut eset = IndexSet::new();
    let mut cmps: IndexMap<QryCol, QueryColPredicates> = IndexMap::new();
    for e in es.drain(..) {
        match &e {
            ExprKind::Const(c) => {
                if let Some(zero) = c.is_zero() {
                    if zero {
                        return Some(ExprKind::const_bool(false));
                    }
                    // otherwise, remove it as redundant
                    *eff |= RuleEffect::EXPR;
                } else {
                    match null_coalesce {
                        NullCoalesce::Null => {
                            if !eset.insert(e) {
                                *eff |= RuleEffect::EXPR;
                            }
                        }
                        NullCoalesce::False => return Some(ExprKind::const_bool(false)),
                        NullCoalesce::True => {
                            // remove it
                            *eff |= RuleEffect::EXPR;
                        }
                    }
                }
            }
            _ => {
                if let ExprKind::Pred(Pred::Func { kind: new_k, args }) = &e {
                    match (new_k, &args[..]) {
                        (
                            PredFuncKind::Equal
                            | PredFuncKind::NotEqual
                            | PredFuncKind::Greater
                            | PredFuncKind::GreaterEqual
                            | PredFuncKind::Less
                            | PredFuncKind::LessEqual,
                            [ExprKind::Col(Col {
                                gid,
                                kind: ColKind::Query(qry_id),
                                idx,
                            }), ExprKind::Const(new_c)],
                        ) => {
                            let ent = cmps.entry(QryCol(*qry_id, *idx)).or_insert_with(|| {
                                QueryColPredicates {
                                    gid: *gid,
                                    preds: vec![],
                                }
                            });
                            match append_conj_cmp(
                                &mut ent.preds,
                                (*gid, *qry_id, *idx),
                                *new_k,
                                new_c.clone(),
                                null_coalesce,
                                &mut eset,
                            ) {
                                ConjCmpEffect::None => (),
                                ConjCmpEffect::Reduce => {
                                    *eff |= RuleEffect::EXPR;
                                }
                                ConjCmpEffect::Reject => return Some(ExprKind::const_bool(false)),
                            }
                        }
                        _ => {
                            if !eset.insert(e) {
                                *eff |= RuleEffect::EXPR;
                            }
                        }
                    }
                } else if !eset.insert(e) {
                    *eff |= RuleEffect::EXPR;
                }
            }
        }
    }
    for (QryCol(qid, idx), mut pes) in cmps {
        if pes.preds.len() == 1 {
            // keep it as is
            let (kind, c) = pes.preds.pop().unwrap();
            let e = ExprKind::pred_func(
                kind,
                vec![ExprKind::query_col(pes.gid, qid, idx), ExprKind::Const(c)],
            );
            eset.insert(e);
        } else {
            // iteratively simplify cmps
            loop {
                let mut progress = false;
                let tmp = mem::take(&mut pes.preds);
                for (new_k, new_c) in tmp {
                    match append_conj_cmp(
                        &mut pes.preds,
                        (pes.gid, qid, idx),
                        new_k,
                        new_c,
                        null_coalesce,
                        &mut eset,
                    ) {
                        ConjCmpEffect::None => (),
                        ConjCmpEffect::Reduce => {
                            progress = true;
                        }
                        ConjCmpEffect::Reject => return Some(ExprKind::const_bool(false)),
                    }
                }
                if !progress {
                    break;
                }
            }
            for (k, c) in pes.preds {
                let e = ExprKind::pred_func(
                    k,
                    vec![ExprKind::query_col(pes.gid, qid, idx), ExprKind::Const(c)],
                );
                eset.insert(e);
            }
        }
    }
    // push back to original list
    for e in eset {
        es.push(e)
    }
    None
}

struct QueryColPredicates {
    // global id of this query column.
    gid: GlobalID,
    preds: Vec<(PredFuncKind, Const)>,
}

#[inline]
fn check_ord_left_eq(ord: Ordering, r_kind: PredFuncKind) -> bool {
    match (ord, r_kind) {
        // a=2 and a>1, a=2 and a>=1, a=2 and a!=1
        (
            Ordering::Greater,
            PredFuncKind::Greater | PredFuncKind::GreaterEqual | PredFuncKind::NotEqual,
        ) => true,
        (Ordering::Greater, _) => false,
        // a=1 and a<2, a=1 and a <=2, a=1 and a!=2
        (Ordering::Less, PredFuncKind::Less | PredFuncKind::LessEqual | PredFuncKind::NotEqual) => {
            true
        }
        (Ordering::Less, _) => false,
        // a=1 and a>=1, a=1 and a<=1, a=1 and a=1
        (
            Ordering::Equal,
            PredFuncKind::GreaterEqual | PredFuncKind::LessEqual | PredFuncKind::Equal,
        ) => true,
        (Ordering::Equal, _) => false,
    }
}

/// Simplify expressions in conjunctive normal form.
/// The provided expressions should be already simplified.
#[inline]
pub(crate) fn simplify_conj(
    es: &mut Vec<ExprKind>,
    null_coalesce: NullCoalesce,
) -> Result<RuleEffect> {
    let mut eff = RuleEffect::empty();
    if let Some(e) = simplify_conj_short_circuit(es, null_coalesce, &mut eff) {
        es.clear();
        es.push(e);
        eff |= RuleEffect::EXPR;
    }
    Ok(eff)
}

/// Try updating the expression, and then simplify it.
#[inline]
fn update_simplify_single<F: FnMut(&mut ExprKind) -> Result<()>>(
    e: &mut ExprKind,
    null_coalesce: NullCoalesce,
    mut f: F,
) -> Result<RuleEffect> {
    let mut eff = RuleEffect::empty();
    // we don't count the replacement as expression change
    f(e)?;
    match e {
        ExprKind::Func { kind, args, .. } => {
            if let Some(new) = simplify_func(*kind, args)? {
                *e = new;
                eff |= RuleEffect::EXPR;
            }
        }
        ExprKind::Pred(p) => {
            // todo: add NullCoalesce
            if let Some(new) = simplify_pred(p, null_coalesce)? {
                *e = new;
                eff |= RuleEffect::EXPR;
            }
        }
        _ => (), // All other kinds are skipped in constant folding
    }
    Ok(eff)
}

/// Simplify function.
///
/// 1. remove pair of negating, e.g.
///    --e => e
/// 2. compute negating constant, e.g.
///    -c => new_c
/// 3. compute addition of constants, e.g.
///    1+1 => 2
/// 4. remote adding zero, e.g.
///    e+0 => e
/// 5. swap order of variable in addtion, e.g.
///    1+e => e+1
/// 6. associative, e.g.
///    (e+1)+2 => e+3
///    Note: (1+e)+2 => e+3 -- won't happen after rule 5, only for add/mul
/// 7. commutative and associative, e.g.
///    1+(e+2) => e+3
///    Note: 1+(2+e) => e+3 -- won't happen after rule 5, only for add/mul
/// 8. commutative and associative, e.g.
///    (e1+1)+(e2+2) => (e1+e2)+3
fn simplify_func(fkind: FuncKind, fargs: &mut [ExprKind]) -> Result<Option<ExprKind>> {
    let res =
        match fkind {
            FuncKind::Neg => match &mut fargs[0] {
                // rule 1: --e => e
                // todo: should cast to f64 if original expression is not numeric
                ExprKind::Func { kind, args, .. } if *kind == FuncKind::Neg => {
                    Some(mem::take(&mut args[0]))
                }
                // rule 2: -c => new_c
                ExprKind::Const(c) => fold_neg_const(c)?.map(ExprKind::Const),
                _ => None,
            },
            FuncKind::Add => match fargs {
                // rule 3: 1+1 => 2
                [ExprKind::Const(c1), ExprKind::Const(c2)] => {
                    fold_add_const(c1, c2)?.map(ExprKind::Const)
                }
                [e, ExprKind::Const(c1)] => {
                    if c1.is_zero().unwrap_or_default() {
                        // rule 4: e+0 => e
                        let e = mem::take(e);
                        Some(coerce_numeric(e))
                    } else if let ExprKind::Func { kind, args, .. } = e {
                        match (kind, &mut args[..]) {
                            // rule 6: (e1+c2)+c1 => e1 + (c2+c1)
                            (FuncKind::Add, [e1, ExprKind::Const(c2)]) => fold_add_const(c2, c1)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    expr_add_const(e1, c3)
                                }),
                            // rule 6: (e1-c2)+c1 => e1 - (c2-c1)
                            (FuncKind::Sub, [e1, ExprKind::Const(c2)]) => fold_sub_const(c2, c1)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    expr_sub_const(e1, c3)
                                }),
                            // rule 6: (c2-e1)+c1 => (c2+c1) - e1
                            (FuncKind::Sub, [ExprKind::Const(c2), e1]) => fold_add_const(c2, c1)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    const_sub_expr(c3, e1)
                                }),

                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                [ExprKind::Const(c1), e] => {
                    if c1.is_zero().unwrap_or_default() {
                        // rule 4: 0+e => e
                        let e = mem::take(e);
                        Some(coerce_numeric(e))
                    } else {
                        match e {
                            ExprKind::Func { kind, args, .. } => {
                                match (kind, &mut args[..]) {
                                    // rule 7: c1 + (e1+c2) => e1 + (c1+c2)
                                    (FuncKind::Add, [e1, ExprKind::Const(c2)]) => {
                                        fold_add_const(c1, c2)?.map(|c3| {
                                            let e1 = mem::take(e1);
                                            expr_add_const(e1, c3)
                                        })
                                    }
                                    // rule 7: c1 + (e1-c2) => e1 + (c1-c2)
                                    (FuncKind::Sub, [e1, ExprKind::Const(c2)]) => {
                                        fold_sub_const(c1, c2)?.map(|c3| {
                                            let e1 = mem::take(e1);
                                            expr_add_const(e1, c3)
                                        })
                                    }
                                    // rule 7: c1 + (c2-e1) => (c1+c2) - e1
                                    (FuncKind::Sub, [ExprKind::Const(c2), e1]) => {
                                        fold_add_const(c1, c2)?.map(|c3| {
                                            let e1 = mem::take(e1);
                                            const_sub_expr(c3, e1)
                                        })
                                    }
                                    // rule 5: c1 + e1 => e1 + c1
                                    _ => {
                                        let e1 = mem::take(e);
                                        let c1 = mem::take(c1);
                                        Some(ExprKind::func(
                                            FuncKind::Add,
                                            vec![e1, ExprKind::Const(c1)],
                                        ))
                                    }
                                }
                            }
                            // rule 5
                            _ => {
                                let e1 = mem::take(e);
                                let c1 = mem::take(c1);
                                Some(ExprKind::func(FuncKind::Add, vec![e1, ExprKind::Const(c1)]))
                            }
                        }
                    }
                }
                [ExprKind::Func {
                    kind: k1, args: a1, ..
                }, ExprKind::Func {
                    kind: k2, args: a2, ..
                }] => {
                    match (k1, k2, &mut a1[..], &mut a2[..]) {
                        // rule 8.1: (e1+c1)+(e2+c2) => (e1+e2) + (c1+c2)
                        (
                            FuncKind::Add,
                            FuncKind::Add,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            expr_add_const(e, c3)
                        }),
                        // rule 8.2: (e1+c1)+(e2-c2) => (e1+e2) + (c1-c2)
                        (
                            FuncKind::Add,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            expr_add_const(e, c3)
                        }),
                        // rule 8.3: (e1-c1)+(e2+c2) => (e1+e2) - (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Add,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            expr_sub_const(e, c3)
                        }),
                        // rule 8.4: (e1-c1)+(e2-c2) => (e1+e2) - (c1+c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            expr_sub_const(e, c3)
                        }),
                        // rule 8.5: (c1-e1)+(e2-c2) => (e2-e1) + (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [ExprKind::Const(c1), e1],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e2, e1]);
                            expr_add_const(e, c3)
                        }),
                        // rule 8.6: (c1-e1)+(e2+c2) => (e2-e1) + (c1+c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Add,
                            [ExprKind::Const(c1), e1],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e2, e1]);
                            expr_add_const(e, c3)
                        }),
                        // rule 8.7: (c1-e1)+(c2-e2) => (c1+c2) - (e1+e2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [ExprKind::Const(c1), e1],
                            [ExprKind::Const(c2), e2],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            const_sub_expr(c3, e)
                        }),
                        // rule 8.8: (e1-c1)+(c2-e2) => (e1-e2) - (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [ExprKind::Const(c2), e2],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e1, e2]);
                            expr_sub_const(e, c3)
                        }),
                        // rule 8.9: (e1+c1)+(c2-e2) => (e1-e2) + (c1+c2)
                        (
                            FuncKind::Add,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [ExprKind::Const(c2), e2],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e1, e2]);
                            expr_add_const(e, c3)
                        }),
                        _ => None,
                    }
                }
                _ => None,
            },
            FuncKind::Sub => match fargs {
                // rule 3: 1-1 => 0
                [ExprKind::Const(c1), ExprKind::Const(c2)] => {
                    fold_sub_const(c1, c2)?.map(ExprKind::Const)
                }
                [e, ExprKind::Const(c1)] => {
                    if c1.is_zero().unwrap_or_default() {
                        // rule 4: e-0 => e
                        let e = mem::take(e);
                        Some(coerce_numeric(e))
                    } else if let ExprKind::Func { kind, args, .. } = e {
                        match (kind, &mut args[..]) {
                            // rule 6: (e1+c2)-c1 => e1 + (c2-c1)
                            (FuncKind::Add, [e1, ExprKind::Const(c2)]) => fold_sub_const(c2, c1)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    expr_add_const(e1, c3)
                                }),
                            // rule 6: (e1-c2)-c1 => e1 - (c2+c1)
                            (FuncKind::Sub, [e1, ExprKind::Const(c2)]) => fold_add_const(c2, c1)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    expr_sub_const(e1, c3)
                                }),
                            // rule 6: (c2-e1)-c1 => (c2-c1) - e1
                            (FuncKind::Sub, [ExprKind::Const(c2), e1]) => fold_sub_const(c2, c1)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    const_sub_expr(c3, e1)
                                }),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                [ExprKind::Const(c1), e] => {
                    match e {
                        ExprKind::Func { kind, args, .. } => match (kind, &mut args[..]) {
                            // rule 7: c1 - (e1+c2) => (c1-c2) - e1
                            (FuncKind::Add, [e1, ExprKind::Const(c2)]) => fold_sub_const(c1, c2)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    const_sub_expr(c3, e1)
                                }),
                            // rule 7: c1 - (e1-c2) => (c1+c2) - e1
                            (FuncKind::Sub, [e1, ExprKind::Const(c2)]) => fold_add_const(c1, c2)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    const_sub_expr(c3, e1)
                                }),
                            // rule 7: c1 - (c2-e1) => e1 + (c1-c2)
                            (FuncKind::Sub, [ExprKind::Const(c2), e1]) => fold_sub_const(c1, c2)?
                                .map(|c3| {
                                    let e1 = mem::take(e1);
                                    expr_add_const(e1, c3)
                                }),
                            _ => {
                                if c1.is_zero().unwrap_or_default() {
                                    // rule 4: 0-e => -e
                                    let e = mem::take(e);
                                    Some(negate(e))
                                } else {
                                    None
                                }
                            }
                        },
                        _ => {
                            if c1.is_zero().unwrap_or_default() {
                                // rule 4
                                let e = mem::take(e);
                                Some(negate(e))
                            } else {
                                None
                            }
                        }
                    }
                }
                [ExprKind::Func {
                    kind: k1, args: a1, ..
                }, ExprKind::Func {
                    kind: k2, args: a2, ..
                }] => {
                    match (k1, k2, &mut a1[..], &mut a2[..]) {
                        // rule 8.1: (e1+c1)-(e2+c2) => (e1-e2) + (c1-c2)
                        (
                            FuncKind::Add,
                            FuncKind::Add,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e1, e2]);
                            expr_add_const(e, c3)
                        }),
                        // rule 8.2: (e1-c1)-(e2+c2) => (e1-e2) - (c1+c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Add,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e1, e2]);
                            expr_sub_const(e, c3)
                        }),
                        // rule 8.3: (e1+c1)-(e2-c2) => (e1-e2) + (c1+c2)
                        (
                            FuncKind::Add,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e1, e2]);
                            expr_add_const(e, c3)
                        }),
                        // rule 8.4: (e1-c1)-(e2-c2) => (e1-e2) - (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e1, e2]);
                            expr_sub_const(e, c3)
                        }),
                        // rule 8.5: (c1-e1)-(e2-c2) => (c1+c2) - (e1+e2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [ExprKind::Const(c1), e1],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            const_sub_expr(c3, e)
                        }),
                        // rule 8.6: (c1-e1)-(e2+c2) => (c1-c2) - (e1+e2)
                        (
                            FuncKind::Sub,
                            FuncKind::Add,
                            [ExprKind::Const(c1), e1],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            const_sub_expr(c3, e)
                        }),
                        // rule 8.7: (c1-e1)-(c2-e2) => (e2-e1) + (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [ExprKind::Const(c1), e1],
                            [ExprKind::Const(c2), e2],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Sub, vec![e2, e1]);
                            expr_add_const(e, c3)
                        }),
                        // rule 8.8: (e1-c1)-(c2-e2) => (e1+e2) - (c1+c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [ExprKind::Const(c2), e2],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            expr_sub_const(e, c3)
                        }),
                        // rule 8.9: (e1+c1)-(c2-e2) => (e1+e2) + (c1-c2)
                        (
                            FuncKind::Add,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [ExprKind::Const(c2), e2],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            expr_add_const(e, c3)
                        }),
                        _ => None,
                    }
                }
                _ => None,
            },
            _ => None,
        };
    Ok(res)
}

#[inline]
fn expr_add_const(e: ExprKind, c: Const) -> ExprKind {
    if c.is_zero().unwrap_or_default() {
        coerce_numeric(e)
    } else {
        ExprKind::func(FuncKind::Add, vec![e, ExprKind::Const(c)])
    }
}

#[inline]
fn coerce_numeric(e: ExprKind) -> ExprKind {
    // todo: add casting
    e
}

#[inline]
fn expr_sub_const(e: ExprKind, c: Const) -> ExprKind {
    if c.is_zero().unwrap_or_default() {
        coerce_numeric(e)
    } else {
        ExprKind::func(FuncKind::Sub, vec![e, ExprKind::Const(c)])
    }
}

#[inline]
fn negate(e: ExprKind) -> ExprKind {
    ExprKind::func(FuncKind::Neg, vec![e])
}

#[inline]
fn const_sub_expr(c: Const, e: ExprKind) -> ExprKind {
    if c.is_zero().unwrap_or_default() {
        negate(e)
    } else {
        ExprKind::func(FuncKind::Sub, vec![ExprKind::Const(c), e])
    }
}

/// Simplify predicate.
///
/// 1. NOT
///    1.1. not Exists => NotExists
///    1.2. not NotExists => Exists
///    1.3. not In => NotIn
///    1.4. not NotIn => In
///    1.5. not cmp => logical flipped cmp
///    1.6. not const => new_const
///    1.7. not not e => cast(e as bool)
///
/// 2. Comparison
///    2.1. null cmp const1 => null, except for SafeEqual
///    2.2. const1 cmp const2 => new_const
///    2.3. e1 + c1 cmp c2 => e1 cmp new_c
///    2.4. c1 cmp e1 + c2 => new_c cmp e1 => e1 flip_cmp new_c
///    2.5. c1 cmp e1 => e1 flip_cmp c1
///    2.6. e1 + c1 cmp e2 + c2 => e1 cmp e2 + new_c
///    Note: 2.3 ~ 2.6 requires cmp operator can be positional flipped.
///
/// 3. CNF
///
/// 4. DNF
///
/// 5. EXISTS/NOT EXISTS
///
/// 6. IN/NOT IN
fn simplify_pred(p: &mut Pred, null_coalesce: NullCoalesce) -> Result<Option<ExprKind>> {
    let res = match p {
        Pred::Not(e) => match &mut **e {
            // 1.1
            ExprKind::Pred(Pred::Exists(subq)) => {
                Some(ExprKind::Pred(Pred::NotExists(mem::take(subq))))
            }
            // 1.2
            ExprKind::Pred(Pred::NotExists(subq)) => {
                Some(ExprKind::Pred(Pred::Exists(mem::take(subq))))
            }
            // 1.3
            ExprKind::Pred(Pred::InSubquery(lhs, subq)) => Some(ExprKind::Pred(
                Pred::NotInSubquery(mem::take(lhs), mem::take(subq)),
            )),
            // 1.4
            ExprKind::Pred(Pred::NotInSubquery(lhs, subq)) => Some(ExprKind::Pred(
                Pred::InSubquery(mem::take(lhs), mem::take(subq)),
            )),
            // 1.5
            ExprKind::Pred(Pred::Func { kind, args }) => kind.logic_flip().map(|kind| {
                let args = mem::take(args);
                ExprKind::Pred(Pred::Func { kind, args })
            }),
            // 1.6
            ExprKind::Const(c) => fold_not_const(c)?.map(ExprKind::Const),
            // 1.7 todo
            ExprKind::Pred(Pred::Not(_e)) => None,
            _ => None,
        },
        Pred::Func { kind, args } => {
            // 2.1 and 2.2
            let res = match kind {
                PredFuncKind::Equal => fold_eq(&args[0], &args[1])?,
                PredFuncKind::Greater => fold_gt(&args[0], &args[1])?,
                PredFuncKind::GreaterEqual => fold_ge(&args[0], &args[1])?,
                PredFuncKind::Less => fold_lt(&args[0], &args[1])?,
                PredFuncKind::LessEqual => fold_le(&args[0], &args[1])?,
                PredFuncKind::NotEqual => fold_ne(&args[0], &args[1])?,
                PredFuncKind::SafeEqual => fold_safeeq(&args[0], &args[1])?,
                PredFuncKind::IsNull => fold_isnull(&args[0])?,
                PredFuncKind::IsNotNull => fold_isnotnull(&args[0])?,
                PredFuncKind::IsTrue => fold_istrue(&args[0])?,
                PredFuncKind::IsNotTrue => fold_isnottrue(&args[0])?,
                PredFuncKind::IsFalse => fold_isfalse(&args[0])?,
                PredFuncKind::IsNotFalse => fold_isnotfalse(&args[0])?,
                _ => None, // todo
            }
            .map(ExprKind::Const);
            if res.is_some() {
                // already folded as constant
                res
            } else if let Some(flipped_kind) = kind.pos_flip() {
                match &mut args[..] {
                    // 2.3: e1 + c1 cmp c2 => e1 cmp c3
                    [ExprKind::Func {
                        kind: FuncKind::Add,
                        args: fargs,
                        ..
                    }, ExprKind::Const(c2)] => match &mut fargs[..] {
                        [e1, ExprKind::Const(c1)] => fold_sub_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(*kind, e1, ExprKind::Const(c3))
                        }),
                        _ => None,
                    },
                    [ExprKind::Func {
                        kind: FuncKind::Sub,
                        args: fargs,
                        ..
                    }, ExprKind::Const(c2)] => match &mut fargs[..] {
                        // 2.3: e1 - c1 cmp c2
                        [e1, ExprKind::Const(c1)] => fold_add_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(*kind, e1, ExprKind::Const(c3))
                        }),
                        // 2.3: c1 - e1 cmp c2
                        [ExprKind::Const(c1), e1] => {
                            // e1 flip_cmp (c1-c2)
                            fold_sub_const(c1, c2)?.map(|c3| {
                                let e1 = mem::take(e1);
                                coerce_cmp_func(flipped_kind, e1, ExprKind::Const(c3))
                            })
                        }
                        _ => None,
                    },
                    // 2.4: c1 cmp e1 + c2 => e1 flip_cmp c3
                    [ExprKind::Const(c1), ExprKind::Func {
                        kind: FuncKind::Add,
                        args: fargs,
                        ..
                    }] => match &mut fargs[..] {
                        [e1, ExprKind::Const(c2)] => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(flipped_kind, e1, ExprKind::Const(c3))
                        }),
                        _ => None,
                    },
                    [ExprKind::Const(c1), ExprKind::Func {
                        kind: FuncKind::Sub,
                        args: fargs,
                        ..
                    }] => match &mut fargs[..] {
                        // 2.4: c1 cmp e1 - c2 => e1 flip_cmp (c1+c2)
                        [e1, ExprKind::Const(c2)] => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(flipped_kind, e1, ExprKind::Const(c3))
                        }),
                        // 2.4: c1 cmp c2 - e1 => e1 cmp (c2-c1)
                        [ExprKind::Const(c2), e1] => fold_sub_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(*kind, e1, ExprKind::Const(c3))
                        }),
                        _ => None,
                    },
                    // 2.5: c1 cmp e1 => e1 flip_cmp c1
                    [c1 @ ExprKind::Const(_), e1] => {
                        let c1 = mem::take(c1);
                        let e1 = mem::take(e1);
                        Some(coerce_cmp_func(flipped_kind, e1, c1))
                    }
                    [ExprKind::Func {
                        kind: kind1,
                        args: args1,
                        ..
                    }, ExprKind::Func {
                        kind: kind2,
                        args: args2,
                        ..
                    }] => match (kind1, kind2, &mut args1[..], &mut args2[..]) {
                        // 2.6: e1 + c1 cmp e2 + c2 => e1 cmp e2 + (c2-c1)
                        (
                            FuncKind::Add,
                            FuncKind::Add,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_add_const(e2, c3);
                            coerce_cmp_func(*kind, e1, right)
                        }),
                        // 2.6: e1 + c1 cmp e2 - c2 => e1 cmp e2 - (c2+c1)
                        (
                            FuncKind::Add,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_sub_const(e2, c3);
                            coerce_cmp_func(*kind, e1, right)
                        }),
                        // 2.6: e1 + c1 cmp c2 - e2 => e1 + e2 cmp (c2-c1)
                        (
                            FuncKind::Add,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [ExprKind::Const(c2), e2],
                        ) => fold_sub_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            coerce_cmp_func(*kind, e, ExprKind::Const(c3))
                        }),
                        // 2.6: e1 - c1 cmp e2 + c2 => e1 cmp e2 + (c2+c1)
                        (
                            FuncKind::Sub,
                            FuncKind::Add,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_add_const(e2, c3);
                            coerce_cmp_func(*kind, e1, right)
                        }),
                        // 2.6: e1 - c1 cmp e2 - c2 => e1 cmp e2 - (c2-c1)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_sub_const(e2, c3);
                            coerce_cmp_func(*kind, e1, right)
                        }),
                        // 2.6: e1 - c1 cmp c2 - e2 => e1 + e2 cmp (c2+c1)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [e1, ExprKind::Const(c1)],
                            [ExprKind::Const(c2), e2],
                        ) => fold_add_const(c2, c1)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e1, e2]);
                            coerce_cmp_func(*kind, e, ExprKind::Const(c3))
                        }),
                        // 2.6: c1 - e1 cmp e2 + c2 => e2 + e1 flip_cmp (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Add,
                            [ExprKind::Const(c1), e1],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e2, e1]);
                            coerce_cmp_func(flipped_kind, e, ExprKind::Const(c3))
                        }),
                        // 2.6: c1 - e1 cmp e2 - c2 => e2 + e1 flip_cmp (c1+c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [ExprKind::Const(c1), e1],
                            [e2, ExprKind::Const(c2)],
                        ) => fold_add_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = ExprKind::func(FuncKind::Add, vec![e2, e1]);
                            coerce_cmp_func(flipped_kind, e, ExprKind::Const(c3))
                        }),
                        // 2.6: c1 - e1 cmp c2 - e2 => e1 flip_cmp e2 + (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [ExprKind::Const(c1), e1],
                            [ExprKind::Const(c2), e2],
                        ) => fold_sub_const(c1, c2)?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_add_const(e2, c3);
                            coerce_cmp_func(flipped_kind, e1, right)
                        }),
                        _ => None,
                    },
                    _ => None,
                }
            } else {
                None
            }
        }
        Pred::Conj(es) => {
            let eff = simplify_conj(es, null_coalesce)?;
            if eff.is_empty() {
                None
            } else if es.len() == 1 {
                Some(es.pop().unwrap())
            } else {
                Some(ExprKind::pred_conj(mem::take(es)))
            }
        }
        _ => None, // todo: handle other predicates
    };
    Ok(res)
}

#[inline]
fn coerce_cmp_func(kind: PredFuncKind, e1: ExprKind, e2: ExprKind) -> ExprKind {
    // todo: coerce casting
    ExprKind::pred_func(kind, vec![e1, e2])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{
        assert_j_plan1, assert_j_plan2, get_subq_filt_expr, j_catalog, print_plan,
    };
    use crate::lgc::LgcPlan;

    #[test]
    fn test_expr_simplify_neg_neg() {
        let cat = j_catalog();
        assert_j_plan2(
            &cat,
            "select c1 from t1 where --c1",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify_neg() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select c1 from t1 where -(2)", assert_no_filt_expr)
    }

    #[test]
    fn test_expr_simplify_consts_add_sub() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select c1 from t1 where 1+1", assert_no_filt_expr);
        assert_j_plan2(
            &cat,
            "select c1 from t1 where 1-1",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify_add_sub_zero() {
        let cat = j_catalog();
        assert_j_plan2(
            &cat,
            "select c1 from t1 where c1+0",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where c1-0",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        );
    }

    #[test]
    fn test_expr_simplify_zero_add_sub() {
        let cat = j_catalog();
        assert_j_plan2(
            &cat,
            "select c1 from t1 where 0+c1",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where 0-c1",
            "select c1 from t1 where -c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t2 where 0-(c1+c2)",
            "select c1 from t2 where -(c1+c2)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t2 where 0-(c1-c2)",
            "select c1 from t2 where -(c1-c2)",
            assert_eq_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify6_comm() {
        let cat = j_catalog();
        assert_j_plan2(
            &cat,
            "select c1 from t1 where 1+c1",
            "select c1 from t1 where c1+1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where 1+(c1+c0)",
            "select c1 from t1 where (c1+c0)+1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where 1-c1",
            "select c1 from t1 where 1-c1",
            assert_eq_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify_comm_assoc1() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("c1+1+2", "c1+3"),
            ("c1+1-2", "c1+(-1)"),
            ("c1-1+2", "c1-(-1)"),
            ("c1-1-2", "c1-3"),
            ("1-c1-2", "-1-c1"),
            ("1-c1+2", "3-c1"),
        ] {
            let s1 = format!("select c1 from t2 where {}", s1);
            let s2 = format!("select c1 from t2 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_comm_assoc2() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("1+(c1+2)", "c1+3"),
            ("1+(2+c1)", "c1+3"),
            ("1+(c1-2)", "c1+(-1)"),
            ("1+(2-c1)", "3-c1"),
            ("1-(c1+2)", "-1-c1"),
            ("1-(c1-2)", "3-c1"),
            ("1-(2-c1)", "c1+(-1)"),
        ] {
            let s1 = format!("select c1 from t2 where {}", s1);
            let s2 = format!("select c1 from t2 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_comm_assoc3() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("(c1+1)+(c2+2)", "(c1+c2)+3"),
            ("(c1-1)+(c2+2)", "(c1+c2)-(-1)"),
            ("(1-c1)+(c2+2)", "(c2-c1)+3"),
            ("(c1+1)+(c2-2)", "(c1+c2)+(-1)"),
            ("(c1-1)+(c2-2)", "(c1+c2)-3"),
            ("(1-c1)+(c2-2)", "(c2-c1)+(-1)"),
            ("(c1+1)+(2-c2)", "(c1-c2)+3"),
            ("(c1-1)+(2-c2)", "(c1-c2)-(-1)"),
            ("(1-c1)+(2-c2)", "3-(c1+c2)"),
            ("(1-c1)+(c1-c2)", "(1-c1)+(c1-c2)"),
        ] {
            let s1 = format!("select c1 from t2 where {}", s1);
            let s2 = format!("select c1 from t2 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_comm_assoc4() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("(c1+1)-(c2+2)", "(c1-c2)+(-1)"),
            ("(c1-1)-(c2+2)", "(c1-c2)-3"),
            ("(1-c1)-(c2+2)", "-1-(c1+c2)"),
            ("(c1+1)-(c2-2)", "(c1-c2)+3"),
            ("(c1-1)-(c2-2)", "(c1-c2)-(-1)"),
            ("(1-c1)-(c2-2)", "3-(c1+c2)"),
            ("(c1+1)-(2-c2)", "(c1+c2)+(-1)"),
            ("(c1-1)-(2-c2)", "(c1+c2)-3"),
            ("(1-c1)-(2-c2)", "(c2-c1)+(-1)"),
            ("(1-c1)-(c1-c2)", "(1-c1)-(c1-c2)"),
        ] {
            let s1 = format!("select c1 from t2 where {}", s1);
            let s2 = format!("select c1 from t2 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    // fold pred 1.1
    #[test]
    fn test_expr_simplify_exists() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c1 from t1 where not exists (select 1 from t1)",
            |s1, mut q1| {
                expr_simplify(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                match get_subq_filt_expr(q1.root_query().unwrap()).as_slice() {
                    [ExprKind::Pred(Pred::NotExists(_))] => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where not not exists (select 1 from t1)",
            |s1, mut q1| {
                expr_simplify(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                match get_subq_filt_expr(q1.root_query().unwrap()).as_slice() {
                    [ExprKind::Pred(Pred::Exists(_))] => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        )
    }

    #[test]
    fn test_expr_simplify_in() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c1 from t1 where not c1 in (select c1 from t2)",
            |s1, mut q1| {
                expr_simplify(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                match get_subq_filt_expr(q1.root_query().unwrap()).as_slice() {
                    [ExprKind::Pred(Pred::NotInSubquery(..))] => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c1 in (select c1 from t2)",
            "select c1 from t1 where c1 not in (select c1 from t2)",
            assert_eq_filt_expr,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where not c1 not in (select c1 from t2)",
            |s1, mut q1| {
                expr_simplify(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                match get_subq_filt_expr(q1.root_query().unwrap()).as_slice() {
                    [ExprKind::Pred(Pred::InSubquery(..))] => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c1 not in (select c1 from t2)",
            "select c1 from t1 where c1 in (select c1 from t2)",
            assert_eq_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify_not_cmp() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("not c0 = c1", "c0 <> c1"),
            ("not c0 > c1", "c0 <= c1"),
            ("not c0 >= c1", "c0 < c1"),
            ("not c0 < c1", "c0 >= c1"),
            ("not c0 <= c1", "c0 > c1"),
            ("not c0 <> c1", "c0 = c1"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_not_is() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("not c0 is null", "c0 is not null"),
            ("not c0 is not null", "c0 is null"),
            ("not c0 is true", "c0 is not true"),
            ("not c0 is not true", "c0 is true"),
            ("not c0 is false", "c0 is not false"),
            ("not c0 is not false", "c0 is false"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_not_match() {
        let cat = j_catalog();
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c0 like '1'",
            "select c1 from t1 where c0 not like '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c0 not like '1'",
            "select c1 from t1 where c0 like '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c0 regexp '1'",
            "select c1 from t1 where c0 not regexp '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c0 not regexp '1'",
            "select c1 from t1 where c0 regexp '1'",
            assert_eq_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify_not_range() {
        let cat = j_catalog();
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c0 in (1,2,3)",
            "select c1 from t1 where c0 not in (1,2,3)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c0 not in (1,2,3)",
            "select c1 from t1 where c0 in (1,2,3)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c0 between 0 and 2",
            "select c1 from t1 where c0 not between 0 and 2",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not c0 not between 0 and 2",
            "select c1 from t1 where c0 between 0 and 2",
            assert_eq_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify_not_const() {
        let cat = j_catalog();
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not 1",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not 2",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not 1.5",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where not 1.5e5",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan1(&cat, "select c1 from t1 where not 0", assert_no_filt_expr);
        assert_j_plan1(&cat, "select c1 from t1 where not 0.0", assert_no_filt_expr);
        assert_j_plan1(
            &cat,
            "select c1 from t1 where not 0.0e-1",
            assert_no_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify_null_cmp() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("1 = null", "false"),
            ("null = 1", "false"),
            ("null = null", "false"),
            ("c0 = null", "false"),
            ("null = c0", "false"),
            ("1 <=> null", "false"),
            ("c0 <=> null", "c0 <=> null"),
            ("1 is null", "false"),
            ("null is not null", "false"),
            ("null is true", "false"),
            ("0 is true", "false"),
            ("1 is not true", "false"),
            ("null is false", "false"),
            ("1 is false", "false"),
            ("0 is not false", "false"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
        for s1 in vec![
            "null is null",
            "1 is not null",
            "1 is true",
            "0 is not true",
            "0 is false",
            "null is not false",
            "1 is not false",
            "null <=> null",
            "null is not true",
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            assert_j_plan1(&cat, &s1, assert_no_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_cmp_const() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select c1 from t1 where 1 = 1", assert_no_filt_expr);
        assert_j_plan1(
            &cat,
            "select c1 from t1 where 1.0 = true",
            assert_no_filt_expr,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where '1.0' = 1",
            assert_no_filt_expr,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where 'abc' = 0",
            assert_no_filt_expr,
        )
    }

    #[test]
    fn test_expr_simplify_arith_cmp() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("c0 + 1 = 2", "c0 = 1"),
            ("c0 - 1 = 2", "c0 = 3"),
            ("1 - c0 >= 2", "c0 <= -1"),
            ("1 = c0 + 2", "c0 = -1"),
            ("1 = c0 - 2", "c0 = 3"),
            ("1 >= 2 - c0", "c0 >= 1"),
            ("1 = c0", "c0 = 1"),
            ("1 > c0", "c0 < 1"),
            ("1 >= c0", "c0 <= 1"),
            ("1 < c0", "c0 > 1"),
            ("1 < c0", "c0 > 1"),
            ("1 <= c0", "c0 >= 1"),
            ("1 <> c0", "c0 <> 1"),
            ("1 + 1 = c0", "c0 = 2"),
            ("c0 + 1 = c1 + 1", "c0 = c1"),
            ("c0 + 1 = c1 + 2", "c0 = c1 + 1"),
            ("c0 - 1 = c1 + 2", "c0 = c1 + 3"),
            ("1 - c0 > c1 + 2", "c1 + c0 < -1"),
            ("c0 + 1 >= c1 - 2", "c0 >= c1 - 3"),
            ("c0 - 1 < c1 - 2", "c0 < c1 - 1"),
            ("1 - c0 > c1 - 2", "c1 + c0 < 3"),
            ("c0 + 1 <= 2 - c1", "c0 + c1 <= 1"),
            ("c0 - 1 < 2 - c1", "c0 + c1 < 3"),
            ("1 - c0 >= 2 - c1", "c0 <= c1 + (-1)"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_normalize_cmp() {
        let cat = j_catalog();
        assert_j_plan2(
            &cat,
            "select c1 from t1 where c1 > c0",
            "select c1 from t1 where c0 < c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select c1 from t1 where c1 = c0",
            "select c1 from t1 where c0 = c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            &cat,
            "select t1.c1 from t1, t2 where t2.c0 > t1.c1",
            "select t1.c1 from t1, t2 where t1.c1 < t2.c0",
            assert_eq_filt_expr,
        );
    }

    #[test]
    fn test_expr_simplify_conj_redundant() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select c1 from t1 where 1 and 1", assert_no_filt_expr);
        for (s1, s2) in vec![
            ("0 and 1", "false"),
            ("1 and c0 > 0", "c0 > 0"),
            ("c0 > 0 and 1", "c0 > 0"),
            ("false and c0 > 0", "false"),
            ("c0 > 0 and false", "false"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_conj_ge() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("c1 >= 1 and c1 > 3", "c1 > 3"),
            ("c1 >= 1 and c1 > 0", "c1 >= 1"),
            ("c1 >= 1 and c1 >= 3", "c1 >= 3"),
            ("c1 >= 1 and c1 >= 0", "c1 >= 1"),
            ("c1 >= 1 and c1 >= 1", "c1 >= 1"),
            ("c1 >= 1 and c1 < 3", "c1 >= 1 and c1 < 3"),
            ("c1 >= 1 and c1 < 0", "false"),
            ("not(c1 >= 1 and c1 < 0)", "c1 is not null"),
            ("c1 >= 1 and c1 <= 3", "c1 >= 1 and c1 <= 3"),
            ("c1 >= 1 and c1 <= 0", "false"),
            ("not(c1 >= 1 and c1 <= 0)", "c1 is not null"),
            ("c1 >= 1 and c1 <= 1", "c1 = 1"),
            ("c1 >= 1 and c1 = 3", "c1 = 3"),
            ("c1 >= 1 and c1 = 1", "c1 = 1"),
            ("c1 >= 1 and c1 = 0", "false"),
            ("not(c1 >= 1 and c1 = 0)", "c1 is not null"),
            ("c1 >= 1 and c1 <> 3", "c1 >= 1 and c1 <> 3"),
            ("c1 >= 1 and c1 <> 0", "c1 >= 1"),
            ("c1 >= 1 and c1 <> 1", "c1 > 1"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_conj_gt() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("c1 > 1 and c1 > 3", "c1 > 3"),
            ("c1 > 1 and c1 > 0", "c1 > 1"),
            ("c1 > 1 and c1 >= 3", "c1 >= 3"),
            ("c1 > 1 and c1 >= 0", "c1 > 1"),
            ("c1 > 1 and c1 < 3", "c1 > 1 and c1 < 3"),
            ("c1 > 1 and c1 < 0", "false"),
            ("not(c1 > 1 and c1 < 0)", "c1 is not null"),
            ("c1 > 1 and c1 <= 3", "c1 > 1 and c1 <= 3"),
            ("c1 > 1 and c1 <= 0", "false"),
            ("not(c1 > 1 and c1 <= 0)", "c1 is not null"),
            ("c1 > 1 and c1 <= 1", "false"),
            ("not(c1 > 1 and c1 <= 1)", "c1 is not null"),
            ("c1 > 1 and c1 = 3", "c1 = 3"),
            ("c1 > 1 and c1 = 0", "false"),
            ("not(c1 > 1 and c1 = 0)", "c1 is not null"),
            ("c1 > 1 and c1 <> 3", "c1 > 1 and c1 <> 3"),
            ("c1 > 1 and c1 <> 0", "c1 > 1"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_conj_le() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("c1 <= 1 and c1 > 3", "false"),
            ("not(c1 <= 1 and c1 > 3)", "c1 is not null"),
            ("c1 <= 1 and c1 > 0", "c1 <= 1 and c1 > 0"),
            ("c1 <= 1 and c1 >= 3", "false"),
            ("not(c1 <= 1 and c1 >= 3)", "c1 is not null"),
            ("c1 <= 1 and c1 >= 0", "c1 <= 1 and c1 >= 0"),
            ("c1 <= 1 and c1 >= 1", "c1 = 1"),
            ("c1 <= 1 and c1 < 3", "c1 <= 1"),
            ("c1 <= 1 and c1 < 0", "c1 < 0"),
            ("c1 <= 1 and c1 <= 3", "c1 <= 1"),
            ("c1 <= 1 and c1 <= 0", "c1 <= 0"),
            ("c1 <= 1 and c1 <= 1", "c1 <= 1"),
            ("c1 <= 1 and c1 = 1", "c1 = 1"),
            ("c1 <= 1 and c1 = 3", "false"),
            ("not(c1 <= 1 and c1 = 3)", "c1 is not null"),
            ("c1 <= 1 and c1 = 0", "c1 = 0"),
            ("c1 <= 1 and c1 <> 3", "c1 <= 1"),
            ("c1 <= 1 and c1 <> 0", "c1 <= 1 and c1 <> 0"),
            ("c1 <= 1 and c1 <> 1", "c1 < 1"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_conj_lt() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("c1 < 1 and c1 > 3", "false"),
            ("not(c1 < 1 and c1 > 3)", "c1 is not null"),
            ("c1 < 1 and c1 > 0", "c1 < 1 and c1 > 0"),
            ("c1 < 1 and c1 >= 3", "false"),
            ("not(c1 < 1 and c1 >= 3)", "c1 is not null"),
            ("c1 < 1 and c1 >= 0", "c1 < 1 and c1 >= 0"),
            ("c1 < 1 and c1 < 3", "c1 < 1"),
            ("c1 < 1 and c1 < 0", "c1 < 0"),
            ("c1 < 1 and c1 <= 3", "c1 < 1"),
            ("c1 < 1 and c1 <= 0", "c1 <= 0"),
            ("c1 < 1 and c1 >= 0", "c1 < 1 and c1 >= 0"),
            ("c1 < 1 and c1 = 3", "false"),
            ("not(c1 < 1 and c1 = 3)", "c1 is not null"),
            ("c1 < 1 and c1 = 0", "c1 = 0"),
            ("c1 < 1 and c1 <> 3", "c1 < 1"),
            ("c1 < 1 and c1 <> 0", "c1 < 1 and c1 <> 0"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_conj_eq() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("c1 = 1 and c1 > 0", "c1 = 1"),
            ("c1 = 1 and c1 > 3", "false"),
            ("not(c1 = 1 and c1 > 3)", "c1 is not null"),
            ("c1 = 1 and c1 >= 0", "c1 = 1"),
            ("c1 = 1 and c1 >= 3", "false"),
            ("not(c1 = 1 and c1 >= 3)", "c1 is not null"),
            ("c1 = 1 and c1 >= 1", "c1 = 1"),
            ("c1 = 1 and c1 < 0", "false"),
            ("not(c1 = 1 and c1 < 0)", "c1 is not null"),
            ("c1 = 1 and c1 < 3", "c1 = 1"),
            ("c1 = 1 and c1 <= 0", "false"),
            ("not(c1 = 1 and c1 <= 0)", "c1 is not null"),
            ("c1 = 1 and c1 <= 3", "c1 = 1"),
            ("c1 = 1 and c1 <= 1", "c1 = 1"),
            ("c1 = 1 and c1 = 0", "false"),
            ("not(c1 = 1 and c1 = 0)", "c1 is not null"),
            ("c1 = 1 and c1 = 1", "c1 = 1"),
            ("c1 = 1 and c1 <> 0", "c1 = 1"),
            ("c1 = 1 and c1 <> 1", "false"),
            ("not(c1 = 1 and c1 <> 1)", "c1 is not null"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_expr_simplify_conj_ne() {
        let cat = j_catalog();
        for (s1, s2) in vec![
            ("c1 <> 1 and c1 > 3", "c1 > 3"),
            ("c1 <> 1 and c1 > 0", "c1 <> 1 and c1 > 0"),
            ("c1 <> 1 and c1 >= 3", "c1 >= 3"),
            ("c1 <> 1 and c1 >= 0", "c1 <> 1 and c1 >= 0"),
            ("c1 <> 1 and c1 >= 1", "c1 > 1"),
            ("c1 <> 1 and c1 < 3", "c1 <> 1 and c1 < 3"),
            ("c1 <> 1 and c1 < 0", "c1 < 0"),
            ("c1 <> 1 and c1 <= 3", "c1 <> 1 and c1 <= 3"),
            ("c1 <> 1 and c1 <= 0", "c1 <= 0"),
            ("c1 <> 1 and c1 <= 1", "c1 < 1"),
            ("c1 <> 1 and c1 = 3", "c1 = 3"),
            ("c1 <> 1 and c1 = 1", "false"),
            ("not(c1 <> 1 and c1 = 1)", "c1 is not null"),
            ("c1 <> 1 and c1 <> 3", "c1 <> 1 and c1 <> 3"),
            ("c1 <> 1 and c1 <> 1", "c1 <> 1"),
        ] {
            let s1 = format!("select c1 from t1 where {}", s1);
            let s2 = format!("select c1 from t1 where {}", s2);
            assert_j_plan2(&cat, &s1, &s2, assert_eq_filt_expr)
        }
    }

    #[test]
    fn test_col_rejects_null() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 = 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 > 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 >= 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 < 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 <= 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 <> 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 <=> 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 is not null",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 is true",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 is false",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 + 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 - 0",
            assert_col_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where not c1",
            assert_col_rejects_null,
        );
        assert_j_plan1(&cat, "select c1 from t1 where -c1", assert_col_rejects_null);
    }

    #[test]
    fn test_col_not_rejects_null() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 <=> null",
            assert_col_not_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 is null",
            assert_col_not_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 is not true",
            assert_col_not_rejects_null,
        );
        assert_j_plan1(
            &cat,
            "select c1 from t1 where c1 is not false",
            assert_col_not_rejects_null,
        );
    }

    fn assert_eq_filt_expr(s1: &str, mut q1: LgcPlan, s2: &str, q2: LgcPlan) {
        use crate::lgc::reflect;
        use aosa::StringArena;
        expr_simplify(&mut q1.qry_set, q1.root).unwrap();
        print_plan(s1, &q1);
        print_plan(s2, &q2);
        // as we add GlobalID to column, it's not safe to compare expression.
        // instead we convert expression to AST and then compare.
        let cat = j_catalog();
        let arena = StringArena::with_capacity(1024);
        let stmt1 = reflect(&q1, &arena, &cat).unwrap();
        let stmt2 = reflect(&q1, &arena, &cat).unwrap();
        assert_eq!(stmt1, stmt2);
    }

    fn assert_no_filt_expr(s1: &str, mut q1: LgcPlan) {
        expr_simplify(&mut q1.qry_set, q1.root).unwrap();
        print_plan(s1, &q1);
        let filt = get_subq_filt_expr(q1.root_query().unwrap());
        assert!(filt.is_empty());
    }

    fn assert_col_rejects_null(s1: &str, q1: LgcPlan) {
        print_plan(s1, &q1);
        let filter = get_subq_filt_expr(q1.root_query().unwrap());
        assert!(expr_rejects_null(&ExprKind::pred_conj(filter)))
    }

    fn assert_col_not_rejects_null(s1: &str, q1: LgcPlan) {
        print_plan(s1, &q1);
        let filter = get_subq_filt_expr(q1.root_query().unwrap());
        assert!(!expr_rejects_null(&ExprKind::pred_conj(filter)))
    }

    // convert all columns in expressions to null and check if it rejects null
    fn expr_rejects_null(e: &ExprKind) -> bool {
        e.clone()
            .reject_null(|e| match e {
                ExprKind::Col(_) => *e = ExprKind::const_null(),
                _ => (),
            })
            .unwrap()
    }
}
