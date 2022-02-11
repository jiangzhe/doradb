use crate::error::Result;
use crate::op::Op;
use crate::op::OpMutVisitor;
use crate::query::{QueryPlan, QuerySet};
use std::mem;
use xngin_expr::fold::*;
use xngin_expr::{
    Const, Expr, ExprMutVisitor, Func, FuncKind, Pred, PredFunc, PredFuncKind, QueryID,
};

/// Simplify expressions.
#[inline]
pub fn expr_simplify(QueryPlan { queries, root }: &mut QueryPlan) -> Result<()> {
    let mut subqueries = vec![*root];
    while let Some(qry_id) = subqueries.pop() {
        simplify_single(queries, qry_id, &mut subqueries)?
    }
    Ok(())
}

fn simplify_single(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    subqueries: &mut Vec<QueryID>,
) -> Result<()> {
    if let Some(subq) = qry_set.get_mut(&qry_id) {
        for (_, subq_id) in subq.scope.query_aliases.iter().rev() {
            subqueries.push(*subq_id)
        }
        let mut es = ExprSimplify { res: Ok(()) };
        let _ = subq.root.walk_mut(&mut es);
        es.res?
    }
    Ok(())
}

struct ExprSimplify {
    res: Result<()>,
}

impl OpMutVisitor for ExprSimplify {
    #[inline]
    fn enter(&mut self, op: &mut Op) -> bool {
        for e in op.exprs_mut() {
            let _ = e.walk_mut(self);
        }
        self.res.is_ok()
    }

    #[inline]
    fn leave(&mut self, _op: &mut Op) -> bool {
        true
    }
}

impl ExprMutVisitor for ExprSimplify {
    #[inline]
    fn enter(&mut self, _e: &mut Expr) -> bool {
        true
    }

    /// simplify expression bottom up.
    #[inline]
    fn leave(&mut self, e: &mut Expr) -> bool {
        self.res = const_fold(e);
        self.res.is_ok()
    }
}

/// Fold constants such as 1 + 1, 2 < 3, etc.
fn const_fold(e: &mut Expr) -> Result<()> {
    match e {
        Expr::Func(f) => {
            if let Some(new) = simplify_func(f)? {
                *e = new
            }
        }
        Expr::Pred(p) => {
            if let Some(new) = simplify_pred(p)? {
                *e = new
            }
        }
        _ => (), // All other kinds are skipped in constant folding
    }
    Ok(())
}

/// Simplify function.
///
/// 1. remove pair of negating, e.g.
/// --e => e
/// 2. compute negating constant, e.g.
/// -c => new_c
/// 3. compute addition of constants, e.g.
/// 1+1 => 2
/// 4. remote adding zero, e.g.
/// e+0 => e
/// 5. swap order of variable in addtion, e.g.
/// 1+e => e+1
/// 6. associative, e.g.
/// (e+1)+2 => e+3
/// Note: (1+e)+2 => e+3 -- won't happen after rule 5, only for add/mul
/// 7. commutative and associative, e.g.
/// 1+(e+2) => e+3
/// Note: 1+(2+e) => e+3 -- won't happen after rule 5, only for add/mul
/// 8. commutative and associative, e.g.
/// (e1+1)+(e2+2) => (e1+e2)+3
fn simplify_func(f: &mut Func) -> Result<Option<Expr>> {
    let res = match f.kind {
        FuncKind::Neg => match &mut f.args[0] {
            // rule 1: --e => e
            // todo: should cast to f64 if original expression is not numeric
            Expr::Func(Func { kind, args }) if *kind == FuncKind::Neg => {
                Some(mem::take(&mut args[0]))
            }
            // rule 2: -c => new_c
            Expr::Const(c) => FoldNeg(c).fold()?.map(Expr::Const),
            _ => None,
        },
        FuncKind::Add => match f.args.as_mut() {
            // rule 3: 1+1 => 2
            [Expr::Const(c1), Expr::Const(c2)] => FoldAdd(c1, c2).fold()?.map(Expr::Const),
            [e, Expr::Const(c1)] => {
                if c1.is_zero().unwrap_or_default() {
                    // rule 4: e+0 => e
                    let e = mem::take(e);
                    Some(coerce_numeric(e))
                } else if let Expr::Func(Func { kind, args }) = e {
                    match (kind, args.as_mut()) {
                        // rule 6: (e1+c2)+c1 => e1 + (c2+c1)
                        (FuncKind::Add, [e1, Expr::Const(c2)]) => {
                            FoldAdd(c2, c1).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                expr_add_const(e1, c3)
                            })
                        }
                        // rule 6: (e1-c2)+c1 => e1 - (c2-c1)
                        (FuncKind::Sub, [e1, Expr::Const(c2)]) => {
                            FoldSub(c2, c1).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                expr_sub_const(e1, c3)
                            })
                        }
                        // rule 6: (c2-e1)+c1 => (c2+c1) - e1
                        (FuncKind::Sub, [Expr::Const(c2), e1]) => {
                            FoldAdd(c2, c1).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                const_sub_expr(c3, e1)
                            })
                        }

                        _ => None,
                    }
                } else {
                    None
                }
            }
            [Expr::Const(c1), e] => {
                if c1.is_zero().unwrap_or_default() {
                    // rule 4: 0+e => e
                    let e = mem::take(e);
                    Some(coerce_numeric(e))
                } else {
                    match e {
                        Expr::Func(Func { kind, args }) => match (kind, args.as_mut()) {
                            // rule 7: c1 + (e1+c2) => e1 + (c1+c2)
                            (FuncKind::Add, [e1, Expr::Const(c2)]) => {
                                FoldAdd(c1, c2).fold()?.map(|c3| {
                                    let e1 = mem::take(e1);
                                    expr_add_const(e1, c3)
                                })
                            }
                            // rule 7: c1 + (e1-c2) => e1 + (c1-c2)
                            (FuncKind::Sub, [e1, Expr::Const(c2)]) => {
                                FoldSub(c1, c2).fold()?.map(|c3| {
                                    let e1 = mem::take(e1);
                                    expr_add_const(e1, c3)
                                })
                            }
                            // rule 7: c1 + (c2-e1) => (c1+c2) - e1
                            (FuncKind::Sub, [Expr::Const(c2), e1]) => {
                                FoldAdd(c1, c2).fold()?.map(|c3| {
                                    let e1 = mem::take(e1);
                                    const_sub_expr(c3, e1)
                                })
                            }
                            // rule 5: c1 + e1 => e1 + c1
                            _ => {
                                let e1 = mem::take(e);
                                let c1 = mem::take(c1);
                                Some(Expr::func(FuncKind::Add, vec![e1, Expr::Const(c1)]))
                            }
                        },
                        // rule 5
                        _ => {
                            let e1 = mem::take(e);
                            let c1 = mem::take(c1);
                            Some(Expr::func(FuncKind::Add, vec![e1, Expr::Const(c1)]))
                        }
                    }
                }
            }
            [Expr::Func(Func { kind: k1, args: a1 }), Expr::Func(Func { kind: k2, args: a2 })] => {
                match (k1, k2, a1.as_mut(), a2.as_mut()) {
                    // rule 8.1: (e1+c1)+(e2+c2) => (e1+e2) + (c1+c2)
                    (
                        FuncKind::Add,
                        FuncKind::Add,
                        [e1, Expr::Const(c1)],
                        [e2, Expr::Const(c2)],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        expr_add_const(e, c3)
                    }),
                    // rule 8.2: (e1+c1)+(e2-c2) => (e1+e2) + (c1-c2)
                    (
                        FuncKind::Add,
                        FuncKind::Sub,
                        [e1, Expr::Const(c1)],
                        [e2, Expr::Const(c2)],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        expr_add_const(e, c3)
                    }),
                    // rule 8.3: (e1-c1)+(e2+c2) => (e1+e2) - (c1-c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Add,
                        [e1, Expr::Const(c1)],
                        [e2, Expr::Const(c2)],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        expr_sub_const(e, c3)
                    }),
                    // rule 8.4: (e1-c1)+(e2-c2) => (e1+e2) - (c1+c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Sub,
                        [e1, Expr::Const(c1)],
                        [e2, Expr::Const(c2)],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        expr_sub_const(e, c3)
                    }),
                    // rule 8.5: (c1-e1)+(e2-c2) => (e2-e1) + (c1-c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Sub,
                        [Expr::Const(c1), e1],
                        [e2, Expr::Const(c2)],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e2, e1]);
                        expr_add_const(e, c3)
                    }),
                    // rule 8.6: (c1-e1)+(e2+c2) => (e2-e1) + (c1+c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Add,
                        [Expr::Const(c1), e1],
                        [e2, Expr::Const(c2)],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e2, e1]);
                        expr_add_const(e, c3)
                    }),
                    // rule 8.7: (c1-e1)+(c2-e2) => (c1+c2) - (e1+e2)
                    (
                        FuncKind::Sub,
                        FuncKind::Sub,
                        [Expr::Const(c1), e1],
                        [Expr::Const(c2), e2],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        const_sub_expr(c3, e)
                    }),
                    // rule 8.8: (e1-c1)+(c2-e2) => (e1-e2) - (c1-c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Sub,
                        [e1, Expr::Const(c1)],
                        [Expr::Const(c2), e2],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e1, e2]);
                        expr_sub_const(e, c3)
                    }),
                    // rule 8.9: (e1+c1)+(c2-e2) => (e1-e2) + (c1+c2)
                    (
                        FuncKind::Add,
                        FuncKind::Sub,
                        [e1, Expr::Const(c1)],
                        [Expr::Const(c2), e2],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e1, e2]);
                        expr_add_const(e, c3)
                    }),
                    _ => None,
                }
            }
            _ => None,
        },
        FuncKind::Sub => match f.args.as_mut() {
            // rule 3: 1-1 => 0
            [Expr::Const(c1), Expr::Const(c2)] => FoldSub(c1, c2).fold()?.map(Expr::Const),
            [e, Expr::Const(c1)] => {
                if c1.is_zero().unwrap_or_default() {
                    // rule 4: e-0 => e
                    let e = mem::take(e);
                    Some(coerce_numeric(e))
                } else if let Expr::Func(Func { kind, args }) = e {
                    match (kind, args.as_mut()) {
                        // rule 6: (e1+c2)-c1 => e1 + (c2-c1)
                        (FuncKind::Add, [e1, Expr::Const(c2)]) => {
                            FoldSub(c2, c1).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                expr_add_const(e1, c3)
                            })
                        }
                        // rule 6: (e1-c2)-c1 => e1 - (c2+c1)
                        (FuncKind::Sub, [e1, Expr::Const(c2)]) => {
                            FoldAdd(c2, c1).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                expr_sub_const(e1, c3)
                            })
                        }
                        // rule 6: (c2-e1)-c1 => (c2-c1) - e1
                        (FuncKind::Sub, [Expr::Const(c2), e1]) => {
                            FoldSub(c2, c1).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                const_sub_expr(c3, e1)
                            })
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            [Expr::Const(c1), e] => {
                match e {
                    Expr::Func(Func { kind, args }) => match (kind, args.as_mut()) {
                        // rule 7: c1 - (e1+c2) => (c1-c2) - e1
                        (FuncKind::Add, [e1, Expr::Const(c2)]) => {
                            FoldSub(c1, c2).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                const_sub_expr(c3, e1)
                            })
                        }
                        // rule 7: c1 - (e1-c2) => (c1+c2) - e1
                        (FuncKind::Sub, [e1, Expr::Const(c2)]) => {
                            FoldAdd(c1, c2).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                const_sub_expr(c3, e1)
                            })
                        }
                        // rule 7: c1 - (c2-e1) => e1 + (c1-c2)
                        (FuncKind::Sub, [Expr::Const(c2), e1]) => {
                            FoldSub(c1, c2).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                expr_add_const(e1, c3)
                            })
                        }
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
            [Expr::Func(Func { kind: k1, args: a1 }), Expr::Func(Func { kind: k2, args: a2 })] => {
                match (k1, k2, a1.as_mut(), a2.as_mut()) {
                    // rule 8.1: (e1+c1)-(e2+c2) => (e1-e2) + (c1-c2)
                    (
                        FuncKind::Add,
                        FuncKind::Add,
                        [e1, Expr::Const(c1)],
                        [e2, Expr::Const(c2)],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e1, e2]);
                        expr_add_const(e, c3)
                    }),
                    // rule 8.2: (e1-c1)-(e2+c2) => (e1-e2) - (c1+c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Add,
                        [e1, Expr::Const(c1)],
                        [e2, Expr::Const(c2)],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e1, e2]);
                        expr_sub_const(e, c3)
                    }),
                    // rule 8.3: (e1+c1)-(e2-c2) => (e1-e2) + (c1+c2)
                    (
                        FuncKind::Add,
                        FuncKind::Sub,
                        [e1, Expr::Const(c1)],
                        [e2, Expr::Const(c2)],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e1, e2]);
                        expr_add_const(e, c3)
                    }),
                    // rule 8.4: (e1-c1)-(e2-c2) => (e1-e2) - (c1-c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Sub,
                        [e1, Expr::Const(c1)],
                        [e2, Expr::Const(c2)],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e1, e2]);
                        expr_sub_const(e, c3)
                    }),
                    // rule 8.5: (c1-e1)-(e2-c2) => (c1+c2) - (e1+e2)
                    (
                        FuncKind::Sub,
                        FuncKind::Sub,
                        [Expr::Const(c1), e1],
                        [e2, Expr::Const(c2)],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        const_sub_expr(c3, e)
                    }),
                    // rule 8.6: (c1-e1)-(e2+c2) => (c1-c2) - (e1+e2)
                    (
                        FuncKind::Sub,
                        FuncKind::Add,
                        [Expr::Const(c1), e1],
                        [e2, Expr::Const(c2)],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        const_sub_expr(c3, e)
                    }),
                    // rule 8.7: (c1-e1)-(c2-e2) => (e2-e1) + (c1-c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Sub,
                        [Expr::Const(c1), e1],
                        [Expr::Const(c2), e2],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Sub, vec![e2, e1]);
                        expr_add_const(e, c3)
                    }),
                    // rule 8.8: (e1-c1)-(c2-e2) => (e1+e2) - (c1+c2)
                    (
                        FuncKind::Sub,
                        FuncKind::Sub,
                        [e1, Expr::Const(c1)],
                        [Expr::Const(c2), e2],
                    ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        expr_sub_const(e, c3)
                    }),
                    // rule 8.9: (e1+c1)-(c2-e2) => (e1+e2) + (c1-c2)
                    (
                        FuncKind::Add,
                        FuncKind::Sub,
                        [e1, Expr::Const(c1)],
                        [Expr::Const(c2), e2],
                    ) => FoldSub(c1, c2).fold()?.map(|c3| {
                        let e1 = mem::take(e1);
                        let e2 = mem::take(e2);
                        let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                        expr_add_const(e, c3)
                    }),
                    _ => None,
                }
            }
            _ => None,
        },
        _ => todo!(),
    };
    Ok(res)
}

fn expr_add_const(e: Expr, c: Const) -> Expr {
    if c.is_zero().unwrap_or_default() {
        coerce_numeric(e)
    } else {
        Expr::func(FuncKind::Add, vec![e, Expr::Const(c)])
    }
}

fn coerce_numeric(e: Expr) -> Expr {
    // todo: add casting
    e
}

fn expr_sub_const(e: Expr, c: Const) -> Expr {
    if c.is_zero().unwrap_or_default() {
        coerce_numeric(e)
    } else {
        Expr::func(FuncKind::Sub, vec![e, Expr::Const(c)])
    }
}

fn negate(e: Expr) -> Expr {
    Expr::func(FuncKind::Neg, vec![e])
}

fn const_sub_expr(c: Const, e: Expr) -> Expr {
    if c.is_zero().unwrap_or_default() {
        negate(e)
    } else {
        Expr::func(FuncKind::Sub, vec![Expr::Const(c), e])
    }
}

/// Simplify predicate.
///
/// 1. NOT
/// 1.1. not Exists => NotExists
/// 1.2. not NotExists => Exists
/// 1.3. not In => NotIn
/// 1.4. not NotIn => In
/// 1.5. not cmp => logical flipped cmp
/// 1.6. not const => new_const
/// 1.7. not not e => cast(e as bool)
///
/// 2. Comparison
/// 2.1. null cmp const1 => null, except for SafeEqual
/// 2.2. const1 cmp const2 => new_const
/// 2.3. e1 + c1 cmp c2 => e1 cmp new_c
/// 2.4. c1 cmp e1 + c2 => new_c cmp e1 => e1 flip_cmp new_c
/// 2.5. c1 cmp e1 => e1 flip_cmp c1
/// 2.6. e1 + c1 cmp e2 + c2 => e1 cmp e2 + new_c
/// Note: 2.3 ~ 2.6 requires cmp operator can be positional flipped.
///
/// 3. CNF
///
/// 4. DNF
///
/// 5. EXISTS/NOT EXISTS
///
/// 6. IN/NOT IN
fn simplify_pred(p: &mut Pred) -> Result<Option<Expr>> {
    let res = match p {
        Pred::Not(e) => match e.as_mut() {
            // 1.1
            Expr::Pred(Pred::Exists(subq)) => Some(Expr::Pred(Pred::NotExists(mem::take(subq)))),
            // 1.2
            Expr::Pred(Pred::NotExists(subq)) => Some(Expr::Pred(Pred::Exists(mem::take(subq)))),
            // 1.3
            Expr::Pred(Pred::InSubquery(lhs, subq)) => Some(Expr::Pred(Pred::NotInSubquery(
                mem::take(lhs),
                mem::take(subq),
            ))),
            // 1.4
            Expr::Pred(Pred::NotInSubquery(lhs, subq)) => Some(Expr::Pred(Pred::InSubquery(
                mem::take(lhs),
                mem::take(subq),
            ))),
            // 1.5
            Expr::Pred(Pred::Func(PredFunc { kind, args })) => kind.logic_flip().map(|kind| {
                let args = mem::take(args);
                Expr::Pred(Pred::Func(PredFunc { kind, args }))
            }),
            // 1.6
            Expr::Const(c) => FoldNot(c).fold()?.map(Expr::Const),
            // 1.7 todo
            Expr::Pred(Pred::Not(_e)) => None,
            _ => None,
        },
        Pred::Func(PredFunc { kind, args }) => {
            // 2.1 and 2.2
            let res = match kind {
                PredFuncKind::Equal => FoldEqual(&args[0], &args[1]).fold()?,
                PredFuncKind::Greater => FoldGreater(&args[0], &args[1]).fold()?,
                PredFuncKind::GreaterEqual => FoldGreaterEqual(&args[0], &args[1]).fold()?,
                PredFuncKind::Less => FoldLess(&args[0], &args[1]).fold()?,
                PredFuncKind::LessEqual => FoldLessEqual(&args[0], &args[1]).fold()?,
                PredFuncKind::NotEqual => FoldNotEqual(&args[0], &args[1]).fold()?,
                PredFuncKind::SafeEqual => FoldSafeEqual(&args[0], &args[1]).fold()?,
                PredFuncKind::IsNull => FoldIsNull(&args[0]).fold()?,
                PredFuncKind::IsNotNull => FoldIsNotNull(&args[0]).fold()?,
                PredFuncKind::IsTrue | PredFuncKind::IsNotFalse => FoldIsTrue(&args[0]).fold()?,
                PredFuncKind::IsFalse | PredFuncKind::IsNotTrue => FoldIsFalse(&args[0]).fold()?,
                _ => None, // todo
            }
            .map(Expr::Const);
            if res.is_some() {
                // already folded as constant
                res
            } else if let Some(flipped_kind) = kind.pos_flip() {
                match args.as_mut() {
                    // 2.3: e1 + c1 cmp c2 => e1 cmp c3
                    [Expr::Func(Func {
                        kind: FuncKind::Add,
                        args: fargs,
                    }), Expr::Const(c2)] => match fargs.as_mut() {
                        [e1, Expr::Const(c1)] => FoldSub(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(*kind, e1, Expr::Const(c3))
                        }),
                        _ => None,
                    },
                    [Expr::Func(Func {
                        kind: FuncKind::Sub,
                        args: fargs,
                    }), Expr::Const(c2)] => match fargs.as_mut() {
                        // 2.3: e1 - c1 cmp c2
                        [e1, Expr::Const(c1)] => FoldAdd(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(*kind, e1, Expr::Const(c3))
                        }),
                        // 2.3: c1 - e1 cmp c2
                        [Expr::Const(c1), e1] => {
                            // e1 flip_cmp (c1-c2)
                            FoldSub(c1, c2).fold()?.map(|c3| {
                                let e1 = mem::take(e1);
                                coerce_cmp_func(flipped_kind, e1, Expr::Const(c3))
                            })
                        }
                        _ => None,
                    },
                    // 2.4: c1 cmp e1 + c2 => e1 flip_cmp c3
                    [Expr::Const(c1), Expr::Func(Func {
                        kind: FuncKind::Add,
                        args: fargs,
                    })] => match fargs.as_mut() {
                        [e1, Expr::Const(c2)] => FoldSub(c1, c2).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(flipped_kind, e1, Expr::Const(c3))
                        }),
                        _ => None,
                    },
                    [Expr::Const(c1), Expr::Func(Func {
                        kind: FuncKind::Sub,
                        args: fargs,
                    })] => match fargs.as_mut() {
                        // 2.4: c1 cmp e1 - c2 => e1 flip_cmp (c1+c2)
                        [e1, Expr::Const(c2)] => FoldAdd(c1, c2).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(flipped_kind, e1, Expr::Const(c3))
                        }),
                        // 2.4: c1 cmp c2 - e1 => e1 cmp (c2-c1)
                        [Expr::Const(c2), e1] => FoldSub(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            coerce_cmp_func(*kind, e1, Expr::Const(c3))
                        }),
                        _ => None,
                    },
                    // 2.5: c1 cmp e1 => e1 flip_cmp c1
                    [c1 @ Expr::Const(_), e1] => {
                        let c1 = mem::take(c1);
                        let e1 = mem::take(e1);
                        Some(coerce_cmp_func(flipped_kind, e1, c1))
                    }
                    [Expr::Func(Func {
                        kind: kind1,
                        args: args1,
                    }), Expr::Func(Func {
                        kind: kind2,
                        args: args2,
                    })] => match (kind1, kind2, args1.as_mut(), args2.as_mut()) {
                        // 2.6: e1 + c1 cmp e2 + c2 => e1 cmp e2 + (c2-c1)
                        (
                            FuncKind::Add,
                            FuncKind::Add,
                            [e1, Expr::Const(c1)],
                            [e2, Expr::Const(c2)],
                        ) => FoldSub(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_add_const(e2, c3);
                            coerce_cmp_func(*kind, e1, right)
                        }),
                        // 2.6: e1 + c1 cmp e2 - c2 => e1 cmp e2 - (c2+c1)
                        (
                            FuncKind::Add,
                            FuncKind::Sub,
                            [e1, Expr::Const(c1)],
                            [e2, Expr::Const(c2)],
                        ) => FoldAdd(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_sub_const(e2, c3);
                            coerce_cmp_func(*kind, e1, right)
                        }),
                        // 2.6: e1 + c1 cmp c2 - e2 => e1 + e2 cmp (c2-c1)
                        (
                            FuncKind::Add,
                            FuncKind::Sub,
                            [e1, Expr::Const(c1)],
                            [Expr::Const(c2), e2],
                        ) => FoldSub(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                            coerce_cmp_func(*kind, e, Expr::Const(c3))
                        }),
                        // 2.6: e1 - c1 cmp e2 + c2 => e1 cmp e2 + (c2+c1)
                        (
                            FuncKind::Sub,
                            FuncKind::Add,
                            [e1, Expr::Const(c1)],
                            [e2, Expr::Const(c2)],
                        ) => FoldAdd(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_add_const(e2, c3);
                            coerce_cmp_func(*kind, e1, right)
                        }),
                        // 2.6: e1 - c1 cmp e2 - c2 => e1 cmp e2 - (c2-c1)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [e1, Expr::Const(c1)],
                            [e2, Expr::Const(c2)],
                        ) => FoldSub(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let right = expr_sub_const(e2, c3);
                            coerce_cmp_func(*kind, e1, right)
                        }),
                        // 2.6: e1 - c1 cmp c2 - e2 => e1 + e2 cmp (c2+c1)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [e1, Expr::Const(c1)],
                            [Expr::Const(c2), e2],
                        ) => FoldAdd(c2, c1).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = Expr::func(FuncKind::Add, vec![e1, e2]);
                            coerce_cmp_func(*kind, e, Expr::Const(c3))
                        }),
                        // 2.6: c1 - e1 cmp e2 + c2 => e2 + e1 flip_cmp (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Add,
                            [Expr::Const(c1), e1],
                            [e2, Expr::Const(c2)],
                        ) => FoldSub(c1, c2).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = Expr::func(FuncKind::Add, vec![e2, e1]);
                            coerce_cmp_func(flipped_kind, e, Expr::Const(c3))
                        }),
                        // 2.6: c1 - e1 cmp e2 - c2 => e2 + e1 flip_cmp (c1+c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [Expr::Const(c1), e1],
                            [e2, Expr::Const(c2)],
                        ) => FoldAdd(c1, c2).fold()?.map(|c3| {
                            let e1 = mem::take(e1);
                            let e2 = mem::take(e2);
                            let e = Expr::func(FuncKind::Add, vec![e2, e1]);
                            coerce_cmp_func(flipped_kind, e, Expr::Const(c3))
                        }),
                        // 2.6: c1 - e1 cmp c2 - e2 => e1 flip_cmp e2 + (c1-c2)
                        (
                            FuncKind::Sub,
                            FuncKind::Sub,
                            [Expr::Const(c1), e1],
                            [Expr::Const(c2), e2],
                        ) => FoldSub(c1, c2).fold()?.map(|c3| {
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
        _ => None, // todo: handle other predicates
    };
    Ok(res)
}

fn coerce_cmp_func(kind: PredFuncKind, e1: Expr, e2: Expr) -> Expr {
    // todo: coerce casting
    Expr::pred_func(kind, vec![e1, e2])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan, assert_j_plan2, get_filt_expr, print_plan};

    // fold func 1
    #[test]
    fn test_expr_simplify1() {
        assert_j_plan2(
            "select c1 from t1 where --c1",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        )
    }

    // fold func 2
    #[test]
    fn test_expr_simplify2() {
        assert_j_plan2(
            "select c1 from t1 where -(2)",
            "select c1 from t1 where -2",
            assert_eq_filt_expr,
        )
    }

    // fold func 3
    #[test]
    fn test_expr_simplify3() {
        assert_j_plan2(
            "select c1 from t1 where 1+1",
            "select c1 from t1 where 2",
            assert_eq_filt_expr,
        )
    }

    // fold func 4
    #[test]
    fn test_expr_simplify4() {
        assert_j_plan2(
            "select c1 from t1 where c1+0",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c1-0",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        );
    }

    // fold func 4
    #[test]
    fn test_expr_simplify5() {
        assert_j_plan2(
            "select c1 from t1 where 0+c1",
            "select c1 from t1 where c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 0-c1",
            "select c1 from t1 where -c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where 0-(c1+c2)",
            "select c1 from t2 where -(c1+c2)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where 0-(c1-c2)",
            "select c1 from t2 where -(c1-c2)",
            assert_eq_filt_expr,
        )
    }

    // fold func 5
    #[test]
    fn test_expr_simplify6() {
        assert_j_plan2(
            "select c1 from t1 where 1+c1",
            "select c1 from t1 where c1+1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1+(c1+c0)",
            "select c1 from t1 where (c1+c0)+1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1-c1",
            "select c1 from t1 where 1-c1",
            assert_eq_filt_expr,
        )
    }

    // fold func 6
    #[test]
    fn test_expr_simplify7() {
        assert_j_plan2(
            "select c1 from t1 where c1+1+2",
            "select c1 from t1 where c1+3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c1+1-2",
            "select c1 from t1 where c1+(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c1-1+2",
            "select c1 from t1 where c1-(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c1-1-2",
            "select c1 from t1 where c1-3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1-c1-2",
            "select c1 from t1 where -1-c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1-c1+2",
            "select c1 from t1 where 3-c1",
            assert_eq_filt_expr,
        );
    }

    // fold func 7
    #[test]
    fn test_expr_simplify8() {
        assert_j_plan2(
            "select c1 from t1 where 1+(c1+2)",
            "select c1 from t1 where c1+3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1+(2+c1)",
            "select c1 from t1 where c1+3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1+(c1-2)",
            "select c1 from t1 where c1+(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1+(2-c1)",
            "select c1 from t1 where 3-c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1-(c1+2)",
            "select c1 from t1 where -1-c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1-(c1-2)",
            "select c1 from t1 where 3-c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1-(2-c1)",
            "select c1 from t1 where c1+(-1)",
            assert_eq_filt_expr,
        );
    }

    // fold func 8
    #[test]
    fn test_expr_simplify9() {
        assert_j_plan2(
            "select c1 from t2 where (c1+1)+(c2+2)",
            "select c1 from t2 where (c1+c2)+3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1-1)+(c2+2)",
            "select c1 from t2 where (c1+c2)-(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (1-c1)+(c2+2)",
            "select c1 from t2 where (c2-c1)+3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1+1)+(c2-2)",
            "select c1 from t2 where (c1+c2)+(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1-1)+(c2-2)",
            "select c1 from t2 where (c1+c2)-3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (1-c1)+(c2-2)",
            "select c1 from t2 where (c2-c1)+(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1+1)+(2-c2)",
            "select c1 from t2 where (c1-c2)+3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1-1)+(2-c2)",
            "select c1 from t2 where (c1-c2)-(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (1-c1)+(2-c2)",
            "select c1 from t2 where 3-(c1+c2)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (1-c1)+(c1-c2)",
            "select c1 from t2 where (1-c1)+(c1-c2)",
            assert_eq_filt_expr,
        );
    }

    // fold func 8
    #[test]
    fn test_expr_simplify10() {
        assert_j_plan2(
            "select c1 from t2 where (c1+1)-(c2+2)",
            "select c1 from t2 where (c1-c2)+(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1-1)-(c2+2)",
            "select c1 from t2 where (c1-c2)-3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (1-c1)-(c2+2)",
            "select c1 from t2 where -1-(c1+c2)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1+1)-(c2-2)",
            "select c1 from t2 where (c1-c2)+3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1-1)-(c2-2)",
            "select c1 from t2 where (c1-c2)-(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (1-c1)-(c2-2)",
            "select c1 from t2 where 3-(c1+c2)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1+1)-(2-c2)",
            "select c1 from t2 where (c1+c2)+(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (c1-1)-(2-c2)",
            "select c1 from t2 where (c1+c2)-3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (1-c1)-(2-c2)",
            "select c1 from t2 where (c2-c1)+(-1)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t2 where (1-c1)-(c1-c2)",
            "select c1 from t2 where (1-c1)-(c1-c2)",
            assert_eq_filt_expr,
        );
    }

    // fold pred 1.1
    #[test]
    fn test_expr_simplify11() {
        assert_j_plan(
            "select c1 from t1 where not exists (select 1 from t1)",
            |s1, mut q1| {
                expr_simplify(&mut q1).unwrap();
                print_plan(s1, &q1);
                match get_filt_expr(&q1) {
                    Some(Expr::Pred(Pred::NotExists(_))) => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        )
    }

    // fold pred 1.2
    #[test]
    fn test_expr_simplify12() {
        assert_j_plan(
            "select c1 from t1 where not not exists (select 1 from t1)",
            |s1, mut q1| {
                expr_simplify(&mut q1).unwrap();
                print_plan(s1, &q1);
                match get_filt_expr(&q1) {
                    Some(Expr::Pred(Pred::Exists(_))) => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        )
    }

    // fold pred 1.3
    #[test]
    fn test_expr_simplify13() {
        assert_j_plan(
            "select c1 from t1 where not c1 in (select c1 from t2)",
            |s1, mut q1| {
                expr_simplify(&mut q1).unwrap();
                print_plan(s1, &q1);
                match get_filt_expr(&q1) {
                    Some(Expr::Pred(Pred::NotInSubquery(..))) => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        );
        assert_j_plan2(
            "select c1 from t1 where not c1 in (select c1 from t2)",
            "select c1 from t1 where c1 not in (select c1 from t2)",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.4
    #[test]
    fn test_expr_simplify14() {
        assert_j_plan(
            "select c1 from t1 where not c1 not in (select c1 from t2)",
            |s1, mut q1| {
                expr_simplify(&mut q1).unwrap();
                print_plan(s1, &q1);
                match get_filt_expr(&q1) {
                    Some(Expr::Pred(Pred::InSubquery(..))) => (),
                    other => panic!("unmatched filter: {:?}", other),
                }
            },
        );
        assert_j_plan2(
            "select c1 from t1 where not c1 not in (select c1 from t2)",
            "select c1 from t1 where c1 in (select c1 from t2)",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.5
    #[test]
    fn test_expr_simplify15() {
        assert_j_plan2(
            "select c1 from t1 where not c0 = c1",
            "select c1 from t1 where c0 <> c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 > c1",
            "select c1 from t1 where c0 <= c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 >= c1",
            "select c1 from t1 where c0 < c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 < c1",
            "select c1 from t1 where c0 >= c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 <= c1",
            "select c1 from t1 where c0 > c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 <> c1",
            "select c1 from t1 where c0 = c1",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.5
    #[test]
    fn test_expr_simplify16() {
        assert_j_plan2(
            "select c1 from t1 where not c0 is null",
            "select c1 from t1 where c0 is not null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is not null",
            "select c1 from t1 where c0 is null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is true",
            "select c1 from t1 where c0 is not true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is not true",
            "select c1 from t1 where c0 is true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is false",
            "select c1 from t1 where c0 is not false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 is not false",
            "select c1 from t1 where c0 is false",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.5
    #[test]
    fn test_expr_simplify17() {
        assert_j_plan2(
            "select c1 from t1 where not c0 like '1'",
            "select c1 from t1 where c0 not like '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 not like '1'",
            "select c1 from t1 where c0 like '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 regexp '1'",
            "select c1 from t1 where c0 not regexp '1'",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 not regexp '1'",
            "select c1 from t1 where c0 regexp '1'",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.5
    #[test]
    fn test_expr_simplify18() {
        assert_j_plan2(
            "select c1 from t1 where not c0 in (1,2,3)",
            "select c1 from t1 where c0 not in (1,2,3)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 not in (1,2,3)",
            "select c1 from t1 where c0 in (1,2,3)",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 between 0 and 2",
            "select c1 from t1 where c0 not between 0 and 2",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not c0 not between 0 and 2",
            "select c1 from t1 where c0 between 0 and 2",
            assert_eq_filt_expr,
        )
    }

    // fold pred 1.6
    #[test]
    fn test_expr_simplify19() {
        assert_j_plan2(
            "select c1 from t1 where not 1",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 2",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 1.5",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 1.5e5",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 0",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 0.0",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where not 0.0e-1",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        )
    }

    // fold pred 2.1
    #[test]
    fn test_expr_simplify20() {
        assert_j_plan2(
            "select c1 from t1 where 1 = null",
            "select c1 from t1 where null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where null = 1",
            "select c1 from t1 where null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where null = null",
            "select c1 from t1 where null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 = null",
            "select c1 from t1 where null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where null = c0",
            "select c1 from t1 where null",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where null <=> null",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 <=> null",
            "select c1 from t1 where false",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 <=> null",
            "select c1 from t1 where c0 <=> null",
            assert_eq_filt_expr,
        );
    }

    // fold pred 2.2
    #[test]
    fn test_expr_simplify21() {
        assert_j_plan2(
            "select c1 from t1 where 1 = 1",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1.0 = true",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where '1.0' = 1",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 'abc' = 0",
            "select c1 from t1 where true",
            assert_eq_filt_expr,
        )
    }

    // fold pred 2.3
    #[test]
    fn test_expr_simplify22() {
        assert_j_plan2(
            "select c1 from t1 where c0 + 1 = 2",
            "select c1 from t1 where c0 = 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 - 1 = 2",
            "select c1 from t1 where c0 = 3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 - c0 >= 2",
            "select c1 from t1 where c0 <= -1",
            assert_eq_filt_expr,
        );
    }

    // fold pred 2.4
    #[test]
    fn test_expr_simplify23() {
        assert_j_plan2(
            "select c1 from t1 where 1 = c0 + 2",
            "select c1 from t1 where c0 = -1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 = c0 - 2",
            "select c1 from t1 where c0 = 3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 >= 2 - c0",
            "select c1 from t1 where c0 >= 1",
            assert_eq_filt_expr,
        );
    }

    // fold pred 2.5
    #[test]
    fn test_expr_simplify24() {
        assert_j_plan2(
            "select c1 from t1 where 1 = c0",
            "select c1 from t1 where c0 = 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 > c0",
            "select c1 from t1 where c0 < 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 >= c0",
            "select c1 from t1 where c0 <= 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 < c0",
            "select c1 from t1 where c0 > 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 < c0",
            "select c1 from t1 where c0 > 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 <= c0",
            "select c1 from t1 where c0 >= 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 <> c0",
            "select c1 from t1 where c0 <> 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 + 1 = c0",
            "select c1 from t1 where c0 = 2",
            assert_eq_filt_expr,
        );
    }

    // fold pred 2.6
    #[test]
    fn test_expr_simplify25() {
        assert_j_plan2(
            "select c1 from t1 where c0 + 1 = c1 + 1",
            "select c1 from t1 where c0 = c1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 + 1 = c1 + 2",
            "select c1 from t1 where c0 = c1 + 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 - 1 = c1 + 2",
            "select c1 from t1 where c0 = c1 + 3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 - c0 > c1 + 2",
            "select c1 from t1 where c1 + c0 < -1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 + 1 >= c1 - 2",
            "select c1 from t1 where c0 >= c1 - 3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 - 1 < c1 - 2",
            "select c1 from t1 where c0 < c1 - 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 - c0 > c1 - 2",
            "select c1 from t1 where c1 + c0 < 3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 + 1 <= 2 - c1",
            "select c1 from t1 where c0 + c1 <= 1",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where c0 - 1 < 2 - c1",
            "select c1 from t1 where c0 + c1 < 3",
            assert_eq_filt_expr,
        );
        assert_j_plan2(
            "select c1 from t1 where 1 - c0 >= 2 - c1",
            "select c1 from t1 where c0 <= c1 + (-1)",
            assert_eq_filt_expr,
        );
    }

    fn assert_eq_filt_expr(s1: &str, mut q1: QueryPlan, _s2: &str, q2: QueryPlan) {
        expr_simplify(&mut q1).unwrap();
        print_plan(s1, &q1);
        assert_eq!(get_filt_expr(&q1), get_filt_expr(&q2));
    }
}
