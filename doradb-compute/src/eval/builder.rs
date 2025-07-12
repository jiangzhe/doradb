use crate::arith::ArithKind;
use crate::cmp::CmpKind;
use crate::error::{Error, Result};
use crate::eval::{Eval, EvalPlan, EvalRef};
use crate::logic::LogicKind;
use doradb_catalog::ColIndex;
use doradb_datatype::PreciseType;
use doradb_expr::{DataSourceID, ExprKind, FuncKind, Pred, TypeInfer, TypeInferer};
use smallvec::{smallvec, SmallVec};
use std::collections::HashMap;

#[derive(Debug)]
pub(super) struct EvalBuilder<'a, T, I> {
    input: Vec<(T, ColIndex)>,
    input_map: HashMap<InputKey<T>, (usize, PreciseType)>,
    // The third parameter is the base condition for current evaluation.
    cache: Vec<(Eval, usize)>,
    expr_map: HashMap<ExprKey<'a>, (usize, PreciseType)>,
    // for some intermediate evaluation, there is no corresponding expression,
    // so we just store it in a separate map for reuse.
    eval_map: HashMap<EvalKey, usize>,
    // type inferer
    inferer: &'a mut I,
}

impl<'a, T, I> EvalBuilder<'a, T, I> {
    #[inline]
    pub(super) fn new(inferer: &'a mut I) -> Self {
        EvalBuilder {
            input: vec![],
            input_map: HashMap::new(),
            cache: vec![],
            expr_map: HashMap::new(),
            eval_map: HashMap::new(),
            inferer,
        }
    }
}

impl<'a, T: DataSourceID, I: TypeInferer> EvalBuilder<'a, T, I> {
    /// Create evaluation plan with list of expression.
    #[inline]
    pub fn build<E: IntoIterator<Item = &'a ExprKind>>(mut self, exprs: E) -> Result<EvalPlan<T>> {
        let mut output = vec![];
        for e in exprs {
            let (out, _) = self.find_ref(e, None)?;
            output.push(out);
        }
        Ok(EvalPlan {
            input: self.input,
            output,
            evals: self.cache,
            sel_idx: None,
        })
    }

    /// Create evaluation plan with list of expression and filter.
    #[inline]
    pub fn with_filter<E: IntoIterator<Item = &'a ExprKind>>(
        mut self,
        exprs: E,
        filter: &'a ExprKind,
    ) -> Result<EvalPlan<T>> {
        let mut output = vec![];
        let (sel, _) = self.find_ref(filter, None)?;
        let sel_idx = sel.idx();
        for e in exprs {
            let (out, _) = self.find_ref(e, Some(sel))?;
            output.push(out);
        }
        Ok(EvalPlan {
            input: self.input,
            output,
            evals: self.cache,
            sel_idx: Some(sel_idx),
        })
    }

    /// Try to find evaluation ref from cache, if not exists, generate and store it.
    #[inline]
    fn find_ref(
        &mut self,
        e: &'a ExprKind,
        base: Option<EvalRef>,
    ) -> Result<(EvalRef, PreciseType)> {
        // first search and generate column evaluation
        if let Some((dsid, idx)) = T::from_expr(e) {
            // here we can ignore condition because input is already computed,
            // and apply cond here is just waste of CPU.
            let key = InputKey(dsid, idx);
            if let Some((idx, ty)) = self.input_map.get(&key) {
                return Ok((EvalRef::Input(*idx), *ty));
            }
            // otherwise, we should check input directly
            let in_idx = self.input.len();
            let ty = e.infer(self.inferer)?;
            self.input_map.insert(key, (in_idx, ty));
            self.input.push((dsid, idx));
            return Ok((EvalRef::Input(in_idx), ty));
        }
        // otherwise, generate and store evaluation in cache
        self.gen_expr(e, base)
    }

    /// Generate evaluation based on expression and store it in cache.
    #[inline]
    fn gen_expr(
        &mut self,
        e: &'a ExprKind,
        base: Option<EvalRef>,
    ) -> Result<(EvalRef, PreciseType)> {
        let key = ExprKey::One(e, base);
        if let Some((idx, ty)) = self.expr_map.get(&key) {
            return Ok((EvalRef::Cache(*idx), *ty));
        }
        let eval = self.create(e, base)?;
        let ty = eval.ty;
        let cache_idx = self.cache.len();
        self.expr_map.insert(key, (cache_idx, ty));
        self.cache.push((eval, cache_idx));
        Ok((EvalRef::Cache(cache_idx), ty))
    }

    /// Generate evaluation by expression.
    #[inline]
    fn create(&mut self, e: &'a ExprKind, base: Option<EvalRef>) -> Result<Eval> {
        let res = match e {
            ExprKind::Col(_) => unreachable!(), // column should be resolved via input cache.
            // const evaluation always ignore condition.
            ExprKind::Const(c) => Eval::new_const(c.clone()),
            ExprKind::Func {
                kind: FuncKind::Add,
                args,
            } => {
                let ty = e.infer(self.inferer)?;
                let (l_ref, l_ty) = self.find_ref(&args[0], base)?;
                let lhs = Eval::new_ref(l_ref, l_ty);
                let (r_ref, r_ty) = self.find_ref(&args[1], base)?;
                let rhs = Eval::new_ref(r_ref, r_ty);
                Eval::arith(ArithKind::Add, lhs, rhs, ty)
            }
            // Conjunctive expression is different from others, due to the nature of conditional
            // evaluation.
            // Not all values are computed under the given cond.
            ExprKind::Pred(Pred::Conj(conjs)) => {
                let conj_exprs: SmallVec<[&'a ExprKind; 2]> = conjs.iter().collect();
                self.gen_conj(&conj_exprs, base)?
            }
            ExprKind::Pred(Pred::Func { kind, args }) => {
                if let Some(kind) = CmpKind::from_pred(*kind) {
                    let (l_ref, l_ty) = self.find_ref(&args[0], base)?;
                    let lhs = Eval::new_ref(l_ref, l_ty);
                    let (r_ref, r_ty) = self.find_ref(&args[1], base)?;
                    let rhs = Eval::new_ref(r_ref, r_ty);
                    Eval::cmp(kind, lhs, rhs)
                } else {
                    return Err(Error::UnsupportedEval);
                }
            }
            other => todo!("unimplemented evaluation of {:?}", other),
        };
        Ok(res)
    }

    #[inline]
    fn gen_conj(&mut self, conj_exprs: &[&'a ExprKind], base: Option<EvalRef>) -> Result<Eval> {
        match conj_exprs.len() {
            0 | 1 => unreachable!(),
            2 => {
                // The entire expression has been searched and no match found in cache,
                // so we generate the very basic `E1 and E2` CNF evaluation.
                let (lhs, _) = self.find_ref(conj_exprs[0], base)?;
                let sel = self.gen_conj_sel(base, lhs)?;
                // resolve right, here we should use condition generated by base and lhs result as base.
                let (rhs, _) = self.find_ref(conj_exprs[1], Some(sel))?;
                Ok(Eval::logic(
                    LogicKind::And,
                    vec![Eval::new_bool_ref(lhs), Eval::new_bool_ref(rhs)],
                ))
            }
            _ => {
                // There are more than 2 expressions chained within CNF context.
                // 1. split conjunctive expressions.
                let (last, head) = conj_exprs.split_last().unwrap();
                // 2. try to find in cache
                // e.g. If we have SQL: "SELECT a > 0 and b < 0, a > 0 and c < 0",
                // The common expression "a > 0" can be generated only once and reused.
                // The key must include base condition
                let conj_exprs: SmallVec<[&'a ExprKind; 2]> = head.iter().copied().collect();
                let key = ExprKey::And(conj_exprs, base);
                let lhs = if let Some((idx, _)) = self.expr_map.get(&key) {
                    EvalRef::Cache(*idx)
                } else {
                    // generate evaluation and store in cache
                    let conj_exprs = key.exprs();
                    let eval = self.gen_conj(&conj_exprs, base)?;
                    if let Some((idx, _)) = self.expr_map.get(&key) {
                        EvalRef::Cache(*idx)
                    } else {
                        let cache_idx = self.cache.len();
                        self.expr_map.insert(key, (cache_idx, eval.ty));
                        self.cache.push((eval, cache_idx));
                        EvalRef::Cache(cache_idx)
                    }
                };
                let sel = self.gen_conj_sel(base, lhs)?;
                let (rhs, _) = self.find_ref(last, Some(sel))?;
                Ok(Eval::logic(
                    LogicKind::And,
                    vec![Eval::new_bool_ref(lhs), Eval::new_bool_ref(rhs)],
                ))
            }
        }
    }

    /// Generate selection in CNF context.
    /// Only false values are excluded in further evaluation.
    /// That means true and nulls are all treated as selected.
    #[inline]
    fn gen_conj_sel(&mut self, base: Option<EvalRef>, sel: EvalRef) -> Result<EvalRef> {
        let args = if let Some(base) = base {
            // two arguments, we must merge them in evaluation.
            vec![Eval::new_bool_ref(base), Eval::new_bool_ref(sel)]
        } else {
            vec![Eval::new_bool_ref(sel)]
        };
        let eval = Eval::logic(LogicKind::AndThen, args);
        let key = EvalKey::AndThen(eval);
        if let Some(idx) = self.eval_map.get(&key) {
            return Ok(EvalRef::Cache(*idx));
        }
        let cache_idx = self.cache.len();
        self.cache.push((key.eval(), cache_idx));
        self.eval_map.insert(key, cache_idx);
        Ok(EvalRef::Cache(cache_idx))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InputKey<T>(T, ColIndex);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ExprKey<'a> {
    One(&'a ExprKind, Option<EvalRef>),
    And(SmallVec<[&'a ExprKind; 2]>, Option<EvalRef>),
    Or(SmallVec<[&'a ExprKind; 2]>, Option<EvalRef>),
}

impl<'a> ExprKey<'a> {
    #[inline]
    fn exprs(&self) -> SmallVec<[&'a ExprKind; 2]> {
        match self {
            ExprKey::One(e, _) => smallvec![*e],
            ExprKey::And(es, _) | ExprKey::Or(es, _) => es.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum EvalKey {
    // Evaluation self contains the base condition.
    AndThen(Eval),
    OrElse(Eval),
}

impl EvalKey {
    #[inline]
    fn eval(&self) -> Eval {
        match self {
            EvalKey::AndThen(e) | EvalKey::OrElse(e) => e.clone(),
        }
    }
}
