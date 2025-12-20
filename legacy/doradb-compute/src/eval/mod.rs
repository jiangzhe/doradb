mod builder;
mod plan;

pub use plan::{CacheEntry, EvalCache, EvalPlan, QueryEvalPlan, TableEvalPlan};

use crate::arith::ArithKind;
use crate::cmp::CmpKind;
use crate::error::{Error, Result};
use crate::logic::LogicKind;
use builder::EvalBuilder;
use doradb_datatype::{PreciseType, Typed};
use doradb_expr::Const;
use doradb_storage::col::attr::Attr;
use doradb_storage::col::chunk::Chunk;
use doradb_storage::col::codec::Single;
use doradb_storage::col::sel::Sel;

/// Eval is similar to [`doradb_expr::Expr`], but only for evaluation.
/// It supports deterministic scalar expressions and is restricted to
/// evaluate within single block.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Eval {
    pub kind: EvalKind,
    pub ty: PreciseType,
}

impl Eval {
    /// Create new evaluation on constant.
    #[inline]
    pub fn new_const(value: Const) -> Self {
        let ty = value.pty();
        Eval {
            kind: EvalKind::Const(value),
            ty,
        }
    }

    /// Create new evaluation on reference, which is already
    /// computed.
    #[inline]
    pub fn new_ref(r: EvalRef, ty: PreciseType) -> Self {
        Eval {
            kind: EvalKind::Ref(r),
            ty,
        }
    }

    /// Create new evaluation on bool reference.
    #[inline]
    pub fn new_bool_ref(r: EvalRef) -> Self {
        Eval {
            kind: EvalKind::Ref(r),
            ty: PreciseType::bool(),
        }
    }

    /// Create new evaluation on arithmetic expression.
    #[inline]
    pub fn arith(kind: ArithKind, lhs: Eval, rhs: Eval, ty: PreciseType) -> Self {
        Eval {
            kind: EvalKind::Arith {
                kind,
                args: vec![lhs, rhs].into_boxed_slice(),
            },
            ty,
        }
    }

    /// Create new evaluation on logic expression.
    #[inline]
    pub fn logic(kind: LogicKind, args: Vec<Eval>) -> Self {
        Eval {
            kind: EvalKind::Logic {
                kind,
                args: args.into_boxed_slice(),
            },
            ty: PreciseType::bool(),
        }
    }

    /// Create new evaluation on comparison expression.
    #[inline]
    pub fn cmp(kind: CmpKind, lhs: Eval, rhs: Eval) -> Self {
        Eval {
            kind: EvalKind::Cmp {
                kind,
                args: vec![lhs, rhs].into_boxed_slice(),
            },
            ty: PreciseType::bool(),
        }
    }

    /// Evaluate with input and cache.
    #[inline]
    pub fn eval(
        &self,
        input: &Chunk,
        cache: &mut EvalCache,
        idx: usize,
        sel: Option<&Sel>,
    ) -> Result<EvalRef> {
        let res = match &self.kind {
            EvalKind::Ref(r) => return Ok(*r),
            EvalKind::Const(c) => self.eval_const(input, c),
            EvalKind::Arith { kind, args } => {
                let l = args[0].get(input, cache)?;
                let r = args[1].get(input, cache)?;
                kind.eval(l, r, sel)
            }
            EvalKind::Cmp { kind, args } => {
                let l = args[0].get(input, cache)?;
                let r = args[1].get(input, cache)?;
                kind.eval(l, r, sel)
            }
            EvalKind::Logic { kind, args } => match args.as_ref() {
                [e0] => {
                    let a = e0.get(input, cache)?;
                    kind.eval_unary(a, sel)
                }
                [e0, e1] => {
                    let a0 = e0.get(input, cache)?;
                    let a1 = e1.get(input, cache)?;
                    kind.eval_binary(a0, a1, sel)
                }
                _ => return Err(Error::UnsupportedEval),
            },
        }?;
        debug_assert_eq!(self.ty, res.ty);
        cache[idx] = CacheEntry::Some(res);
        Ok(EvalRef::Cache(idx))
    }

    #[inline]
    pub fn get<'a>(&self, input: &'a Chunk, cache: &'a EvalCache) -> Result<&'a Attr> {
        match &self.kind {
            EvalKind::Ref(r) => load_attr(input, cache, *r),
            _ => Err(Error::InvalidEvalPlan),
        }
    }

    /// Evaluate constant.
    #[inline]
    fn eval_const(&self, input: &Chunk, c: &Const) -> Result<Attr> {
        let res = match c {
            Const::I64(i) => Attr::new_single(
                PreciseType::i64(),
                Single::new(*i, input.n_records),
                Sel::All(input.n_records),
            ),
            _ => todo!(),
        };
        Ok(res)
    }
}

/// Load attribute from input or cache by given evaluation reference.
#[inline]
fn load_attr<'a>(input: &'a Chunk, cache: &'a EvalCache, r: EvalRef) -> Result<&'a Attr> {
    let res = match r {
        EvalRef::Input(idx) => input.data.get(idx),
        EvalRef::Cache(idx) => cache.get(idx).and_then(|v| match v {
            CacheEntry::Some(attr) => Some(attr),
            _ => None,
        }),
    };
    res.ok_or(Error::MissingAttr)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EvalKind {
    Ref(EvalRef),
    Const(Const),
    Arith { kind: ArithKind, args: Box<[Eval]> },
    Cmp { kind: CmpKind, args: Box<[Eval]> },
    Logic { kind: LogicKind, args: Box<[Eval]> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvalRef {
    Input(usize),
    Cache(usize),
}

impl EvalRef {
    /// Returns index of the reference.
    #[inline]
    pub fn idx(&self) -> usize {
        match self {
            EvalRef::Input(idx) | EvalRef::Cache(idx) => *idx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use doradb_catalog::ColIndex;
    use doradb_expr::util::{TypeFix, TypeInferer};
    use doradb_expr::{Col, ColKind, ExprKind, FuncKind, GlobalID, PredFuncKind, QueryID};
    use doradb_storage::col::attr::Attr;
    use doradb_storage::col::chunk::Chunk;

    #[test]
    fn test_build_eval() {
        let col1 = ExprKind::query_col(GlobalID::from(1), QueryID::from(0), ColIndex::from(0));
        let col2 = ExprKind::query_col(GlobalID::from(2), QueryID::from(0), ColIndex::from(1));
        for (exprs, (n_input, n_cache, n_output)) in vec![
            // select c1
            (vec![col1.clone()], (1, 0, 1)),
            // select c1 + 1
            (
                vec![ExprKind::func(
                    FuncKind::Add,
                    vec![col1.clone(), ExprKind::Const(Const::I64(1))],
                )],
                (1, 2, 1),
            ),
            // select c1 + c2
            (
                vec![ExprKind::func(
                    FuncKind::Add,
                    vec![col1.clone(), col2.clone()],
                )],
                (2, 1, 1),
            ),
            // select c1 + 1 + c2
            (
                vec![ExprKind::func(
                    FuncKind::Add,
                    vec![
                        ExprKind::func(
                            FuncKind::Add,
                            vec![col1.clone(), ExprKind::Const(Const::I64(1))],
                        ),
                        col2.clone(),
                    ],
                )],
                (2, 3, 1),
            ),
            // select c1, c2
            (vec![col1.clone(), col2.clone()], (2, 0, 2)),
            // select c1+1, c1+1
            (
                vec![
                    ExprKind::func(
                        FuncKind::Add,
                        vec![col1.clone(), ExprKind::Const(Const::I64(1))],
                    ),
                    ExprKind::func(
                        FuncKind::Add,
                        vec![col1.clone(), ExprKind::Const(Const::I64(1))],
                    ),
                ],
                (1, 2, 2),
            ),
            // select c1 >= 0 and c1 <= 100 and c1 <> 10
            (
                vec![ExprKind::pred_conj(vec![
                    ExprKind::pred_func(
                        PredFuncKind::GreaterEqual,
                        vec![col1.clone(), ExprKind::Const(Const::I64(0))],
                    ),
                    ExprKind::pred_func(
                        PredFuncKind::LessEqual,
                        vec![col1.clone(), ExprKind::Const(Const::I64(100))],
                    ),
                    ExprKind::pred_func(
                        PredFuncKind::NotEqual,
                        vec![col1.clone(), ExprKind::Const(Const::I64(10))],
                    ),
                ])],
                // evals=["0", "c1>=0", AndThen(..), "100", "c1<=100", And(..), AndThen(..), "10", "c1<>10", And(..)]
                (1, 3 + 4 + 3, 1),
            ),
        ] {
            let plan = build_plan(exprs);
            assert_eq!(n_input, plan.input.len());
            assert_eq!(n_cache, plan.evals.len());
            assert_eq!(n_output, plan.output.len());
        }
    }

    #[test]
    fn test_run_eval() {
        let size = 1024i32;
        let attr1 = Attr::from((0..size).map(|i| i as i64));
        let attr2 = attr1.to_owned();
        let block = Chunk::new(1024, vec![attr1, attr2]);
        let col1 = ExprKind::query_col(GlobalID::from(1), QueryID::from(0), ColIndex::from(0));
        let col2 = ExprKind::query_col(GlobalID::from(2), QueryID::from(0), ColIndex::from(1));
        // select c1
        let es = vec![col1.clone()];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_all());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| i as i64).collect();
        assert_eq!(&expected, data);
        // select c1 + 1
        let es = vec![ExprKind::func(
            FuncKind::Add,
            vec![col1.clone(), ExprKind::Const(Const::I64(1))],
        )];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_all());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + 1) as i64).collect();
        assert_eq!(&expected, data);
        // select c1 + 1, c1 + 1
        let es = vec![
            ExprKind::func(
                FuncKind::Add,
                vec![col1.clone(), ExprKind::Const(Const::I64(1))],
            ),
            ExprKind::func(
                FuncKind::Add,
                vec![col1.clone(), ExprKind::Const(Const::I64(1))],
            ),
        ];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(2, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        assert_eq!(PreciseType::i64(), res[1].ty);
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + 1) as i64).collect();
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_all());
        assert_eq!(&expected, data);
        let data: &[i64] = res[1].codec.as_array().unwrap().cast_slice();
        assert!(res[1].validity.is_all());
        assert_eq!(&expected, data);
        // select c1 + c2
        let es = vec![ExprKind::func(
            FuncKind::Add,
            vec![col1.clone(), col2.clone()],
        )];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_all());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + i) as i64).collect();
        assert_eq!(&expected, data);
        // select c1 + 1, c1 + c2
        let es = vec![
            ExprKind::func(
                FuncKind::Add,
                vec![col1.clone(), ExprKind::Const(Const::I64(1))],
            ),
            ExprKind::func(FuncKind::Add, vec![col1.clone(), col2.clone()]),
        ];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(2, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        assert_eq!(PreciseType::i64(), res[1].ty);
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_all());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + 1) as i64).collect();
        assert_eq!(&expected, data);
        let data: &[i64] = res[1].codec.as_array().unwrap().cast_slice();
        assert!(res[1].validity.is_all());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + i) as i64).collect();
        assert_eq!(&expected, data);
        // select c1 >= 0 and c1 <= 100 and c1 <> 10
        let es = vec![ExprKind::pred_conj(vec![
            ExprKind::pred_func(
                PredFuncKind::GreaterEqual,
                vec![col1.clone(), ExprKind::Const(Const::I64(0))],
            ),
            ExprKind::pred_func(
                PredFuncKind::LessEqual,
                vec![col1.clone(), ExprKind::Const(Const::I64(100))],
            ),
            ExprKind::pred_func(
                PredFuncKind::NotEqual,
                vec![col1.clone(), ExprKind::Const(Const::I64(10))],
            ),
        ])];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(PreciseType::bool(), res[0].ty);
        let data = res[0].codec.as_bitmap().unwrap();
        assert_eq!(1024, data.len());
        assert_eq!(100, data.true_count());
    }

    #[test]
    fn test_sel_eval() {
        let col1 = ExprKind::query_col(GlobalID::from(1), QueryID::from(0), ColIndex::from(0));
        let filter = ExprKind::pred_func(
            PredFuncKind::Equal,
            vec![col1.clone(), ExprKind::const_i64(0)],
        );
        let plan = QueryEvalPlan::with_filter(&[col1], &filter, &mut IntColInferer).unwrap();
        assert_eq!(1, plan.input.len());
        assert_eq!(1, plan.output.len());
        let attr1 = Attr::from((0..1024).map(|i| i as i64));
        let block = Chunk::new(1024, vec![attr1]);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(1, res[0].n_records());
        let attr2 = Attr::from((0..1024).map(|i| if i < 100 { 0 } else { i as i64 }));
        let block = Chunk::new(1024, vec![attr2]);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(100, res[0].n_records());
    }

    #[test]
    fn test_conj_eval() {
        let col1 = ExprKind::query_col(GlobalID::from(1), QueryID::from(0), ColIndex::from(0));
        let f1 = ExprKind::pred_func(
            PredFuncKind::Greater,
            vec![col1.clone(), ExprKind::const_i64(0)],
        );
        let f2 = ExprKind::pred_func(
            PredFuncKind::Less,
            vec![col1.clone(), ExprKind::const_i64(10)],
        );
        let filter = ExprKind::pred_conj(vec![f1, f2]);
        let plan: QueryEvalPlan = EvalBuilder::new(&mut IntColInferer)
            .with_filter(&[col1], &filter)
            .unwrap();
        assert_eq!(1, plan.input.len());
        assert_eq!(1, plan.output.len());
        let attr1 = Attr::from((0..1024).map(|i| i as i64));
        let block = Chunk::new(1024, vec![attr1]);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(9, res[0].n_records());
        let array = res[0].codec.as_array().unwrap();
        assert_eq!(&[1i64, 2, 3, 4, 5, 6, 7, 8, 9], array.cast_slice::<i64>());
    }

    #[inline]
    fn build_plan(mut exprs: Vec<ExprKind>) -> QueryEvalPlan {
        let mut inferer = IntColInferer;
        for e in &mut exprs {
            e.fix(&mut inferer).unwrap();
        }
        QueryEvalPlan::new(&exprs, &mut inferer).unwrap()
    }

    struct IntColInferer;
    impl TypeInferer for IntColInferer {
        fn confirm(&mut self, e: &ExprKind) -> Option<PreciseType> {
            match e {
                ExprKind::Col(Col { kind, .. }) => match kind {
                    ColKind::Query(..) | ColKind::Correlated(..) => Some(PreciseType::i64()),
                    _ => None,
                },
                _ => None,
            }
        }
    }
}
