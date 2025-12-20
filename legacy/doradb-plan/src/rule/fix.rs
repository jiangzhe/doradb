use crate::error::{Error, Result};
use crate::join::{Join, QualifiedJoin};
use crate::lgc::{Aggr, Op, OpOutput, OpKind, OpMutVisitor, QuerySet, ProjCol};
use std::collections::HashMap;
use std::sync::Arc;
use std::mem;
use doradb_datatype::PreciseType;
use doradb_expr::controlflow::{Branch, ControlFlow, Unbranch};
use doradb_expr::util::{fix_bools, TypeFix};
use doradb_expr::{GlobalID, Col, ColKind, ColIndex, Expr, ExprKind, QueryID};

/// Fix types of all expressions and generate intra columns for execution.
/// In initialization of logical plan, all type info, except table columns, are left as empty.
/// It's convenient to exclude types in other optimization rules.
/// But at the end, we need to fix types of all expressions to enable efficient evaluation.
/// And in some opeartors, such as aggregation, sort, there might be non-trivial expressions
/// involved, we will add projection to replace these expressions with intra columns to reduce
/// the complexity.
#[inline]
pub fn fix(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    // let mut qry_types = HashMap::new();
    // let mut intra_types = HashMap::new();
    // fix_type(qry_set, qry_id, &mut qry_types, &mut intra_types)
    Fix::new(qry_set).fix_qry(qry_id)
}

// #[inline]
// fn fix_type(
//     qry_set: &mut QuerySet,
//     qry_id: QueryID,
//     qry_types: &mut HashMap<(QueryID, ColIndex), PreciseType>,
//     intra_types: &mut HashMap<GlobalID, PreciseType>,
// ) -> Result<()> {
//     qry_set.transform_op(qry_id, |qry_set, _, op| {
//         let mut ft = FixType { qry_set, qry_types, intra_types };
//         op.walk_mut(&mut ft).unbranch()
//     })?
// }

struct Fix<'a> {
    qry_set: &'a mut QuerySet,
    // query column mapping.
    // convert query column to intra column.
    qry_col_mapping: HashMap<(QueryID, ColIndex), (GlobalID, PreciseType)>,
    // intra column mapping.
    intra_col_mapping: HashMap<GlobalID, (u8, ColIndex, PreciseType)>,
    // global id for intra columns.
    gid: GlobalID,
}

impl TypeFix for Fix<'_> {
    #[inline]
    fn fix_col(&mut self, col: &mut Col) -> Option<PreciseType> {
        match &col.kind {
            ColKind::Intra(_) => self.intra_col_mapping.get(&col.gid).map(|v| &v.2).cloned(),
            ColKind::Query(qid)  => {
                let key = (*qid, col.idx);
                if let Some((gid, ty)) = self.qry_col_mapping.get(&key) {
                    // replace query column with intra column
                    let (id, col_idx, ty) = self.intra_col_mapping.get(gid).unwrap();
                    *col = Col{gid: *gid, idx: *col_idx, kind: ColKind::Intra(*id)};
                    Some(*ty)
                } else {
                    None
                }
            },
            ColKind::Correlated(qid) => {
                let key = (*qid, col.idx);
                self.qry_col_mapping.get(&key).map(|v| &v.1).cloned()
            }
            ColKind::Table(..) => unreachable!("table column should have type specified"),
        }
    }
}

impl Fix<'_> {

    #[inline]
    fn new(qry_set: &mut QuerySet) -> Self {
        Fix{
            qry_set,
            qry_col_mapping: HashMap::new(),
            intra_col_mapping: HashMap::new(),
            gid: GlobalID::from(0),
        }
    }

    #[inline]
    fn next_gid(&mut self) -> GlobalID {
        self.gid.fetch_inc()
    }

    #[inline]
    fn fix_qry(&mut self, qry_id: QueryID) -> Result<()> {
        self.qry_set.transform_op(qry_id, |qry_set, _, op| {
            // let mut ft = FixType { qry_set, qry_types, intra_types };
            op.walk_mut(self).unbranch()
        })?
    }

    #[inline]
    fn fix_expr(&mut self, e: &mut Expr) -> Result<()> {
        self.fix_rec(e).map_err(Into::into)
    }

    /// fix type of an aggr expression. 
    /// if the aggr expression is complex, generate a new intra column
    /// to replace it.
    #[inline]
    fn fix_aggr_expr(&mut self, idx: ColIndex, e: &mut Expr) -> Result<Option<Col>> {
        todo!()
    }

    /// fix type of a group expression.
    /// if the group expression is complex, generate a new intra column
    /// to replace it.
    #[inline]
    fn fix_group_expr(&mut self, idx: ColIndex, e: &mut Expr) -> Result<Option<Col>> {
        todo!()
    }
}

impl OpMutVisitor for Fix<'_> {
    type Cont = ();
    type Break = Error;
    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match &mut op.kind {
            OpKind::Query(qry_id) => {
                // first, recursively fix types in child query
                // let mut intra_types = HashMap::new();
                self.fix_qry(*qry_id).branch()?;
                // fix_type(self.qry_set, qry_id, self.qry_types, &mut intra_types).branch()?;
                // then populate type map with out columns
                let subq = self.qry_set.get(qry_id).unwrap();
                let out_cols = subq.out_cols();
                let mut outputs = Vec::with_capacity(out_cols.len());
                for (i, c) in out_cols.iter().enumerate() {
                    if c.expr.ty.is_unknown() {
                        return ControlFlow::Break(Error::TypeInferFailed);
                    } else {
                        // initialize intra column.
                        let gid = self.next_gid();
                        // initial intra column index is same as output.
                        // this may be changed by other operator, e.g. join.
                        let idx = ColIndex::from(i as u32);
                        let ty = c.expr.ty;
                        let output = ProjCol::no_alias(Expr::intra_col(gid, 0, idx));
                        outputs.push(output);
                        self.qry_col_mapping
                            .insert((*qry_id, ColIndex::from(i as u32)), (gid, ty));
                        self.intra_col_mapping
                            .insert(gid, (0, idx, ty));
                    }
                }
                op.output = OpOutput::Ref(Arc::new(outputs));
                ControlFlow::Continue(())
            }
            OpKind::Filt { pred, input } => {
                for e in pred.iter_mut() {
                    self.fix_expr(e).branch()?
                }
                fix_bools(pred.as_mut());
                op.output = input.output.clone(); // propagate output from child
                ControlFlow::Continue(())
            }
            OpKind::Join(join) => {
                match join.as_mut() {
                    Join::Cross(_) => (),
                    Join::Qualified(QualifiedJoin { cond, filt, left, right, ..}) => {
                        // because join concats two children's output.
                        // we need to adjust child index of right children
                        if let OpOutput::Ref(right_arc) = mem::take(&mut right.output) {
                            if let Some(mut right_out) = Arc::into_inner(right_arc) {
                                for out in &mut right_out {
                                    match &mut out.expr.kind {
                                        ExprKind::Col(Col{gid, kind: ColKind::Intra(id), ..}) => {
                                            *id = 1;
                                            // also update mapping
                                            let (id, _, _) = self.intra_col_mapping.get_mut(gid).unwrap();
                                            *id = 1;
                                        }
                                        _ => (),
                                    }
                                }
                                right.output = OpOutput::Ref(Arc::new(right_out));
                            }
                        } else {
                            unreachable!("right child output unspecified");
                        }
                        for e in cond.iter_mut() {
                            self.fix_expr(e).branch()?
                        }
                        fix_bools(cond.as_mut());
                        for e in filt.iter_mut() {
                            self.fix_expr(e).branch()?
                        }
                        fix_bools(filt.as_mut());
                        // propagate output and update intra column index
                        let mut out = left.output.extract().unwrap();
                        let right = right.output.extract().unwrap();
                        let mut ridx = out.len() as u32;
                        for mut rout in right {
                            match rout.expr.kind
                        }
                    }
                }
                ControlFlow::Continue(())
            }
            OpKind::Aggr(aggr) => {
                let Aggr {
                    groups, proj, filt, ..
                } = aggr.as_mut();
                // here we examine group, project and filter expressions.
                // for example "SELECT sum(c1)+1 FROM t1 GROUP BY c2+1 HAVING sum(c1) > 0",
                // two projection operators will be generated.
                // one is above the aggr to project sum(c1)+1,
                // the other is under the aggr to project c2+1.
                // one filter operator will be generated.
                for (i, e) in groups.iter_mut().enumerate() {
                    let idx = ColIndex::from(i as u32);
                    if let Some(c) = self.fix_group_expr(idx, e).branch()? {
                        let ty = e.ty;
                        // let new_expr = Expr{kind: ExprKind::Col(c), ty};
                        // *e = new_expr;
                    } // otherwise do nothing
                }
                for c in proj.iter_mut() {
                    self.fix(&mut c.expr).branch()?
                }
                for e in filt.iter_mut() {
                    self.fix(e).branch()?
                }
                fix_bools(filt.as_mut());
                ControlFlow::Continue(())
            }
            _ => {
                for e in op.kind.exprs_mut() {
                    self.fix(e).branch()?
                }
                ControlFlow::Continue(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{build_plan, get_filt_expr, print_plan};
    use crate::lgc::LgcPlan;
    use doradb_catalog::mem_impl::MemCatalog;
    use doradb_catalog::{Catalog, ColumnAttr, ColumnSpec, TableSpec};

    macro_rules! vec2 {
        ($e:expr) => {
            vec![$e, $e]
        };
    }

    #[test]
    fn test_type_fix_add_sub() {
        use doradb_datatype::PreciseType as PT;
        let cat = ty_catalog();
        for (sql, tys) in vec![
            // constants
            (
                "select 1, 1.0, 1e0 from ty1",
                vec![PT::i64(), PT::decimal(2, 1), PT::f64()],
            ),
            // float
            (
                "select c_f64 + c_f64, c_f64 + c_i32, c_i32 + c_f64 from ty1",
                vec![PT::f64(), PT::f64(), PT::f64()],
            ),
            (
                "select c_f32 + c_f64, c_f64 + c_f32 from ty1",
                vec![PT::f64(), PT::f64()],
            ),
            // decimal
            (
                "select c_dec + c_dec, c_dec + c_i32, c_i32 + c_dec from ty1",
                vec![PT::decimal(18, 2), PT::decimal(18, 2), PT::decimal(18, 2)],
            ),
            (
                "select d50 + d50, d50 + d40, d50 + d60 from dec1",
                vec![PT::decimal(5, 0), PT::decimal(5, 0), PT::decimal(6, 0)],
            ),
            (
                "select d51 + d40, d51 + d62 from dec1",
                vec![PT::decimal(5, 1), PT::decimal(6, 2)],
            ),
            (
                "select d61 + d40, d40 + d61 from dec1",
                vec![PT::decimal(6, 1), PT::decimal(6, 1)],
            ),
            (
                "select d52 + d61, d61 + d52 from dec1",
                vec![PT::decimal(7, 2), PT::decimal(7, 2)],
            ),
            // bool
            (
                "select c_bool + c_bool, c_bool + c_i32, c_i32 + c_bool from ty1",
                vec![PT::u64(), PT::u64(), PT::u64()],
            ),
            // int
            (
                "select c_i32 + c_i32, c_i32 + c_u32, c_u32 + c_i32, c_u32 + c_u32 from ty1",
                vec![PT::i32(), PT::u32(), PT::u32(), PT::u32()],
            ),
            (
                "select c_i32 + c_i64, c_i64 + c_i32, c_u32 + c_i64, c_u64 + c_i32 from ty1",
                vec![PT::i64(), PT::i64(), PT::i64(), PT::u64()],
            ),
            // datetime
            (
                "select c_dm + interval '1' day, interval '1' day + c_dm, 'C' + c_dm from ty1",
                vec![PT::datetime(0), PT::datetime(0), PT::f64()],
            ),
            (
                "select dm0 + c_i32, c_i32 + dm0, dm3 + c_i32, c_i32 + dm3 from ty1, dm1",
                vec![PT::i64(), PT::i64(), PT::decimal(18, 3), PT::decimal(18, 3)]
            ),
            (
                "select dm0 + c_bool, dm3 + c_bool from ty1, dm1",
                vec![PT::u64(), PT::decimal(18, 3)],
            ),
            (
                "select dm0 + dm0, dm3 + dm3, dm3 + c_dt from ty1, dm1",
                vec![PT::i64(), PT::decimal(18, 3), PT::decimal(18, 3)],
            ),
            // date
            (
                "select c_dt + interval '1' day, interval '1' day + c_dt, c_dt + c_ascii from ty1",
                vec![PT::date(), PT::date(), PT::f64()],
            ),
            (
                "select c_dt + c_i32, c_i32 + c_dt, c_dt + c_bool, c_dt + c_dt from ty1",
                vec![PT::i64(), PT::i64(), PT::u64(), PT::i64()],
            ),
            // time
            (
                "select c_tm + interval '1' hour, c_tm + c_i32, c_tm + c_ascii, c_tm + c_bool from ty1",
                vec![PT::time(0), PT::i64(), PT::f64(), PT::u64()],
            ),
            (
                "select tm0 + tm0, tm3 + tm3, tm3 + c_dt from ty1, dm1",
                vec![PT::i64(), PT::decimal(18, 3), PT::decimal(18, 3)],
            ),
            // string
            (
                "select a1 + a1, b1 + b1, u1 + u1, a1 + b1, a1 + u1, b1 + u1 from str1",
                vec![PT::f64(), PT::f64(), PT::f64(), PT::f64(), PT::f64(), PT::f64()],
            ),
            (
                "select va1 + va1, vb1 + vb1, vu1 + vu1, va1 + vb1, va1 + vu1, vb1 + vu1 from str1",
                vec![PT::f64(), PT::f64(), PT::f64(), PT::f64(), PT::f64(), PT::f64()],
            ),
        ] {
            assert_ty_plan1(&cat, sql, |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                assert_eq!(tys, extract_proj_types(&p));
            })
        }
    }

    #[test]
    fn test_type_fix_cmp() {
        use doradb_datatype::PreciseType as PT;
        let cat = ty_catalog();
        for (sql, tys) in vec![
            // float
            (
                "select c_f64 = c_f64, c_f64 > c_i32, c_i32 >= c_f64, c_f64 <> 'abc' from ty1",
                vec![vec2![PT::f64()], vec2![PT::f64()], vec2![PT::f64()], vec2![PT::f64()]],
            ),
            (
                "select c_f64 = c_f32, c_f32 = c_f64 from ty1",
                vec![vec2![PT::f64()], vec2![PT::f64()]],
            ),
            // decimal
            (
                "select c_dec < c_dec, c_dec <= c_i32, c_i32 <=> c_dec from ty1",
                vec![vec2![PT::decimal(18, 2)], vec2![PT::decimal(18, 2)], vec2![PT::decimal(18, 2)]],
            ),
            (
                "select d50 > d40, d50 < d60 from dec1",
                vec![vec2![PT::decimal(5, 0)], vec2![PT::decimal(6, 0)]],
            ),
            (
                "select d51 = d40, d40 = d51 from dec1",
                vec![vec2![PT::decimal(5, 1)], vec2![PT::decimal(5, 1)]],
            ),
            (
                "select d61 = d40, d40 = d61 from dec1",
                vec![vec2![PT::decimal(6, 1)], vec2![PT::decimal(6, 1)]],
            ),
            (
                "select d62 = d50, d50 = d62 from dec1",
                vec![vec2![PT::decimal(7, 2)], vec2![PT::decimal(7, 2)]],
            ),
            // bool
            (
                "select c_bool = c_bool, c_bool = c_i32, c_i32 = c_bool from ty1",
                vec![vec![PT::bool(), PT::bool()], vec![PT::bool(), PT::bool()], vec![PT::bool(), PT::bool()]],
            ),
            // int
            (
                "select c_i32 = c_i32, c_i32 = c_u32, c_u64 = c_i64 from ty1",
                vec![vec2![PT::i32()], vec![PT::i32(), PT::u32()], vec![PT::u64(), PT::i64()]],
            ),
            (
                "select c_i32 = c_i64, c_u64 = c_i32 from ty1",
                vec![vec2![PT::i64()], vec![PT::u64(), PT::i64()]],
            ),
            // datetime
            (
                "select c_dm = c_dm, c_dm = c_i32, c_u32 = c_dm, c_dm = '1' from ty1",
                vec![vec![PT::datetime(0), PT::datetime(0)], vec![PT::i64(), PT::i64()], vec![PT::u64(), PT::u64()], vec![PT::datetime(6), PT::datetime(6)]],
            ),
            (
                "select dm0 < dm3, dm3 < dm0 from dm1",
                vec![vec2![PT::datetime(3)], vec2![PT::datetime(3)]],
            ),
            // date
            (
                "select c_dt = c_dt, c_dt = c_u32, c_i64 = c_dt, c_dt = c_ascii, c_dt = c_bool from ty1",
                vec![vec![PT::date(), PT::date()], vec![PT::u64(), PT::u64()], vec![PT::i64(), PT::i64()], vec![PT::datetime(6), PT::datetime(6)], vec![PT::u64(), PT::u64()]],
            ),
            // time
            (
                "select c_tm = c_tm, c_tm = c_i32, c_u32 = c_tm, c_bool = c_tm from ty1",
                vec![vec![PT::time(0), PT::time(0)], vec![PT::i64(), PT::i64()], vec![PT::u64(), PT::u64()], vec![PT::u64(), PT::u64()]],
            ),
            (
                "select tm0 <= tm3, tm3 >= tm0 from dm1",
                vec![vec2![PT::time(3)], vec2![PT::time(3)]],
            ),
            // string
            (
                "select c_ascii = c_ascii, c_ascii = c_utf8, c_ascii = c_bin from ty1",
                vec![vec![PT::ascii(1), PT::ascii(1)], vec![PT::var_utf8(10), PT::var_utf8(10)], vec![PT::var_bytes(1024), PT::var_bytes(1024)]],
            ),
            (
                "select a1 = a1, a1 = a10, a10 = a1 from str1",
                vec![vec2![PT::ascii(1)], vec2![PT::ascii(10)], vec2![PT::ascii(10)]],
            ),
            (
                "select u1 = u1, u1 = u10, u10 = u1 from str1",
                vec![vec2![PT::utf8(1)], vec2![PT::utf8(10)], vec2![PT::utf8(10)]],
            ),
            (
                "select b1 = b1, b1 = b10, b10 = b1 from str1",
                vec![vec2![PT::bytes(1)], vec2![PT::bytes(10)], vec2![PT::bytes(10)]],
            ),
            (
                "select a10 = b10, a10 = u10, b10 = u10 from str1",
                vec![vec2![PT::bytes(10)], vec2![PT::utf8(10)], vec2![PT::bytes(10)]],
            ),
            (
                "select va1 = va10, va10 = va1 from str1",
                vec![vec![PT::var_ascii(1), PT::var_ascii(10)], vec![PT::var_ascii(10), PT::var_ascii(1)]],
            ),
            (
                "select vu1 = vu10, vu10 = vu1 from str1",
                vec![vec![PT::var_utf8(1), PT::var_utf8(10)], vec![PT::var_utf8(10), PT::var_utf8(1)]],
            ),
            (
                "select vb1 = vb10, vb10 = vb1 from str1",
                vec![vec![PT::var_bytes(1), PT::var_bytes(10)], vec![PT::var_bytes(10), PT::var_bytes(1)]],
            ),
            (
                "select va10 = vb10, va10 = vu10, vb10 = vu10 from str1",
                vec![vec2![PT::var_bytes(10)], vec2![PT::var_utf8(10)], vec2![PT::var_bytes(10)]],
            ),
        ] {
            assert_ty_plan1(&cat, sql, |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                assert_eq!(tys, extract_child_types(&p));
            })
        }
    }

    #[test]
    fn test_type_fix_other_pred() {
        let cat = ty_catalog();
        assert_ty_plan1(
            &cat,
            "select c_i32 and c_u32, c_f64 or c_ascii, c_dec xor c_ascii, not c_utf8 from ty1",
            |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                for ty in extract_child_types(&p) {
                    assert!(ty.iter().all(PreciseType::is_bool))
                }
            },
        )
    }

    #[test]
    fn test_type_fix_filt() {
        let cat = ty_catalog();
        assert_ty_plan1(
            &cat,
            "select 1 from ty1 where c_i32 and c_dec",
            |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                for e in get_filt_expr(&p) {
                    assert!(e.ty.is_bool())
                }
            },
        )
    }

    #[test]
    fn test_type_fix_join() {
        use crate::lgc::{preorder, OpKind};
        let cat = ty_catalog();
        assert_ty_plan1(
            &cat,
            "select 1 from ty1 join ty1 as ty2 on ty1.c_i32 = ty2.c_i64 and ty1.c_u64 and ty2.c_dec", |sql, mut p| {
            type_fix(&mut p.qry_set, p.root).unwrap();
            print_plan(sql, &p);
            let subq = p.root_query().unwrap();
            subq.root.walk(&mut preorder(|op| match &op.kind {
                j @ OpKind::Join(_) => {
                    for e in j.exprs() {
                        assert!(e.ty.is_bool());
                    }
                }
                _ => (),
            }));
        })
    }

    #[test]
    fn test_type_fix_aggr() {
        use crate::lgc::{preorder, OpKind};
        let cat = ty_catalog();
        assert_ty_plan1(
            &cat,
            "select c_i32 from ty1 group by c_i32 having c_i32",
            |sql, mut p| {
                type_fix(&mut p.qry_set, p.root).unwrap();
                print_plan(sql, &p);
                let subq = p.root_query().unwrap();
                subq.root.walk(&mut preorder(|op| match &op.kind {
                    OpKind::Aggr(aggr) => {
                        for e in &aggr.filt {
                            assert!(e.ty.is_bool());
                        }
                    }
                    _ => (),
                }));
            },
        )
    }

    #[inline]
    fn ty_catalog() -> MemCatalog {
        let cat = MemCatalog::default();
        cat.create_schema("ty").unwrap();
        cat.create_table(TableSpec::new(
            "ty",
            "ty1",
            vec![
                ColumnSpec::new("c_i32", PreciseType::i32(), ColumnAttr::empty()),
                ColumnSpec::new("c_u32", PreciseType::u32(), ColumnAttr::empty()),
                ColumnSpec::new("c_i64", PreciseType::i64(), ColumnAttr::empty()),
                ColumnSpec::new("c_u64", PreciseType::u64(), ColumnAttr::empty()),
                ColumnSpec::new("c_f32", PreciseType::f32(), ColumnAttr::empty()),
                ColumnSpec::new("c_f64", PreciseType::f64(), ColumnAttr::empty()),
                ColumnSpec::new("c_dec", PreciseType::decimal(18, 2), ColumnAttr::empty()),
                ColumnSpec::new("c_bool", PreciseType::bool(), ColumnAttr::empty()),
                ColumnSpec::new("c_dm", PreciseType::datetime(0), ColumnAttr::empty()),
                ColumnSpec::new("c_tm", PreciseType::time(0), ColumnAttr::empty()),
                ColumnSpec::new("c_dt", PreciseType::date(), ColumnAttr::empty()),
                ColumnSpec::new("c_ascii", PreciseType::ascii(1), ColumnAttr::empty()),
                ColumnSpec::new("c_utf8", PreciseType::var_utf8(10), ColumnAttr::empty()),
                ColumnSpec::new("c_bin", PreciseType::var_bytes(1024), ColumnAttr::empty()),
            ],
        ))
        .unwrap();
        cat.create_table(TableSpec::new(
            "ty",
            "dec1",
            vec![
                ColumnSpec::new("d40", PreciseType::decimal(4, 0), ColumnAttr::empty()),
                ColumnSpec::new("d50", PreciseType::decimal(5, 0), ColumnAttr::empty()),
                ColumnSpec::new("d60", PreciseType::decimal(6, 0), ColumnAttr::empty()),
                ColumnSpec::new("d41", PreciseType::decimal(4, 1), ColumnAttr::empty()),
                ColumnSpec::new("d51", PreciseType::decimal(5, 1), ColumnAttr::empty()),
                ColumnSpec::new("d61", PreciseType::decimal(6, 1), ColumnAttr::empty()),
                ColumnSpec::new("d42", PreciseType::decimal(4, 2), ColumnAttr::empty()),
                ColumnSpec::new("d52", PreciseType::decimal(5, 2), ColumnAttr::empty()),
                ColumnSpec::new("d62", PreciseType::decimal(6, 2), ColumnAttr::empty()),
            ],
        ))
        .unwrap();
        cat.create_table(TableSpec::new(
            "ty",
            "dm1",
            vec![
                ColumnSpec::new("dm0", PreciseType::datetime(0), ColumnAttr::empty()),
                ColumnSpec::new("dm3", PreciseType::datetime(3), ColumnAttr::empty()),
                ColumnSpec::new("dm6", PreciseType::datetime(6), ColumnAttr::empty()),
                ColumnSpec::new("tm0", PreciseType::time(0), ColumnAttr::empty()),
                ColumnSpec::new("tm3", PreciseType::time(3), ColumnAttr::empty()),
                ColumnSpec::new("tm6", PreciseType::time(6), ColumnAttr::empty()),
            ],
        ))
        .unwrap();
        cat.create_table(TableSpec::new(
            "ty",
            "str1",
            vec![
                ColumnSpec::new("a1", PreciseType::ascii(1), ColumnAttr::empty()),
                ColumnSpec::new("a10", PreciseType::ascii(10), ColumnAttr::empty()),
                ColumnSpec::new("va1", PreciseType::var_ascii(1), ColumnAttr::empty()),
                ColumnSpec::new("va10", PreciseType::var_ascii(10), ColumnAttr::empty()),
                ColumnSpec::new("u1", PreciseType::utf8(1), ColumnAttr::empty()),
                ColumnSpec::new("u10", PreciseType::utf8(10), ColumnAttr::empty()),
                ColumnSpec::new("vu1", PreciseType::var_utf8(1), ColumnAttr::empty()),
                ColumnSpec::new("vu10", PreciseType::var_utf8(10), ColumnAttr::empty()),
                ColumnSpec::new("b1", PreciseType::bytes(1), ColumnAttr::empty()),
                ColumnSpec::new("b10", PreciseType::bytes(10), ColumnAttr::empty()),
                ColumnSpec::new("vb1", PreciseType::var_bytes(1), ColumnAttr::empty()),
                ColumnSpec::new("vb10", PreciseType::var_bytes(10), ColumnAttr::empty()),
            ],
        ))
        .unwrap();
        cat
    }

    fn assert_ty_plan1<C: Catalog, F: FnOnce(&str, LgcPlan)>(cat: &C, sql: &str, f: F) {
        let plan = build_plan(cat, "ty", sql);
        f(sql, plan)
    }

    #[inline]
    fn extract_proj_types(p: &LgcPlan) -> Vec<PreciseType> {
        if let Some(subq) = p.root_query() {
            subq.out_cols().iter().map(|c| c.expr.ty).collect()
        } else {
            vec![]
        }
    }

    #[inline]
    fn extract_child_types(p: &LgcPlan) -> Vec<Vec<PreciseType>> {
        if let Some(subq) = p.root_query() {
            subq.out_cols()
                .iter()
                .map(|c| c.expr.args().into_iter().map(|arg| arg.ty).collect())
                .collect()
        } else {
            vec![]
        }
    }
}
