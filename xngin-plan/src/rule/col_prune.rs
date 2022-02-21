use crate::error::{Error, Result};
use crate::op::{Op, OpMutVisitor, OpVisitor};
use crate::query::{Location, QueryPlan, QuerySet, Subquery};
use fnv::FnvHashMap;
use smol_str::SmolStr;
use std::collections::BTreeMap;
use std::mem;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::{Col, Expr, ExprMutVisitor, ExprVisitor, QueryID};

/// Column pruning will remove unnecessary columns from the given plan.
/// It is invoked top down. First collect all output columns from current
/// query, then apply the pruning to each of its child query.
#[inline]
pub fn col_prune(QueryPlan { qry_set, root }: &mut QueryPlan) -> Result<()> {
    let mut use_set = FnvHashMap::default();
    prune_col(qry_set, *root, &mut use_set)
}

fn prune_col(
    qry_set: &mut QuerySet,
    qry_id: QueryID,
    use_set: &mut FnvHashMap<QueryID, BTreeMap<u32, u32>>,
) -> Result<()> {
    if let Some(subq) = qry_set.get_mut(&qry_id) {
        if subq.location != Location::Intermediate {
            // skip other types of queries
            return Ok(());
        }
        let mut curr_use_set = FnvHashMap::default();
        let mut c = Collect(&mut curr_use_set);
        let _ = subq.root.walk(&mut c);
        // add variables used in correlated subqueries
        for (query_id, idx) in &subq.scope.cor_vars {
            curr_use_set.entry(*query_id).or_default().insert(*idx, 0);
        }
        update_use_set(&mut curr_use_set);
        // merge into global use set
        for (k, v) in curr_use_set {
            use_set.insert(k, v);
        }
        let mut op = mem::take(&mut subq.root);
        let mut m = Modify { qry_set, use_set };
        let res = op.walk_mut(&mut m).unbranch();
        qry_set.get_mut(&qry_id).unwrap().root = op; // update back
        res
    } else {
        Err(Error::QueryNotFound(qry_id))
    }
}

struct Collect<'a>(&'a mut FnvHashMap<QueryID, BTreeMap<u32, u32>>);
impl OpVisitor for Collect<'_> {
    type Break = ();
    #[inline]
    fn enter(&mut self, op: &Op) -> ControlFlow<()> {
        for e in op.exprs() {
            let _ = e.walk(self);
        }
        ControlFlow::Continue(())
    }
}
impl ExprVisitor for Collect<'_> {
    type Break = ();
    #[inline]
    fn enter(&mut self, e: &Expr) -> ControlFlow<()> {
        if let Expr::Col(Col::QueryCol(qry_id, idx)) = e {
            self.0.entry(*qry_id).or_default().insert(*idx, 0);
        }
        ControlFlow::Continue(())
    }
}

struct Modify<'a> {
    qry_set: &'a mut QuerySet,
    use_set: &'a mut FnvHashMap<QueryID, BTreeMap<u32, u32>>,
    // res: Result<()>,
}

impl OpMutVisitor for Modify<'_> {
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match op {
            Op::Query(qry_id) => {
                // modify child query
                let mapping = self.use_set.get(qry_id);
                self.qry_set
                    .transform_subq(*qry_id, |subq| modify_subq(subq, mapping))
                    .branch()?;
                // recursively prune child
                prune_col(self.qry_set, *qry_id, self.use_set).branch()
            }
            _ => {
                for e in op.exprs_mut() {
                    let _ = e.walk_mut(self);
                }
                ControlFlow::Continue(())
            }
        }
    }
}

impl ExprMutVisitor for Modify<'_> {
    type Break = Error;
    #[inline]
    fn enter(&mut self, e: &mut Expr) -> ControlFlow<Error> {
        match e {
            Expr::Col(Col::QueryCol(qry_id, idx)) | Expr::Col(Col::CorrelatedCol(qry_id, idx)) => {
                if let Some(new) = self.use_set.get(qry_id).and_then(|m| m.get(idx).cloned()) {
                    *idx = new;
                }
            }
            Expr::Subq(_, qry_id) => {
                // self.res = prune_col(self.qry_set, *qry_id, self.use_set);
                // return self.res.is_ok();
                prune_col(self.qry_set, *qry_id, self.use_set).branch()?;
            }
            _ => (),
        }
        ControlFlow::Continue(())
    }
}

fn modify_subq(subq: &mut Subquery, mapping: Option<&BTreeMap<u32, u32>>) {
    match &mut subq.root {
        Op::Proj(proj) => {
            proj.cols = retain(mem::take(&mut proj.cols), mapping);
        }
        Op::Aggr(aggr) => {
            aggr.proj = retain(mem::take(&mut aggr.proj), mapping);
        }
        Op::Row(row) => {
            *row = retain(mem::take(row), mapping);
        }
        _ => {
            let cols: Vec<_> = if let Some(mapping) = mapping {
                subq.out_cols()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, (expr, alias))| {
                        if mapping.contains_key(&(i as u32)) {
                            Some((expr.clone(), alias.clone()))
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                vec![]
            };
            let old = mem::take(&mut subq.root);
            subq.root = Op::proj(cols, old);
        }
    }
}

/// retain outputs in provided mapping
fn retain<I>(cols: I, mapping: Option<&BTreeMap<u32, u32>>) -> Vec<(Expr, SmolStr)>
where
    I: IntoIterator<Item = (Expr, SmolStr)>,
{
    if let Some(mapping) = mapping {
        cols.into_iter()
            .enumerate()
            .filter_map(|(i, e)| {
                if mapping.contains_key(&(i as u32)) {
                    Some(e)
                } else {
                    None
                }
            })
            .collect()
    } else {
        vec![]
    }
}

fn update_use_set(use_set: &mut FnvHashMap<QueryID, BTreeMap<u32, u32>>) {
    for mapping in use_set.values_mut() {
        for (i, old) in mapping.values_mut().enumerate() {
            *old = i as u32;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan, get_lvl_queries, print_plan};

    #[test]
    fn test_col_prune_const() {
        assert_j_plan("select 1 from t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(4, subq[0].out_cols().len());
            col_prune(&mut plan).unwrap();
            print_plan(sql, &plan);
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq.len());
            // no columns required, should be specially handled in execution phase
            assert_eq!(0, subq[0].out_cols().len());
        })
    }

    #[test]
    fn test_col_prune_multi_cols() {
        assert_j_plan("select c1, c3 from t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(4, subq[0].out_cols().len());
            col_prune(&mut plan).unwrap();
            print_plan(sql, &plan);
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq.len());
            assert_eq!(2, subq[0].out_cols().len());
        })
    }

    #[test]
    fn test_col_prune_simple_col_order() {
        assert_j_plan("select c3, c1 from t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(4, subq[0].out_cols().len());
            col_prune(&mut plan).unwrap();
            print_plan(sql, &plan);
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(2, subq[0].out_cols().len());
        })
    }

    #[test]
    fn test_col_prune_dup_cols() {
        assert_j_plan("select c2, c2, c2 from t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(4, subq[0].out_cols().len());
            col_prune(&mut plan).unwrap();
            print_plan(sql, &plan);
            let subq = get_lvl_queries(&plan, 1);
            // remove duplicates and keep 1 column from source
            assert_eq!(1, subq[0].out_cols().len());
        })
    }

    #[test]
    fn test_col_prune_cross_join() {
        assert_j_plan("select t2.c0 from t2, t3", |sql, mut plan| {
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(2, subq.len());
            assert_eq!(3, subq[0].out_cols().len());
            assert_eq!(4, subq[1].out_cols().len());
            col_prune(&mut plan).unwrap();
            print_plan(sql, &plan);
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq[0].out_cols().len());
            assert_eq!(0, subq[1].out_cols().len());
        })
    }

    #[test]
    fn test_col_prune_implicit_join() {
        assert_j_plan(
            "select t2.c0 from t2, t3 where t2.c1 = t3.c1",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                // additional column is requied from t2 for filter
                assert_eq!(2, subq[0].out_cols().len());
                assert_eq!(1, subq[1].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_inner_join() {
        assert_j_plan(
            "select t2.c0 from t2 join t3 on t2.c1 = t3.c1 and t2.c2 = t3.c2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                // additional column is required from t2 for join
                assert_eq!(3, subq[0].out_cols().len());
                // additional column is required from t3 for join
                assert_eq!(2, subq[1].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_row() {
        assert_j_plan(
            "select t1.c0, tmp.x from t1, (select 1 as x, 2 as y) tmp",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].out_cols().len());
                assert_eq!(1, subq[1].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_aggr_col() {
        assert_j_plan("select sum(c2) from t2", |sql, mut plan| {
            col_prune(&mut plan).unwrap();
            print_plan(sql, &plan);
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(1, subq[0].out_cols().len());
        })
    }

    #[test]
    fn test_col_prune_aggr_asterisk() {
        assert_j_plan("select count(*) from t2", |sql, mut plan| {
            col_prune(&mut plan).unwrap();
            print_plan(sql, &plan);
            let subq = get_lvl_queries(&plan, 1);
            assert_eq!(0, subq[0].out_cols().len());
        })
    }

    #[test]
    fn test_col_prune_join_aggr() {
        assert_j_plan(
            "select count(*) from t2, t3 where t2.c2 = t3.c2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].out_cols().len());
                assert_eq!(1, subq[1].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_cor_subq_simple() {
        assert_j_plan(
            "select (select c0 from t3 where t3.c1 = t2.c1) from t2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_cor_subq_proj() {
        assert_j_plan(
            "select (select sum(c0) from t3 where t3.c1 = t2.c1 and t3.c2 = t2.c2) from t2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(2, subq[0].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_cor_subq_cross() {
        assert_j_plan(
            "select (select sum(c0) from t3 where t3.c1 = t2.c1 and t3.c2 = t2.c2), c0 from t2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(3, subq[0].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_derived_dup_cols() {
        assert_j_plan(
            "select (select sum(c0) from t3 where t3.c1 = t2.c1 and t3.c2 = t2.c2), c1 from t2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(2, subq[0].out_cols().len());
                print_plan(sql, &plan)
            },
        )
    }

    #[test]
    fn test_col_prune_derived_aggr() {
        assert_j_plan(
            "select s1 from (select sum(c1) as s1, sum(c2) as s2 from t2) x2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_single_derived_row() {
        assert_j_plan(
            "select s1 from (select 1 as s1, 2 as s2) x2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(1, subq[0].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_nested_aggr() {
        assert_j_plan(
            "select count(*) from (select count(*) as c from t1) x2",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 1);
                assert_eq!(0, subq[0].out_cols().len());
            },
        )
    }

    #[test]
    fn test_col_prune_derived_filt() {
        assert_j_plan(
            "select x from (select c1+1 as x from t1 where c1 > 0) t",
            |sql, mut plan| {
                col_prune(&mut plan).unwrap();
                print_plan(sql, &plan);
                let subq = get_lvl_queries(&plan, 2);
                assert_eq!(1, subq[0].out_cols().len());
            },
        )
    }
}
