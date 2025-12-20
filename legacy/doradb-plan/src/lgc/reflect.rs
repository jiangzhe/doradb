use crate::error::{Error, Result};
use crate::join::{self, Join, JoinKind, JoinOp};
use crate::lgc::col::{AliasKind, ProjCol};
use crate::lgc::op::{Op, OpKind, SortItem};
use crate::lgc::query::QuerySet;
use crate::lgc::LgcPlan;
use aosa::StringArena;
use doradb_catalog::Catalog;
use doradb_datatype::{self as datatype, TimeUnit, DEFAULT_DATE_FORMAT};
use doradb_expr::{self as expr, ColKind, Const, ExprKind, Farg, FuncKind, PredFuncKind, QueryID};
use doradb_sql::ast::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

#[inline]
pub fn reflect<'a, C: Catalog>(
    plan: &LgcPlan,
    arena: &'a StringArena,
    catalog: &C,
) -> Result<Statement<'a>> {
    // todo: handle CTEs
    let mut ctx = AstContext::default();
    let with = None;
    let op = reflect_root_query(&mut ctx, arena, &plan.qry_set, plan.root, catalog)?;
    let res = match op {
        AstOp::Partial(AstPartial::Derived { .. }) => {
            return Err(Error::IncompleteLgcPlanReflection)
        }
        AstOp::Partial(AstPartial::TableScan {
            tbl, filt, cols, ..
        }) => {
            if cols.is_empty() {
                return Err(Error::IncompleteLgcPlanReflection);
            }
            Statement::Select(QueryExpr {
                with,
                query: Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![TableRef::primitive(TablePrimitive::Named(tbl, None))],
                    filter: filt,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            })
        }
        AstOp::SelectRow(row) => Statement::Select(QueryExpr {
            with,
            query: Query::Row(row),
        }),
        AstOp::SelectTable(select_table, _) => Statement::Select(QueryExpr {
            with,
            query: Query::table(select_table),
        }),
        _ => todo!(),
    };
    Ok(res)
}

/// Reduce a independent plan to SQL statement.
#[inline]
fn reflect_root_query<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    root: QueryID,
    catalog: &C,
) -> Result<AstOp<'a>> {
    let q = qs.get(&root).ok_or(Error::QueryNotFound(root))?;
    let op = reflect_op(ctx, arena, qs, root, &q.root, catalog)?;
    // here we don't need to wrap query, instead directly output it.
    Ok(op)
}

/// Reduce query to partial AST for future processing.
#[inline]
fn reflect_query<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    root: QueryID,
    catalog: &C,
) -> Result<AstOp<'a>> {
    let q = qs.get(&root).ok_or(Error::QueryNotFound(root))?;
    let op = reflect_op(ctx, arena, qs, root, &q.root, catalog)?;
    // always assign query a unique alias
    let alias = ctx.qry_to_alias(root, arena)?;
    wrap_query(op, alias)
}

#[inline]
fn reflect_op<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    root: QueryID,
    op: &Op,
    catalog: &C,
) -> Result<AstOp<'a>> {
    let res = match &op.kind {
        OpKind::Proj { cols, input } => {
            let child = reflect_op(ctx, arena, qs, root, input, catalog)?;
            reflect_proj(ctx, arena, cols, qs, child, catalog)?
        }
        OpKind::Filt { pred, input } => {
            let child = reflect_op(ctx, arena, qs, root, input, catalog)?;
            reflect_filt(ctx, arena, pred, qs, child, catalog)?
        }
        OpKind::Aggr(aggr) => {
            let child = reflect_op(ctx, arena, qs, root, &aggr.input, catalog)?;
            reflect_aggr(
                ctx,
                arena,
                &aggr.groups,
                &aggr.proj,
                &aggr.filt,
                qs,
                child,
                catalog,
            )?
        }
        OpKind::Join(join) => reflect_join(ctx, arena, qs, root, join, true, catalog)?,
        OpKind::Sort {
            items,
            limit,
            input,
        } => {
            let child = reflect_op(ctx, arena, qs, root, input, catalog)?;
            reflect_sort(ctx, arena, items, *limit, qs, child, catalog)?
        }
        OpKind::Limit { start, end, input } => {
            let child = reflect_op(ctx, arena, qs, root, input, catalog)?;
            reflect_limit(*start, *end, child)?
        }
        OpKind::Scan(scan) => {
            let schema = Ident::regular(arena.add(&*scan.schema)?);
            let tbl = Ident::regular(arena.add(&*scan.table)?);
            // here we generate alias from table name if neccessary.
            let alias = ctx.table_name_to_alias(root, tbl, arena)?;
            AstOp::Partial(AstPartial::TableScan {
                tbl: TableName::new(Some(schema), tbl),
                filt: None,
                cols: vec![],
                origin: true,
                alias,
            })
        }
        OpKind::Row(cols) => {
            let cols = if let Some(cols) = cols {
                &cols[..]
            } else {
                return Err(Error::InvalidPlanStructureForReflection);
            };
            let mut row = Vec::with_capacity(cols.len());
            for c in cols {
                let e = transform_expr(ctx, arena, qs, &c.expr, catalog)?;
                let alias = if c.alias_kind == AliasKind::Explicit {
                    Ident::regular(arena.add(c.alias.as_str())?)
                } else {
                    Ident::auto_alias(arena.add(c.alias.as_str())?)
                };
                let dc = DerivedCol::new(e, alias);
                row.push(dc);
            }
            AstOp::SelectRow(row)
        }
        OpKind::Query(qry_id) => reflect_query(ctx, arena, qs, *qry_id, catalog)?,
        _ => todo!(),
    };
    Ok(res)
}

#[inline]
fn reflect_proj<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    proj_cols: &[ProjCol],
    qs: &QuerySet,
    child: AstOp<'a>,
    catalog: &C,
) -> Result<AstOp<'a>> {
    let res = match child {
        AstOp::Partial(AstPartial::TableScan {
            tbl,
            filt,
            origin,
            alias,
            ..
        }) => {
            // Partial table scan, no equivalent SQL statement.
            // keep it as is because the project list is strict sub-set of table columns
            // and no impact on future processing.
            let cols = transform_proj_cols(ctx, arena, qs, proj_cols, catalog)?;
            AstOp::Partial(AstPartial::TableScan {
                tbl,
                filt,
                cols,
                origin,
                alias,
            })
        }
        AstOp::Partial(AstPartial::Derived { qry, alias }) => {
            // handle cases like "SELECT ... FROM (...) AS t1"
            let cols = transform_proj_cols(ctx, arena, qs, proj_cols, catalog)?;
            let tr = TableRef::primitive(TablePrimitive::derived(qry, alias));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: None,
                    having: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                },
                false,
            )
        }
        AstOp::Partial(AstPartial::CrossJoin { left, right }) => {
            let cols = transform_proj_cols(ctx, arena, qs, proj_cols, catalog)?;
            let tr = TableRef::joined(TableJoin::cross(left, right));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: None,
                    having: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                },
                false,
            )
        }
        AstOp::Partial(AstPartial::ImplicitCross { tbls }) => {
            let cols = transform_proj_cols(ctx, arena, qs, proj_cols, catalog)?;
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: tbls,
                    filter: None,
                    having: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                },
                false,
            )
        }
        AstOp::Partial(AstPartial::QualifiedJoin {
            left,
            right,
            ty,
            cond,
        }) => {
            let cols = transform_proj_cols(ctx, arena, qs, proj_cols, catalog)?;
            let tr = TableRef::joined(TableJoin::qualified(left, right, ty, cond));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: None,
                    having: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                },
                false,
            )
        }
        // todo: handle agg
        AstOp::SelectTable(mut tbl, agg) => {
            if tbl.cols.is_empty() {
                // projection is not processed yet.
                let cols = transform_proj_cols(ctx, arena, qs, proj_cols, catalog)?;
                tbl.cols = cols;
                AstOp::SelectTable(tbl, agg)
            } else {
                return Err(Error::InvalidPlanStructureForReflection);
            }
        }
        AstOp::SelectRow(..) | AstOp::SelectSet(..) => todo!(),
    };
    Ok(res)
}

#[inline]
fn reflect_filt<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    preds: &[expr::ExprKind],
    qs: &QuerySet,
    child: AstOp<'a>,
    catalog: &C,
) -> Result<AstOp<'a>> {
    let res = match child {
        AstOp::Partial(AstPartial::TableScan {
            tbl,
            filt,
            cols,
            origin,
            alias,
        }) => {
            // construct a plain SQL with pattern like "SELECT ... FROM ... WHERE ..."
            let pred = transform_conj_preds(ctx, arena, qs, preds, catalog)?;
            let filt = if let Some(old) = filt {
                let mut es = old.into_conj();
                es.extend(pred.into_conj());
                Expr::pred_conj(es)
            } else {
                pred
            };
            AstOp::Partial(AstPartial::TableScan {
                tbl,
                filt: Some(filt),
                cols,
                origin,
                alias,
            })
        }
        AstOp::Partial(AstPartial::Derived { qry, alias }) => {
            // handle cases like "FROM (...) AS t1 WHERE ..."
            let pred = transform_conj_preds(ctx, arena, qs, preds, catalog)?;
            let tr = TableRef::primitive(TablePrimitive::derived(qry, alias));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![],
                    from: vec![tr],
                    filter: Some(pred),
                    having: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                },
                false,
            )
        }
        AstOp::Partial(AstPartial::CrossJoin { left, right }) => {
            let pred = transform_conj_preds(ctx, arena, qs, preds, catalog)?;
            let tr = TableRef::joined(TableJoin::cross(left, right));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![],
                    from: vec![tr],
                    filter: Some(pred),
                    having: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                },
                false,
            )
        }
        AstOp::Partial(AstPartial::ImplicitCross { tbls }) => {
            let pred = transform_conj_preds(ctx, arena, qs, preds, catalog)?;
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![],
                    from: tbls,
                    filter: Some(pred),
                    having: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                },
                false,
            )
        }
        AstOp::Partial(AstPartial::QualifiedJoin {
            left,
            right,
            ty,
            cond,
        }) => {
            let pred = transform_conj_preds(ctx, arena, qs, preds, catalog)?;
            let tr = TableRef::joined(TableJoin::qualified(left, right, ty, cond));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![],
                    from: vec![tr],
                    filter: Some(pred),
                    having: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                },
                false,
            )
        }
        AstOp::SelectTable(mut tbl, agg) => {
            let pred = transform_conj_preds(ctx, arena, qs, preds, catalog)?;
            if agg {
                // can only append to HAVING clause
                let having = if let Some(old) = tbl.having.take() {
                    let mut es = old.into_conj();
                    es.extend(pred.into_conj());
                    Expr::pred_conj(es)
                } else {
                    pred
                };
                tbl.having = Some(having);
                AstOp::SelectTable(tbl, agg)
            } else {
                let filt = if let Some(old) = tbl.filter.take() {
                    let mut es = old.into_conj();
                    es.extend(pred.into_conj());
                    Expr::pred_conj(es)
                } else {
                    pred
                };
                tbl.filter = Some(filt);
                AstOp::SelectTable(tbl, agg)
            }
        }
        AstOp::SelectRow(..) | AstOp::SelectSet(..) => todo!(),
    };
    Ok(res)
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn reflect_aggr<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    groups: &[expr::ExprKind],
    proj: &[ProjCol],
    having: &[expr::ExprKind],
    qs: &QuerySet,
    child: AstOp<'a>,
    catalog: &C,
) -> Result<AstOp<'a>> {
    let res = match child {
        AstOp::Partial(AstPartial::TableScan {
            tbl, filt, origin, ..
        }) => {
            if origin {
                // currently aggregation is not pushed down to table scan.
                return Err(Error::InvalidPlanStructureForReflection);
            }
            let group_by = transform_exprs(ctx, arena, qs, groups, catalog)?;
            let cols = transform_proj_cols(ctx, arena, qs, proj, catalog)?;
            let having = if having.is_empty() {
                None
            } else {
                let pred = transform_conj_preds(ctx, arena, qs, having, catalog)?;
                Some(pred)
            };
            let tr = TableRef::primitive(TablePrimitive::Named(tbl, None));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: filt,
                    having,
                    group_by,
                    order_by: vec![],
                    limit: None,
                },
                true,
            )
        }
        AstOp::Partial(AstPartial::Derived { qry, alias }) => {
            // handle cases like "FROM (...) AS t1 GROUP BY ..."
            let group_by = transform_exprs(ctx, arena, qs, groups, catalog)?;
            let cols = transform_proj_cols(ctx, arena, qs, proj, catalog)?;
            let having = if having.is_empty() {
                None
            } else {
                let pred = transform_conj_preds(ctx, arena, qs, having, catalog)?;
                Some(pred)
            };
            let tr = TableRef::primitive(TablePrimitive::derived(qry, alias));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: None,
                    having,
                    group_by,
                    order_by: vec![],
                    limit: None,
                },
                true,
            )
        }
        AstOp::Partial(AstPartial::CrossJoin { left, right }) => {
            let group_by = transform_exprs(ctx, arena, qs, groups, catalog)?;
            let cols = transform_proj_cols(ctx, arena, qs, proj, catalog)?;
            let having = if having.is_empty() {
                None
            } else {
                let pred = transform_conj_preds(ctx, arena, qs, having, catalog)?;
                Some(pred)
            };
            let tr = TableRef::joined(TableJoin::cross(left, right));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: None,
                    having,
                    group_by,
                    order_by: vec![],
                    limit: None,
                },
                true,
            )
        }
        AstOp::Partial(AstPartial::ImplicitCross { tbls }) => {
            let group_by = transform_exprs(ctx, arena, qs, groups, catalog)?;
            let cols = transform_proj_cols(ctx, arena, qs, proj, catalog)?;
            let having = if having.is_empty() {
                None
            } else {
                let pred = transform_conj_preds(ctx, arena, qs, having, catalog)?;
                Some(pred)
            };
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: tbls,
                    filter: None,
                    having,
                    group_by,
                    order_by: vec![],
                    limit: None,
                },
                true,
            )
        }
        AstOp::Partial(AstPartial::QualifiedJoin {
            left,
            right,
            ty,
            cond,
        }) => {
            let group_by = transform_exprs(ctx, arena, qs, groups, catalog)?;
            let cols = transform_proj_cols(ctx, arena, qs, proj, catalog)?;
            let having = if having.is_empty() {
                None
            } else {
                let pred = transform_conj_preds(ctx, arena, qs, having, catalog)?;
                Some(pred)
            };
            let tr = TableRef::joined(TableJoin::qualified(left, right, ty, cond));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: None,
                    having,
                    group_by,
                    order_by: vec![],
                    limit: None,
                },
                true,
            )
        }
        AstOp::SelectTable(mut tbl, agg) => {
            if agg || !tbl.order_by.is_empty() || tbl.limit.is_some() {
                // currently, two aggregations within single query block is not allowed.
                // order by or limit is not allowed to precede aggregation.
                return Err(Error::InvalidPlanStructureForReflection);
            }
            // plain projection with optional filter.
            // just replace with aggr.
            let group_by = transform_exprs(ctx, arena, qs, groups, catalog)?;
            let cols = transform_proj_cols(ctx, arena, qs, proj, catalog)?;
            let having = if having.is_empty() {
                None
            } else {
                let pred = transform_conj_preds(ctx, arena, qs, having, catalog)?;
                Some(pred)
            };
            tbl.group_by = group_by;
            tbl.cols = cols;
            tbl.having = having;
            AstOp::SelectTable(tbl, true)
        }
        AstOp::SelectRow(..) | AstOp::SelectSet(..) => todo!(),
    };
    Ok(res)
}

#[inline]
fn reflect_join<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    root: QueryID,
    join: &join::Join,
    implicit_enabled: bool,
    catalog: &C,
) -> Result<AstOp<'a>> {
    match join {
        Join::Cross(jos) => {
            reflect_cross_join(ctx, arena, qs, root, jos, implicit_enabled, catalog)
        }
        Join::Qualified(qj) => reflect_qualified_join(ctx, arena, qs, root, qj, catalog),
    }
}

#[inline]
fn reflect_sort<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    items: &[SortItem],
    limit: Option<u64>,
    qs: &QuerySet,
    child: AstOp<'a>,
    catalog: &C,
) -> Result<AstOp<'a>> {
    let elems = transform_sort_items(ctx, arena, qs, items, catalog)?;
    let res = match child {
        AstOp::Partial(AstPartial::TableScan {
            tbl,
            filt,
            cols,
            alias,
            ..
        }) => {
            let tr = TableRef::primitive(TablePrimitive::Named(tbl, alias));
            let limit = limit.map(|limit| Limit {
                limit,
                offset: None,
            });
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: filt,
                    group_by: vec![],
                    having: None,
                    order_by: elems,
                    limit,
                },
                false,
            )
        }
        AstOp::SelectTable(mut tbl, agg) => {
            if tbl.cols.is_empty() || tbl.limit.is_some() {
                return Err(Error::InvalidPlanStructureForReflection);
            }
            tbl.order_by = elems;
            tbl.limit = limit.map(|limit| Limit {
                limit,
                offset: None,
            });
            AstOp::SelectTable(tbl, agg)
        }
        _ => todo!(),
    };
    Ok(res)
}

#[inline]
fn reflect_limit(start: u64, end: u64, child: AstOp) -> Result<AstOp> {
    let limit = if start == 0 {
        Some(Limit {
            limit: end,
            offset: None,
        })
    } else {
        Some(Limit {
            limit: end - start,
            offset: Some(start),
        })
    };
    let res = match child {
        AstOp::Partial(AstPartial::TableScan {
            tbl,
            filt,
            cols,
            alias,
            ..
        }) => {
            let tr = TableRef::primitive(TablePrimitive::Named(tbl, alias));
            AstOp::SelectTable(
                SelectTable {
                    q: SetQuantifier::All,
                    cols,
                    from: vec![tr],
                    filter: filt,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit,
                },
                false,
            )
        }
        AstOp::SelectTable(mut tbl, agg) => {
            if tbl.cols.is_empty() || tbl.limit.is_some() {
                return Err(Error::InvalidPlanStructureForReflection);
            }
            tbl.limit = limit;
            AstOp::SelectTable(tbl, agg)
        }
        _ => todo!(),
    };
    Ok(res)
}

#[inline]
fn reflect_cross_join<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    root: QueryID,
    jos: &[JoinOp],
    implicit_enabled: bool,
    catalog: &C,
) -> Result<AstOp<'a>> {
    // check if cross join can be reflected as multiple implicit table references.
    // The precondition is all participants are tables/queries.
    if implicit_enabled {
        let tbls = implicit_cross_join(ctx, arena, qs, root, jos, catalog)?;
        if !tbls.is_empty() {
            return Ok(AstOp::Partial(AstPartial::ImplicitCross { tbls }));
        }
    }
    // convert to explicit cross joins.
    let (first_jo, mid) = jos
        .split_first()
        .ok_or(Error::InvalidPlanStructureForReflection)?;
    let (last_jo, mid) = mid
        .split_last()
        .ok_or(Error::InvalidPlanStructureForReflection)?;
    // handle first join
    let mut first = reflect_join_op(ctx, arena, qs, root, first_jo, catalog)?;
    for next_jo in mid {
        let next = reflect_join_op(ctx, arena, qs, root, next_jo, catalog)?;
        // due to SQL limitation, the right side of join operator must be table primitive.
        // That means bushy tree will not be allowed.
        // todo: construct derived table if right side is join. Note: this will change column references.
        match next {
            TableRef::Primitive(tp) => {
                first = TableRef::joined(TableJoin::cross(first, *tp));
            }
            _ => return Err(Error::InvalidPlanStructureForReflection),
        }
    }
    let last = reflect_join_op(ctx, arena, qs, root, last_jo, catalog)?;
    let last = match last {
        TableRef::Primitive(tp) => *tp,
        _ => return Err(Error::InvalidPlanStructureForReflection),
    };
    Ok(AstOp::Partial(AstPartial::CrossJoin {
        left: first,
        right: last,
    }))
}

#[inline]
fn implicit_cross_join<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    root: QueryID,
    jos: &[JoinOp],
    catalog: &C,
) -> Result<Vec<TableRef<'a>>> {
    let mut tbls = Vec::with_capacity(jos.len());
    for jo in jos {
        match &jo.as_ref().kind {
            OpKind::Join(_) => {
                // join inside cross join is not supported
                tbls.clear();
                return Ok(tbls);
            }
            OpKind::Query(_) => {
                let tbl = reflect_join_op(ctx, arena, qs, root, jo, catalog)?;
                tbls.push(tbl);
            }
            _ => unreachable!(),
        }
    }
    Ok(tbls)
}

#[inline]
fn reflect_qualified_join<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    root: QueryID,
    join: &join::QualifiedJoin,
    catalog: &C,
) -> Result<AstOp<'a>> {
    if !join.filt.is_empty() {
        // post filter in join is not supported.
        return Err(Error::InvalidPlanStructureForReflection);
    }
    let left = reflect_join_op(ctx, arena, qs, root, &join.left, catalog)?;
    let right = match reflect_join_op(ctx, arena, qs, root, &join.right, catalog)? {
        TableRef::Primitive(tp) => *tp,
        // todo: support table join on right side of join operator.
        _ => return Err(Error::InvalidPlanStructureForReflection),
    };
    let exprs = transform_exprs(ctx, arena, qs, &join.cond, catalog)?;
    let cond = exprs_to_filter(exprs).map(JoinCondition::Conds);
    let ty = match join.kind {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Left => JoinType::Left,
        JoinKind::Full => JoinType::Full,
        // todo: support more join types.
        _ => return Err(Error::InvalidPlanStructureForReflection),
    };
    Ok(AstOp::Partial(AstPartial::QualifiedJoin {
        left,
        right,
        ty,
        cond,
    }))
}

#[inline]
fn reflect_join_op<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    root: QueryID,
    jo: &Op,
    catalog: &C,
) -> Result<TableRef<'a>> {
    let res = match &jo.kind {
        OpKind::Query(qry_id) => {
            let ast = reflect_query(ctx, arena, qs, *qry_id, catalog)?;
            match ast {
                AstOp::Partial(AstPartial::TableScan {
                    tbl, filt, alias, ..
                }) => {
                    if filt.is_some() {
                        // post filter in join is not supported.
                        return Err(Error::InvalidPlanStructureForReflection);
                    }
                    TableRef::primitive(TablePrimitive::Named(tbl, alias))
                }
                AstOp::Partial(AstPartial::Derived { qry, alias }) => {
                    TableRef::primitive(TablePrimitive::derived(qry, alias))
                }
                _ => unreachable!(),
            }
        }
        OpKind::Join(join) => match &**join {
            Join::Cross(curr_jos) => {
                // unfold nested cross joins
                let ast = reflect_cross_join(ctx, arena, qs, root, curr_jos, false, catalog)?;
                match ast {
                    AstOp::Partial(AstPartial::CrossJoin { left, right }) => {
                        TableRef::joined(TableJoin::cross(left, right))
                    }
                    // only cross join is returned.
                    // implicit cross is disabled in this case.
                    _ => unreachable!(),
                }
            }
            Join::Qualified(qj) => {
                let ast = reflect_qualified_join(ctx, arena, qs, root, qj, catalog)?;
                match ast {
                    AstOp::Partial(AstPartial::QualifiedJoin {
                        left,
                        right,
                        ty,
                        cond,
                    }) => TableRef::joined(TableJoin::Qualified(QualifiedJoin {
                        left,
                        right,
                        ty,
                        cond,
                    })),
                    // only qualified join is returned.
                    _ => unreachable!(),
                }
            }
        },
        // JoinOp has only two kinds: join and query.
        _ => unreachable!(),
    };
    Ok(res)
}

/// Wrap a complete query as a derived table.
#[inline]
fn wrap_query<'a>(op: AstOp<'a>, alias: Ident<'a>) -> Result<AstOp<'a>> {
    let res = match op {
        AstOp::Partial(AstPartial::TableScan {
            tbl,
            filt,
            cols,
            origin,
            alias: opt_alias,
        }) => {
            if origin {
                // set origin to false, so next time it will be converted to derived table.
                AstOp::Partial(AstPartial::TableScan {
                    tbl,
                    filt,
                    cols,
                    origin: false,
                    alias: opt_alias,
                })
            } else {
                AstOp::Partial(AstPartial::Derived {
                    qry: QueryExpr {
                        with: None,
                        query: Query::table(SelectTable {
                            q: SetQuantifier::All,
                            cols,
                            from: vec![TableRef::primitive(TablePrimitive::Named(tbl, None))],
                            filter: filt,
                            group_by: vec![],
                            having: None,
                            order_by: vec![],
                            limit: None,
                        }),
                    },
                    alias,
                })
            }
        }
        AstOp::SelectTable(select_table, _) => AstOp::Partial(AstPartial::Derived {
            qry: QueryExpr {
                with: None,
                query: Query::table(select_table),
            },
            alias,
        }),
        _ => todo!(),
    };
    Ok(res)
}

#[inline]
fn transform_proj_cols<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    proj_cols: &[ProjCol],
    catalog: &C,
) -> Result<Vec<DerivedCol<'a>>> {
    let mut cols = Vec::with_capacity(proj_cols.len());
    for c in proj_cols {
        let e = transform_expr(ctx, arena, qs, &c.expr, catalog)?;
        let alias = if c.alias_kind == AliasKind::Explicit {
            Ident::regular(arena.add(c.alias.as_str())?)
        } else {
            Ident::auto_alias(arena.add(c.alias.as_str())?)
        };
        let dc = DerivedCol::new(e, alias);
        cols.push(dc);
    }
    Ok(cols)
}

#[inline]
fn transform_conj_preds<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    preds: &[expr::ExprKind],
    catalog: &C,
) -> Result<Expr<'a>> {
    if preds.len() == 1 {
        return transform_expr(ctx, arena, qs, &preds[0], catalog);
    }
    let exprs = transform_exprs(ctx, arena, qs, preds, catalog)?;
    Ok(Expr::pred_conj(exprs))
}

#[inline]
fn transform_exprs<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    exprs: &[expr::ExprKind],
    catalog: &C,
) -> Result<Vec<Expr<'a>>> {
    let mut res = Vec::with_capacity(exprs.len());
    for e in exprs {
        let e = transform_expr(ctx, arena, qs, e, catalog)?;
        res.push(e);
    }
    Ok(res)
}

#[inline]
fn transform_sort_items<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    items: &[SortItem],
    catalog: &C,
) -> Result<Vec<OrderElement<'a>>> {
    let mut res = Vec::with_capacity(items.len());
    for it in items {
        let e = transform_expr(ctx, arena, qs, &it.expr, catalog)?;
        res.push(OrderElement::new(e, it.desc));
    }
    Ok(res)
}

#[inline]
fn transform_expr<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    expr: &ExprKind,
    catalog: &C,
) -> Result<Expr<'a>> {
    let res = match expr {
        ExprKind::Const(c) => {
            let lit = transform_const(c, arena, &mut ctx.str_buf)?;
            Expr::Literal(lit)
        }
        ExprKind::Col(c) => match &c.kind {
            ColKind::Table(_, col_name, _) => {
                let col_name = arena.add(col_name.as_str())?;
                // just column name is enough, as table is its single source.
                Expr::ColumnRef(vec![Ident::regular(col_name)])
            }
            ColKind::Intra(..) => todo!("cannot reflect intra column"),
            ColKind::Query(qry_id) | ColKind::Correlated(qry_id) => {
                let tbl_alias = ctx
                    .find_qry_alias(qry_id)
                    .ok_or(Error::QueryNotFound(*qry_id))?;
                let subq = qs.get(qry_id).ok_or(Error::QueryNotFound(*qry_id))?;
                let col = subq.find_out_col(c.idx.value() as usize).ok_or_else(|| {
                    Error::UnknownColumn(format!(
                        "column with index {} in query '{}' not found",
                        c.idx.value(),
                        tbl_alias.as_str(),
                    ))
                })?;
                // refer to query column with alias
                let col_alias = Ident::regular(arena.add(col.alias.as_str())?);
                Expr::ColumnRef(vec![tbl_alias, col_alias])
            }
            ColKind::Setop(..) => unreachable!("setop columns are not transformable"),
        },
        ExprKind::Pred(pred) => transform_pred(ctx, arena, qs, pred, catalog)?,
        ExprKind::Aggf { kind, q, arg } => transform_aggf(ctx, arena, qs, *kind, *q, arg, catalog)?,
        ExprKind::Tuple(es) => {
            let values = transform_exprs(ctx, arena, qs, es, catalog)?;
            Expr::Tuple(values)
        }
        ExprKind::Func { kind, args } => transform_func(ctx, arena, qs, *kind, args, catalog)?,
        ExprKind::Farg(arg) => match arg {
            Farg::None => Expr::FuncArg(ConstArg::None),
            Farg::TimeUnit(unit) => {
                let unit = datetime_unit(*unit);
                Expr::FuncArg(ConstArg::DatetimeUnit(unit))
            }
        },
        // althrough subquery can be non-scalar, we use the same logic to reflect.
        // so here we temporarily store the result as "ScalarSubquery",
        // but convert to other subquery after it is returned.
        ExprKind::Attval(qry_id) | ExprKind::Subq(_, qry_id) => {
            // here we treat the subquery as root query, so no derived table will be generated.
            let ast = reflect_root_query(ctx, arena, qs, *qry_id, catalog)?;
            match ast {
                AstOp::SelectTable(tbl, _) => Expr::scalar_subquery(QueryExpr {
                    with: None,
                    query: Query::table(tbl),
                }),
                AstOp::Partial(AstPartial::TableScan {
                    tbl,
                    filt,
                    cols,
                    alias,
                    ..
                }) => {
                    let tr = TableRef::primitive(TablePrimitive::Named(tbl, alias));
                    let select_table = SelectTable {
                        q: SetQuantifier::All,
                        cols,
                        from: vec![tr],
                        filter: filt,
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    };
                    Expr::scalar_subquery(QueryExpr {
                        with: None,
                        query: Query::table(select_table),
                    })
                }
                _ => todo!(),
            }
        }
        ExprKind::Case { op, acts, fallback } => {
            let op = op
                .as_ref()
                .map(|e| transform_expr(ctx, arena, qs, e, catalog))
                .transpose()?
                .map(Box::new);
            let mut branches = Vec::with_capacity(acts.len());
            for (when, then) in acts {
                let when = transform_expr(ctx, arena, qs, when, catalog)?;
                let then = transform_expr(ctx, arena, qs, then, catalog)?;
                branches.push((when, then));
            }
            let fallback = fallback
                .as_ref()
                .map(|e| transform_expr(ctx, arena, qs, e, catalog))
                .transpose()?
                .map(Box::new);
            Expr::case_when(op, branches, fallback)
        }
        _ => todo!(),
    };
    Ok(res)
}

#[inline]
fn transform_pred<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    pred: &expr::Pred,
    catalog: &C,
) -> Result<Expr<'a>> {
    let res = match pred {
        expr::Pred::Conj(es) => {
            let exprs = transform_exprs(ctx, arena, qs, es, catalog)?;
            Expr::pred_conj(exprs)
        }
        expr::Pred::Disj(es) => {
            let exprs = transform_exprs(ctx, arena, qs, es, catalog)?;
            Expr::pred_disj(exprs)
        }
        expr::Pred::Xor(es) => {
            let exprs = transform_exprs(ctx, arena, qs, es, catalog)?;
            Expr::pred_xor(exprs)
        }
        expr::Pred::Func { kind, args } => {
            let mut exprs = transform_exprs(ctx, arena, qs, args, catalog)?;
            match kind {
                PredFuncKind::Equal => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_cmp(CompareOp::Equal, left, right)
                }
                PredFuncKind::Greater => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_cmp(CompareOp::Greater, left, right)
                }
                PredFuncKind::GreaterEqual => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_cmp(CompareOp::GreaterEqual, left, right)
                }
                PredFuncKind::Less => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_cmp(CompareOp::Less, left, right)
                }
                PredFuncKind::LessEqual => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_cmp(CompareOp::LessEqual, left, right)
                }
                PredFuncKind::NotEqual => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_cmp(CompareOp::NotEqual, left, right)
                }
                PredFuncKind::IsNull => Expr::pred_is_null(exprs.pop().unwrap()),
                PredFuncKind::IsNotNull => Expr::pred_not_null(exprs.pop().unwrap()),
                PredFuncKind::IsTrue => Expr::pred_is_true(exprs.pop().unwrap()),
                PredFuncKind::IsNotTrue => Expr::pred_not_true(exprs.pop().unwrap()),
                PredFuncKind::IsFalse => Expr::pred_is_false(exprs.pop().unwrap()),
                PredFuncKind::IsNotFalse => Expr::pred_not_false(exprs.pop().unwrap()),
                PredFuncKind::SafeEqual => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_safeeq(left, right)
                }
                PredFuncKind::Like => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_like(left, right)
                }
                PredFuncKind::NotLike => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_nlike(left, right)
                }
                PredFuncKind::Regexp => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_regexp(left, right)
                }
                PredFuncKind::NotRegexp => {
                    let (left, right) = extract_two_exprs(exprs);
                    Expr::pred_nregexp(left, right)
                }
                PredFuncKind::InValues => {
                    let (lhs, rhs) = extract_two_exprs(exprs);
                    let values = rhs.into_tuple().unwrap();
                    Expr::pred_in_values(lhs, values)
                }
                PredFuncKind::NotInValues => {
                    let (lhs, rhs) = extract_two_exprs(exprs);
                    let values = rhs.into_tuple().unwrap();
                    Expr::pred_nin_values(lhs, values)
                }
                PredFuncKind::Between => {
                    let (left, middle, right) = extract_three_exprs(exprs);
                    Expr::pred_btw(left, middle, right)
                }
                PredFuncKind::NotBetween => {
                    let (left, middle, right) = extract_three_exprs(exprs);
                    Expr::pred_nbtw(left, middle, right)
                }
            }
        }
        expr::Pred::Not(e) => {
            let e = transform_expr(ctx, arena, qs, e, catalog)?;
            Expr::logical_not(e)
        }
        expr::Pred::InSubquery(lhs, subq) => {
            let lhs = transform_expr(ctx, arena, qs, lhs, catalog)?;
            let qe = transform_expr(ctx, arena, qs, subq, catalog)?;
            let subq =
                extract_subq_from_expr(qe).ok_or(Error::InvalidPlanStructureForReflection)?;
            Expr::pred_in_subquery(lhs, subq)
        }
        expr::Pred::NotInSubquery(lhs, subq) => {
            let lhs = transform_expr(ctx, arena, qs, lhs, catalog)?;
            let qe = transform_expr(ctx, arena, qs, subq, catalog)?;
            let subq =
                extract_subq_from_expr(qe).ok_or(Error::InvalidPlanStructureForReflection)?;
            Expr::pred_nin_subquery(lhs, subq)
        }
        expr::Pred::Exists(subq) => {
            let qe = transform_expr(ctx, arena, qs, subq, catalog)?;
            let subq =
                extract_subq_from_expr(qe).ok_or(Error::InvalidPlanStructureForReflection)?;
            Expr::exists(subq)
        }
        expr::Pred::NotExists(subq) => {
            let qe = transform_expr(ctx, arena, qs, subq, catalog)?;
            let subq =
                extract_subq_from_expr(qe).ok_or(Error::InvalidPlanStructureForReflection)?;
            Expr::logical_not(Expr::exists(subq))
        }
    };
    Ok(res)
}

#[inline]
fn transform_func<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    kind: FuncKind,
    args: &[expr::ExprKind],
    catalog: &C,
) -> Result<Expr<'a>> {
    let mut exprs = transform_exprs(ctx, arena, qs, args, catalog)?;
    let res = match kind {
        FuncKind::Uninit => unreachable!(),
        FuncKind::Neg => Expr::unary(UnaryOp::Neg, exprs.pop().unwrap()),
        FuncKind::BitInv => Expr::bit_inv(exprs.pop().unwrap()),
        FuncKind::Add => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::Add, lhs, rhs)
        }
        FuncKind::Sub => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::Sub, lhs, rhs)
        }
        FuncKind::Mul => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::Mul, lhs, rhs)
        }
        FuncKind::Div => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::Div, lhs, rhs)
        }
        FuncKind::IntDiv => todo!(),
        FuncKind::BitAnd => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::BitAnd, lhs, rhs)
        }
        FuncKind::BitOr => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::BitOr, lhs, rhs)
        }
        FuncKind::BitXor => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::BitXor, lhs, rhs)
        }
        FuncKind::BitShl => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::BitShl, lhs, rhs)
        }
        FuncKind::BitShr => {
            let (lhs, rhs) = extract_two_exprs(exprs);
            Expr::binary(BinaryOp::BitShr, lhs, rhs)
        }
        FuncKind::Extract => Expr::func(FuncType::Extract, exprs),
        FuncKind::Substring => Expr::func(FuncType::Substring, exprs),
    };
    Ok(res)
}

#[inline]
fn transform_aggf<'a, C: Catalog>(
    ctx: &mut AstContext<'a>,
    arena: &'a StringArena,
    qs: &QuerySet,
    kind: expr::AggKind,
    q: expr::Setq,
    arg: &expr::ExprKind,
    catalog: &C,
) -> Result<Expr<'a>> {
    let kind = match kind {
        expr::AggKind::Count => AggrFuncKind::Count,
        expr::AggKind::Sum => AggrFuncKind::Sum,
        expr::AggKind::Max => AggrFuncKind::Max,
        expr::AggKind::Min => AggrFuncKind::Min,
        expr::AggKind::Avg => AggrFuncKind::Avg,
    };
    let q = match q {
        expr::Setq::All => SetQuantifier::All,
        expr::Setq::Distinct => SetQuantifier::Distinct,
    };
    let e = transform_expr(ctx, arena, qs, arg, catalog)?;
    Ok(Expr::AggrFunc(AggrFunc {
        kind,
        q,
        expr: Box::new(e),
    }))
}

#[inline]
fn exprs_to_filter(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        None
    } else if exprs.len() == 1 {
        Some(exprs.pop().unwrap())
    } else {
        Some(Expr::pred_conj(exprs))
    }
}

#[inline]
fn extract_two_exprs(mut exprs: Vec<Expr>) -> (Expr, Expr) {
    let right = exprs.pop().unwrap();
    let left = exprs.pop().unwrap();
    (left, right)
}

#[inline]
fn extract_three_exprs(mut exprs: Vec<Expr>) -> (Expr, Expr, Expr) {
    let right = exprs.pop().unwrap();
    let middle = exprs.pop().unwrap();
    let left = exprs.pop().unwrap();
    (left, middle, right)
}

#[inline]
fn extract_subq_from_expr(expr: Expr) -> Option<QueryExpr> {
    match expr {
        Expr::ScalarSubquery(qe) => Some(*qe),
        _ => None,
    }
}

#[inline]
fn transform_const<'a>(
    c: &expr::Const,
    arena: &'a StringArena,
    buf: &mut String,
) -> Result<Literal<'a>> {
    use std::fmt::Write;
    buf.clear();
    let res = match c {
        Const::I64(i) => {
            write!(buf, "{i}").unwrap();
            let s = arena.add(&buf)?;
            Literal::Numeric(s)
        }
        Const::U64(u) => {
            write!(buf, "{u}").unwrap();
            let s = arena.add(&buf)?;
            Literal::Numeric(s)
        }
        Const::F64(f) => {
            write!(buf, "{}", f.value()).unwrap();
            let s = arena.add(&buf)?;
            Literal::Numeric(s)
        }
        Const::Decimal(dec) => {
            // SAFETY:
            //
            // append utf8 only
            unsafe {
                let bbuf = buf.as_mut_vec();
                dec.append_str_buf(bbuf, -1);
                let s = arena.add(&buf)?;
                Literal::Numeric(s)
            }
        }
        Const::String(s) => {
            let s = arena.add(s)?;
            Literal::CharStr(CharStr::from(s))
        }
        Const::Date(dt) => {
            let res = dt
                .format(&DEFAULT_DATE_FORMAT)
                .map_err(|_| Error::InvalidExprTransformationForReflection)?;
            let s = arena.add(&res)?;
            Literal::Date(s)
        }
        Const::Interval(datatype::Interval { value, unit }) => {
            let unit = datetime_unit(*unit);
            let value = format!("{value}");
            let value = arena.add(&value)?;
            Literal::Interval(Interval { unit, value })
        }
        Const::Bool(b) => Literal::Bool(*b),
        Const::Null => Literal::Null,
        _ => todo!(),
    };
    Ok(res)
}

#[inline]
fn datetime_unit(unit: TimeUnit) -> DatetimeUnit {
    match unit {
        TimeUnit::Microsecond => DatetimeUnit::Microsecond,
        TimeUnit::Second => DatetimeUnit::Second,
        TimeUnit::Minute => DatetimeUnit::Minute,
        TimeUnit::Hour => DatetimeUnit::Hour,
        TimeUnit::Day => DatetimeUnit::Day,
        TimeUnit::Week => DatetimeUnit::Week,
        TimeUnit::Month => DatetimeUnit::Month,
        TimeUnit::Quarter => DatetimeUnit::Quarter,
        TimeUnit::Year => DatetimeUnit::Year,
    }
}

#[derive(Debug)]
enum AstOp<'a> {
    SelectSet(SelectSet<'a>),
    SelectTable(SelectTable<'a>, bool),
    SelectRow(Vec<DerivedCol<'a>>),
    Partial(AstPartial<'a>),
}

#[derive(Debug)]
enum AstPartial<'a> {
    TableScan {
        tbl: TableName<'a>,
        filt: Option<Expr<'a>>,
        cols: Vec<DerivedCol<'a>>,
        // whether the table scan is performed on origin table
        // once a projection is done, it should be converted to
        // derived table.
        origin: bool,
        alias: Option<Ident<'a>>,
    },
    Derived {
        qry: QueryExpr<'a>,
        alias: Ident<'a>,
    },
    CrossJoin {
        left: TableRef<'a>,
        right: TablePrimitive<'a>,
    },
    ImplicitCross {
        tbls: Vec<TableRef<'a>>,
    },
    QualifiedJoin {
        left: TableRef<'a>,
        right: TablePrimitive<'a>,
        ty: JoinType,
        cond: Option<JoinCondition<'a>>,
    },
}

#[derive(Default)]
struct AstContext<'a> {
    q2a: HashMap<QueryID, Ident<'a>>,
    a2q: HashMap<Ident<'a>, QueryID>,
    tbl_alias_id: u32,
    str_buf: String,
}

impl<'a> AstContext<'a> {
    /// Convert table name to alias.
    /// If table name is unique, use its original value.
    /// Otherwise, generate "t(n)" as its alias.
    /// Returned flag indicates whether the table must be aliased.
    #[inline]
    pub fn table_name_to_alias(
        &mut self,
        qry_id: QueryID,
        tbl_name: Ident<'a>,
        arena: &'a StringArena,
    ) -> Result<Option<Ident<'a>>> {
        // try table name only
        match self.a2q.entry(tbl_name) {
            Entry::Occupied(occ) => {
                let existing_qry_id = *occ.get();
                if qry_id == existing_qry_id {
                    return Err(Error::DuplicatedQueryID(qry_id));
                }
                let (_, alias) = next_avail_tbl_alias_id(
                    &mut self.tbl_alias_id,
                    &mut self.str_buf,
                    &self.a2q,
                    arena,
                )?;
                self.q2a.insert(qry_id, alias);
                self.a2q.insert(alias, qry_id);
                Ok(Some(alias))
            }
            Entry::Vacant(vac) => {
                // just save original table name
                vac.insert(qry_id);
                if self.q2a.insert(qry_id, tbl_name).is_some() {
                    return Err(Error::DuplicatedQueryID(qry_id));
                }
                Ok(None)
            }
        }
    }

    #[inline]
    pub fn qry_to_alias(&mut self, qry_id: QueryID, arena: &'a StringArena) -> Result<Ident<'a>> {
        match self.q2a.entry(qry_id) {
            Entry::Occupied(occ) => Ok(*occ.get()),
            Entry::Vacant(vac) => {
                let (_, alias) = next_avail_tbl_alias_id(
                    &mut self.tbl_alias_id,
                    &mut self.str_buf,
                    &self.a2q,
                    arena,
                )?;
                vac.insert(alias);
                self.a2q.insert(alias, qry_id);
                Ok(alias)
            }
        }
    }

    #[inline]
    pub fn find_qry_alias(&self, qry_id: &QueryID) -> Option<Ident<'a>> {
        self.q2a.get(qry_id).cloned()
    }
}

#[inline]
fn next_avail_tbl_alias_id<'a>(
    tbl_alias_id: &mut u32,
    buf: &mut String,
    a2q: &HashMap<Ident<'a>, QueryID>,
    arena: &'a StringArena,
) -> Result<(u32, Ident<'a>)> {
    use std::fmt::Write;
    loop {
        *tbl_alias_id += 1;
        buf.clear();
        write!(buf, "t{}", *tbl_alias_id).unwrap(); // won't fail
        let alias = Ident::regular(&**buf);
        if !a2q.contains_key(&alias) {
            break;
        }
    }
    // here alias id is always unique.
    let alias = arena.add(&buf)?;
    Ok((*tbl_alias_id, Ident::regular(alias)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lgc::tests::{build_plan, j_catalog};
    use doradb_sql::pretty::{PrettyConf, PrettyFormat};

    #[test]
    fn test_reflect_queries() {
        let cat = j_catalog();
        let mut conf = PrettyConf::default();
        conf.upper_kw = false;
        conf.newline = false;
        for (sql, expected) in vec![
            ("select 1.0", "select 1.0"),
            ("select 18446744073709551615", "select 18446744073709551615"),
            ("select 1.5e0", "select 1.5"),
            ("select date '2020-01-01'", "select date '2020-01-01'"),
            ("select interval '1' day", "select interval '1' day"),
            ("select c0 + c1, c0 - c1, c0 * c1, c0 / c1 from t1", "select t1.c0 + t1.c1, t1.c0 - t1.c1, t1.c0 * t1.c1, t1.c0 / t1.c1 from j.t1"),
            ("select c0 & c1, c0 | c1, c0 << c1, c0 >> c1 from t1", "select t1.c0 & t1.c1, t1.c0 | t1.c1, t1.c0 << t1.c1, t1.c0 >> t1.c1 from j.t1"),
            ("select -c0, ~c0, c0 ^ c1 from t1", "select -t1.c0, ~t1.c0, t1.c0 ^ t1.c1 from j.t1"),
            ("select c0 from t0", "select t0.c0 from j.t0"),
            ("select c0 from (select c0 from t0) as t1", "select t1.c0 from (select t0.c0 from j.t0) as t1"),
            ("select c0 from (select c0 from (select c0 from t0) as t1) as t2", "select t2.c0 from (select t1.c0 from (select t0.c0 from j.t0) as t1) as t2"),
            ("select c0 from t0 where c0 > 0", "select t0.c0 from j.t0 where t0.c0 > 0"),
            ("select c0 from t0 where c0 > 0 and c0 < 10", "select t0.c0 from j.t0 where t0.c0 > 0 and t0.c0 < 10"),
            ("select c0 from (select c0 from t0) as t1 where c0 > 0", "select t1.c0 from (select t0.c0 from j.t0) as t1 where t1.c0 > 0"),
            ("select count(*) from t0", "select count(1) from j.t0"),
            ("select max(c0) from (select c0 from t0) as t1", "select max(t1.c0) from (select t0.c0 from j.t0) as t1"),
            ("select min(c0) from (select c0 from t0 where c0 > 0) as t1", "select min(t1.c0) from (select t0.c0 from j.t0 where t0.c0 > 0) as t1"),
            ("select avg(c0) from (select c0 from t0) as t1 where c0 > 0", "select avg(t1.c0) from (select t0.c0 from j.t0) as t1 where t1.c0 > 0"),
            ("select 1 from t0, t1", "select 1 from j.t0, j.t1"),
            ("select count(distinct c1) from t0, t1", "select count(distinct t1.c1) from j.t0, j.t1"),
            ("select c1 from t0, t1 group by c1", "select t1.c1 from j.t0, j.t1 group by t1.c1"),
            ("select c1 from t0, t1 where t0.c0 > 0", "select t1.c1 from j.t0, j.t1 where t0.c0 > 0"),
            ("select t5.c0 from t1, t0, t0 as t5", "select t2.c0 from j.t1, j.t0, j.t0 as t2"),
            ("select c0, sum(1) from t0 group by c0 having c0 > 0", "select t0.c0, sum(1) from j.t0 group by t0.c0 having t0.c0 > 0"),
            ("select c0 from t0 where c0 > 0 having c0 < 10", "select t0.c0 from j.t0 where t0.c0 > 0 and t0.c0 < 10"),
            ("select c1 from t0, (select 1 as c1 from t1) as t1", "select t2.c1 from j.t0, (select 1 as c1 from j.t1) as t2"),
            ("select c0 from t0 where c0 < 0 or c0 > 10", "select t0.c0 from j.t0 where t0.c0 < 0 or t0.c0 > 10"),
            ("select c0 from t0 where c0 < 0 xor c0 > 10", "select t0.c0 from j.t0 where t0.c0 < 0 xor t0.c0 > 10"),
            ("select c0 from t4 where c0 = 0 and c1 > 0 and c2 >= 0 and c3 < 0 and c4 <= 0", "select t4.c0 from j.t4 where t4.c0 = 0 and t4.c1 > 0 and t4.c2 >= 0 and t4.c3 < 0 and t4.c4 <= 0"),
            ("select c0 from t4 where c0 <> 0 and c1 <=> 0 and c2 is null and c3 is not null and c4 is true", "select t4.c0 from j.t4 where t4.c0 <> 0 and t4.c1 <=> 0 and t4.c2 is null and t4.c3 is not null and t4.c4 is true"),
            ("select c0 from t4 where c0 is not true and c1 is false and c2 is not false and c3 like '0' and c4 not like '0'", "select t4.c0 from j.t4 where t4.c0 is not true and t4.c1 is false and t4.c2 is not false and t4.c3 like '0' and t4.c4 not like '0'"),
            ("select c0 from t4 where c0 regexp '0' and c1 not regexp '0' and c2 in (1, 2) and c3 not in (1, 2) and not c4", "select t4.c0 from j.t4 where t4.c0 regexp '0' and t4.c1 not regexp '0' and t4.c2 in (1, 2) and t4.c3 not in (1, 2) and not t4.c4"),
            ("select c0 from t4 where c0 between 1 and 2 and c1 not between 1 and 2", "select t4.c0 from j.t4 where t4.c0 between 1 and 2 and t4.c1 not between 1 and 2"),
            ("select c1 from t0 join t1", "select t1.c1 from j.t0 join j.t1"),
            ("select c1 from t0 inner join t1", "select t1.c1 from j.t0 join j.t1"),
            ("select c1 from t0 left join t1", "select t1.c1 from j.t0 left join j.t1"),
            ("select c1 from t0 full join t1", "select t1.c1 from j.t0 full join j.t1"),
            ("select c2 from t0 inner join t1 left join t2", "select t2.c2 from j.t0 join j.t1 left join j.t2"),
            ("select c2 from t0 join t1, t2", "select t2.c2 from j.t0 join j.t1 cross join j.t2"),
            ("select c2 from t0 join t1, t2 where c2 > 0", "select t2.c2 from j.t0 join j.t1 cross join j.t2 where t2.c2 > 0"),
            ("select sum(1) from t0 join t1, t2", "select sum(1) from j.t0 join j.t1 cross join j.t2"),
            ("select c1 from t0 join t1 where c1 > 0", "select t1.c1 from j.t0 join j.t1 where t1.c1 > 0"),
            ("select sum(1) from t0 join t1", "select sum(1) from j.t0 join j.t1"),
            ("select c4 from t0 join t1, t2, t3, t4", "select t4.c4 from j.t0 join j.t1 cross join j.t2 cross join j.t3 cross join j.t4"),
            ("select c3 from t0 cross join t1, t2, t3", "select t3.c3 from j.t0 cross join j.t1 cross join j.t2 cross join j.t3"),
            ("select c0 from t0 order by c0", "select t0.c0 from j.t0 order by t0.c0"),
            ("select c0, sum(1) from t0 group by c0 order by sum(1) desc", "select t0.c0, sum(1) from j.t0 group by t0.c0 order by sum(1) desc"),
            ("select c1 from t0, t1 order by c1", "select t1.c1 from j.t0, j.t1 order by t1.c1"),
            ("select c1 from t0 left join t1 order by c1", "select t1.c1 from j.t0 left join j.t1 order by t1.c1"),
            ("select c0 from t0 limit 1", "select t0.c0 from j.t0 limit 1"),
            ("select c0 from t0 limit 1 offset 10", "select t0.c0 from j.t0 limit 1 offset 10"),
            ("select c0 from t0 where c0 > 0 limit 1", "select t0.c0 from j.t0 where t0.c0 > 0 limit 1"),
            ("select sum(1) from t0 limit 1", "select sum(1) from j.t0 limit 1"),
            ("select c0 from t0 where c0 = (select max(c1) from t1)", "select t0.c0 from j.t0 where t0.c0 = (select max(t1.c1) from j.t1)"),
            ("select c0 from t0 where c0 in (1, 2, 3)", "select t0.c0 from j.t0 where t0.c0 in (1, 2, 3)"),
            ("select c0 from t0 where c0 not in (1, 2, 3)", "select t0.c0 from j.t0 where t0.c0 not in (1, 2, 3)"),
            ("select c0 from t0 where not c0 in (1, 2, 3)", "select t0.c0 from j.t0 where t0.c0 not in (1, 2, 3)"),
            ("select c0 from t0 where not c0 not in (1, 2, 3)", "select t0.c0 from j.t0 where t0.c0 in (1, 2, 3)"),
            ("select c0 from t0 where c0 in (select c1 from t1)", "select t0.c0 from j.t0 where t0.c0 in (select t1.c1 from j.t1)"),
            ("select c0 from t0 where not c0 in (select c1 from t1)", "select t0.c0 from j.t0 where t0.c0 not in (select t1.c1 from j.t1)"),
            ("select c0 from t0 where c0 not in (select 1 from t1)", "select t0.c0 from j.t0 where t0.c0 not in (select 1 from j.t1)"),
            ("select c0 from t0 where not c0 not in (select 1 from t1)", "select t0.c0 from j.t0 where t0.c0 in (select 1 from j.t1)"),
            ("select c0 from t0 where exists (select 1 from t1 where t0.c0 = t1.c1)", "select t0.c0 from j.t0 where exists (select 1 from j.t1 where t0.c0 = t1.c1)"),
            ("select c0 from t0 where not exists (select 1 from t1 where c1 = t0.c0)", "select t0.c0 from j.t0 where not exists (select 1 from j.t1 where t1.c1 = t0.c0)"),
            ("select case when c0 = 1 then 1 end from t0", "select case when t0.c0 = 1 then 1 end from j.t0"),
            // CTE not support
            // ("with a as (select c0 from t0) select c0 from a", "with a as (select t0.c0 from t0) select a.c0 from a"),
        ] {
            let plan = build_plan(&cat, "j", sql);
            let arena = StringArena::with_capacity(1024);
            let stmt = reflect(&plan, &arena, &cat).unwrap();
            let mut output = String::new();
            stmt.pretty_fmt(&mut output, &conf, 0).unwrap();
            println!("output={}", output);
            assert_eq!(output, expected);
        }
    }
}
