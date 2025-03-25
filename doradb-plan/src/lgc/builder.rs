use crate::error::{Error, Result, ToResult};
use crate::join::{JoinKind, JoinOp};
use crate::lgc::alias::QueryAliases;
use crate::lgc::col::{AliasKind, ColGen, ProjCol};
use crate::lgc::op::{Op, OpKind, OpMutVisitor, SortItem};
use crate::lgc::query::{Location, QuerySet, Subquery};
use crate::lgc::resolv::{ExprResolve, PlaceholderCollector, PlaceholderQuery, Resolution};
use crate::lgc::scope::{Scope, Scopes};
use crate::lgc::setop::{SetopKind, SubqOp};
use crate::lgc::LgcPlan;
use crate::rule::expr_simplify::{simplify_nested, NullCoalesce};
use doradb_catalog::{Catalog, ColIndex, SchemaID, TableID};
use doradb_expr::controlflow::ControlFlow;
use doradb_expr::{
    self as expr, ExprExt, ExprKind, ExprMutVisitor, Plhd, PredFuncKind, QueryID, Setq, SubqKind,
};
use doradb_sql::ast::*;
use semistr::SemiStr;

pub struct LgcBuilder<'a, C: Catalog> {
    catalog: &'a C,
    default_schema_id: SchemaID,
    default_schema: SemiStr,
    qs: QuerySet,
    scopes: Scopes,
    attaches: Vec<QueryID>,
}

impl<'c, C: Catalog> LgcBuilder<'c, C> {
    #[inline]
    pub(super) fn new(catalog: &'c C, default_schema: &str) -> Result<Self> {
        let qs = QuerySet::default();
        let default_schema_id = if let Some(s) = catalog.find_schema_by_name(default_schema) {
            s.id
        } else {
            return Err(Error::SchemaNotExists(default_schema.to_string()));
        };
        Ok(LgcBuilder {
            catalog,
            default_schema_id,
            default_schema: SemiStr::new(default_schema),
            qs,
            scopes: Scopes::default(),
            attaches: vec![],
        })
    }

    #[inline]
    pub(super) fn build(mut self, QueryExpr { with, query }: &QueryExpr<'_>) -> Result<LgcPlan> {
        let mut colgen = ColGen::default();
        let (root, _) = self.build_subquery(with, query, false, &mut colgen)?;
        Ok(LgcPlan {
            qry_set: self.qs,
            root,
            attaches: self.attaches,
            max_cid: colgen.curr_gid(),
        })
    }

    #[inline]
    fn build_subquery<'a>(
        &mut self,
        with: &'a Option<With<'a>>,
        query: &'a Query<'a>,
        transitive: bool,
        colgen: &mut ColGen,
    ) -> Result<(QueryID, bool)> {
        // create new scope to collect aliases.
        self.scopes.push(Scope::new(transitive));
        if let Some(with) = with {
            // with clause is within current scope
            self.setup_with(with, transitive, colgen)?
        }
        let mut phc = PlaceholderCollector::new(transitive);
        let (mut root, location) = self.setup_query(query, &mut phc, colgen)?;
        // first setup subqueries
        for PlaceholderQuery { uid, kind, qry, .. } in phc.subqueries {
            let QueryExpr { with, query } = qry;
            let (qry_id, correlated) = self.build_subquery(with, query, true, colgen)?;
            let _ = root.walk_mut(&mut ReplaceSubq {
                uid,
                kind,
                qry_id,
                correlated,
            });
            if kind == SubqKind::Scalar && !correlated {
                // this means the subquery can be executed separately, so attach it
                // to main plan.
                self.attaches.push(qry_id);
            }
        }
        // then resolve correlated columns against outer scopes.
        let mut scope = self.scopes.pop().unwrap(); // won't fail
        let cor_cols = self.setup_correlated_cols(&mut root, phc.idents, colgen)?;
        // We can identify whether current subquery is correlated by check the size of cor_cols
        let correlated = !cor_cols.is_empty();
        scope.cor_cols = cor_cols;
        let (query_id, subquery) = self.qs.insert_empty();
        subquery.root = root;
        subquery.scope = scope;
        subquery.location = location;
        Ok((query_id, correlated))
    }

    /// Setup correlated columns.
    /// This method is only used in correlated subquery context, in which
    /// the column reference can not be mapped to any columns in the context.
    /// Then traverse outer context and match through all query sources.
    /// Returns true if correlated column idendified.
    #[inline]
    fn setup_correlated_cols(
        &mut self,
        root: &mut Op,
        idents: Vec<(u32, Vec<SemiStr>, &'static str)>,
        colgen: &mut ColGen,
    ) -> Result<Vec<ExprKind>> {
        if idents.is_empty() {
            return Ok(vec![]);
        }
        let mut ccols = Vec::with_capacity(idents.len());
        for (uid, ident, location) in &idents {
            let e = match &ident[..] {
                [schema_name, tbl_alias, col_alias] => self.find_correlated_schema_tbl_col(
                    schema_name,
                    tbl_alias,
                    col_alias,
                    location,
                    colgen,
                )?,
                [tbl_alias, col_alias] => {
                    self.find_correlated_tbl_col(tbl_alias, col_alias, location, colgen)?
                }
                [col_alias] => self.find_correlated_col(col_alias, location, colgen)?,
                _ => return Err(Error::unknown_column_idents(ident, location)),
            };
            ccols.push(e.clone());
            // Replace placeholder with found correlated column
            let _ = root.walk_mut(&mut ReplaceCorrelatedCol(*uid, e));
        }
        Ok(ccols)
    }

    #[inline]
    fn find_correlated_schema_tbl_col(
        &mut self,
        schema_name: &str,
        tbl_alias: &str,
        col_alias: &str,
        location: &'static str,
        colgen: &mut ColGen,
    ) -> Result<ExprKind> {
        let schema = self.catalog.find_schema_by_name(schema_name).must_ok()?;
        for s in self.scopes.iter_mut().rev() {
            for (from_alias, query_id) in s.query_aliases.iter() {
                if from_alias == tbl_alias {
                    // found table by alias
                    let subquery = self.qs.get(query_id).must_ok()?;
                    let (schema_id, _) = subquery.find_table().ok_or_else(|| {
                        Error::unknown_column_full_name(schema_name, tbl_alias, col_alias, location)
                    })?;
                    if schema_id != schema.id {
                        return Err(Error::unknown_column_full_name(
                            schema_name,
                            tbl_alias,
                            col_alias,
                            location,
                        ));
                    }
                    let idx = subquery.position_out_col(col_alias).ok_or_else(|| {
                        Error::unknown_column_full_name(schema_name, tbl_alias, col_alias, location)
                    })?;
                    // mark cor_vars in the matched scope, so column pruning will also take them
                    // into consideration.
                    let idx = ColIndex::from(idx as u32);
                    let ccol = colgen.gen_qry_col(*query_id, idx);
                    s.cor_vars.insert((*query_id, idx));
                    return Ok(ccol);
                }
            }
            if !s.transitive {
                break; // stop at the first non-correlated scope
            }
        }
        Err(Error::unknown_column_full_name(
            schema_name,
            tbl_alias,
            col_alias,
            location,
        ))
    }

    #[inline]
    fn find_correlated_tbl_col(
        &mut self,
        tbl_alias: &str,
        col_alias: &str,
        location: &'static str,
        colgen: &mut ColGen,
    ) -> Result<ExprKind> {
        for s in self.scopes.iter_mut().rev() {
            for (from_alias, query_id) in s.query_aliases.iter() {
                if from_alias == tbl_alias {
                    // found table by alias
                    let subquery = self.qs.get(query_id).must_ok()?;
                    let idx = subquery.position_out_col(col_alias).ok_or_else(|| {
                        Error::unknown_column_partial_name(tbl_alias, col_alias, location)
                    })?;
                    // mark cor_vars in the matched scope, so column pruning will also take them
                    // into consideration.
                    let idx = ColIndex::from(idx as u32);
                    let ccol = colgen.gen_qry_col(*query_id, idx);
                    s.cor_vars.insert((*query_id, idx));
                    return Ok(ccol);
                }
            }
            if !s.transitive {
                break; // stop at the first non-correlated scope
            }
        }
        Err(Error::unknown_column_partial_name(
            tbl_alias, col_alias, location,
        ))
    }

    #[inline]
    fn find_correlated_col(
        &mut self,
        col_alias: &str,
        location: &'static str,
        colgen: &mut ColGen,
    ) -> Result<ExprKind> {
        for s in self.scopes.iter_mut().rev() {
            let mut matched = None;
            for (_, query_id) in s.query_aliases.iter() {
                let subquery = self.qs.get(query_id).must_ok()?;
                if let Some(idx) = subquery.position_out_col(col_alias) {
                    if matched.is_some() {
                        return Err(Error::DuplicatedColumnAlias(col_alias.to_string()));
                    }
                    let idx = ColIndex::from(idx as u32);
                    let ccol = colgen.gen_qry_col(*query_id, idx);
                    let cvar = (*query_id, idx);
                    matched = Some((ccol, cvar))
                }
            }
            if let Some((c, v)) = matched {
                // mark cor_vars in the matched scope, so column pruning will also take them
                // into consideration.
                s.cor_vars.insert(v);
                return Ok(c);
            }
            if !s.transitive {
                break; // stop at the first non-correlated scope
            }
        }
        Err(Error::unknown_column_name(col_alias, location))
    }

    /// Setup with clause.
    ///
    /// Recursive CTE is not supported.
    /// All CTEs are stored as normal queries.
    ///
    /// if correlated is set to true, column can refer to sources in outer scopes.
    #[inline]
    fn setup_with(
        &mut self,
        with: &With<'_>,
        allow_unknown_ident: bool,
        colgen: &mut ColGen,
    ) -> Result<()> {
        if with.recursive {
            return Err(Error::UnsupportedSqlSyntax("Recursive CTE".to_string()));
        }
        for elem in &with.elements {
            let alias = elem.name.to_lower();
            // check first no duplicated table alias within current scope.
            if self.scopes.curr_scope().cte_aliases.contains_key(&alias) {
                return Err(Error::DuplicatedTableAlias(alias));
            }
            // For each CTE declaration, we inherit the placeholder strategy and process separately.
            // todo: handle correlated CTE
            let (query_id, _) = {
                let QueryExpr { with, query } = &elem.query_expr;
                self.build_subquery(with, query, allow_unknown_ident, colgen)?
            };
            if !elem.cols.is_empty() {
                // as output columns aliases are explicit specified,
                // we need to update the output alias list of generated tree.
                self.qs.transform_subq(query_id, |subq| {
                    let out_cols = subq.out_cols_mut().ok_or(Error::MustOK)?;
                    if elem.cols.len() != out_cols.len() {
                        return Err(Error::ColumnCountMismatch);
                    }
                    for (c, new) in out_cols.iter_mut().zip(elem.cols.iter()) {
                        // update with new aliases defined by WITH statement.
                        c.alias_kind = AliasKind::Explicit;
                        c.alias = new.to_lower();
                    }
                    Ok(())
                })??
            }
            // add to CTE aliases of current scope, with duplication check
            self.scopes
                .curr_scope_mut()
                .cte_aliases
                .insert_query(alias, query_id)?;
        }
        Ok(())
    }

    #[inline]
    fn setup_query<'a>(
        &mut self,
        query: &'a Query<'a>,
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<(Op, Location)> {
        match query {
            Query::Row(row) => {
                let mut cols = Vec::with_capacity(row.len());
                let resolver = ResolveNone(self.catalog);
                for c in row {
                    match c {
                        DerivedCol::Asterisk(_) => {
                            return Err(Error::InvalidSingleRowData(
                                "Asterisk not allowed".to_string(),
                            ))
                        }
                        DerivedCol::Expr(expr, alias) => {
                            let (e, _) =
                                resolver.resolve_expr(expr, "field list", phc, false, colgen)?;
                            let alias_kind = if alias.kind == IdentKind::AutoAlias {
                                AliasKind::Implicit
                            } else {
                                AliasKind::Explicit
                            };
                            let alias = alias.to_lower();
                            cols.push(ProjCol::new(e, alias, alias_kind));
                            // alias conflict is detected when referring, here no need to check duplication
                        }
                    }
                }
                Ok((self.gen_op(OpKind::Row(Some(cols))), Location::Virtual))
            }
            Query::Table(select_table) => self.setup_select_table(select_table, phc, colgen),
            Query::Set(select_set) => self.setup_select_set(select_set, phc, colgen),
        }
    }

    /// Build operator tree from a comment SELECT statement, includes following steps:
    /// 1. translate FROM clause
    /// 2. translate SELECT clause
    /// 3. translate WHERE clause
    /// 4. translate GROUP BY clause
    /// 5. translate HAVING clause
    /// 6. translate ORDER BY clause
    /// 7. translate LIMIT clause
    /// The final operator tree will conditinally reorder and merge them.
    #[inline]
    fn setup_select_table<'a>(
        &mut self,
        select_table: &'a SelectTable<'a>,
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<(Op, Location)> {
        // 1. translate FROM clause
        let from = self.setup_table_refs(&select_table.from, phc, colgen)?;
        // After analyzing from clause, we have from aliases populated, then we need to make a copy
        // and pass it to other clauses, due to the limitation of borrowing on structual data.
        // We have to make sure the copy in sync with the source.
        let query_aliases = &self.scopes.curr_scope().query_aliases;
        // 2. translate SELECT clause
        let (proj_cols, distinct) = {
            let resolver = ResolveProjOrFilt {
                catalog: self.catalog,
                qs: &self.qs,
                query_aliases,
            };
            let proj_cols =
                self.setup_proj_cols(&resolver, &select_table.cols, "field list", phc, colgen)?;
            (proj_cols, select_table.q == SetQuantifier::Distinct)
        };
        // 3. translate WHERE clause
        let filter = if let Some(filter) = &select_table.filter {
            let resolver = ResolveProjOrFilt {
                catalog: self.catalog,
                qs: &self.qs,
                query_aliases,
            };
            let (pred, _) = resolver.resolve_expr(filter, "where clause", phc, false, colgen)?;
            Some(pred)
        } else {
            None
        };
        // 4. translate GROUP BY clause
        let (mut groups, scalar_aggr) = if !select_table.group_by.is_empty() {
            let resolver = ResolveGroup {
                catalog: self.catalog,
                qs: &self.qs,
                query_aliases,
                proj_cols: &proj_cols,
            };
            let gs = self.setup_group(&resolver, &select_table.group_by, phc, colgen)?;
            let scalar_aggr = validate_proj_aggr(&proj_cols, &gs)?;
            (gs, scalar_aggr)
        } else {
            (vec![], validate_proj_aggr(&proj_cols, &[])?)
        };
        if groups.is_empty() && !scalar_aggr && distinct {
            // convert DISTINCT to GROUP BY
            groups.extend(proj_cols.iter().map(|c| c.expr.clone()));
        }
        // 5. translate HAVING clause
        let having = if let Some(having) = &select_table.having {
            let group_cols = if groups.is_empty() {
                None
            } else {
                Some(groups.as_slice())
            };
            let resolver = ResolveHavingOrOrder {
                catalog: self.catalog,
                qs: &self.qs,
                query_aliases,
                proj_cols: &proj_cols,
                group_cols,
            };
            let (having, _) = resolver.resolve_expr(having, "having clause", phc, false, colgen)?;
            validate_having(&proj_cols, &groups, scalar_aggr, &having)?;
            Some(having)
        } else {
            None
        };
        // 6. translate ORDER BY clause
        let order = if !select_table.order_by.is_empty() {
            let resolver = ResolveHavingOrOrder {
                catalog: self.catalog,
                qs: &self.qs,
                query_aliases,
                proj_cols: &proj_cols,
                group_cols: None,
            };
            let order = self.setup_order(&resolver, &select_table.order_by, phc, colgen)?;
            validate_order(&groups, scalar_aggr, &order)?;
            order
        } else {
            vec![]
        };
        // 7. translate LIMIT clause
        let limit = if let Some(limit) = &select_table.limit {
            // pushdown to sort if possible
            let start = limit.offset.unwrap_or(0);
            let end = start + limit.limit;
            Some((start, end))
        } else {
            None
        };
        // build the whole operator tree
        // a) build root operator
        let mut root = if from.len() == 1 {
            from.into_iter().next().unwrap().into()
        } else {
            self.gen_op(OpKind::cross_join(from))
        };
        // b) build Filt operator
        if let Some(pred) = filter {
            root = self.gen_op(OpKind::filt(pred.into_conj(), root));
        }
        // c) build Aggr operator
        if !groups.is_empty() || scalar_aggr {
            root = self.gen_op(OpKind::aggr(groups, root));
        }
        // d) build Proj operator or merge into Aggr
        match &mut root.kind {
            OpKind::Aggr(aggr) => {
                aggr.proj = proj_cols;
            }
            _ => {
                root = self.gen_op(OpKind::proj(proj_cols, root));
            }
        }
        // e) build Filter operator from HAVING clause
        if let Some(pred) = having {
            root = self.gen_op(OpKind::filt(pred.into_conj(), root));
        }
        // f) build Sort operator
        if !order.is_empty() {
            // scalar aggr query only returns 1 row, ingore SORT operation
            root = self.gen_op(OpKind::sort(order, root));
        }
        // g) build Limit operator
        if let Some((start, end)) = limit {
            root = self.gen_op(OpKind::limit(start, end, root));
        }
        Ok((root, Location::Intermediate))
    }

    #[inline]
    fn setup_select_set<'a>(
        &mut self,
        select_set: &'a SelectSet<'a>,
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<(Op, Location)> {
        let kind = match select_set.op {
            SetOp::Union => SetopKind::Union,
            SetOp::Except => SetopKind::Except,
            SetOp::Intersect => SetopKind::Intersect,
        };
        let q = if select_set.distinct {
            Setq::Distinct
        } else {
            Setq::All
        };
        // todo: handle correlated subquery
        let (qry_id, _) =
            self.build_subquery(&None, &select_set.left, phc.allow_unknown_ident, colgen)?;
        let left = SubqOp::query(qry_id);
        let left_out: Vec<_> = self
            .qs
            .get(&qry_id)
            .unwrap()
            .out_cols()
            .iter()
            .enumerate()
            .map(|(i, pc)| {
                (
                    colgen.gen_qry_col(qry_id, ColIndex::from(i as u32)),
                    pc.alias.clone(),
                )
            })
            .collect();
        let (qry_id, _) =
            self.build_subquery(&None, &select_set.right, phc.allow_unknown_ident, colgen)?;
        let right = SubqOp::query(qry_id);
        // let right_out = self.qs.get(&qry_id).unwrap().out_cols();
        let right_out: Vec<_> = self
            .qs
            .get(&qry_id)
            .unwrap()
            .out_cols()
            .iter()
            .enumerate()
            .map(|(i, _)| colgen.gen_qry_col(qry_id, ColIndex::from(i as u32)))
            .collect();
        if left_out.len() != right_out.len() {
            return Err(Error::ColumnCountMismatch);
        }
        let cols: Vec<_> = left_out
            .into_iter()
            .zip(right_out)
            .enumerate()
            .map(|(i, ((l, alias), r))| {
                let e = colgen.gen_setop_col(ColIndex::from(i as u32), l, r);
                ProjCol::implicit_alias(e, alias)
            })
            .collect();
        Ok((
            self.gen_op(OpKind::setop(kind, q, left, right, cols)),
            Location::Intermediate,
        ))
    }

    /// process ORDER BY clause and generate sort items.
    /// ORDER BY is different from WHERE or GROUP BY,
    /// as it can refer to aliases in SELECT clause.
    /// e.g. "SELECT c0 as c1, c1 as c0 FROM t1 ORDER BY c0"
    /// The order item is actually the column `c1` in table `t1`,
    /// because the projection assigns the alias `c0` to it and
    /// ORDER BY pick it as higher priority to column names in
    /// original table.
    #[inline]
    fn setup_order<'a>(
        &self,
        resolver: &dyn ExprResolve<C>,
        elems: &'a [OrderElement<'a>],
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<Vec<SortItem>> {
        let mut res = Vec::with_capacity(elems.len());
        for elem in elems {
            let (expr, _) = resolver.resolve_expr(&elem.expr, "order by", phc, false, colgen)?;
            let item = SortItem {
                expr,
                desc: elem.desc,
            };
            res.push(item)
        }
        Ok(res)
    }

    /// Setup columns in projection.
    /// a) Expand asterisk.
    /// b) Collect aggr functions.
    /// c) Collect expression list with aliases.
    ///
    /// To expand asterisk, we need to consider of different scenarios:
    ///
    /// 1. Single source.
    /// a). Table: "SELECT t.* FROM t"
    /// Derive projection by looking up from catalog
    ///
    /// b). Subquery: "SELECT * FROM (SELECT c0 FROM t) t2"
    /// Derive projection from output list.
    ///
    /// 2. query from multiple sources, including explicit joins.
    /// e.g.
    /// "SELECT * FROM t0, t1"
    /// "SELECT t0.* FROM t0 JOIN (SELECT * FROM t1) t2"
    /// "SELECT * FROM t0, t0 as t1 WHERE t0.c0 = t1.c1"
    /// In this case, we need to
    /// a) Convert table to subquery, then derive projection from it.
    /// b) For each subquery, derive its output.
    /// NOTE: The order of output columns is important, because later we will
    ///       reorder the sources(join reorder).
    #[inline]
    fn setup_proj_cols<'a>(
        &self,
        resolver: &dyn ExprResolve<C>,
        cols: &'a [DerivedCol<'a>],
        location: &'static str,
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<Vec<ProjCol>> {
        let mut proj_cols = vec![];
        let mut wildcard = false; // multiple unqualified asterisk are not allowed in projection.
        for dc in cols {
            match dc {
                DerivedCol::Asterisk(q) => {
                    if q.is_empty() {
                        // wildcard without qualifier can only appear once.
                        if wildcard {
                            return Err(Error::DulicatedAsterisksInFieldList);
                        } else {
                            wildcard = true;
                        }
                    }
                    let es = resolver.resolve_asterisk(q, location, colgen)?;
                    proj_cols.extend(es);
                }
                DerivedCol::Expr(e, alias) => {
                    let (e, _) = resolver.resolve_expr(e, location, phc, false, colgen)?;
                    let alias_kind = if alias.kind == IdentKind::AutoAlias {
                        AliasKind::Implicit
                    } else {
                        AliasKind::Explicit
                    };
                    let alias = alias.to_lower();
                    proj_cols.push(ProjCol::new(e, alias, alias_kind));
                }
            }
        }
        Ok(proj_cols)
    }

    /// Process table references in FROM clauase.
    /// All table refs are initialized as sub-trees in cross join, and will be
    /// optimized to qualified joins in canonical rewrite phase.
    /// As MySQL does not support correlated derived table, we won't handle it.
    ///
    /// Example of correlated derived table:
    ///
    /// ```sql
    /// SELECT t0.c0, t1.c1
    /// FROM
    ///     t0,
    ///     (SELECT c1 FROM t1 WHERE t0.c0 = t1.c0) derived
    /// ```
    #[inline]
    fn setup_table_refs<'a>(
        &mut self,
        table_refs: &'a [TableRef<'a>],
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<Vec<JoinOp>> {
        let mut jos = vec![];
        for tr in table_refs {
            let (jo, _) = self.setup_table_ref(tr, phc, colgen)?;
            jos.push(jo)
        }
        Ok(jos)
    }

    /// Setups each table reference and returns an operator.
    /// The operator returned should be either plan or join.
    fn setup_table_ref<'a>(
        &mut self,
        table_ref: &'a TableRef<'a>,
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<(JoinOp, Vec<SemiStr>)> {
        let (plan, aliases) = match table_ref {
            TableRef::Primitive(tp) => self.setup_table_primitive(tp, phc, colgen)?,
            TableRef::Joined(tj) => match tj.as_ref() {
                TableJoin::Cross(cj) => {
                    let (left, mut aliases) = self.setup_table_ref(&cj.left, phc, colgen)?;
                    let (right, right_aliases) =
                        self.setup_table_primitive(&cj.right, phc, colgen)?;
                    aliases.extend(right_aliases);
                    (JoinOp::cross(vec![left, right]), aliases)
                }
                TableJoin::Natural(_) => {
                    // Currently natural join not supported.
                    // In order to support natural join,
                    // we need to do following:
                    // 1. validate join condition.
                    // a) reject if join key is not unique.
                    // b) convert to cross join if no identical column
                    //    found in both side.
                    // c) otherwise, convert to inner join.
                    // 2. expand non-qualified asterisk for natural join
                    //    because natural join eliminate identical column
                    //    and change the output sequence of original columns.
                    return Err(Error::InternalError(
                        "natural join not supported".to_string(),
                    ));
                }
                TableJoin::Qualified(qj) => {
                    let (left, mut left_aliases) = self.setup_table_ref(&qj.left, phc, colgen)?;
                    let (right, right_aliases) =
                        self.setup_table_primitive(&qj.right, phc, colgen)?;
                    let (cond, aliases) = if let Some(cond) = &qj.cond {
                        match cond {
                            JoinCondition::NamedCols(ncs) => {
                                let mut left_queries = Vec::with_capacity(left_aliases.len());
                                for qry_alias in &left_aliases {
                                    let query_id = self
                                        .scopes
                                        .curr_scope()
                                        .query_aliases
                                        .get(qry_alias)
                                        .must_ok()?;
                                    let subq = self.qs.get(query_id).must_ok()?;
                                    left_queries.push((*query_id, subq));
                                }
                                let mut right_queries = Vec::with_capacity(right_aliases.len());
                                for qry_alias in &right_aliases {
                                    let query_id = self
                                        .scopes
                                        .curr_scope()
                                        .query_aliases
                                        .get(qry_alias)
                                        .must_ok()?;
                                    let subq = self.qs.get(query_id).must_ok()?;
                                    right_queries.push((*query_id, subq));
                                }
                                let mut preds = Vec::with_capacity(ncs.len());
                                let mut left_col = None;
                                let mut right_col = None;
                                for nc in ncs {
                                    let col_alias = nc.to_lower();
                                    // find named column from left side
                                    for (query_id, subq) in &left_queries {
                                        if let Some(idx) = subq.position_out_col(&col_alias) {
                                            if left_col.is_some() {
                                                return Err(Error::DuplicatedColumnAlias(
                                                    col_alias.to_string(),
                                                ));
                                            }
                                            let idx = ColIndex::from(idx as u32);
                                            let col = colgen.gen_qry_col(*query_id, idx);
                                            left_col = Some(col);
                                        }
                                    }
                                    if left_col.is_none() {
                                        return Err(Error::unknown_column_name(
                                            &col_alias,
                                            "join condition",
                                        ));
                                    }
                                    // find named column from right side
                                    for (query_id, subq) in &right_queries {
                                        if let Some(idx) = subq.position_out_col(&col_alias) {
                                            if right_col.is_some() {
                                                return Err(Error::DuplicatedColumnAlias(
                                                    col_alias.to_string(),
                                                ));
                                            }
                                            let idx = ColIndex::from(idx as u32);
                                            let col = colgen.gen_qry_col(*query_id, idx);
                                            right_col = Some(col);
                                        }
                                    }
                                    if right_col.is_none() {
                                        return Err(Error::unknown_column_name(
                                            &col_alias,
                                            "join condition",
                                        ));
                                    }
                                    preds.push(ExprKind::pred_func(
                                        PredFuncKind::Equal,
                                        vec![left_col.take().unwrap(), right_col.take().unwrap()],
                                    ));
                                }
                                left_aliases.extend(right_aliases);
                                (preds, left_aliases)
                            }
                            JoinCondition::Conds(e) => {
                                left_aliases.extend(right_aliases);
                                let from_aliases = self
                                    .scopes
                                    .curr_scope()
                                    .restrict_from_aliases(&left_aliases);
                                let resolver = ResolveProjOrFilt {
                                    catalog: self.catalog,
                                    qs: &self.qs,
                                    query_aliases: &from_aliases,
                                };
                                let (pred, _) = resolver.resolve_expr(
                                    e,
                                    "join condition",
                                    phc,
                                    false,
                                    colgen,
                                )?;
                                (pred.into_conj(), left_aliases)
                            }
                        }
                    } else {
                        left_aliases.extend(right_aliases);
                        (vec![], left_aliases)
                    };
                    // join type in JOIN clause only support INNER, LEFT, RIGHT, FULL.
                    let op = match qj.ty {
                        JoinType::Inner => {
                            JoinOp::qualified(JoinKind::Inner, left, right, cond, vec![])
                        }
                        JoinType::Left => {
                            JoinOp::qualified(JoinKind::Left, left, right, cond, vec![])
                        }
                        // It's safe to convert right join to left join because the query aliases stores tables in original sequence,
                        // and the projection list is generated using that sequence.
                        JoinType::Right => {
                            JoinOp::qualified(JoinKind::Left, right, left, cond, vec![])
                        }
                        JoinType::Full => {
                            JoinOp::qualified(JoinKind::Full, left, right, cond, vec![])
                        }
                    };
                    (op, aliases)
                }
            },
        };
        Ok((plan, aliases))
    }

    /// Currently lateral derived table is not supported, so PlaceholderCollector is not used.
    #[inline]
    fn setup_table_primitive<'a>(
        &mut self,
        tp: &TablePrimitive<'a>,
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<(JoinOp, Vec<SemiStr>)> {
        let (plan, alias) = match tp {
            TablePrimitive::Named(tn, alias) => {
                let tbl_name = tn.table.to_lower();
                let tbl_alias = alias
                    .map(|a| a.to_lower())
                    .unwrap_or_else(|| tbl_name.clone());
                let query_id = if let Some(schema_name) = &tn.schema {
                    // schema is present, must be a concrete table
                    let schema_name = schema_name.to_lower();
                    let schema_id = if let Some(s) = self.catalog.find_schema_by_name(&schema_name)
                    {
                        s.id
                    } else {
                        return Err(Error::SchemaNotExists(schema_name.to_string()));
                    };
                    let table_id =
                        if let Some(t) = self.catalog.find_table_by_name(schema_id, &tbl_name) {
                            t.id
                        } else {
                            return Err(Error::TableNotExists(format!(
                                "{}.{}",
                                schema_name, tbl_name
                            )));
                        };
                    // now manually construct a subquery to expose all columns in the table
                    self.table_to_subquery(schema_id, schema_name, table_id, tbl_name, colgen)?
                } else if let Some(query_id) = self.find_cte(&tbl_name) {
                    // schema not present, first check CTEs
                    let query_id = *query_id;
                    // CTE matches:
                    // currently make a new copy.
                    // todo: maybe a share(copy on write) is better for future optimization.
                    self.qs.copy_query(&query_id)?
                } else if let Some(t) = self
                    .catalog
                    .find_table_by_name(self.default_schema_id, &tbl_name)
                {
                    self.table_to_subquery(
                        t.schema_id,
                        self.default_schema.clone(),
                        t.id,
                        t.name,
                        colgen,
                    )?
                } else {
                    return Err(Error::TableNotExists(tbl_name.to_string()));
                };
                // update from aliases
                self.scopes
                    .curr_scope_mut()
                    .query_aliases
                    .insert_query(tbl_alias.clone(), query_id)?;
                (JoinOp::query(query_id), tbl_alias)
            }
            TablePrimitive::Derived(subq, alias) => {
                // Non-lateral derived table can not refer to outer values.
                // But a derived table within subquery could refer to outer values.
                // We need to inherit allow_unknown_ident flag in phc
                let QueryExpr { with, query } = subq.as_ref();
                // todo: handle correlated derived table in subquery
                let (query_id, _) =
                    self.build_subquery(with, query, phc.allow_unknown_ident, colgen)?;
                let alias = alias.to_lower();
                self.scopes
                    .curr_scope_mut()
                    .query_aliases
                    .insert_query(alias.clone(), query_id)?;
                (JoinOp::query(query_id), alias)
            }
        };
        Ok((plan, vec![alias]))
    }

    #[inline]
    fn setup_group<'a>(
        &self,
        resolver: &dyn ExprResolve<C>,
        group_by: &'a [Expr<'a>],
        phc: &mut PlaceholderCollector<'a>,
        colgen: &mut ColGen,
    ) -> Result<Vec<ExprKind>> {
        let mut items = Vec::with_capacity(group_by.len());
        for g in group_by {
            let (e, _) = resolver.resolve_expr(g, "group by", phc, false, colgen)?;
            if e.contains_aggr_func() {
                return Err(Error::InvalidUsageOfAggrFunc);
            }
            // the alias is temporary, will be updated in select clause
            items.push(e)
        }
        Ok(items)
    }

    #[inline]
    fn table_to_subquery(
        &mut self,
        schema_id: SchemaID,
        schema_name: SemiStr,
        table_id: TableID,
        table_name: SemiStr,
        colgen: &mut ColGen,
    ) -> Result<QueryID> {
        let all_cols = self.catalog.all_columns_in_table(table_id);
        let mut proj_cols = Vec::with_capacity(all_cols.len());
        // placeholder for table query.
        let (qry_id, subquery) = self.qs.insert_empty();
        for c in all_cols {
            let idx = ColIndex::from(c.idx);
            let col = colgen.gen_tbl_col(table_id, idx, c.pty, c.name.clone());
            proj_cols.push(ProjCol::implicit_alias(col, c.name))
        }
        let scan = OpKind::table(schema_id, schema_name, table_id, table_name, proj_cols);
        subquery.root = Op::new(scan);
        // todo: currently we assume all tables are located on disk.
        subquery.location = Location::Disk;
        Ok(qry_id)
    }

    // CTE is like scoped view, we need to search from current scope until top
    #[inline]
    fn find_cte(&self, alias: &str) -> Option<&QueryID> {
        self.scopes
            .iter()
            .rev()
            .find_map(|s| s.cte_aliases.get(alias))
    }

    #[inline]
    fn gen_op(&mut self, kind: OpKind) -> Op {
        Op::new(kind)
    }
}

/// Validate proj and aggr.
/// The rules are:
/// 1. aggr-funcs are not allowed as group items in aggr.
/// 2. all non-aggr expressions in proj must be group items in aggr.
/// 3. proj can have constant values, can also eliminate group items in aggr.
/// Returns true if the aggregation is scalar aggregation.
#[inline]
fn validate_proj_aggr(proj_cols: &[ProjCol], aggr_groups: &[ExprKind]) -> Result<bool> {
    // Rule 1
    if aggr_groups.iter().any(|e| e.contains_aggr_func()) {
        return Err(Error::AggrFuncInGroupBy);
    }
    // Rule 2
    // We need to identify columns outside aggregate function and group item in projection.
    // e.g. SELECT a, a + sum(1) FROM t GROUP BY a
    // From projection, save first "a" as group item, save second "a" in "a + sum(1)" as
    // column outside aggregate function.
    // Then we require all columns outside aggregate function must be group item.
    // This means following SQL is illegal: SELECT a+1, a+1 + sum(1) FROM t GROUP BY a+1
    // which is logically valid.
    let mut proj_cols_outside_aggr = Vec::new();
    let mut proj_groups = Vec::new();
    let mut proj_has_aggr = false;
    for c in proj_cols {
        // collect column outside aggr
        let (cols, ag) = c.expr.collect_non_aggr_cols();
        if ag {
            proj_cols_outside_aggr.extend(cols)
        } else {
            // constants can also be present in SELECT list
            let mut new_e = c.expr.clone();
            simplify_nested(&mut new_e, NullCoalesce::Null)?;
            if !new_e.is_const() {
                proj_groups.push(&c.expr);
            }
        }
        proj_has_aggr |= ag;
    }
    if aggr_groups.is_empty() {
        // No GROUP BY specified, so projection can either has no aggr functions,
        // or has all aggr functions with optional constants.
        if !proj_has_aggr {
            return Ok(false);
        }
        if !proj_groups.is_empty() || !proj_cols_outside_aggr.is_empty() {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
        // scalar aggregation: no group by, all projection are aggregation function without column reference outside
        return Ok(true);
    }
    // all proj_groups must exist in aggr_groups, constants already excluded
    for pg in &proj_groups {
        if !aggr_groups.contains(pg) {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
    }
    // all proj_cols_outside_aggr must exist in aggr_groups
    for pc in proj_cols_outside_aggr {
        if aggr_groups.iter().all(|e| !e.is_col(&pc)) {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
    }
    Ok(false)
}

/// Validate HAVING.
/// 1. For scalar aggregation, non-aggr columns are disallowed.
/// 2. For other aggregation, all non-aggr columns must exist in aggr groups.
/// 3. For flat projection, aggr functions are disallowed.
#[inline]
fn validate_having(
    proj_cols: &[ProjCol],
    aggr_groups: &[ExprKind],
    scalar_aggr: bool,
    having: &ExprKind,
) -> Result<()> {
    if scalar_aggr {
        // disallow non-aggr columns in HAVING
        if having.contains_non_aggr_cols() {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
        return Ok(());
    }
    if !aggr_groups.is_empty() {
        // all non-aggr columns must exist in aggr groups
        let (non_aggr_cols, _) = having.collect_non_aggr_cols();
        for c in non_aggr_cols {
            if aggr_groups.iter().all(|e| !e.is_col(&c)) {
                return Err(Error::FieldsSelectedNotInGroupBy);
            }
        }
        return Ok(());
    }
    // aggr functions are not allowed.
    let (non_aggr_cols, aggr) = having.collect_non_aggr_cols();
    if aggr {
        return Err(Error::FieldsSelectedNotInGroupBy);
    }
    // columns must match projected expressions
    for c in non_aggr_cols {
        if proj_cols.iter().all(|pc| !pc.expr.is_col(&c)) {
            // todo: notify column name
            return Err(Error::UnknownColumn("Unknown column".to_string()));
        }
    }
    Ok(())
}

/// Validate ORDER BY.
/// 1. For scalar aggregation, non-aggr columns are disallowed.
/// 2. For other aggregation, all non-aggr columns must exist in aggr groups.
/// 3. For flat projection, aggr functions are disallowed.
#[inline]
fn validate_order(aggr_groups: &[ExprKind], scalar_aggr: bool, order: &[SortItem]) -> Result<()> {
    if scalar_aggr {
        // disallow non-aggr columns in ORDER BY
        if order.iter().any(|si| si.expr.contains_non_aggr_cols()) {
            return Err(Error::FieldsSelectedNotInGroupBy);
        }
        return Ok(());
    }
    if !aggr_groups.is_empty() {
        // all non-aggr columns must exist in aggr groups
        let mut non_aggr_cols = vec![];
        for si in order {
            si.expr.collect_non_aggr_cols_into(&mut non_aggr_cols);
        }
        for c in non_aggr_cols {
            if aggr_groups.iter().all(|e| !e.is_col(&c)) {
                return Err(Error::FieldsSelectedNotInGroupBy);
            }
        }
        return Ok(());
    }
    // aggr functions are not allowed.
    if order.iter().any(|si| si.expr.contains_aggr_func()) {
        return Err(Error::FieldsSelectedNotInGroupBy);
    }
    Ok(())
}

/// no-op resolver for row subquery.
pub struct ResolveNone<'a, C: Catalog>(&'a C);

impl<'a, C: Catalog> ExprResolve<C> for ResolveNone<'a, C> {
    #[inline]
    fn catalog(&self) -> &C {
        self.0
    }

    #[inline]
    fn find_query(&self, _tbl_alias: &str) -> Option<(QueryID, &Subquery)> {
        None
    }

    #[inline]
    fn get_query(&self, _query_id: &QueryID) -> Option<&Subquery> {
        None
    }

    #[inline]
    fn queries(&self) -> Vec<(SemiStr, QueryID)> {
        vec![]
    }
}

/// Resolver for SELECT, WHERE
pub struct ResolveProjOrFilt<'a, C> {
    catalog: &'a C,
    qs: &'a QuerySet,
    query_aliases: &'a QueryAliases,
}

impl<'a, C: Catalog> ExprResolve<C> for ResolveProjOrFilt<'a, C> {
    #[inline]
    fn catalog(&self) -> &C {
        self.catalog
    }

    #[inline]
    fn find_query(&self, tbl_alias: &str) -> Option<(QueryID, &Subquery)> {
        self.query_aliases
            .get(tbl_alias)
            .and_then(|query_id| self.qs.get(query_id).map(|subq| (*query_id, subq)))
    }

    #[inline]
    fn get_query(&self, query_id: &QueryID) -> Option<&Subquery> {
        self.qs.get(query_id)
    }

    #[inline]
    fn queries(&self) -> Vec<(SemiStr, QueryID)> {
        self.query_aliases
            .iter()
            .map(|(a, q)| (a.clone(), *q))
            .collect()
    }
}

/// Resolver for GROUP BY
pub struct ResolveGroup<'a, C: Catalog> {
    catalog: &'a C,
    qs: &'a QuerySet,
    query_aliases: &'a QueryAliases,
    proj_cols: &'a [ProjCol],
}

impl<C: Catalog> ExprResolve<C> for ResolveGroup<'_, C> {
    #[inline]
    fn catalog(&self) -> &C {
        self.catalog
    }

    #[inline]
    fn find_query(&self, tbl_alias: &str) -> Option<(QueryID, &Subquery)> {
        self.query_aliases
            .get(tbl_alias)
            .and_then(|query_id| self.qs.get(query_id).map(|subq| (*query_id, subq)))
    }

    #[inline]
    fn get_query(&self, query_id: &QueryID) -> Option<&Subquery> {
        self.qs.get(query_id)
    }

    #[inline]
    fn queries(&self) -> Vec<(SemiStr, QueryID)> {
        self.query_aliases
            .iter()
            .map(|(a, q)| (a.clone(), *q))
            .collect()
    }

    /// try to match proj alias first.
    fn find_col(
        &self,
        col_alias: SemiStr,
        _location: &str,
        colgen: &mut ColGen,
    ) -> Result<Resolution> {
        for c in self.proj_cols {
            if c.alias_kind != AliasKind::None && c.alias == col_alias {
                return Ok(Resolution::Expr(c.expr.clone(), Some(col_alias)));
            }
        }
        self.find_col_by_default(col_alias, colgen)
    }
}

/// Resolver for HAVING or ORDER BY
/// The difference between them is HAVING does not care about
/// tables/queries in FROM clause.
pub struct ResolveHavingOrOrder<'a, C: Catalog> {
    catalog: &'a C,
    qs: &'a QuerySet,
    query_aliases: &'a QueryAliases,
    proj_cols: &'a [ProjCol],
    group_cols: Option<&'a [ExprKind]>,
}

impl<C: Catalog> ExprResolve<C> for ResolveHavingOrOrder<'_, C> {
    #[inline]
    fn catalog(&self) -> &C {
        self.catalog
    }

    #[inline]
    fn find_query(&self, tbl_alias: &str) -> Option<(QueryID, &Subquery)> {
        self.query_aliases
            .get(tbl_alias)
            .and_then(|query_id| self.qs.get(query_id).map(|subq| (*query_id, subq)))
    }

    #[inline]
    fn get_query(&self, query_id: &QueryID) -> Option<&Subquery> {
        self.qs.get(query_id)
    }

    #[inline]
    fn queries(&self) -> Vec<(SemiStr, QueryID)> {
        self.query_aliases
            .iter()
            .map(|(a, q)| (a.clone(), *q))
            .collect()
    }

    #[inline]
    fn find_col(
        &self,
        col_alias: SemiStr,
        _location: &str,
        colgen: &mut ColGen,
    ) -> Result<Resolution> {
        // proj alias has higher priority
        for c in self.proj_cols {
            if c.alias_kind != AliasKind::None && c.alias == col_alias {
                return Ok(Resolution::Expr(c.expr.clone(), Some(col_alias)));
            }
        }
        // if we find no match in proj alias,
        // then match alias to outputs of queries, but require matched
        // alias is unique, or it exists in proj alias list as expression.
        let mut matched = vec![];
        for (_, query_id) in self.query_aliases.iter() {
            let subquery = self.qs.get(query_id).must_ok()?;
            if let Some(idx) = subquery.position_out_col(&col_alias) {
                let idx = ColIndex::from(idx as u32);
                // check existence in proj aliases
                for c in self.proj_cols {
                    if expr_match_qry_col(&c.expr, *query_id, idx) {
                        return Ok(Resolution::Expr(c.expr.clone(), Some(col_alias)));
                    }
                }
                // then try group cols if exists, this is only for HAVING case,
                // which also respect GROUP BY clause for search
                if let Some(group_cols) = self.group_cols {
                    for ge in group_cols {
                        if expr_match_qry_col(ge, *query_id, idx) {
                            return Ok(Resolution::Expr(ge.clone(), Some(col_alias)));
                        }
                    }
                }
                // not exists, save to check uniqueness
                // in this case we need to assign unique global id.
                let col = colgen.gen_qry_col(*query_id, idx);
                matched.push(col);
            }
        }
        match matched.len() {
            0 => Ok(Resolution::Unknown(vec![col_alias])),
            1 => Ok(Resolution::Expr(matched.pop().unwrap(), Some(col_alias))),
            _ => Err(Error::DuplicatedColumnAlias(col_alias.to_string())),
        }
    }
}

struct ReplaceCorrelatedCol(u32, ExprKind);

impl OpMutVisitor for ReplaceCorrelatedCol {
    type Cont = ();
    type Break = ();
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<()> {
        for e in op.kind.exprs_mut() {
            e.walk_mut(self)?
        }
        ControlFlow::Continue(())
    }
}

impl ExprMutVisitor for ReplaceCorrelatedCol {
    type Cont = ();
    type Break = ();
    #[inline]
    fn enter(&mut self, e: &mut ExprKind) -> ControlFlow<()> {
        match e {
            ExprKind::Plhd(Plhd::Ident(uid)) if *uid == self.0 => {
                *e = self.1.clone();
                return ControlFlow::Break(());
            }
            _ => (),
        }
        ControlFlow::Continue(())
    }
}

struct ReplaceSubq {
    uid: u32,
    kind: SubqKind,
    qry_id: QueryID,
    // updated: bool,
    correlated: bool,
}

impl OpMutVisitor for ReplaceSubq {
    type Cont = ();
    type Break = ();
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<()> {
        for e in op.kind.exprs_mut() {
            e.walk_mut(self)?
        }
        ControlFlow::Continue(())
    }
}

impl ExprMutVisitor for ReplaceSubq {
    type Cont = ();
    type Break = ();
    #[inline]
    fn enter(&mut self, e: &mut ExprKind) -> ControlFlow<()> {
        match e {
            ExprKind::Plhd(Plhd::Subquery(_, uid)) if *uid == self.uid => {
                if !self.correlated && self.kind == SubqKind::Scalar {
                    *e = ExprKind::attval(self.qry_id);
                } else {
                    *e = ExprKind::subq(self.kind, self.qry_id);
                }
                return ControlFlow::Break(());
            }
            _ => (),
        }
        ControlFlow::Continue(())
    }
}

#[inline]
fn expr_match_qry_col(expr: &ExprKind, query_id: QueryID, col_idx: ColIndex) -> bool {
    match expr {
        ExprKind::Col(expr::Col {
            kind: expr::ColKind::Query(qry_id),
            idx,
            ..
        }) => *qry_id == query_id && *idx == col_idx,
        _ => false,
    }
}
