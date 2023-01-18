use crate::col::{AliasKind, ProjCol};
use crate::error::{Error, Result};
use crate::join::{Join, JoinOp};
use crate::op::{Op, OpMutVisitor, OpVisitor};
use crate::scope::Scope;
use fnv::FnvHashMap;
use std::collections::HashSet;
use std::mem;
use std::ops::ControlFlow;
use std::ops::Deref;
use xngin_catalog::{SchemaID, TableID};
use xngin_expr::QueryID;

/// Query wraps logical operator with additional syntax information.
/// group operators as a tree, with output column list.
/// it's equivalent to a simple SELECT statement, like below:
///
/// ```sql
/// SELECT ...
/// FROM ...
/// WHERE ...
/// GROUP BY ... HAVING ...
/// ORDER BY ... LIMIT ...
/// ```
/// The operator tree will be like(from top to bottom):
/// Limit -> Sort -> Proj -> Filt -> Aggr -> Filt -> Table/Join
#[derive(Debug, Clone)]
pub struct Subquery {
    // root operator
    pub root: Op,
    // scope contains information incrementally collected during
    // build phase
    pub scope: Scope,
    // location of the subquery
    pub location: Location,
}

impl Subquery {
    /// Construct an empty subquery, which will be updated immediately.
    pub fn empty() -> Self {
        Subquery {
            root: Op::Empty,
            scope: Scope::default(),
            location: Location::Disk,
        }
    }

    /// Construct a subquery using given root operator and scope.
    #[inline]
    pub fn new(root: Op, scope: Scope, location: Location) -> Self {
        Subquery {
            root,
            scope,
            location,
        }
    }

    #[inline]
    pub fn out_cols(&self) -> &[ProjCol] {
        self.root.out_cols().unwrap()
    }

    #[inline]
    pub fn out_cols_mut(&mut self) -> &mut [ProjCol] {
        self.root.out_cols_mut().unwrap()
    }

    #[inline]
    pub fn position_out_col(&self, alias: &str) -> Option<usize> {
        self.out_cols().iter().position(|c| match c.alias_kind {
            AliasKind::Explicit | AliasKind::Implicit => c.alias == alias,
            AliasKind::None => false,
        })
    }

    #[inline]
    pub fn find_table(&self) -> Option<(SchemaID, TableID)> {
        struct FindTable(Option<(SchemaID, TableID)>);
        impl OpVisitor for FindTable {
            type Cont = ();
            type Break = ();
            fn enter(&mut self, op: &Op) -> ControlFlow<()> {
                match op {
                    Op::Table(schema_id, table_id) => {
                        self.0 = Some((*schema_id, *table_id));
                        ControlFlow::Break(())
                    }
                    _ => ControlFlow::Continue(()),
                }
            }
        }
        let mut ft = FindTable(None);
        let _ = self.root.walk(&mut ft);
        ft.0
    }

    #[inline]
    pub fn find_out_col(&self, idx: usize) -> Option<&ProjCol> {
        let out_cols = self.out_cols();
        if idx >= out_cols.len() {
            return None;
        }
        Some(&out_cols[idx])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Location {
    Intermediate,
    Disk,
    Memory,
    Network,
    // virtual table which is computed directly by expression
    Virtual,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QryIDs {
    Empty,
    Single(QueryID),
    Multi(HashSet<QueryID>),
}

impl Default for QryIDs {
    fn default() -> Self {
        QryIDs::Empty
    }
}

/// QuerySet stores all sub-subqeries and provide lookup and update methods.
#[derive(Debug, Default)]
pub struct QuerySet(Vec<Subquery>);

impl QuerySet {
    #[inline]
    pub fn insert_empty(&mut self) -> (QueryID, &mut Subquery) {
        // let query = Subquery::default();
        let qry_id = self.0.len();
        self.0.push(Subquery::empty());
        (QueryID::from(qry_id as u32), &mut self.0[qry_id])
    }

    #[inline]
    pub fn get(&self, qry_id: &QueryID) -> Option<&Subquery> {
        self.0.get(**qry_id as usize)
    }

    #[inline]
    pub fn get_mut(&mut self, qry_id: &QueryID) -> Option<&mut Subquery> {
        self.0.get_mut(**qry_id as usize)
    }

    #[inline]
    pub fn transform_subq<T, F>(&mut self, qry_id: QueryID, f: F) -> Result<T>
    where
        F: FnOnce(&mut Subquery) -> T,
    {
        if let Some(subq) = self.0.get_mut(*qry_id as usize) {
            return Ok(f(subq));
        }
        Err(Error::QueryNotFound(qry_id))
    }

    #[inline]
    pub fn transform_op<T, F>(&mut self, qry_id: QueryID, f: F) -> Result<T>
    where
        F: FnOnce(&mut QuerySet, Location, &mut Op) -> T,
    {
        if let Some(subq) = self.0.get_mut(*qry_id as usize) {
            let location = subq.location;
            let mut root = mem::take(&mut subq.root);
            let res = f(self, location, &mut root);
            // update root back, this always succeed because query set does not allow deletion.
            self.0[*qry_id as usize].root = root;
            return Ok(res);
        }
        Err(Error::QueryNotFound(qry_id))
    }

    /// Deep copy a query given its id.
    /// The logic is to find all subqueries in it,
    /// then recursively copy them one by one, replacing
    /// original id with new generated id.
    /// NOTE: Inner scope contains variables referring changed query ids.
    ///       We should also change them.
    #[inline]
    pub fn copy_query(&mut self, qry_id: &QueryID) -> Result<QueryID> {
        if let Some(sq) = self.get(qry_id) {
            let sq = sq.clone();
            Ok(self.upsert_query(sq))
        } else {
            Err(Error::QueryNotFound(*qry_id))
        }
    }

    #[inline]
    fn upsert_query(&mut self, mut sq: Subquery) -> QueryID {
        let mut mapping = FnvHashMap::default();
        let mut upsert = UpsertQuery {
            qs: self,
            mapping: &mut mapping,
        };
        sq.root.walk_mut(&mut upsert);
        // update from aliases in subquery's scope
        for (_, query_id) in sq.scope.query_aliases.iter_mut() {
            if let Some(new_query_id) = mapping.get(query_id) {
                *query_id = *new_query_id;
            }
        }
        // self.insert(sq)
        let (qry_id, tgt) = self.insert_empty();
        *tgt = sq;
        qry_id
    }
}

impl Deref for QuerySet {
    type Target = [Subquery];
    #[inline]
    fn deref(&self) -> &[Subquery] {
        &self.0
    }
}

struct UpsertQuery<'a> {
    qs: &'a mut QuerySet,
    mapping: &'a mut FnvHashMap<QueryID, QueryID>,
}

impl UpsertQuery<'_> {
    #[inline]
    fn modify_join_op(&mut self, jo: &mut JoinOp) {
        match jo.as_mut() {
            Op::Query(query_id) => {
                let query = self.qs.get(query_id).cloned().unwrap(); // won't fail
                let new_query_id = self.qs.upsert_query(query);
                self.mapping.insert(*query_id, new_query_id);
                *query_id = new_query_id;
            }
            Op::Join(j) => {
                self.modify_join(j);
            }
            _ => unreachable!(),
        }
    }

    #[inline]
    fn modify_join(&mut self, j: &mut Join) {
        match j {
            Join::Cross(cj) => {
                for jo in cj {
                    self.modify_join_op(jo)
                }
            }
            Join::Qualified(qj) => {
                self.modify_join_op(&mut qj.left);
                self.modify_join_op(&mut qj.right);
            }
        }
    }
}

impl<'a> OpMutVisitor for UpsertQuery<'a> {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, op: &mut Op) -> ControlFlow<()> {
        match op {
            Op::Query(query_id) => {
                // only perform additional copy to subquery
                let query = self.qs.get(query_id).cloned().unwrap(); // won't fail
                let new_query_id = self.qs.upsert_query(query);
                self.mapping.insert(*query_id, new_query_id);
                *query_id = new_query_id;
            }
            Op::Join(join) => match join.as_mut() {
                Join::Cross(cj) => {
                    for jo in cj {
                        self.modify_join_op(jo)
                    }
                }
                Join::Qualified(qj) => {
                    self.modify_join_op(&mut qj.left);
                    self.modify_join_op(&mut qj.right);
                }
            },
            _ => (), // others are safe to copy
        }
        ControlFlow::Continue(())
    }
}
