use doradb_catalog::TableID;
use doradb_datatype::PreciseType;
use doradb_expr::{ColIndex, ExprKind, GlobalID, QryCol, QueryID};
use semistr::SemiStr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjCol {
    pub expr: ExprKind,
    pub alias: SemiStr,
    pub alias_kind: AliasKind,
}

impl ProjCol {
    #[inline]
    pub fn new(expr: ExprKind, alias: SemiStr, alias_kind: AliasKind) -> Self {
        ProjCol {
            expr,
            alias,
            alias_kind,
        }
    }

    /// Create a projection column with no alias.
    #[inline]
    pub fn no_alias(expr: ExprKind) -> Self {
        ProjCol {
            expr,
            alias: SemiStr::default(),
            alias_kind: AliasKind::None,
        }
    }

    /// Create a projection column with explicit alias.
    #[inline]
    pub fn explicit_alias(expr: ExprKind, alias: SemiStr) -> Self {
        ProjCol {
            expr,
            alias,
            alias_kind: AliasKind::Explicit,
        }
    }

    /// Create a projection column with implicit alias.
    #[inline]
    pub fn implicit_alias(expr: ExprKind, alias: SemiStr) -> Self {
        ProjCol {
            expr,
            alias,
            alias_kind: AliasKind::Implicit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AliasKind {
    None,
    Implicit,
    Explicit,
}

#[derive(Default)]
pub struct ColGen {
    gid: GlobalID,
    qm: HashMap<QryCol, GlobalID>,
}

impl ColGen {
    #[inline]
    pub fn curr_gid(&self) -> GlobalID {
        self.gid
    }

    /// Generate query column, with global id.
    /// If the same column is found, reuse global id.
    #[inline]
    pub fn gen_qry_col(&mut self, qry_id: QueryID, idx: ColIndex) -> ExprKind {
        let gid = self.find_or_inc_cid(qry_id, idx);
        ExprKind::query_col(gid, qry_id, idx)
    }

    /// Generate setop column, with global id.
    #[inline]
    pub fn gen_setop_col(&mut self, idx: ColIndex, left: ExprKind, right: ExprKind) -> ExprKind {
        let gid = self.gid.inc_fetch();
        ExprKind::setop_col(gid, idx, left, right)
    }

    /// Generate table column.
    /// If same table appears multiple times in one query, we assign different
    /// query id to each table, therefore global id will also be different.
    #[inline]
    pub fn gen_tbl_col(
        &mut self,
        table_id: TableID,
        idx: ColIndex,
        ty: PreciseType,
        col_name: SemiStr,
    ) -> ExprKind {
        let gid = self.gid.inc_fetch();
        ExprKind::table_col(gid, table_id, idx, ty, col_name)
    }

    #[inline]
    fn find_or_inc_cid(&mut self, qry_id: QueryID, idx: ColIndex) -> GlobalID {
        match self.qm.entry(QryCol(qry_id, idx)) {
            Entry::Occupied(occ) => *occ.get(),
            Entry::Vacant(vac) => {
                let gid = self.gid.inc_fetch();
                vac.insert(gid); // store new cid
                gid
            }
        }
    }
}
