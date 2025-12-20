use crate::error::{Error, Result};
use doradb_expr::QueryID;
use indexmap::{map, IndexMap};
use semistr::SemiStr;
use std::ops::{Deref, DerefMut};

/// QueryAliases stores the aliases of tables.
/// MySQL allows subquery and table has same alias.
/// This has rare use cases, but causes very
/// confusing name resolution.
///
/// For example:
/// SELECT db1.t3.val FROM db1.t1 AS t3, (SELECT * FROM db1.t2) AS t3
/// SELECT     t3.val FROM db1.t1 AS t3, (SELECT * FROM db1.t2) AS t3
/// The two SQL may have different output according to order/priority
/// of column reference resolution.
///
/// I've decided not to keep the compatibility.
/// If user does not use duplicated aliases, it's all fine.
///
/// Let aliases within single query block to be unique.
#[derive(Debug, Clone, Default)]
pub struct QueryAliases(IndexMap<SemiStr, QueryID>);

impl Deref for QueryAliases {
    type Target = IndexMap<SemiStr, QueryID>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for QueryAliases {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl QueryAliases {
    #[inline]
    pub fn new(plans: IndexMap<SemiStr, QueryID>) -> Self {
        QueryAliases(plans)
    }

    #[inline]
    pub fn insert_query(&mut self, name_or_alias: SemiStr, query_id: QueryID) -> Result<()> {
        match self.0.entry(name_or_alias) {
            map::Entry::Vacant(ent) => {
                ent.insert(query_id);
                Ok(())
            }
            map::Entry::Occupied(ent) => Err(Error::NotUniqueAliasOrTable(ent.key().to_string())),
        }
    }
}
