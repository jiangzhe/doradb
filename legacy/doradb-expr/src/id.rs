use doradb_catalog::ColIndex;
use std::ops::Deref;

/// ColIndex wraps u32 to be the index of column in current table/subquery.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GlobalID(u32);

impl GlobalID {
    /// Returns next id.
    #[inline]
    pub fn next(self) -> Self {
        GlobalID(self.0 + 1)
    }

    // #[inline]
    // pub fn fetch_inc(&mut self) -> Self {
    //     let val = self.0;
    //     self.0 += 1;
    //     GlobalID(val)
    // }

    #[inline]
    pub fn inc_fetch(&mut self) -> Self {
        self.0 += 1;
        GlobalID(self.0)
    }

    #[inline]
    pub fn value(&self) -> u32 {
        self.0
    }
}

impl From<u32> for GlobalID {
    fn from(src: u32) -> Self {
        GlobalID(src)
    }
}

pub const INVALID_GLOBAL_ID: GlobalID = GlobalID(0);

/// QueryID wraps u32 to be the identifier of subqueries in single query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueryID(u32);

impl QueryID {
    #[inline]
    pub fn value(&self) -> u32 {
        self.0
    }
}

impl From<u32> for QueryID {
    fn from(src: u32) -> Self {
        debug_assert!(src != !0, "Constructing QueryID from !0 is not allowed");
        QueryID(src)
    }
}

impl std::fmt::Display for QueryID {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "q{}", self.0)
    }
}

impl Deref for QueryID {
    type Target = u32;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub const INVALID_QUERY_ID: QueryID = QueryID(0);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QryCol(pub QueryID, pub ColIndex);
