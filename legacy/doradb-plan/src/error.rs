use doradb_expr::QueryID;
use doradb_sql::ast::Ident;
use semistr::SemiStr;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub trait ToResult {
    type Output;

    fn must_ok(self) -> Result<Self::Output>;
}

impl<T> ToResult for Option<T> {
    type Output = T;

    fn must_ok(self) -> Result<Self::Output> {
        self.ok_or(Error::MustOK)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unsupported SQL syntax '{0}'")]
    UnsupportedSqlSyntax(String),
    #[error("Duplicated table alias '{0}'")]
    DuplicatedTableAlias(SemiStr),
    #[error("Duplicated column alias '{0}'")]
    DuplicatedColumnAlias(String),
    #[error("Invalid single row data '{0}'")]
    InvalidSingleRowData(String),
    #[error("Invalid Identifier '{0}'")]
    InvalidIdentifier(String),
    #[error("Schema not exists '{0}'")]
    SchemaNotExists(String),
    #[error("Table not exists '{0}'")]
    TableNotExists(String),
    #[error("Column not exists '{0}'")]
    ColumnNotExists(String),
    #[error("Schema id not found '{0}'")]
    SchemaIdNotFound(u32),
    #[error("Table id not found '{0}'")]
    TableIdNotFound(u32),
    #[error("Column count mismatch")]
    ColumnCountMismatch,
    #[error("Not unique alias or table '{0}'")]
    NotUniqueAliasOrTable(String),
    #[error("Not unique table '{0}'")]
    NotUniqueTable(String),
    #[error("Unknown table: '{0}'")]
    UnknownTable(String),
    #[error("{0}")]
    UnknownColumn(String),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Fields selected not in GROUP BY clause")]
    FieldsSelectedNotInGroupBy,
    #[error("ORDER BY contains non-selected fields")]
    OrderByNonSelectedFields,
    #[error("Duplicated asterisks in field list")]
    DulicatedAsterisksInFieldList,
    #[error("Aggregate functions in GROUP BY clause")]
    AggrFuncInGroupBy,
    #[error("Invalid usage of aggregate function")]
    InvalidUsageOfAggrFunc,
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    ParseDecimalError(#[from] doradb_datatype::DecimalError),
    #[error(transparent)]
    ParseDatetimeError(#[from] doradb_datatype::DatetimeParseError),
    #[error("Internal error MustOK")]
    MustOK,
    #[error(transparent)]
    ExprError(#[from] doradb_expr::error::Error),
    #[error("Too many tables to join")]
    TooManyTablesToJoin,
    #[error("Query {0} not found")]
    QueryNotFound(QueryID),
    #[error("Invalid join vertex set")]
    InvalidJoinVertexSet,
    #[error("Invalid join transformation")]
    InvalidJoinTransformation,
    #[error("Invalid operator transformation")]
    InvalidOpertorTransformation,
    #[error("Invalid join condition")]
    InvalidJoinCondition,
    #[error("Join estimation not support")]
    JoinEstimationNotSupport,
    #[error("Hyperedges not initialized")]
    HyperedgesNotInitialized,
    #[error("Cross join not supported")]
    CrossJoinNotSupport,
    #[error("Type infer failed")]
    TypeInferFailed,
    #[error(transparent)]
    StringArenaError(#[from] aosa::Error),
    #[error("Duplicated query id")]
    DuplicatedQueryID(QueryID),
    #[error("Incomplete query generation from logical plan")]
    IncompleteLgcPlanReflection,
    #[error("Invalid plan structure for logical plan reflection")]
    InvalidPlanStructureForReflection,
    #[error("Invalid expr transformation for logical plan reflection")]
    InvalidExprTransformationForReflection,
    #[error("Break")]
    Break,
    #[error("Empty plan")]
    EmptyPlan,
    #[error(transparent)]
    Compute(#[from] doradb_compute::error::Error),
    #[error("Unsupported physical table scan")]
    UnsupportedPhyTableScan,
}

impl Error {
    #[inline]
    pub fn unknown_column(ids: &[Ident<'_>], location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            let (head, tail) = ids.split_first().unwrap();
            s.push_str(head.as_str());
            for id in tail {
                s.push('.');
                s.push_str(id.as_str());
            }
        })
    }

    #[inline]
    pub fn unknown_column_full_name(schema: &str, tbl: &str, col: &str, location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            s.push_str(schema);
            s.push('.');
            s.push_str(tbl);
            s.push('.');
            s.push_str(col);
        })
    }

    #[inline]
    pub fn unknown_column_partial_name(tbl: &str, col: &str, location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            s.push_str(tbl);
            s.push('.');
            s.push_str(col);
        })
    }

    #[inline]
    pub fn unknown_column_name(col: &str, location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            s.push_str(col);
        })
    }

    #[inline]
    pub fn unknown_column_idents(ids: &[SemiStr], location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            let (head, tail) = ids.split_first().unwrap();
            s.push_str(head);
            for id in tail {
                s.push('.');
                s.push_str(id.as_str());
            }
        })
    }

    #[inline]
    fn unknown_column_apply<F: FnOnce(&mut String)>(location: &str, f: F) -> Self {
        let mut s = String::from("Unknown column '");
        f(&mut s);
        s.push_str("' in '");
        s.push_str(location);
        s.push('\'');
        Error::UnknownColumn(s)
    }

    #[inline]
    pub fn unknown_asterisk_column(qualifiers: &[Ident<'_>], location: &str) -> Self {
        Self::unknown_column_apply(location, |s| {
            for q in qualifiers {
                s.push_str(q.as_str());
                s.push('.');
            }
            s.push('*');
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn limit_plan_error_size() {
        assert!(std::mem::size_of::<Error>() <= 64);
    }
}
