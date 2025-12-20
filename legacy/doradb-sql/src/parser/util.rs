use crate::ast::*;
use crate::parser::{ident, preceded_tag, ParseInput};
use nom::branch::alt;
use nom::combinator::{cut, map};
use nom::error::ParseError;
use nom::IResult;

use super::dml::{delete, insert, update};
use super::query::query_expr;

parse!(
    /// Parse a USE statement.
    fn use_db -> 'a UseDB<'a> = {
        map(preceded_tag("use", cut(ident)), |name| UseDB{name})
    }
);

parse!(
    /// Parse an EXPLAIN statement.
    fn explain -> 'a Explain<'a> = {
        preceded_tag("explain", cut(alt((
            map(query_expr, Explain::Select),
            map(insert, Explain::Insert),
            map(delete, Explain::Delete),
            map(update, Explain::Update),
        ))))
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::MySQL;
    use nom::error::Error;

    #[test]
    fn test_use_db() -> anyhow::Result<()> {
        for (i, u) in vec![
            (
                "use db1",
                UseDB {
                    name: Ident::regular("db1"),
                },
            ),
            (
                "use `db1`",
                UseDB {
                    name: Ident::quoted("db1"),
                },
            ),
        ] {
            let (i, o) = use_db::<'_, _, Error<_>>(MySQL(i))?;
            assert!(i.is_empty());
            assert_eq!(o, u);
        }
        Ok(())
    }

    #[test]
    fn test_explain() -> anyhow::Result<()> {
        for i in vec![
            "explain select * from t1",
            "explain insert into t1 values (1)",
            "explain update t1 set v1 = 0",
            "explain delete from t1 where v1 = 0",
        ] {
            let (i, _) = explain::<'_, _, Error<_>>(MySQL(i))?;
            assert!(i.is_empty());
        }
        Ok(())
    }
}
