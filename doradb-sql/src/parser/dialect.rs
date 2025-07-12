//! Extension to nom parser.
//! It implements a new input type record original string and offset,
//! with configurable identifier delimiter.
use nom::error::ParseError;
use nom::{
    AsBytes, Compare, CompareResult, Err, FindSubstring, FindToken, IResult, InputIter,
    InputLength, InputTake, InputTakeAtPosition, Needed, Offset, Slice,
};
use std::fmt::Debug;
use std::ops::{Deref, Range, RangeFrom, RangeFull, RangeTo};
use std::str::{CharIndices, Chars};

// Defines parsing details of SQL dialects.
pub trait Dialect {
    /// Delimiter of identfier.
    fn ident_quote() -> char;

    /// Escape string of delimited identifier.
    fn ident_escape() -> &'static str;
}

/// Composite trait for parsing input
pub trait ParseInput<'a>:
    Clone
    + Copy
    + Debug
    + InputLength
    + AsBytes
    + InputTake
    + InputIter<Item = char>
    + InputTakeAtPosition<Item = char>
    + Slice<RangeFrom<usize>>
    + Slice<RangeTo<usize>>
    + Slice<RangeFull>
    + Slice<Range<usize>>
    + Deref<Target = str>
    + Into<&'a str>
    + Offset
    + Compare<&'a str>
    + FindSubstring<&'a str>
    + Dialect
{
}

define_dialect!(Ansi);

impl Dialect for Ansi<'_> {
    #[inline]
    fn ident_quote() -> char {
        '"'
    }

    #[inline]
    fn ident_escape() -> &'static str {
        "\"\""
    }
}

impl<'a> ParseInput<'a> for Ansi<'a> {}

define_dialect!(MySQL);

impl Dialect for MySQL<'_> {
    #[inline]
    fn ident_quote() -> char {
        '`'
    }

    #[inline]
    fn ident_escape() -> &'static str {
        "``"
    }
}

impl<'a> ParseInput<'a> for MySQL<'a> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;
    use crate::parser::ident;
    use nom::error::VerboseError;

    #[test]
    fn test_mysql_dialect() -> anyhow::Result<()> {
        // success cases
        for c in vec![
            ("a", ("", Ident::regular("a"))),
            ("abc", ("", Ident::regular("abc"))),
            ("abc123", ("", Ident::regular("abc123"))),
            ("``", ("", Ident::quoted(""))),
            ("`abc`", ("", Ident::quoted("abc"))),
            ("`abc``def`", ("", Ident::quoted("abc``def"))),
        ] {
            let res = ident::<'_, _, VerboseError<_>>(MySQL(c.0))?;
            assert_eq!(&*res.0, c.1 .0);
            assert_eq!(res.1, c.1 .1);
        }
        Ok(())
    }
}
