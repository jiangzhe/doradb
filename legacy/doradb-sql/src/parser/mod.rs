//! This module defines all parsing functions to generate AST from SQL texts.
//! Macros are used to eliminate idential parts of generic function signatures
//! required by nom parsing framework.
pub(crate) mod ddl;
pub mod dialect;
pub(crate) mod dml;
pub(crate) mod expr;
pub(crate) mod query;
pub(crate) mod util;

use crate::ast::*;
use crate::error::{Error, Result};
use crate::parser::ddl::{create, drop};
use crate::parser::dml::{delete, insert, update};
use crate::parser::expr::{char_sp0, expr_sp0};
use crate::parser::query::query_expr;
use crate::parser::util::{explain, use_db};
use nom::branch::alt;
use nom::bytes::complete::{
    tag, tag_no_case, take, take_till, take_till1, take_until, take_while1,
};
use nom::character::complete::{char, multispace1};
use nom::combinator::{cut, map, opt, recognize, rest, value};
use nom::error::ParseError;
use nom::multi::fold_many0;
use nom::sequence::{delimited, pair, preceded, terminated};
use std::marker::PhantomData;

pub use crate::parser::dialect::ParseInput;
pub use nom::error::convert_error;
pub use nom::error::{Error as NomError, VerboseError};
pub use nom::{Err as NomErr, IResult};

/// fast query parsing
#[inline]
pub fn parse_query<'a, I: ParseInput<'a>>(input: I) -> Result<QueryExpr<'a>> {
    terminated::<_, _, _, NomError<I>, _, _>(query::query_expr, spcmt0)(input)
        .map(|(_, o)| o)
        .map_err(convert_simple_error)
}

/// verbose query parsing, if error occurs, use `convert_error` to find more details
#[inline]
pub fn parse_query_verbose<'a, I: ParseInput<'a>>(input: I) -> Result<QueryExpr<'a>> {
    terminated(query_expr, spcmt0)(input)
        .map(|(_, o)| o)
        .map_err(|e| convert_verbose_error(input, e))
}

/// fast statement parsing
#[inline]
pub fn parse_stmt<'a, I: ParseInput<'a>>(input: I) -> Result<Statement<'a>> {
    terminated::<_, _, _, NomError<I>, _, _>(statement, spcmt0)(input)
        .map(|(_, o)| o)
        .map_err(convert_simple_error)
}

/// verbose statement parsing, if error occurs, use `convert_error` to find more details
#[inline]
pub fn parse_stmt_verbose<'a, I: ParseInput<'a>>(input: I) -> Result<Statement<'a>> {
    terminated(statement, spcmt0)(input)
        .map(|(_, o)| o)
        .map_err(|e| convert_verbose_error(input, e))
}

#[inline]
pub fn parse_multi_stmts<'a, I: ParseInput<'a>>(
    input: I,
    delim: char,
) -> ParseMultiStmtIter<'a, I> {
    ParseMultiStmtIter {
        input,
        delim,
        _marker: PhantomData,
    }
}

pub struct ParseMultiStmtIter<'a, I: 'a> {
    input: I,
    delim: char,
    _marker: PhantomData<&'a mut I>,
}

impl<'a, I: ParseInput<'a>> Iterator for ParseMultiStmtIter<'a, I> {
    type Item = Result<Statement<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.input.is_empty() {
            return None;
        }
        match statement::<_, NomError<I>>(self.input) {
            Ok((i, stmt)) => {
                // eat whitespaces and comments between statements.
                let (i, _) = spcmt0::<_, NomError<I>>(i).unwrap(); // won't fail
                let (i, _) = char_sp0::<_, NomError<I>>(self.delim)(i).unwrap(); // won't fail
                self.input = i;
                Some(Ok(stmt))
            }
            Err(e) => {
                let e = convert_simple_error(e);
                Some(Err(e))
            }
        }
    }
}

reserved_keywords!(
    /// Reserved keywords defined by MySQL.
    /// match patterns of all keywords should be snake cases.
    /// A static hash map will be initialized for fast lookup.
    "accessible"        => Accessible,
    "add"               => Add,
    "all"               => All,
    "alter"             => Alter,
    "analyze"           => Analyze,
    "and"               => And,
    "any"               => Any,
    "as"                => As,
    "asc"               => Asc,
    "asensitive"        => Asensitive,
    "before"            => Before,
    "between"           => Between,
    "bigint"            => Bigint,
    "binary"            => Binary,
    "blob"              => Blob,
    "both"              => Both,
    "by"                => By,
    "call"              => Call,
    "cascade"           => Cascade,
    "case"              => Case,
    "change"            => Change,
    "char"              => Char,
    "character"         => Character,
    "check"             => Check,
    "collate"           => Collate,
    "column"            => Column,
    "condition"         => Condition,
    "constraint"        => Constraint,
    "continue"          => Continue,
    "convert"           => Convert,
    "create"            => Create,
    "cross"             => Cross,
    "cube"              => Cube, // 8.0.1
    "cume_dist"         => CumeDist, // 8.0.2
    "current_date"      => CurrentDate,
    "current_time"      => CurrentTime,
    "current_timstamp"  => CurrentTimestamp,
    "current_user"      => CurrentUser,
    "cursor"            => Cursor,
    "database"          => Database,
    "databases"         => Databases,
    "day_hour"          => DayHour,
    "day_microsecond"   => DayMicrosecond,
    "day_minute"        => DayMinute,
    "day_second"        => DaySecond,
    "dec"               => Dec,
    "decimal"           => Decimal,
    "declare"           => Declare,
    "default"           => Default,
    "delayed"           => Delayed,
    "delete"            => Delete,
    "dense_rank"        => DenseRank, // 8.0.2
    "desc"              => Desc,
    "describe"          => Describe,
    "deterministic"     => Deterministic,
    "distinct"          => Distinct,
    "distinctrow"       => Distinctrow,
    "div"               => Div,
    "double"            => Double,
    "drop"              => Drop,
    "dual"              => Dual,
    "each"              => Each,
    "else"              => Else,
    "elseif"            => Elseif,
    "empty"             => Empty, // 8.0.4
    "enclosed"          => Enclosed,
    "escaped"           => Escaped,
    "except"            => Except,
    "exists"            => Exists,
    "exit"              => Exit,
    "explain"           => Explain,
    "false"             => False,
    "fetch"             => Fetch,
    "first_value"       => FirstValue, // 8.0.2
    "float"             => Float,
    "float4"            => Float4,
    "float8"            => Float8,
    "for"               => For,
    "force"             => Force,
    "foreign"           => Foreign,
    "from"              => From,
    "full"              => Full, // MySQL does not support FULL JOIN, but here we support
    "fulltext"          => Fulltext,
    "function"          => Function, // 8.0.1
    "generated"         => Generated,
    "get"               => Get,
    "grant"             => Grant,
    "group"             => Group,
    "grouping"          => Grouping, // 8.0.1
    "groups"            => Groups, // 8.0.2
    "having"            => Having,
    "high_priority"     => HighPriority,
    "hour_microsecond"  => HourMicrosecond,
    "hour_minute"       => HourMinute,
    "hour_second"       => HourSecond,
    "if"                => If,
    "ignore"            => Ignore,
    "in"                => In,
    "index"             => Index,
    "infile"            => Infile,
    "inner"             => Inner,
    "inout"             => Inout,
    "insensitive"       => Insensitive,
    "insert"            => Insert,
    "int"               => Int,
    "int1"              => Int1,
    "int2"              => Int2,
    "int3"              => Int3,
    "int4"              => Int4,
    "int8"              => Int8,
    "integer"           => Integer,
    "intersect"         => Intersect, // MySQL does not support INTERSECT, but we support
    "interval"          => Interval,
    "into"              => Into,
    "io_after_gtids"    => IoAfterGtids,
    "io_before_gtids"   => IoBeforeGtids,
    "is"                => Is,
    "iterate"           => Iterate,
    "join"              => Join,
    "json_table"        => JsonTable, // 8.0.4
    "key"               => Key,
    "keys"              => Keys,
    "kill"              => Kill,
    "lag"               => Lag, // 8.0.2
    "last_value"        => LastValue, // 8.0.2
    "lateral"           => Lateral, // 8.0.14
    "lead"              => Lead, // 8.0.2
    "leading"           => Leading,
    "leave"             => Leave,
    "left"              => Left,
    "like"              => Like,
    "limit"             => Limit,
    "linear"            => Linear,
    "lines"             => Lines,
    "localtime"         => Localtime,
    "localtimestamp"    => Localtimestamp,
    "lock"              => Lock,
    "long"              => Long,
    "longblob"          => Longblob,
    "longtext"          => Longtext,
    "loop"              => Loop,
    "low_priority"      => LowPriority,
    "master_bind"       => MasterBind,
    "master_ssl_verify_server_sert" => MasterSslVerifyServerCert,
    "match"             => Match,
    "maxvalue"          => Maxvalue,
    "mediumblob"        => Mediumblob,
    "mediumint"         => Mediumint,
    "mediumtext"        => Mediumtext,
    "middleint"         => Middleint,
    "minute_microsecond" => MinuteMicrosecond,
    "minute_second"     => MinuteSecond,
    "mod"               => Mod,
    "modifies"          => Modifies,
    "natural"           => Natural,
    "not"               => Not,
    "no_write_to_binlog" => NoWriteToBinlog,
    "nth_value"         => NthValue, // 8.0.2
    "ntile"             => Ntile, // 8.0.2
    "null"              => Null,
    "numeric"           => Numeric,
    "of"                => Of, // 8.0.1
    "on"                => On,
    "optimize"          => Optimize,
    "optimizer_costs"   => OptimizerCosts,
    "option"            => Option,
    "optionally"        => Optionally,
    "or"                => Or,
    "order"             => Order,
    "out"               => Out,
    "outer"             => Outer,
    "outfile"           => Outfile,
    "over"              => Over, // 8.0.2
    "partition"         => Partition,
    "percent_rank"      => PercentRank, // 8.0.2
    "precision"         => Precision,
    "primary"           => Primary,
    "procedure"         => Procedure,
    "purge"             => Purge,
    "range"             => Range,
    "rank"              => Rank, // 8.0.2
    "read"              => Read,
    "reads"             => Reads,
    "read_write"        => ReadWrite,
    "real"              => Real,
    "recursive"         => Recursive, // 8.0.1
    "reference"         => References,
    "regexp"            => Regexp,
    "release"           => Release,
    "rename"            => Rename,
    "repeat"            => Repeat,
    "replace"           => Replace,
    "require"           => Require,
    "resignal"          => Resignal,
    "restrict"          => Restrict,
    "return"            => Return,
    "revoke"            => Revoke,
    "right"             => Right,
    "rlike"             => Rlike,
    "row"               => Row, // 8.0.2
    "rows"              => Rows, // 8.0.2
    "row_number"        => RowNumber, // 8.0.2
    "schema"            => Schema,
    "schemas"           => Schemas,
    "second_microsecond" => SecondMicrosecond,
    "select"            => Select,
    "sensitive"         => Sensitive,
    "separator"         => Separator,
    "set"               => Set,
    "show"              => Show,
    "signal"            => Signal,
    "smallint"          => Smallint,
    "spatial"           => Spatial,
    "specific"          => Specific,
    "sql"               => Sql,
    "sqlexception"      => Sqlexception,
    "sqlstate"          => Sqlstate,
    "sqlwarning"        => Sqlwarning,
    "sql_bit_result"    => SqlBigResult,
    "sql_calc_found_rows" => SqlCalcFoundRows,
    "sql_small_result"  => SqlSmallResult,
    "ssl"               => Ssl,
    "starting"          => Starting,
    "stored"            => Stored,
    "straight_join"     => StraightJoin,
    "system"            => System, // 8.0.3
    "table"             => Table,
    "terminated"        => Terminated,
    "then"              => Then,
    "tinyblob"          => Tinyblob,
    "tinyint"           => Tinyint,
    "tinytext"          => Tinytext,
    "to"                => To,
    "trailing"          => Trailing,
    "trigger"           => Trigger,
    "true"              => True,
    "undo"              => Undo,
    "union"             => Union,
    "unique"            => Unique,
    "unlock"            => Unlock,
    "unsigned"          => Unsigned,
    "update"            => Update,
    "usage"             => Usage,
    "use"               => Use,
    "using"             => Using,
    "utc_date"          => UtcDate,
    "utc_time"          => UtcTime,
    "utc_timestamp"     => UtcTimestamp,
    "values"            => Values,
    "varbinary"         => Varbinary,
    "varchar"           => Varchar,
    "varcharacter"      => Varcharacter,
    "varying"           => Varying,
    "virtual"           => Virtual,
    "when"              => When,
    "where"             => Where,
    "while"             => While,
    "window"            => Window, // 8.0.2
    "with"              => With,
    "write"             => Write,
    "xor"               => Xor,
    "year_mohth"        => YearMonth,
    "zerofill"          => Zerofill
);

#[inline]
fn is_reserved_keyword(token: &str) -> bool {
    RESERVED_KEYWORDS.contains_key(&CastAsciiLowerCase(token))
}

#[inline]
fn match_reserved_keyword(token: &str) -> Option<ReservedKeyword> {
    RESERVED_KEYWORDS.get(&CastAsciiLowerCase(token)).cloned()
}

builtin_keywords!(
    /// Keywrods for builtin functions.
    /// The keyword pattern must be immediately followed by a '('
    "extract" => Extract,
    "substring" => Substring
);

#[allow(dead_code)]
#[inline]
fn is_builtin_keyword(token: &str) -> bool {
    BUILTIN_KEYWORDS.contains_key(&CastAsciiLowerCase(token))
}

#[inline]
fn match_builtin_keyword(token: &str) -> Option<BuiltinKeyword> {
    BUILTIN_KEYWORDS.get(&CastAsciiLowerCase(token)).cloned()
}

#[derive(Debug, Clone, Copy, Eq)]
struct CastAsciiLowerCase<'a>(&'a str);

impl std::hash::Hash for CastAsciiLowerCase<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0
            .as_bytes()
            .iter()
            .for_each(|b| b.to_ascii_lowercase().hash(state))
    }
}

impl std::cmp::PartialEq for CastAsciiLowerCase<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0
            .as_bytes()
            .iter()
            .zip(other.0.as_bytes().iter())
            .all(|(a, b)| a.eq_ignore_ascii_case(b))
    }
}

#[inline]
fn convert_simple_error<'a, I: ParseInput<'a>>(e: NomErr<NomError<I>>) -> Error {
    let err_msg = match e {
        NomErr::Incomplete(_) => "Incomplete input".to_string(),
        NomErr::Error(e) | NomErr::Failure(e) => format!("{e:?}"),
    };
    Error::SyntaxError(Box::new(err_msg))
}

#[inline]
fn convert_verbose_error<'a, I: ParseInput<'a>>(input: I, e: NomErr<VerboseError<I>>) -> Error {
    let err_msg = match e {
        nom::Err::Incomplete(_) => "Incomplete input".to_string(),
        nom::Err::Error(e) | nom::Err::Failure(e) => convert_error(input, e),
    };
    Error::SyntaxError(Box::new(err_msg))
}

parse!(
    /// Parse a SQL statement
    fn statement -> 'a Statement<'a> = {
        alt((
            map(query_expr, Statement::Select),
            map(insert, Statement::Insert),
            map(delete, Statement::Delete),
            map(update, Statement::Update),
            map(create, Statement::Create),
            map(drop, Statement::Drop),
            map(use_db, Statement::UseDB),
            map(explain, Statement::Explain),
        ))
    }
);

parse!(
    /// Parse whitespaces and comments, and ignore all.
    /// As we don't have tokenizer before the parsing, we need
    /// to handle comments in parsing.
    /// Here we decided to ignore all comments.
    /// In future, we may support hints.(similar to comment syntax)
    fn spcmt0 -> () = {
        fold_many0(
            alt((
                value((), multispace1),
                value((), comment),
            )),
            || (),
            |_, _| (),
        )
    }
);

parse!(
    /// Parse at least one whitespace or comment.
    fn spcmt1 -> () = {
        fold_many0(
            alt((
                value((), multispace1),
                value((), comment),
            )),
            || (),
            |_, _| (),
        )
    }
);

parse!(
    /// Parse a comment.
    /// ```bnf
    /// <comment> ::= <simple_comment> | <bracketed_comment>
    /// ```
    fn comment -> 'a Comment<'a> = {
        alt((
            map(simple_comment, Comment::simple),
            map(bracketed_comment, Comment::bracketed),
        ))
    }
);

parse!(
    /// Parse a comment in simple format(single line).
    /// ```bnf
    /// <simple_comment> ::= <minus><minus> <comment_characters> <newline>
    /// ```
    fn simple_comment = {
        recognize(
            preceded(
                terminated(tag("--"), take_while1(|c| c == ' ' || c == '\t')), // MySQL uses "--" with at least one whitespace followed
                alt((
                    preceded(take_till(|c| c == '\n'), take(1usize)),
                    rest))))
    }
);

parse!(
    /// Parse a comment in bracketed format(multi lines).
    /// ```bnf
    /// <bracketed_comment> ::= <slash><asterisk> [] <asterisk><slash>
    /// ```
    fn bracketed_comment = {
        recognize(
            delimited(
                tag("/*"),
                take_until("*/"),
                tag("*/")),
        )
    }
);

parse!(
    /// Parse table name.
    /// todo: ends with space is not allowed in MySQL
    /// ```bnf
    /// <table_name> ::= [ <schema_qualifier> <period> ] <qualified_identifier>
    /// ```
    fn table_name -> 'a TableName<'a> = {
        map(
            pair(
                terminated(ident, spcmt0),
                opt(preceded(char_sp0('.'), ident)),
            ),
            |(id0, id1)| match id1 {
                Some(tb) => TableName::new(Some(id0), tb),
                None => TableName::new(None, id0),
            },
        )
    }
);

parse!(
    /// Parse set quantifier.
    /// ```bnf
    /// <set_quantifier> ::= DISTINCT | ALL
    /// ```
    fn set_quantifier -> SetQuantifier = {
        alt((
            value(SetQuantifier::All, ident_tag("all")),
            value(SetQuantifier::Distinct, ident_tag("distinct")),
        ))
    }
);

parse!(
    /// Parse derived column.
    /// ```bnf
    /// <derived_col> ::= <value_expr> [ <as_clause> ]
    /// ```
    fn derived_col -> 'a DerivedCol<'a> = {
        alt((
            value(DerivedCol::Asterisk(vec![]), char('*')),
            next(with_input(expr_sp0), |i, (expr_i, expr)| {
                let expr = if let Expr::ColumnRef(colref) = expr {
                    if let Ok((i, _)) = preceded::<_,_,_,E,_,_>(char_sp0('.'), char_sp0('*'))(i) {
                        return Ok((i, DerivedCol::Asterisk(colref)))
                    }
                    Expr::ColumnRef(colref)
                } else {
                    expr
                };
                // handle alias
                // 2021-12-20: Every column has alias, if not present, use the text of expression itself
                match terminated::<_,_,_,E,_,_>(ident_tag("as"), spcmt0)(i) {
                    Ok((i, _)) => {
                        let (i, alias) = cut(alias)(i)?;
                        Ok((i, DerivedCol::new(expr, alias)))
                    }
                    _ => match alias::<'_, I, E>(i) {
                        Ok((i, alias)) => Ok((i, DerivedCol::new(expr, alias))),
                        // no alias avaiable, use auto-aliasing.
                        _ => if let Expr::ColumnRef(cr) = &expr {
                            let col_name = cr.last().expect("column name is not empty").s;
                            Ok((i, DerivedCol::new(expr, Ident::auto_alias(col_name))))
                        } else {
                            // trim whitespaces at end
                            let expr_i: &str = expr_i.into();
                            Ok((i, DerivedCol::new(expr, Ident::auto_alias(expr_i.trim_end()))))
                        }

                    }
                }
            })
        ))
    }
);

parse!(
    /// Parse an identifier.
    /// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
    /// ```bnf
    /// <identifier> ::= <regular_identifier> | <delimited_identifier>
    /// ```
    fn ident -> 'a Ident<'a> = {
        alt((
            map(regular_ident, Ident::regular),
            map(quoted_ident, Ident::quoted),
        ))
    }
);

/// Parse delimited identifier.
/// Different dialect has different delimiter and escape sequence.
fn quoted_ident<'a, I: ParseInput<'a>, E: ParseError<I>>(i: I) -> IResult<I, I, E> {
    let delim = I::ident_quote();
    let escape = I::ident_escape();
    delimited(
        char(delim),
        recognize(fold_many0(
            alt((take_till1(move |c| c == delim), tag(escape))),
            || (),
            |_, _| (),
        )),
        cut(char(delim)),
    )(i)
}

parse!(
    /// Parse alias.
    fn alias -> 'a Ident<'a> = {
        alt((
            next(regular_ident, |i: I, id: I| {
                if is_reserved_keyword(&id) {
                    Err(nom::Err::Error(E::from_error_kind(i, nom::error::ErrorKind::Verify)))
                } else {
                    Ok((i, Ident::regular(id)))
                }
            }),
            next(quoted_ident, |i: I, id| {
                if id.as_bytes()[id.len()-1].is_ascii_whitespace() { // whitespace not allowed at end of alias
                    Err(nom::Err::Error(E::from_error_kind(i, nom::error::ErrorKind::Verify)))
                } else {
                    Ok((i, Ident::quoted(id)))
                }
            }),
        ))
    }
);

/// Parse a regular identifier, according to MySQL spec:
/// [0-9a-zA-Z$_], but at least one non-numeric character.
fn regular_ident<'a, I: ParseInput<'a>, E: ParseError<I>>(i: I) -> IResult<I, I, E> {
    if i.is_empty() {
        return Err(nom::Err::Error(E::from_error_kind(
            i,
            nom::error::ErrorKind::AlphaNumeric,
        )));
    }
    let mut digits = 0usize;
    for (n, &c) in i.as_bytes().iter().enumerate() {
        if c.is_ascii_digit() {
            digits += 1;
        } else if !(c.is_ascii_alphabetic() || c == b'_' || c == b'$') {
            if n == 0 || digits == n {
                // no any char or all digits
                return Err(nom::Err::Error(E::from_error_kind(
                    i,
                    nom::error::ErrorKind::AlphaNumeric,
                )));
            }
            return Ok(i.take_split(n));
        }
    }
    if digits == i.len() {
        return Err(nom::Err::Error(E::from_error_kind(
            i,
            nom::error::ErrorKind::AlphaNumeric,
        )));
    }
    Ok(i.take_split(i.len()))
}

#[inline]
fn is_ident_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$'
}

fn ident_tag<'a, I: ParseInput<'a>, E: ParseError<I>>(
    id: &'static str,
) -> impl Fn(I) -> IResult<I, I, E> + use<I, E> {
    use std::cmp::Ordering;
    move |i: I| {
        let id = id.as_bytes();
        let id_len = id.len();
        let input = i.as_bytes();
        match i.input_len().cmp(&id_len) {
            Ordering::Less => (),
            Ordering::Equal => {
                if input.eq_ignore_ascii_case(id) {
                    return Ok(i.take_split(id_len));
                }
            }
            Ordering::Greater => {
                if input[..id_len].eq_ignore_ascii_case(id) && !is_ident_char(input[id_len]) {
                    return Ok(i.take_split(id_len));
                }
            }
        }
        Err(nom::Err::Error(E::from_error_kind(
            i,
            nom::error::ErrorKind::Tag,
        )))
    }
}

fn preceded_tag<'a, O, I, F, E>(
    id: &'static str,
    mut f: F,
) -> impl FnMut(I) -> IResult<I, O, E> + use<O, I, F, E>
where
    I: ParseInput<'a>,
    E: ParseError<I>,
    F: nom::Parser<I, O, E>,
{
    move |i: I| {
        let (i, _) = terminated(tag_no_case(id), spcmt1)(i)?;
        f.parse(i)
    }
}

fn preceded_ident_tag<'a, O, I, F, E>(
    id: &'static str,
    mut f: F,
) -> impl FnMut(I) -> IResult<I, O, E> + use<O, I, F, E>
where
    I: ParseInput<'a>,
    E: ParseError<I>,
    F: nom::Parser<I, O, E>,
{
    move |i: I| {
        let (i, _) = terminated(ident_tag(id), spcmt0)(i)?;
        f.parse(i)
    }
}

fn preceded_tag2_cut<'a, O, I, F, E>(
    id1: &'static str,
    id2: &'static str,
    mut f: F,
) -> impl FnMut(I) -> IResult<I, O, E> + use<O, I, F, E>
where
    I: ParseInput<'a>,
    E: ParseError<I>,
    F: nom::Parser<I, O, E>,
{
    move |i: I| {
        let (i, _) = terminated(tag_no_case(id1), spcmt1)(i)?;
        let (i, _) = cut(terminated(tag_no_case(id2), spcmt1))(i)?;
        f.parse(i)
    }
}

fn paren_cut<'a, O, I, F, E>(mut f: F) -> impl FnMut(I) -> IResult<I, O, E> + use<O, I, F, E>
where
    I: ParseInput<'a>,
    E: ParseError<I>,
    F: nom::Parser<I, O, E>,
{
    move |i: I| {
        let (i, _) = char_sp0('(')(i)?;
        let (i, out) = f.parse(i)?;
        let (i, _) = cut(char(')'))(i)?;
        Ok((i, out))
    }
}

fn next<I, O1, O2, E, F, G>(mut parser: F, mut f: G) -> impl FnMut(I) -> IResult<I, O2, E>
where
    F: nom::Parser<I, O1, E>,
    G: FnMut(I, O1) -> IResult<I, O2, E>,
{
    move |input: I| {
        let (input, o1) = parser.parse(input)?;
        f(input, o1)
    }
}

/// apply second parser with first parser's success output,
/// cut the error into failure.
fn next_cut<I, O1, O2, E, F, G>(mut parser: F, mut f: G) -> impl FnMut(I) -> IResult<I, O2, E>
where
    I: Clone,
    E: ParseError<I>,
    F: nom::Parser<I, O1, E>,
    G: FnMut(I, I, O1) -> IResult<I, O2, E>,
{
    move |input: I| {
        let (ni, o1) = parser.parse(input.clone())?;
        match f(input, ni, o1) {
            Err(nom::Err::Error(e)) => Err(nom::Err::Failure(e)),
            rest => rest,
        }
    }
}

fn with_input<I, O, E, F>(mut parser: F) -> impl FnMut(I) -> IResult<I, (I, O), E>
where
    I: Clone + nom::Offset + nom::Slice<std::ops::RangeTo<usize>>,
    E: ParseError<I>,
    F: nom::Parser<I, O, E>,
{
    move |input: I| {
        let i = input.clone();
        match parser.parse(i) {
            Ok((i, o)) => {
                let index = input.offset(&i);
                Ok((i, (input.slice(..index), o)))
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::Ansi;
    use nom::error::{convert_error, Error, VerboseError};

    macro_rules! aliased_expr {
        ( $expr:expr => $lit:literal ) => {
            DerivedCol::new($expr, Ident::regular($lit))
        };
        ( $expr:expr , $lit:literal ) => {
            DerivedCol::new($expr, Ident::auto_alias($lit))
        };
    }

    #[test]
    fn test_parse_identifier() -> anyhow::Result<()> {
        // success cases
        for c in vec![
            ("a", ("", Ident::regular("a"))),
            ("abc", ("", Ident::regular("abc"))),
            ("abc123", ("", Ident::regular("abc123"))),
            ("user_info", ("", Ident::regular("user_info"))),
            ("X", ("", Ident::regular("X"))),
            ("\"\"", ("", Ident::quoted(""))),
            ("\"abc\"", ("", Ident::quoted("abc"))),
            ("\"abc\"\"def\"", ("", Ident::quoted("abc\"\"def"))),
        ] {
            let res = match ident::<'_, _, VerboseError<_>>(Ansi(c.0)) {
                Ok(res) => res,
                Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                    let err_str = convert_error(Ansi(c.0), e);
                    panic!("\nerr is {}", err_str);
                }
                _ => unreachable!(),
            };
            assert_eq!(&*res.0, c.1 .0);
            assert_eq!(res.1, c.1 .1);
        }
        // error cases
        for c in vec!["", "1", "-", "\"\"\""] {
            let res = ident::<'_, _, Error<_>>(Ansi(c));
            if res.is_ok() {
                println!("input={}", c);
                panic!()
            }
        }
        Ok(())
    }

    #[test]
    fn test_parse_table_name() -> anyhow::Result<()> {
        // success cases
        for c in vec![
            ("a", ("", TableName::new(None, "a".into()))),
            ("a1", ("", TableName::new(None, "a1".into()))),
            ("\"a\"", ("", TableName::new(None, Ident::quoted("a")))),
            (
                "\"a\"\"b\"",
                ("", TableName::new(None, Ident::quoted("a\"\"b"))),
            ),
            ("a.a", ("", TableName::new(Some("a".into()), "a".into()))),
            (
                "\"a\".a",
                ("", TableName::new(Some(Ident::quoted("a")), "a".into())),
            ),
            (
                "\"a\".\"a\"",
                (
                    "",
                    TableName::new(Some(Ident::quoted("a")), Ident::quoted("a")),
                ),
            ),
        ] {
            let res = table_name::<'_, _, Error<_>>(Ansi(c.0))?;
            assert_eq!(res.0, Ansi(c.1 .0));
            assert_eq!(res.1, c.1 .1);
        }
        Ok(())
    }

    #[test]
    fn test_parse_derived_column() -> anyhow::Result<()> {
        for c in vec![
            ("1", ("", aliased_expr!(Expr::numeric_lit("1"), "1"))),
            ("1 as a", ("", aliased_expr!(Expr::numeric_lit("1") => "a"))),
            // todo
        ] {
            let res = derived_col::<'_, _, Error<_>>(Ansi(c.0))?;
            assert_eq!(res.0, Ansi(c.1 .0));
            assert_eq!(res.1, c.1 .1);
        }
        Ok(())
    }

    #[test]
    fn test_parse_comment() -> anyhow::Result<()> {
        for c in vec![
            ("-- \n", ("", Comment::Simple("-- \n"))),
            ("--\t\n", ("", Comment::Simple("--\t\n"))),
            ("/**/", ("", Comment::Bracketed("/**/"))),
            ("/* note */", ("", Comment::Bracketed("/* note */"))),
            ("/* \n\t */", ("", Comment::Bracketed("/* \n\t */"))),
        ] {
            let res = comment::<'_, _, Error<_>>(Ansi(c.0))?;
            assert_eq!(res.0, Ansi(c.1 .0));
            assert_eq!(res.1, c.1 .1);
        }
        Ok(())
    }

    #[test]
    fn test_reserved_keywords() {
        for kw in vec![
            "where",
            "Group",
            "having",
            "Order",
            "LiMiT",
            "CrosS",
            "Natural",
            "INNer",
            "OutEr",
            "left",
            "rIGHT",
            "fUll",
            "JOin",
            "on",
            "USiNG",
            "uniON",
            "Except",
            "INTERsECT",
            "winDOW",
            // infix operator
            "and",
            "or",
            "xor",
            "is",
            "like",
            "regexp",
            "in",
            "between",
        ] {
            if !is_reserved_keyword(kw) {
                println!("keyword not support: {}", kw);
                panic!()
            }
        }
    }

    #[test]
    fn test_parse_asterisk() -> anyhow::Result<()> {
        for c in vec![
            ("*", ("", DerivedCol::Asterisk(vec![]))),
            ("a.*", ("", DerivedCol::Asterisk(vec!["a".into()]))),
            (
                "a.b.*",
                ("", DerivedCol::Asterisk(vec!["a".into(), "b".into()])),
            ),
            (
                "a.\"b\".*",
                (
                    "",
                    DerivedCol::Asterisk(vec!["a".into(), Ident::quoted("b")]),
                ),
            ),
        ] {
            let res = derived_col::<'_, _, Error<_>>(Ansi(c.0))?;
            assert_eq!(res.0, Ansi(c.1 .0));
            assert_eq!(res.1, c.1 .1);
        }
        Ok(())
    }
}
