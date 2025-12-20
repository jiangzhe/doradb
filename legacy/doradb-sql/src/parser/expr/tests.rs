use super::*;
use crate::parser::dialect::Ansi;
use nom::error::Error;

macro_rules! col {
    ( $($lit:literal),* ) => {
        Expr::column_ref(vec![ $( ($lit).into() ),* ])
    }
}

macro_rules! aliased_expr {
    ( $expr:expr => $lit:literal ) => {
        DerivedCol::new($expr, Ident::regular($lit))
    };
    ( $expr:expr , $lit:literal ) => {
        DerivedCol::new($expr, Ident::auto_alias($lit))
    };
}

#[test]
fn test_parse_column_ref() -> anyhow::Result<()> {
    for c in vec![
        ("a", Expr::column_ref(vec!["a".into()])),
        ("a.b", Expr::column_ref(vec!["a".into(), "b".into()])),
        (
            "a.b.c",
            Expr::column_ref(vec!["a".into(), "b".into(), "c".into()]),
        ),
        (
            "a. b .c",
            Expr::column_ref(vec!["a".into(), "b".into(), "c".into()]),
        ),
        (
            "\"a\". b .c",
            Expr::column_ref(vec![Ident::quoted("a"), "b".into(), "c".into()]),
        ),
        (
            "\"a\". b .\"c\"",
            Expr::column_ref(vec![Ident::quoted("a"), "b".into(), Ident::quoted("c")]),
        ),
        (
            "\"a\". \"b\" .c",
            Expr::column_ref(vec![Ident::quoted("a"), Ident::quoted("b"), "c".into()]),
        ),
        (
            "\"a\". \"b\" .\"c\"",
            Expr::column_ref(vec![
                Ident::quoted("a"),
                Ident::quoted("b"),
                Ident::quoted("c"),
            ]),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_precedence() -> anyhow::Result<()> {
    use std::ops::{Add, Div, Mul, Sub};

    for c in vec![
        ("+1", Expr::numeric_lit("+1")),
        ("+ 1", Expr::numeric_lit("1")),
        ("-1", Expr::numeric_lit("-1")),
        (
            "1+2",
            Expr::add(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 + 2",
            Expr::add(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1-2",
            Expr::sub(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1*2",
            Expr::mul(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1/2",
            Expr::div(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1+2-3",
            Expr::sub(
                Expr::add(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
                Expr::numeric_lit("3"),
            ),
        ),
        (
            "1-2+3",
            Expr::add(
                Expr::sub(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
                Expr::numeric_lit("3"),
            ),
        ),
        (
            "1*2/3",
            Expr::div(
                Expr::mul(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
                Expr::numeric_lit("3"),
            ),
        ),
        (
            "1/2*3",
            Expr::mul(
                Expr::div(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
                Expr::numeric_lit("3"),
            ),
        ),
        (
            "1+2*3",
            Expr::add(
                Expr::numeric_lit("1"),
                Expr::mul(Expr::numeric_lit("2"), Expr::numeric_lit("3")),
            ),
        ),
        (
            "1+2*3-4",
            Expr::sub(
                Expr::add(
                    Expr::numeric_lit("1"),
                    Expr::mul(Expr::numeric_lit("2"), Expr::numeric_lit("3")),
                ),
                Expr::numeric_lit("4"),
            ),
        ),
        (
            "1+2*3/4",
            Expr::add(
                Expr::numeric_lit("1"),
                Expr::div(
                    Expr::mul(Expr::numeric_lit("2"), Expr::numeric_lit("3")),
                    Expr::numeric_lit("4"),
                ),
            ),
        ),
        (
            "1+(2-3)",
            Expr::add(
                Expr::numeric_lit("1"),
                Expr::sub(Expr::numeric_lit("2"), Expr::numeric_lit("3")),
            ),
        ),
        (
            "1*(2+3)",
            Expr::mul(
                Expr::numeric_lit("1"),
                Expr::add(Expr::numeric_lit("2"), Expr::numeric_lit("3")),
            ),
        ),
        (
            "1*(2+3)",
            Expr::mul(
                Expr::numeric_lit("1"),
                Expr::add(Expr::numeric_lit("2"), Expr::numeric_lit("3")),
            ),
        ),
        (
            "1 and 2 and 3",
            Expr::pred_conj(vec![
                Expr::numeric_lit("1"),
                Expr::numeric_lit("2"),
                Expr::numeric_lit("3"),
            ]),
        ),
        (
            "1 and 2 or 3",
            Expr::logical_or(
                Expr::logical_and(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
                Expr::numeric_lit("3"),
            ),
        ),
        (
            "1 or 2 and 3",
            Expr::logical_or(
                Expr::numeric_lit("1"),
                Expr::logical_and(Expr::numeric_lit("2"), Expr::numeric_lit("3")),
            ),
        ),
        (
            "1 and 2 or 3 and 4",
            Expr::logical_or(
                Expr::logical_and(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
                Expr::logical_and(Expr::numeric_lit("3"), Expr::numeric_lit("4")),
            ),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_aggr_func() -> anyhow::Result<()> {
    for c in vec![
        ("count(*)", Expr::count_asterisk()),
        ("count(1)", Expr::count(Expr::numeric_lit("1"))),
        ("count(a)", Expr::count(Expr::ColumnRef(vec!["a".into()]))),
        (
            "count(distinct a)",
            Expr::count_distinct(Expr::ColumnRef(vec!["a".into()])),
        ),
        ("sum(a)", Expr::sum(Expr::ColumnRef(vec!["a".into()]))),
        (
            "sum(distinct a)",
            Expr::sum_distinct(Expr::ColumnRef(vec!["a".into()])),
        ),
        ("avg(a)", Expr::avg(Expr::ColumnRef(vec!["a".into()]))),
        ("min(a)", Expr::min(Expr::ColumnRef(vec!["a".into()]))),
        ("max(a)", Expr::max(Expr::ColumnRef(vec!["a".into()]))),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_charstr() -> anyhow::Result<()> {
    for c in vec![
        (
            "''",
            Expr::charstr_lit(CharStr {
                charset: None,
                first: "",
                rest: vec![],
            }),
        ),
        (
            "'123'",
            Expr::charstr_lit(CharStr {
                charset: None,
                first: "123",
                rest: vec![],
            }),
        ),
        (
            "'1''23'",
            Expr::charstr_lit(CharStr {
                charset: None,
                first: "1''23",
                rest: vec![],
            }),
        ),
        (
            "'1' '23'",
            Expr::charstr_lit(CharStr {
                charset: None,
                first: "1",
                rest: vec!["23"],
            }),
        ),
        (
            "'1'-- hello\n '23'",
            Expr::charstr_lit(CharStr {
                charset: None,
                first: "1",
                rest: vec!["23"],
            }),
        ),
        (
            "'1' '23'\t '4'",
            Expr::charstr_lit(CharStr {
                charset: None,
                first: "1",
                rest: vec!["23", "4"],
            }),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_bitstr() -> anyhow::Result<()> {
    for c in vec![
        ("B''", Expr::bitstr_lit("")),
        ("b''", Expr::bitstr_lit("")),
        ("B'01'", Expr::bitstr_lit("01")),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_hexstr() -> anyhow::Result<()> {
    for c in vec![
        ("X''", Expr::hexstr_lit("")),
        ("x''", Expr::hexstr_lit("")),
        ("X'c1'", Expr::hexstr_lit("c1")),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_bool_and_null() -> anyhow::Result<()> {
    for c in vec![
        ("true", Expr::bool_lit(true)),
        ("True", Expr::bool_lit(true)),
        ("TRUE", Expr::bool_lit(true)),
        ("tRUE", Expr::bool_lit(true)),
        ("false", Expr::bool_lit(false)),
        ("null", Expr::null_lit()),
        ("Null", Expr::null_lit()),
        ("NULL", Expr::null_lit()),
        ("nULL", Expr::null_lit()),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_datetime() -> anyhow::Result<()> {
    for c in vec![
        ("date''", Expr::date_lit("")),
        ("Date''", Expr::date_lit("")),
        ("DATE''", Expr::date_lit("")),
        ("dATE''", Expr::date_lit("")),
        ("date ''", Expr::date_lit("")),
        ("DATE '2000-01-01'", Expr::date_lit("2000-01-01")),
        ("time''", Expr::time_lit("")),
        ("Time''", Expr::time_lit("")),
        ("TIME''", Expr::time_lit("")),
        ("tIME''", Expr::time_lit("")),
        ("time ''", Expr::time_lit("")),
        ("TIME '01:02:03'", Expr::time_lit("01:02:03")),
        ("timestamp''", Expr::timestamp_lit("")),
        ("Timestamp''", Expr::timestamp_lit("")),
        ("TIMESTAMP''", Expr::timestamp_lit("")),
        ("tIMESTAMP''", Expr::timestamp_lit("")),
        ("timestamp ''", Expr::timestamp_lit("")),
        (
            "TIMESTAMP '2021-12-31 01:02:03'",
            Expr::timestamp_lit("2021-12-31 01:02:03"),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_interval() -> anyhow::Result<()> {
    for c in vec![
        (
            "interval '1' year",
            Expr::interval_lit("1", DatetimeUnit::Year),
        ),
        (
            "interval'1'year",
            Expr::interval_lit("1", DatetimeUnit::Year),
        ),
        (
            "INTERVAL '1' YEAR",
            Expr::interval_lit("1", DatetimeUnit::Year),
        ),
        (
            "interval '1' quarter",
            Expr::interval_lit("1", DatetimeUnit::Quarter),
        ),
        (
            "interval '1' month",
            Expr::interval_lit("1", DatetimeUnit::Month),
        ),
        (
            "interval '1' week",
            Expr::interval_lit("1", DatetimeUnit::Week),
        ),
        (
            "interval '1' day",
            Expr::interval_lit("1", DatetimeUnit::Day),
        ),
        (
            "interval '1' hour",
            Expr::interval_lit("1", DatetimeUnit::Hour),
        ),
        (
            "interval '1' minute",
            Expr::interval_lit("1", DatetimeUnit::Minute),
        ),
        (
            "interval '1' second",
            Expr::interval_lit("1", DatetimeUnit::Second),
        ),
        (
            "interval '1' microsecond",
            Expr::interval_lit("1", DatetimeUnit::Microsecond),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_case_when() -> anyhow::Result<()> {
    for c in vec![
        (
            "case 1 when 2 then 3 end",
            Expr::case_when(
                Some(Box::new(Expr::numeric_lit("1"))),
                vec![(Expr::numeric_lit("2"), Expr::numeric_lit("3"))],
                None,
            ),
        ),
        (
            "case 1 when 2 then 3 else 4 end",
            Expr::case_when(
                Some(Box::new(Expr::numeric_lit("1"))),
                vec![(Expr::numeric_lit("2"), Expr::numeric_lit("3"))],
                Some(Box::new(Expr::numeric_lit("4"))),
            ),
        ),
        (
            "case 1 when 2 then 3 when 4 then 5 else 6 end",
            Expr::case_when(
                Some(Box::new(Expr::numeric_lit("1"))),
                vec![
                    (Expr::numeric_lit("2"), Expr::numeric_lit("3")),
                    (Expr::numeric_lit("4"), Expr::numeric_lit("5")),
                ],
                Some(Box::new(Expr::numeric_lit("6"))),
            ),
        ),
        (
            "case when 1 then 2 end",
            Expr::case_when(
                None,
                vec![(Expr::numeric_lit("1"), Expr::numeric_lit("2"))],
                None,
            ),
        ),
        (
            "case case when 1 then 2 end when 3 then 4 end",
            Expr::case_when(
                Some(Box::new(Expr::case_when(
                    None,
                    vec![(Expr::numeric_lit("1"), Expr::numeric_lit("2"))],
                    None,
                ))),
                vec![(Expr::numeric_lit("3"), Expr::numeric_lit("4"))],
                None,
            ),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_binary_expr() -> anyhow::Result<()> {
    for c in vec![
        ("1+2", Expr::numeric_lit("1") + Expr::numeric_lit("2")),
        ("1-2", Expr::numeric_lit("1") - Expr::numeric_lit("2")),
        ("1*2", Expr::numeric_lit("1") * Expr::numeric_lit("2")),
        ("1/2", Expr::numeric_lit("1") / Expr::numeric_lit("2")),
        (
            "1=2",
            Expr::cmp_eq(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1<=>2",
            Expr::pred_safeeq(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1!=2",
            Expr::cmp_ne(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1<>2",
            Expr::cmp_ne(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1>=2",
            Expr::cmp_ge(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1>2",
            Expr::cmp_gt(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1<=2",
            Expr::cmp_le(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1<2",
            Expr::cmp_lt(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 like 2",
            Expr::pred_like(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 not like 2",
            Expr::pred_nlike(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 regexp 2",
            Expr::pred_regexp(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 not regexp 2",
            Expr::pred_nregexp(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 and 2",
            Expr::logical_and(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 or 2",
            Expr::logical_or(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 xor 2",
            Expr::logical_xor(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1 xor 2 xor 3",
            Expr::pred_xor(vec![
                Expr::numeric_lit("1"),
                Expr::numeric_lit("2"),
                Expr::numeric_lit("3"),
            ]),
        ),
        (
            "1&2",
            Expr::bit_and(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1|2",
            Expr::bit_or(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1^2",
            Expr::bit_xor(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1<<2",
            Expr::bit_shl(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
        (
            "1>>2",
            Expr::bit_shr(Expr::numeric_lit("1"), Expr::numeric_lit("2")),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_unary() -> anyhow::Result<()> {
    for c in vec![
        ("-a", -Expr::column_ref(vec!["a".into()])),
        (
            "not a",
            Expr::logical_not(Expr::column_ref(vec!["a".into()])),
        ),
        ("~a", Expr::bit_inv(Expr::column_ref(vec!["a".into()]))),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_scalar_subquery() -> anyhow::Result<()> {
    for c in vec![
        (
            "(select 1)",
            Expr::scalar_subquery(QueryExpr {
                with: None,
                query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
            }),
        ),
        (
            "(select distinct 1)",
            Expr::scalar_subquery(QueryExpr {
                with: None,
                query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
            }),
        ),
        (
            "(select 1 from a)",
            Expr::scalar_subquery(QueryExpr {
                with: None,
                query: Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![aliased_expr!(Expr::numeric_lit("1"), "1")],
                    from: vec![TableRef::primitive(TablePrimitive::Named("a".into(), None))],
                    filter: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            }),
        ),
        (
            "(select distinct c0 from a)",
            Expr::scalar_subquery(QueryExpr {
                with: None,
                query: Query::table(SelectTable {
                    q: SetQuantifier::Distinct,
                    cols: vec![aliased_expr!(Expr::column_ref(vec!["c0".into()]), "c0")],
                    from: vec![TableRef::primitive(TablePrimitive::Named("a".into(), None))],
                    filter: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            }),
        ),
        (
            "1 = (select 1)",
            Expr::cmp_eq(
                Expr::numeric_lit("1"),
                Expr::scalar_subquery(QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                }),
            ),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_exists_subquery() -> anyhow::Result<()> {
    for c in vec![
        (
            "exists(select 1)",
            Expr::exists(QueryExpr {
                with: None,
                query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
            }),
        ),
        (
            "exists (select 1 from a)",
            Expr::exists(QueryExpr {
                with: None,
                query: Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![aliased_expr!(Expr::numeric_lit("1"), "1")],
                    from: vec![TableRef::primitive(TablePrimitive::Named("a".into(), None))],
                    filter: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            }),
        ),
        (
            "exists (select 1 from a where c0 = c1)",
            Expr::exists(QueryExpr {
                with: None,
                query: Query::table(SelectTable {
                    q: SetQuantifier::All,
                    cols: vec![aliased_expr!(Expr::numeric_lit("1"), "1")],
                    from: vec![TableRef::primitive(TablePrimitive::Named("a".into(), None))],
                    filter: Some(Expr::cmp_eq(col!("c0"), col!("c1"))),
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            }),
        ),
        (
            "not exists (select 1)",
            Expr::logical_not(Expr::exists(QueryExpr {
                with: None,
                query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
            })),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_between() -> anyhow::Result<()> {
    for c in vec![
        (
            "1 between 2 and 3",
            Expr::pred_btw(
                Expr::numeric_lit("1"),
                Expr::numeric_lit("2"),
                Expr::numeric_lit("3"),
            ),
        ),
        (
            "1 not between 2 and 3",
            Expr::pred_nbtw(
                Expr::numeric_lit("1"),
                Expr::numeric_lit("2"),
                Expr::numeric_lit("3"),
            ),
        ),
        (
            "1 between 2+3 and 4",
            Expr::pred_btw(
                Expr::numeric_lit("1"),
                Expr::numeric_lit("2") + Expr::numeric_lit("3"),
                Expr::numeric_lit("4"),
            ),
        ),
        (
            "true and 1 between 2 and 3",
            Expr::logical_and(
                Expr::bool_lit(true),
                Expr::pred_btw(
                    Expr::numeric_lit("1"),
                    Expr::numeric_lit("2"),
                    Expr::numeric_lit("3"),
                ),
            ),
        ),
        (
            "1 + 2 between 3 and 4",
            Expr::pred_btw(
                Expr::numeric_lit("1") + Expr::numeric_lit("2"),
                Expr::numeric_lit("3"),
                Expr::numeric_lit("4"),
            ),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_quant_cmp() -> anyhow::Result<()> {
    for c in vec![
        (
            "1 = ANY(select 1)",
            Expr::pred_quant_cmp(
                CompareOp::Equal,
                CmpQuantifier::Any,
                Expr::numeric_lit("1"),
                QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                },
            ),
        ),
        (
            "1 != all(select 1)",
            Expr::pred_quant_cmp(
                CompareOp::NotEqual,
                CmpQuantifier::All,
                Expr::numeric_lit("1"),
                QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                },
            ),
        ),
        (
            "1 >=all(select 1)",
            Expr::pred_quant_cmp(
                CompareOp::GreaterEqual,
                CmpQuantifier::All,
                Expr::numeric_lit("1"),
                QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                },
            ),
        ),
        (
            "1>any(select 1)",
            Expr::pred_quant_cmp(
                CompareOp::Greater,
                CmpQuantifier::Any,
                Expr::numeric_lit("1"),
                QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                },
            ),
        ),
        (
            "1<any(select 1)",
            Expr::pred_quant_cmp(
                CompareOp::Less,
                CmpQuantifier::Any,
                Expr::numeric_lit("1"),
                QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                },
            ),
        ),
        (
            "1<=all(select 1)",
            Expr::pred_quant_cmp(
                CompareOp::LessEqual,
                CmpQuantifier::All,
                Expr::numeric_lit("1"),
                QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                },
            ),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_is() -> anyhow::Result<()> {
    for c in vec![
        ("1 is null", Expr::pred_is_null(Expr::numeric_lit("1"))),
        ("1 is not null", Expr::pred_not_null(Expr::numeric_lit("1"))),
        ("1 is true", Expr::pred_is_true(Expr::numeric_lit("1"))),
        ("null is not true", Expr::pred_not_true(Expr::null_lit())),
        ("1 is false", Expr::pred_is_false(Expr::numeric_lit("1"))),
        (
            "a is not false",
            Expr::pred_not_false(Expr::column_ref(vec!["a".into()])),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_in() -> anyhow::Result<()> {
    for c in vec![
        (
            "1 in (1)",
            Expr::pred_in_values(Expr::numeric_lit("1"), vec![Expr::numeric_lit("1")]),
        ),
        (
            "1 not in (1)",
            Expr::pred_nin_values(Expr::numeric_lit("1"), vec![Expr::numeric_lit("1")]),
        ),
        (
            "1 in (1,2,3)",
            Expr::pred_in_values(
                Expr::numeric_lit("1"),
                vec![
                    Expr::numeric_lit("1"),
                    Expr::numeric_lit("2"),
                    Expr::numeric_lit("3"),
                ],
            ),
        ),
        (
            "a in (select 1)",
            Expr::pred_in_subquery(
                Expr::column_ref(vec!["a".into()]),
                QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                },
            ),
        ),
        (
            "a not in (select 1)",
            Expr::pred_nin_subquery(
                Expr::column_ref(vec!["a".into()]),
                QueryExpr {
                    with: None,
                    query: Query::row(vec![aliased_expr!(Expr::numeric_lit("1"), "1")]),
                },
            ),
        ),
        (
            "a in (select b from t)",
            Expr::pred_in_subquery(
                Expr::column_ref(vec!["a".into()]),
                QueryExpr {
                    with: None,
                    query: Query::table(SelectTable {
                        q: SetQuantifier::All,
                        cols: vec![aliased_expr!(Expr::column_ref(vec!["b".into()]), "b")],
                        from: vec![TableRef::primitive(TablePrimitive::Named("t".into(), None))],
                        filter: None,
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    }),
                },
            ),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

#[test]
fn test_parse_builtin_func() -> anyhow::Result<()> {
    for c in vec![
        (
            "extract(year from a)",
            Expr::func(
                FuncType::Extract,
                vec![
                    Expr::FuncArg(ConstArg::DatetimeUnit(DatetimeUnit::Year)),
                    Expr::column_ref(vec!["a".into()]),
                ],
            ),
        ),
        (
            "substring(a, 1)",
            Expr::func(
                FuncType::Substring,
                vec![
                    Expr::column_ref(vec!["a".into()]),
                    Expr::numeric_lit("1"),
                    Expr::FuncArg(ConstArg::None),
                ],
            ),
        ),
        (
            "substring(a, 1, 3)",
            Expr::func(
                FuncType::Substring,
                vec![
                    Expr::column_ref(vec!["a".into()]),
                    Expr::numeric_lit("1"),
                    Expr::numeric_lit("3"),
                ],
            ),
        ),
        (
            "substring(a from 1)",
            Expr::func(
                FuncType::Substring,
                vec![
                    Expr::column_ref(vec!["a".into()]),
                    Expr::numeric_lit("1"),
                    Expr::FuncArg(ConstArg::None),
                ],
            ),
        ),
        (
            "substring(a from 1 for 3)",
            Expr::func(
                FuncType::Substring,
                vec![
                    Expr::column_ref(vec!["a".into()]),
                    Expr::numeric_lit("1"),
                    Expr::numeric_lit("3"),
                ],
            ),
        ),
    ] {
        check_expr(c)?;
    }
    Ok(())
}

// this case is taken from https://github.com/datafuselabs/databend/issues/3696
// to check whether parsing a very long expression is supported
#[test]
fn test_parse_very_long_expr() -> anyhow::Result<()> {
    let sql = r#"1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 
    11 + 12 + 13 + 14 + 15 + 16 + 17 + 18 + 19 + 20 + 
    21 + 22 + 23 + 24 + 25 + 26 + 27 + 28 + 29 + 30 + 
    31 + 32 + 33 + 34 + 35 + 36 + 37 + 38 + 39 + 40 + 
    41 + 42 + 43 + 44 + 45 + 46 + 47 + 48 + 49 + 50 + 
    51 + 52 + 53 + 54 + 55 + 56 + 57 + 58 + 59 + 60 + 
    61 + 62 + 63 + 64 + 65 + 66 + 67 + 68 + 69 + 70 + 
    71 + 72 + 73 + 74 + 75 + 76 + 77 + 78 + 79 + 80 + 
    81 + 82 + 83 + 84 + 85 + 86 + 87 + 88 + 89 + 90 + 
    91 + 92 + 93 + 94 + 95 + 96 + 97 + 98 + 99 + 100 + 
    101 + 102 + 103 + 104 + 105 + 106 + 107 + 108 + 109 + 110 + 
    111 + 112 + 113 + 114 + 115 + 116 + 117 + 118 + 119 + 120 + 
    121 + 122 + 123 + 124 + 125 + 126 + 127 + 128 + 129 + 130 + 
    131 + 132 + 133 + 134 + 135 + 136 + 137 + 138 + 139 + 140 + 
    141 + 142 + 143 + 144 + 145 + 146 + 147 + 148 + 149 + 150 + 
    151 + 152 + 153 + 154 + 155 + 156 + 157 + 158 + 159 + 160 + 
    161 + 162 + 163 + 164 + 165 + 166 + 167 + 168 + 169 + 170 + 
    171 + 172 + 173 + 174 + 175 + 176 + 177 + 178 + 179 + 180 + 
    181 + 182 + 183 + 184 + 185 + 186 + 187 + 188 + 189 + 190 + 
    191 + 192 + 193 + 194 + 195 + 196 + 197 + 198 + 199 + 200 + 
    201 + 202 + 203 + 204 + 205 + 206 + 207 + 208 + 209 + 210 + 
    211 + 212 + 213 + 214 + 215 + 216 + 217 + 218 + 219 + 220 + 
    221 + 222 + 223 + 224 + 225 + 226 + 227 + 228 + 229 + 230 + 
    231 + 232 + 233 + 234 + 235 + 236 + 237 + 238 + 239 + 240 + 
    241 + 242 + 243 + 244 + 245 + 246 + 247 + 248 + 249 + 250 + 
    251 + 252 + 253 + 254 + 255 + 256 + 
    1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 
    11 + 12 + 13 + 14 + 15 + 16 + 17 + 18 + 19 + 20 + 
    21 + 22 + 23 + 24 + 25 + 26 + 27 + 28 + 29 + 30 + 
    31 + 32 + 33 + 34 + 35 + 36 + 37 + 38 + 39 + 40 + 
    41 + 42 + 43 + 44 + 45 + 46 + 47 + 48 + 49 + 50 + 
    51 + 52 + 53 + 54 + 55 + 56 + 57 + 58 + 59 + 60 + 
    61 + 62 + 63 + 64 + 65 + 66 + 67 + 68 + 69 + 70 + 
    71 + 72 + 73 + 74 + 75 + 76 + 77 + 78 + 79 + 80 + 
    81 + 82 + 83 + 84 + 85 + 86 + 87 + 88 + 89 + 90 + 
    91 + 92 + 93 + 94 + 95 + 96 + 97 + 98 + 99 + 100 + 
    101 + 102 + 103 + 104 + 105 + 106 + 107 + 108 + 109 + 110 + 
    111 + 112 + 113 + 114 + 115 + 116 + 117 + 118 + 119 + 120 + 
    121 + 122 + 123 + 124 + 125 + 126 + 127 + 128 + 129 + 130 + 
    131 + 132 + 133 + 134 + 135 + 136 + 137 + 138 + 139 + 140 + 
    141 + 142 + 143 + 144 + 145 + 146 + 147 + 148 + 149 + 150 + 
    151 + 152 + 153 + 154 + 155 + 156 + 157 + 158 + 159 + 160 + 
    161 + 162 + 163 + 164 + 165 + 166 + 167 + 168 + 169 + 170 + 
    171 + 172 + 173 + 174 + 175 + 176 + 177 + 178 + 179 + 180 + 
    181 + 182 + 183 + 184 + 185 + 186 + 187 + 188 + 189 + 190 + 
    191 + 192 + 193 + 194 + 195 + 196 + 197 + 198 + 199 + 200 + 
    201 + 202 + 203 + 204 + 205 + 206 + 207 + 208 + 209 + 210 + 
    211 + 212 + 213 + 214 + 215 + 216 + 217 + 218 + 219 + 220 + 
    221 + 222 + 223 + 224 + 225 + 226 + 227 + 228 + 229 + 230 + 
    231 + 232 + 233 + 234 + 235 + 236 + 237 + 238 + 239 + 240 + 
    241 + 242 + 243 + 244 + 245 + 246 + 247 + 248 + 249 + 250 + 
    251 + 252 + 253 + 254 + 255 + 256"#;
    let (i, _) = expr_sp0::<'_, _, Error<_>>(Ansi(sql))?;
    assert!(i.is_empty());
    Ok(())
}

#[inline]
fn check_expr(c: (&'static str, Expr<'_>)) -> anyhow::Result<()> {
    let (i, o) = expr_sp0::<'_, _, Error<_>>(Ansi(c.0))?;
    assert!(i.is_empty());
    assert_eq!(c.1, o);
    Ok(())
}
