use xngin_frontend::parser::dialect::MySQL;
use xngin_frontend::parser::parse_query_verbose;
use xngin_frontend::pretty::{PrettyConf, PrettyFormat};

macro_rules! check_sql {
    ($filename:literal) => {
        let sql = include_str!($filename).trim();
        let (i, res) = match parse_query_verbose(MySQL(sql)) {
            Ok((i, query)) => (i, query),
            Err(err) => {
                eprintln!("Failed to parse query:\n{}", err);
                panic!()
            }
        };
        assert!(i.is_empty());
        let actual = res.pretty_string(PrettyConf::default()).unwrap();
        assert_eq!(sql, actual);
    };
}

#[test]
fn parse_pretty1() {
    check_sql!("../../sql/pretty1.sql");
}

#[test]
fn parse_pretty2() {
    check_sql!("../../sql/pretty2.sql");
}

#[test]
fn parse_pretty3() {
    check_sql!("../../sql/pretty3.sql");
}

#[test]
fn parse_pretty4() {
    check_sql!("../../sql/pretty4.sql");
}

#[test]
fn parse_pretty5() {
    check_sql!("../../sql/pretty5.sql");
}

#[test]
fn parse_pretty6() {
    check_sql!("../../sql/pretty6.sql");
}

#[test]
fn parse_pretty7() {
    check_sql!("../../sql/pretty7.sql");
}

#[test]
fn parse_pretty8() {
    check_sql!("../../sql/pretty8.sql");
}

#[test]
fn parse_pretty9() {
    check_sql!("../../sql/pretty9.sql");
}

#[test]
fn parse_pretty10() {
    check_sql!("../../sql/pretty10.sql");
}

#[test]
fn parse_pretty11() {
    check_sql!("../../sql/pretty11.sql");
}
