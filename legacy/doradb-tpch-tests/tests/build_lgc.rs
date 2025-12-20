use doradb_plan::explain::{Explain, ExplainConf};
use doradb_plan::lgc::LgcPlan;
use doradb_sql::parser::dialect::Ansi;
use doradb_sql::parser::parse_query_verbose;
use doradb_tpch_tests::tpch_catalog;

macro_rules! check_build {
    ( $filename:literal ) => {
        let cat = tpch_catalog();
        let sql = include_str!($filename);
        let qry = parse_query_verbose(Ansi(sql)).unwrap();
        let plan = LgcPlan::new(&cat, "tpch", &qry).unwrap();
        let mut s = String::new();
        assert!(plan.explain(&mut s, &ExplainConf::default()).is_ok());
        println!("Explain plan:\n{}", s)
    };
}

#[test]
fn build_tpch1() {
    check_build!("../../sql/tpch1.sql");
}

#[test]
fn build_tpch2() {
    check_build!("../../sql/tpch2.sql");
}

#[test]
fn build_tpch3() {
    check_build!("../../sql/tpch3.sql");
}

#[test]
fn build_tpch4() {
    check_build!("../../sql/tpch4.sql");
}

#[test]
fn build_tpch5() {
    check_build!("../../sql/tpch5.sql");
}

#[test]
fn build_tpch6() {
    check_build!("../../sql/tpch6.sql");
}

#[test]
fn build_tpch7() {
    check_build!("../../sql/tpch7.sql");
}

#[test]
fn build_tpch8() {
    check_build!("../../sql/tpch8.sql");
}

#[test]
fn build_tpch9() {
    check_build!("../../sql/tpch9.sql");
}

#[test]
fn build_tpch10() {
    check_build!("../../sql/tpch10.sql");
}

#[test]
fn build_tpch11() {
    check_build!("../../sql/tpch11.sql");
}

#[test]
fn build_tpch12() {
    check_build!("../../sql/tpch12.sql");
}

#[test]
fn build_tpch13() {
    check_build!("../../sql/tpch13.sql");
}

#[test]
fn build_tpch14() {
    check_build!("../../sql/tpch14.sql");
}

#[test]
fn build_tpch15() {
    check_build!("../../sql/tpch15.sql");
}

#[test]
fn build_tpch16() {
    check_build!("../../sql/tpch16.sql");
}

#[test]
fn build_tpch17() {
    check_build!("../../sql/tpch17.sql");
}

#[test]
fn build_tpch18() {
    check_build!("../../sql/tpch18.sql");
}

#[test]
fn build_tpch19() {
    check_build!("../../sql/tpch19.sql");
}

#[test]
fn build_tpch20() {
    check_build!("../../sql/tpch20.sql");
}

#[test]
fn build_tpch21() {
    check_build!("../../sql/tpch21.sql");
}

#[test]
fn build_tpch22() {
    check_build!("../../sql/tpch22.sql");
}
