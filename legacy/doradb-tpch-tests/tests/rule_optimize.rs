use doradb_catalog::Catalog;
use doradb_plan::explain::{Explain, ExplainConf};
use doradb_plan::lgc::LgcPlan;
use doradb_plan::rule::rule_optimize;
use doradb_sql::parser::dialect::Ansi;
use doradb_sql::parser::parse_query_verbose;
use doradb_tpch_tests::tpch_catalog;
use std::time::Instant;

#[test]
fn test_tpch_rule_optimize1() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch1.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize2() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch2.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize3() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch3.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize4() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch4.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize5() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch5.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize6() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch6.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize7() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch7.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize8() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch8.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize9() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch9.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize10() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch10.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize11() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch11.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize12() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch12.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize13() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch13.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize14() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch14.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize15() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch15.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize16() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch16.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize17() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch17.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize18() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch18.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize19() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch19.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize20() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch20.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize21() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch21.sql");
    check_tpch_rule_optimize(&cat, sql);
}

#[test]
fn test_tpch_rule_optimize22() {
    let cat = tpch_catalog();
    let sql = include_str!("../../sql/tpch22.sql");
    check_tpch_rule_optimize(&cat, sql);
}

fn check_tpch_rule_optimize<C: Catalog>(cat: &C, sql: &str) {
    let inst = Instant::now();
    let qry = parse_query_verbose(Ansi(sql)).unwrap();
    let dur_parse = inst.elapsed();
    let mut plan = LgcPlan::new(cat, "tpch", &qry).unwrap();
    let dur_build = inst.elapsed();
    rule_optimize(&mut plan).unwrap();
    let dur_opt = inst.elapsed();
    let mut s = String::new();
    assert!(plan.explain(&mut s, &ExplainConf::default()).is_ok());
    println!("Explain plan:\n{}", s);
    println!(
        "vparse={:?}, build={:?}, opt={:?}",
        dur_parse,
        dur_build - dur_parse,
        dur_opt - dur_build
    );
}
