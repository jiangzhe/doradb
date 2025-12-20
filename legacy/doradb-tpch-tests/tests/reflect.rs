use aosa::StringArena;
use doradb_plan::lgc::reflect;
use doradb_plan::lgc::LgcPlan;
use doradb_sql::parser::dialect::Ansi;
use doradb_sql::parser::parse_query_verbose;
use doradb_sql::pretty::{PrettyConf, PrettyFormat};
use doradb_tpch_tests::tpch_catalog;

macro_rules! check_reflect {
    ( $filename:literal ) => {
        let cat = tpch_catalog();
        let sql = include_str!($filename);
        let qry = parse_query_verbose(Ansi(sql)).unwrap();
        let plan = LgcPlan::new(&cat, "tpch", &qry).unwrap();
        let mut s = String::new();
        let arena = StringArena::with_capacity(16384);
        let stmt = reflect(&plan, &arena, &cat).unwrap();
        let conf = PrettyConf::default();
        stmt.pretty_fmt(&mut s, &conf, 0).unwrap();
        println!("Reflected statement:\n{}", s)
    };
}

#[test]
fn reflect_tpch1() {
    check_reflect!("../../sql/tpch1.sql");
}

#[test]
fn reflect_tpch2() {
    check_reflect!("../../sql/tpch2.sql");
}

#[test]
fn reflect_tpch3() {
    check_reflect!("../../sql/tpch3.sql");
}

#[test]
fn reflect_tpch4() {
    check_reflect!("../../sql/tpch4.sql");
}

#[test]
fn reflect_tpch5() {
    check_reflect!("../../sql/tpch5.sql");
}

#[test]
fn reflect_tpch6() {
    check_reflect!("../../sql/tpch6.sql");
}

#[test]
fn reflect_tpch7() {
    check_reflect!("../../sql/tpch7.sql");
}

#[test]
fn reflect_tpch8() {
    check_reflect!("../../sql/tpch8.sql");
}

#[test]
fn reflect_tpch9() {
    check_reflect!("../../sql/tpch9.sql");
}

#[test]
fn reflect_tpch10() {
    check_reflect!("../../sql/tpch10.sql");
}

#[test]
fn reflect_tpch11() {
    check_reflect!("../../sql/tpch11.sql");
}

#[test]
fn reflect_tpch12() {
    check_reflect!("../../sql/tpch12.sql");
}

#[test]
fn reflect_tpch13() {
    check_reflect!("../../sql/tpch13.sql");
}

#[test]
fn reflect_tpch14() {
    check_reflect!("../../sql/tpch14.sql");
}

// CTE support will be added later.
// #[test]
// fn reflect_tpch15() {
//     check_reflect!("../../sql/tpch15.sql");
// }

#[test]
fn reflect_tpch16() {
    check_reflect!("../../sql/tpch16.sql");
}

#[test]
fn reflect_tpch17() {
    check_reflect!("../../sql/tpch17.sql");
}

#[test]
fn reflect_tpch18() {
    check_reflect!("../../sql/tpch18.sql");
}

#[test]
fn reflect_tpch19() {
    check_reflect!("../../sql/tpch19.sql");
}

#[test]
fn reflect_tpch20() {
    check_reflect!("../../sql/tpch20.sql");
}

#[test]
fn reflect_tpch21() {
    check_reflect!("../../sql/tpch21.sql");
}

#[test]
fn reflect_tpch22() {
    check_reflect!("../../sql/tpch22.sql");
}
