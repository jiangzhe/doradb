use doradb_catalog::mem_impl::MemCatalog;
use doradb_catalog::{
    Catalog, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, SchemaSpec,
    TableSpec,
};
use doradb_datatype::PreciseType;

#[inline]
pub fn tpch_catalog() -> MemCatalog {
    let cata = MemCatalog::default();
    let schema_id = cata.create_schema(SchemaSpec::new("tpch")).unwrap();
    let table_id = cata
        .create_table(
            schema_id,
            TableSpec::new(
                "lineitem",
                vec![
                    ColumnSpec::new("l_orderkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("l_partkey", PreciseType::i32(), ColumnAttributes::empty()),
                    ColumnSpec::new("l_suppkey", PreciseType::i32(), ColumnAttributes::empty()),
                    ColumnSpec::new("l_linenumber", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new(
                        "l_quantity",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_extendedprice",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_discount",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_tax",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_returnflag",
                        PreciseType::ascii(1),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_linestatus",
                        PreciseType::ascii(1),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new("l_shipdate", PreciseType::date(), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "l_commitdate",
                        PreciseType::date(),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_receiptdate",
                        PreciseType::date(),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_shipinstruct",
                        PreciseType::ascii(25),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_shipmode",
                        PreciseType::ascii(10),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "l_comment",
                        PreciseType::var_utf8(44),
                        ColumnAttributes::empty(),
                    ),
                ],
            ),
        )
        .unwrap();

    cata.create_index(
        table_id,
        IndexSpec::new(
            "idx_lineitem_orderkey_l_linenumber",
            vec![IndexKey::new(0), IndexKey::new(3)],
            IndexAttributes::PK,
        ),
    )
    .unwrap();

    let table_id = cata
        .create_table(
            schema_id,
            TableSpec::new(
                "orders",
                vec![
                    ColumnSpec::new("o_orderkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("o_custkey", PreciseType::i32(), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "o_orderstatus",
                        PreciseType::ascii(1),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "o_totalprice",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "o_orderdate",
                        PreciseType::date(),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "o_orderpriority",
                        PreciseType::ascii(15),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new("o_clerk", PreciseType::ascii(15), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "o_shippriority",
                        PreciseType::i32(),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "o_comment",
                        PreciseType::var_utf8(79),
                        ColumnAttributes::empty(),
                    ),
                ],
            ),
        )
        .unwrap();

    cata.create_index(
        table_id,
        IndexSpec::new(
            "idx_orders_orderkey",
            vec![IndexKey::new(0)],
            IndexAttributes::PK,
        ),
    )
    .unwrap();

    let table_id = cata
        .create_table(
            schema_id,
            TableSpec::new(
                "customer",
                vec![
                    ColumnSpec::new("c_custkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("c_name", PreciseType::utf8(25), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "c_address",
                        PreciseType::var_utf8(40),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new("c_nationkey", PreciseType::i32(), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "c_phone",
                        PreciseType::var_ascii(15),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "c_acctbal",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "c_mktsegment",
                        PreciseType::ascii(10),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "c_comment",
                        PreciseType::var_utf8(117),
                        ColumnAttributes::empty(),
                    ),
                ],
            ),
        )
        .unwrap();

    cata.create_index(
        table_id,
        IndexSpec::new(
            "idx_customer_custkey",
            vec![IndexKey::new(0)],
            IndexAttributes::PK,
        ),
    )
    .unwrap();

    let table_id = cata
        .create_table(
            schema_id,
            TableSpec::new(
                "partsupp",
                vec![
                    ColumnSpec::new("ps_partkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("ps_suppkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("ps_availqty", PreciseType::i32(), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "ps_supplycost",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "ps_comment",
                        PreciseType::var_utf8(199),
                        ColumnAttributes::empty(),
                    ),
                ],
            ),
        )
        .unwrap();

    cata.create_index(
        table_id,
        IndexSpec::new(
            "idx_partsupp_partkey_suppkey",
            vec![IndexKey::new(0), IndexKey::new(1)],
            IndexAttributes::PK,
        ),
    )
    .unwrap();

    let table_id = cata
        .create_table(
            schema_id,
            TableSpec::new(
                "part",
                vec![
                    ColumnSpec::new("p_partkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("p_name", PreciseType::ascii(55), ColumnAttributes::empty()),
                    ColumnSpec::new("p_mfgr", PreciseType::ascii(25), ColumnAttributes::empty()),
                    ColumnSpec::new("p_brand", PreciseType::ascii(10), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "p_type",
                        PreciseType::var_utf8(25),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new("p_size", PreciseType::i32(), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "p_container",
                        PreciseType::ascii(10),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "p_retailprice",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "p_comment",
                        PreciseType::var_utf8(23),
                        ColumnAttributes::empty(),
                    ),
                ],
            ),
        )
        .unwrap();

    cata.create_index(
        table_id,
        IndexSpec::new(
            "idx_part_partkey",
            vec![IndexKey::new(0)],
            IndexAttributes::PK,
        ),
    )
    .unwrap();

    let table_id = cata
        .create_table(
            schema_id,
            TableSpec::new(
                "supplier",
                vec![
                    ColumnSpec::new("s_suppkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("s_name", PreciseType::utf8(25), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "s_address",
                        PreciseType::var_utf8(40),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new("s_nationkey", PreciseType::i32(), ColumnAttributes::empty()),
                    ColumnSpec::new("s_phone", PreciseType::ascii(15), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "s_acctbal",
                        PreciseType::decimal(18, 2),
                        ColumnAttributes::empty(),
                    ),
                    ColumnSpec::new(
                        "s_comment",
                        PreciseType::var_utf8(101),
                        ColumnAttributes::empty(),
                    ),
                ],
            ),
        )
        .unwrap();

    cata.create_index(
        table_id,
        IndexSpec::new(
            "idx_supplier_suppkey",
            vec![IndexKey::new(0)],
            IndexAttributes::PK,
        ),
    )
    .unwrap();

    let table_id = cata
        .create_table(
            schema_id,
            TableSpec::new(
                "nation",
                vec![
                    ColumnSpec::new("n_nationkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("n_name", PreciseType::ascii(25), ColumnAttributes::empty()),
                    ColumnSpec::new("n_regionkey", PreciseType::i32(), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "n_comment",
                        PreciseType::var_utf8(152),
                        ColumnAttributes::empty(),
                    ),
                ],
            ),
        )
        .unwrap();

    cata.create_index(
        table_id,
        IndexSpec::new(
            "idx_nation_nationkey",
            vec![IndexKey::new(0)],
            IndexAttributes::PK,
        ),
    )
    .unwrap();

    let table_id = cata
        .create_table(
            schema_id,
            TableSpec::new(
                "region",
                vec![
                    ColumnSpec::new("r_regionkey", PreciseType::i32(), ColumnAttributes::INDEX),
                    ColumnSpec::new("r_name", PreciseType::ascii(55), ColumnAttributes::empty()),
                    ColumnSpec::new(
                        "r_comment",
                        PreciseType::utf8(152),
                        ColumnAttributes::empty(),
                    ),
                ],
            ),
        )
        .unwrap();

    cata.create_index(
        table_id,
        IndexSpec::new(
            "idx_region_regionkey",
            vec![IndexKey::new(0)],
            IndexAttributes::PK,
        ),
    )
    .unwrap();

    cata
}
