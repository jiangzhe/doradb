use crate::ast::*;
use crate::parser::expr::{char_sp0, sp0_char};
use crate::parser::{
    ident, ident_tag, paren_cut, preceded_ident_tag, preceded_tag, preceded_tag2_cut, spcmt0,
    spcmt1, table_name, ParseInput,
};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::{char as chr, u16 as parse_u16, u8 as parse_u8};
use nom::combinator::{cut, map, opt, value};
use nom::error::ParseError;
use nom::multi::separated_list1;
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::IResult;

parse!(
    /// Parse create statement.
    fn create -> 'a Create<'a> = {
        preceded_tag("create", cut(alt((
            // create table
            preceded_tag("table", map(
                pair(
                    opt(terminated(if_not_exists, spcmt1)),
                    table_definition,
                ),
                |(if_not_exists, definition)| Create::Table{if_not_exists: if_not_exists.is_some(), definition},
            )),
            // create database
            preceded_tag("database", map(
                pair(
                    opt(terminated(if_not_exists, spcmt1)),
                    database_definition,
                ),
                |(if_not_exists, definition)| Create::Database{if_not_exists: if_not_exists.is_some(), definition},
            ))
        ))))
    }
);

parse!(
    /// Parse drop statement.
    fn drop -> 'a Drop<'a> = {
        preceded_tag("drop", cut(alt((
            // drop table
            preceded_tag("table", map(
                pair(
                    opt(terminated(if_exists, spcmt1)),
                    separated_list1(char_sp0(','), terminated(table_name, spcmt0)),
                ),
                |(if_exists, names)| Drop::Table{if_exists: if_exists.is_some(), names},
            )),
            // drop database
            preceded_tag("database", map(
                pair(opt(terminated(if_exists, spcmt1)), ident),
                |(if_exists, name)| Drop::Database{if_exists: if_exists.is_some(), name},
            )),
        ))))
    }
);

parse!(
    /// Parse `if not exists`.
    fn if_not_exists -> () = {
        map(
            preceded_tag("if", preceded_tag("not", ident_tag("exists"))),
            |_| ())
    }
);

parse!(
    /// Parse `if exists`.
    fn if_exists -> () = {
        map(
            preceded_tag("if", tag_no_case("exists")),
            |_| ())
    }
);

parse!(
    /// Parse a table definition.
    fn table_definition -> 'a TableDefinition<'a> = {
        map(pair(
            terminated(table_name, spcmt1),
            cut(delimited(
                char_sp0('('),
                separated_list1(char_sp0(','), table_element),
                sp0_char(')'),
            )),
        ), |(name, elems)| TableDefinition{name, elems})
    }
);

parse!(
    /// Parse a database definition.
    fn database_definition -> 'a DatabaseDefinition<'a> = {
        map(tuple((
            terminated(ident, spcmt1),
            opt(terminated(tag_no_case("default"), spcmt1)),
            opt(terminated(preceded_tag2_cut("character", "set", preceded(opt(char_sp0('=')), ident)), spcmt0)),
            opt(preceded_tag("collate", preceded(opt(char_sp0('=')), ident))),
        )), |(name, _, charset, collate)| DatabaseDefinition{name, charset, collate})
    }
);

parse!(
    /// Parse a table element.
    fn table_element -> 'a TableElement<'a> = {
        alt((
            index_def,
            constraint,
            map(key, |(ty, name, keys)| TableElement::Key{ty, name, keys}),
            col_def,
        ))
    }
);

parse!(
    /// Parse an index definition.
    fn index_def -> 'a TableElement<'a> = {
        preceded(terminated(alt((ident_tag("index"), ident_tag("key"))), spcmt1),
            map(pair(
                opt(terminated(ident, spcmt1)),
                key_parts,
            ), |(name, keys)| {
                TableElement::Key{ty: TableKeyType::Index, name, keys}
            }),
        )
    }
);

parse!(
    /// Parse a constraint definition.
    fn constraint -> 'a TableElement<'a> = {
        preceded_tag("constraint", cut(alt((
            map(key, |(ty, name, keys)| TableElement::Key{ty, name, keys}),
            map(pair(
                terminated(ident, spcmt1),
                key,
            ), |(name, (ty, _, keys))| TableElement::Key{ty, name: Some(name), keys})
        ))))
    }
);

parse!(
    /// Parse a constraint definition.
    fn key -> 'a (TableKeyType, Option<Ident<'a>>, Vec<KeyPart<'a>>) = {
        alt((
            map(primary_key_parts, |keys| (TableKeyType::PrimaryKey, None, keys)),
            map(unique_key_parts, |(name, keys)| (TableKeyType::UniqueKey, name, keys)),
        ))
    }
);

parse!(
    /// Parse a primary key parts.
    fn primary_key_parts -> 'a Vec<KeyPart<'a>> = {
        preceded_tag2_cut("primary", "key", key_parts)
    }
);

parse!(
    /// Parse a unique key parts.
    fn unique_key_parts -> 'a (Option<Ident<'a>>, Vec<KeyPart<'a>>) = {
        preceded_tag("unique", cut(alt((
            pair(
                alt((
                    preceded(terminated(alt((tag_no_case("index"), tag_no_case("key"))), spcmt1), opt(terminated(ident, spcmt1))),
                    map(terminated(ident, spcmt1), Some),
                )),
                key_parts,
            ),
            map(key_parts, |keys| (None, keys)),
        ))))
    }
);

parse!(
    /// Parse key parts of an index.
    fn key_parts -> 'a Vec<KeyPart<'a>> = {
        delimited(
            char_sp0('('),
            separated_list1(char_sp0(','), terminated(key_part, spcmt0)),
            chr(')'),
        )
    }
);

parse!(
    /// Parse a single key part of an index.
    fn key_part -> 'a KeyPart<'a> = {
        map(
            pair(
                terminated(ident, spcmt1),
                opt(alt((
                    value(false, ident_tag("asc")),
                    value(true, ident_tag("desc")),
                ))),
            ),
            |(col_name, desc)| KeyPart{col_name, desc: desc.unwrap_or_default()},
        )
    }
);

parse!(
    /// Parse a column definition.
    fn col_def -> 'a TableElement<'a> = {
        map(
            tuple((
                ident,
                cut(preceded(spcmt1, data_type)),
                map(opt(preceded(spcmt1, not_null)), |n| n.unwrap_or_default()),
            )),
            |(name, ty, not_null)| TableElement::Column{
                name,
                ty,
                not_null,
                default: None,
                auto_increment: false,
                unique: false,
                primary_key: false,
                comment: None,
                collate: None,
            }
        )

    }
);

parse!(
    /// Parse a data type definition.
    fn data_type -> DataType = {
        alt((
            map(preceded_tag("bigint", opt(preceded(spcmt1, tag_no_case("unsigned")))),
                |u| DataType::Bigint(u.is_some())),
            map(preceded_tag("integer", opt(preceded(spcmt1, tag_no_case("unsigned")))),
                |u| DataType::Int(u.is_some())),
            map(preceded_tag("int", opt(preceded(spcmt1, tag_no_case("unsigned")))),
                |u| DataType::Int(u.is_some())),
            map(preceded_tag("smallint", opt(preceded(spcmt1, tag_no_case("unsigned")))),
                |u| DataType::Smallint(u.is_some())),
            map(preceded_tag("tinyint", opt(preceded(spcmt1, tag_no_case("unsigned")))),
                |u| DataType::Tinyint(u.is_some())),
            map(preceded_ident_tag("varchar", preceded(spcmt0, paren_cut(terminated(parse_u16, spcmt0)))),
                DataType::Varchar),
            map(preceded_ident_tag("char", preceded(spcmt0, paren_cut(terminated(parse_u16, spcmt0)))),
                DataType::Char),
            value(DataType::Date, ident_tag("date")),
            decimal_type,
        ))
    }
);

parse!(
    /// Parse a decimal definition.
    fn decimal_type -> DataType = {
        alt((
            preceded_ident_tag("decimal", preceded(spcmt0, paren_cut(terminated(
                alt((
                    // two numbers
                    map(pair(terminated(parse_u8, spcmt0), preceded(char_sp0(','), preceded(spcmt0, parse_u8))),
                        |(n1, n2)| DataType::Decimal(Some(n1), Some(n2))),
                    // single number
                    map(terminated(parse_u8, spcmt0), |n| DataType::Decimal(Some(n), None)),
                )),
                spcmt0)))),
            value(DataType::Decimal(None, None), ident_tag("decimal")),
        ))
    }
);

parse!(
    fn not_null -> bool = {
        alt((
            value(true, preceded_tag("not", ident_tag("null"))),
            value(false, ident_tag("null")),
        ))
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::MySQL;
    use nom::error::Error;

    #[test]
    fn test_parse_create_table() -> anyhow::Result<()> {
        for (input, tbl) in vec![
            (
                "create table t1 (id int)",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![comm_col("id", DataType::Int(false), false)],
                    },
                },
            ),
            (
                "create table if not exists t1 (id int)",
                Create::Table {
                    if_not_exists: true,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![comm_col("id", DataType::Int(false), false)],
                    },
                },
            ),
            (
                "create table `t1` (c0 int, c1 integer, c2 int)",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::quoted("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), false),
                            comm_col("c1", DataType::Int(false), false),
                            comm_col("c2", DataType::Int(false), false),
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 bigint, c1 int, c2 smallint, c3 tinyint, c4 bigint unsigned, c5 int unsigned, c6 smallint unsigned, c7 tinyint unsigned)",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Bigint(false), false),
                            comm_col("c1", DataType::Int(false), false),
                            comm_col("c2", DataType::Smallint(false), false),
                            comm_col("c3", DataType::Tinyint(false), false),
                            comm_col("c4", DataType::Bigint(true), false),
                            comm_col("c5", DataType::Int(true), false),
                            comm_col("c6", DataType::Smallint(true), false),
                            comm_col("c7", DataType::Tinyint(true), false),
                        ],
                    },
                },
            ),
            (
                "create table `t1` (c0 varchar(255), c1 char(10), c2 decimal, c3 decimal(8), c4 decimal(18, 2))",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::quoted("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Varchar(255), false),
                            comm_col("c1", DataType::Char(10), false),
                            comm_col("c2", DataType::Decimal(None, None), false),
                            comm_col("c3", DataType::Decimal(Some(8), None), false),
                            comm_col("c4", DataType::Decimal(Some(18), Some(2)), false),
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 int not null, c1 int null)",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), true),
                            comm_col("c1", DataType::Int(false), false),
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 int, index (c0))",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), false),
                            TableElement::Key{ty: TableKeyType::Index, name: None, keys: vec![KeyPart{col_name: Ident::regular("c0"), desc: false}]},
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 int, index idx_t1_c0 (c0))",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), false),
                            TableElement::Key{ty: TableKeyType::Index, name: Some(Ident::regular("idx_t1_c0")), keys: vec![KeyPart{col_name: Ident::regular("c0"), desc: false}]},
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 int, constraint primary key (c0))",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), false),
                            TableElement::Key{ty: TableKeyType::PrimaryKey, name: None, keys: vec![KeyPart{col_name: Ident::regular("c0"), desc: false}]},
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 int, primary key (c0))",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), false),
                            TableElement::Key{ty: TableKeyType::PrimaryKey, name: None, keys: vec![KeyPart{col_name: Ident::regular("c0"), desc: false}]},
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 int, constraint pk_t1 primary key (c0))",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), false),
                            TableElement::Key{ty: TableKeyType::PrimaryKey, name: Some(Ident::regular("pk_t1")), keys: vec![KeyPart{col_name: Ident::regular("c0"), desc: false}]},
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 int, c1 int, c2 int, c3 int, c4 int, unique key (c0), unique (c1), unique uk_t1_c2 (c2), unique index uk_t1_c3 (c3), unique (c4 desc))",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), false),
                            comm_col("c1", DataType::Int(false), false),
                            comm_col("c2", DataType::Int(false), false),
                            comm_col("c3", DataType::Int(false), false),
                            comm_col("c4", DataType::Int(false), false),
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: None, keys: vec![KeyPart{col_name: Ident::regular("c0"), desc: false}]},
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: None, keys: vec![KeyPart{col_name: Ident::regular("c1"), desc: false}]},
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: Some(Ident::regular("uk_t1_c2")), keys: vec![KeyPart{col_name: Ident::regular("c2"), desc: false}]},
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: Some(Ident::regular("uk_t1_c3")), keys: vec![KeyPart{col_name: Ident::regular("c3"), desc: false}]},
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: None, keys: vec![KeyPart{col_name: Ident::regular("c4"), desc: true}]},
                        ],
                    },
                },
            ),
            (
                "create table t1 (c0 int, c1 int, c2 int, c3 int, constraint unique key (c0), constraint unique (c1), constraint uk_t1_c2 unique (c2), constraint uk_t1_c3 unique index uk_unused (c3))",
                Create::Table {
                    if_not_exists: false,
                    definition: TableDefinition {
                        name: TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        elems: vec![
                            comm_col("c0", DataType::Int(false), false),
                            comm_col("c1", DataType::Int(false), false),
                            comm_col("c2", DataType::Int(false), false),
                            comm_col("c3", DataType::Int(false), false),
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: None, keys: vec![KeyPart{col_name: Ident::regular("c0"), desc: false}]},
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: None, keys: vec![KeyPart{col_name: Ident::regular("c1"), desc: false}]},
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: Some(Ident::regular("uk_t1_c2")), keys: vec![KeyPart{col_name: Ident::regular("c2"), desc: false}]},
                            TableElement::Key{ty: TableKeyType::UniqueKey, name: Some(Ident::regular("uk_t1_c3")), keys: vec![KeyPart{col_name: Ident::regular("c3"), desc: false}]},
                        ],
                    },
                },
            ),
        ] {
            let (i, o) = create::<'_, _, nom::error::VerboseError<_>>(MySQL(input))?;
            assert!(i.is_empty());
            assert_eq!(tbl, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_create_database() -> anyhow::Result<()> {
        for (i, d) in vec![
            (
                "create database db1",
                Create::Database {
                    if_not_exists: false,
                    definition: DatabaseDefinition {
                        name: Ident::regular("db1"),
                        charset: None,
                        collate: None,
                    },
                },
            ),
            (
                "create database if not exists db1",
                Create::Database {
                    if_not_exists: true,
                    definition: DatabaseDefinition {
                        name: Ident::regular("db1"),
                        charset: None,
                        collate: None,
                    },
                },
            ),
            (
                "create database db1 character set utf8mb4",
                Create::Database {
                    if_not_exists: false,
                    definition: DatabaseDefinition {
                        name: Ident::regular("db1"),
                        charset: Some(Ident::regular("utf8mb4")),
                        collate: None,
                    },
                },
            ),
            (
                "create database db1 default character set utf8mb4",
                Create::Database {
                    if_not_exists: false,
                    definition: DatabaseDefinition {
                        name: Ident::regular("db1"),
                        charset: Some(Ident::regular("utf8mb4")),
                        collate: None,
                    },
                },
            ),
            (
                "create database db1 character set utf8mb4 collate utf8mb4_bin",
                Create::Database {
                    if_not_exists: false,
                    definition: DatabaseDefinition {
                        name: Ident::regular("db1"),
                        charset: Some(Ident::regular("utf8mb4")),
                        collate: Some(Ident::regular("utf8mb4_bin")),
                    },
                },
            ),
            (
                "create database db1 collate utf8mb4_bin",
                Create::Database {
                    if_not_exists: false,
                    definition: DatabaseDefinition {
                        name: Ident::regular("db1"),
                        charset: None,
                        collate: Some(Ident::regular("utf8mb4_bin")),
                    },
                },
            ),
        ] {
            let (i, o) = create::<'_, _, Error<_>>(MySQL(i))?;
            assert!(i.is_empty());
            assert_eq!(d, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_drop_table() -> anyhow::Result<()> {
        for (i, d) in vec![
            (
                "drop table t1",
                Drop::Table {
                    if_exists: false,
                    names: vec![TableName {
                        schema: None,
                        table: Ident::regular("t1"),
                    }],
                },
            ),
            (
                "drop table if exists t1",
                Drop::Table {
                    if_exists: true,
                    names: vec![TableName {
                        schema: None,
                        table: Ident::regular("t1"),
                    }],
                },
            ),
            (
                "drop table t1, t2, t3",
                Drop::Table {
                    if_exists: false,
                    names: vec![
                        TableName {
                            schema: None,
                            table: Ident::regular("t1"),
                        },
                        TableName {
                            schema: None,
                            table: Ident::regular("t2"),
                        },
                        TableName {
                            schema: None,
                            table: Ident::regular("t3"),
                        },
                    ],
                },
            ),
            (
                "drop table db1.t1",
                Drop::Table {
                    if_exists: false,
                    names: vec![TableName {
                        schema: Some(Ident::regular("db1")),
                        table: Ident::regular("t1"),
                    }],
                },
            ),
        ] {
            let (i, o) = drop::<'_, _, Error<_>>(MySQL(i))?;
            assert!(i.is_empty());
            assert_eq!(d, o);
        }
        Ok(())
    }

    #[test]
    fn test_parse_drop_database() -> anyhow::Result<()> {
        for (i, d) in vec![
            (
                "drop database db1",
                Drop::Database {
                    if_exists: false,
                    name: Ident::regular("db1"),
                },
            ),
            (
                "drop database if exists db1",
                Drop::Database {
                    if_exists: true,
                    name: Ident::regular("db1"),
                },
            ),
        ] {
            let (i, o) = drop::<'_, _, Error<_>>(MySQL(i))?;

            assert!(i.is_empty());
            assert_eq!(d, o);
        }
        Ok(())
    }

    fn comm_col<'a>(name: &'a str, ty: DataType, not_null: bool) -> TableElement<'a> {
        TableElement::Column {
            name: Ident::regular(name),
            ty,
            not_null,
            default: None,
            auto_increment: false,
            unique: false,
            primary_key: false,
            comment: None,
            collate: None,
        }
    }
}
