use crate::error::{Error, Result};
use crate::{
    Catalog, ColIndex, Column, ColumnAttr, ColumnID, Key, ObjID, Schema, SchemaID, Table, TableID,
    TableSpec,
};
use indexmap::IndexMap;
use parking_lot::RwLock;
use semistr::SemiStr;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct MemCatalog {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    schemas: IndexMap<SemiStr, Schema>,
    tables: IndexMap<SchemaID, Vec<Table>>,
    table_columns: HashMap<TableID, TableWithColumns>,
    obj_id_gen: ObjID,
}

#[derive(Debug)]
pub struct TableWithColumns {
    table: Table,
    columns: Vec<Column>,
}

impl Catalog for MemCatalog {
    #[inline]
    fn all_schemas(&self) -> Vec<Schema> {
        let inner = self.inner.read();
        inner.schemas.values().cloned().collect()
    }

    #[inline]
    fn exists_schema(&self, schema_name: &str) -> bool {
        let inner = self.inner.read();
        inner.schemas.contains_key(schema_name)
    }

    #[inline]
    fn find_schema_by_name(&self, schema_name: &str) -> Option<Schema> {
        let inner = self.inner.read();
        inner.schemas.get(schema_name).cloned()
    }

    #[inline]
    fn find_schema(&self, schema_id: &SchemaID) -> Option<Schema> {
        let inner = self.inner.read();
        inner.schemas.values().find(|s| &s.id == schema_id).cloned()
    }

    #[inline]
    fn all_tables_in_schema(&self, schema_id: &SchemaID) -> Vec<Table> {
        let inner = self.inner.read();
        inner.tables.get(schema_id).cloned().unwrap_or_default()
    }

    #[inline]
    fn exists_table(&self, schema_id: &SchemaID, table_name: &str) -> bool {
        let inner = self.inner.read();
        inner
            .tables
            .get(schema_id)
            .map(|ts| ts.iter().any(|t| t.name == table_name))
            .unwrap_or_default()
    }

    #[inline]
    fn find_table_by_name(&self, schema_id: &SchemaID, table_name: &str) -> Option<Table> {
        let inner = self.inner.read();
        inner
            .tables
            .get(schema_id)
            .and_then(|ts| ts.iter().find(|t| t.name == table_name).cloned())
    }

    #[inline]
    fn find_table(&self, table_id: &TableID) -> Option<Table> {
        let inner = self.inner.read();
        inner
            .table_columns
            .get(table_id)
            .map(|twc| twc.table.clone())
    }

    #[inline]
    fn all_columns_in_table(&self, table_id: &TableID) -> Vec<Column> {
        let inner = self.inner.read();
        inner
            .table_columns
            .get(table_id)
            .map(|twc| twc.columns.clone())
            .unwrap_or_default()
    }

    #[inline]
    fn exists_column(&self, table_id: &TableID, column_name: &str) -> bool {
        let inner = self.inner.read();
        inner
            .table_columns
            .get(table_id)
            .map(|twc| twc.columns.iter().any(|c| c.name == column_name))
            .unwrap_or_default()
    }

    #[inline]
    fn find_column_by_name(&self, table_id: &TableID, column_name: &str) -> Option<Column> {
        let inner = self.inner.read();
        inner
            .table_columns
            .get(table_id)
            .and_then(|twc| twc.columns.iter().find(|c| c.name == column_name).cloned())
    }

    #[inline]
    fn create_schema(&self, schema_name: &str) -> Result<SchemaID> {
        let mut inner = self.inner.write();
        if inner.schemas.contains_key(schema_name) {
            return Err(Error::SchemaAlreadyExists(SemiStr::new(schema_name)));
        }
        inner.obj_id_gen += 1;
        let id = inner.obj_id_gen;
        let name = SemiStr::new(schema_name);
        let schema = Schema {
            id,
            name: name.clone(),
        };
        inner.schemas.insert(name, schema);
        inner.tables.insert(id, vec![]);
        Ok(id)
    }

    #[inline]
    fn drop_schema(&self, schema_name: &str) -> Result<()> {
        let mut inner = self.inner.write();
        match inner.schemas.remove(schema_name) {
            None => Err(Error::SchemaNotExists(SemiStr::new(schema_name))),
            Some(schema) => {
                if let Some(tables) = inner.tables.remove(&schema.id) {
                    for table in tables {
                        inner.table_columns.remove(&table.id);
                    }
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn create_table(&self, table_spec: TableSpec) -> Result<TableID> {
        let mut inner = self.inner.write();
        let Inner {
            schemas,
            tables,
            table_columns,
            obj_id_gen,
        } = &mut *inner;
        match schemas.get(&table_spec.schema_name) {
            None => Err(Error::SchemaNotExists(table_spec.schema_name)),
            Some(schema) => {
                let tables_in_schema = &tables[&schema.id];
                if tables_in_schema
                    .iter()
                    .any(|t| t.name == table_spec.table_name)
                {
                    return Err(Error::TableAlreadyExists(table_spec.table_name));
                }
                *obj_id_gen += 1;
                let table_id = *obj_id_gen;
                let table_name = SemiStr::new(&table_spec.table_name);
                let table = Table {
                    id: table_id,
                    schema_id: schema.id,
                    name: table_name,
                };
                tables.entry(schema.id).or_default().push(table.clone());
                let mut columns = Vec::with_capacity(table_spec.columns.len());
                for (i, c) in table_spec.columns.iter().enumerate() {
                    *obj_id_gen += 1;
                    let id = *obj_id_gen;
                    let column = Column {
                        id,
                        table_id,
                        name: c.name.clone(),
                        pty: c.pty,
                        attr: c.attr,
                        idx: ColIndex::from(i as u32),
                    };
                    columns.push(column);
                }
                table_columns.insert(table_id, TableWithColumns { table, columns });
                Ok(table_id)
            }
        }
    }

    #[inline]
    fn drop_table(&self, schema_name: &str, table_name: &str) -> Result<()> {
        let mut inner = self.inner.write();
        let Inner {
            schemas,
            tables,
            table_columns,
            ..
        } = &mut *inner;
        match schemas.get(schema_name) {
            None => Err(Error::SchemaNotExists(SemiStr::new(schema_name))),
            Some(schema) => {
                let tables_in_schema = &mut tables[&schema.id];
                match tables_in_schema.iter().position(|t| t.name == table_name) {
                    None => Err(Error::TableNotExists(SemiStr::new(table_name))),
                    Some(idx) => {
                        let table = tables_in_schema.swap_remove(idx);
                        table_columns.remove(&table.id);
                        Ok(())
                    }
                }
            }
        }
    }

    #[inline]
    fn find_keys(&self, table_id: &TableID) -> Vec<Key> {
        let inner = self.inner.read();
        if let Some(columns) = inner.table_columns.get(table_id).map(|twc| &twc.columns) {
            let mut res = vec![];
            let pk: Vec<Column> = columns
                .iter()
                .filter_map(|c| {
                    if c.attr.contains(ColumnAttr::PK) {
                        Some(c.clone())
                    } else {
                        None
                    }
                })
                .collect();
            if !pk.is_empty() {
                res.push(Key::PrimaryKey(pk));
            }
            let uk: Vec<Column> = columns
                .iter()
                .filter_map(|c| {
                    if c.attr.contains(ColumnAttr::UK) {
                        Some(c.clone())
                    } else {
                        None
                    }
                })
                .collect();
            if !uk.is_empty() {
                res.push(Key::UniqueKey(uk));
            }
            return res;
        }
        vec![]
    }
}
