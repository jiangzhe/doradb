use crate::error::{Error, Result};
use crate::{
    Catalog, ColIndex, Column, ColumnAttributes, IndexAttributes, IndexSpec, Key, ObjID, Schema,
    SchemaID, SchemaSpec, Table, TableID, TableSpec,
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
    table_details: HashMap<TableID, TableDetails>,
    obj_id_gen: ObjID,
}

impl Inner {
    #[inline]
    fn all_schemas(&self) -> Vec<Schema> {
        self.schemas.values().cloned().collect()
    }

    #[inline]
    fn exists_schema(&self, schema_name: &str) -> bool {
        self.schemas.contains_key(schema_name)
    }

    #[inline]
    fn find_schema_by_name(&self, schema_name: &str) -> Option<Schema> {
        self.schemas.get(schema_name).cloned()
    }

    #[inline]
    fn find_schema(&self, schema_id: SchemaID) -> Option<Schema> {
        self.schemas.values().find(|s| s.id == schema_id).cloned()
    }

    #[inline]
    fn all_tables_in_schema(&self, schema_id: SchemaID) -> Vec<Table> {
        self.tables.get(&schema_id).cloned().unwrap_or_default()
    }

    #[inline]
    fn exists_table(&self, schema_id: SchemaID, table_name: &str) -> bool {
        self.tables
            .get(&schema_id)
            .map(|ts| ts.iter().any(|t| t.name == table_name))
            .unwrap_or_default()
    }

    #[inline]
    fn find_table_by_name(&self, schema_id: SchemaID, table_name: &str) -> Option<Table> {
        self.tables
            .get(&schema_id)
            .and_then(|ts| ts.iter().find(|t| t.name == table_name).cloned())
    }

    #[inline]
    fn find_table(&self, table_id: TableID) -> Option<Table> {
        self.table_details
            .get(&table_id)
            .map(|twc| twc.table.clone())
    }

    #[inline]
    fn all_columns_in_table(&self, table_id: TableID) -> Vec<Column> {
        self.table_details
            .get(&table_id)
            .map(|twc| twc.columns.clone())
            .unwrap_or_default()
    }

    #[inline]
    fn exists_column(&self, table_id: TableID, column_name: &str) -> bool {
        self.table_details
            .get(&table_id)
            .map(|twc| twc.columns.iter().any(|c| c.name == column_name))
            .unwrap_or_default()
    }

    #[inline]
    fn find_column_by_name(&self, table_id: TableID, column_name: &str) -> Option<Column> {
        self.table_details
            .get(&table_id)
            .and_then(|twc| twc.columns.iter().find(|c| c.name == column_name).cloned())
    }

    #[inline]
    fn create_schema(&mut self, schema: SchemaSpec) -> Result<SchemaID> {
        if self.exists_schema(&schema.schema_name) {
            return Err(Error::SchemaAlreadyExists(schema.schema_name));
        }
        self.obj_id_gen += 1;
        let id = self.obj_id_gen;
        let name = SemiStr::new(&schema.schema_name);
        let schema = Schema {
            id,
            name: name.clone(),
        };
        self.schemas.insert(name, schema);
        self.tables.insert(id, vec![]);
        Ok(id)
    }

    #[inline]
    fn drop_schema(&mut self, schema_name: &str) -> Result<()> {
        match self.schemas.remove(schema_name) {
            None => Err(Error::SchemaNotExists(SemiStr::new(schema_name))),
            Some(schema) => {
                if let Some(tables) = self.tables.remove(&schema.id) {
                    for table in tables {
                        self.table_details.remove(&table.id);
                    }
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn create_table(&mut self, schema_id: SchemaID, table_spec: TableSpec) -> Result<TableID> {
        match self.find_schema(schema_id) {
            None => {
                let s = format!("id={}", schema_id);
                Err(Error::SchemaNotExists(SemiStr::new(&s)))
            }
            Some(schema) => {
                let tables_in_schema = &self.tables[&schema.id];
                if tables_in_schema
                    .iter()
                    .any(|t| t.name == table_spec.table_name)
                {
                    return Err(Error::TableAlreadyExists(table_spec.table_name));
                }
                self.obj_id_gen += 1;
                let table_id = self.obj_id_gen;
                let table_name = SemiStr::new(&table_spec.table_name);
                let table = Table {
                    id: table_id,
                    schema_id: schema.id,
                    name: table_name,
                };
                self.tables
                    .entry(schema.id)
                    .or_default()
                    .push(table.clone());
                let mut columns = Vec::with_capacity(table_spec.columns.len());
                for (i, c) in table_spec.columns.iter().enumerate() {
                    self.obj_id_gen += 1;
                    let id = self.obj_id_gen;
                    let column = Column {
                        id,
                        table_id,
                        name: c.column_name.clone(),
                        pty: c.column_type,
                        attr: c.column_attributes,
                        idx: ColIndex::from(i as u32),
                    };
                    columns.push(column);
                }
                self.table_details.insert(
                    table_id,
                    TableDetails {
                        table,
                        columns,
                        indexes: vec![],
                    },
                );
                Ok(table_id)
            }
        }
    }

    #[inline]
    fn drop_table(&mut self, schema_id: SchemaID, table_name: &str) -> Result<()> {
        match self.find_schema(schema_id) {
            None => {
                let s = format!("id={}", schema_id);
                Err(Error::SchemaNotExists(SemiStr::new(&s)))
            }
            Some(schema) => {
                let tables_in_schema = &mut self.tables[&schema.id];
                match tables_in_schema.iter().position(|t| t.name == table_name) {
                    None => Err(Error::TableNotExists(SemiStr::new(table_name))),
                    Some(idx) => {
                        let table = tables_in_schema.swap_remove(idx);
                        self.table_details.remove(&table.id);
                        Ok(())
                    }
                }
            }
        }
    }

    #[inline]
    fn find_keys(&self, table_id: TableID) -> Vec<Key> {
        self.table_details
            .get(&table_id)
            .map(|table| {
                table
                    .indexes
                    .iter()
                    .filter_map(|i| {
                        if i.index_attributes.contains(IndexAttributes::PK) {
                            Some(Key::PrimaryKey(i.columns.clone()))
                        } else if i.index_attributes.contains(IndexAttributes::UK) {
                            Some(Key::UniqueKey(i.columns.clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    #[inline]
    fn create_index(&mut self, table_id: TableID, index: IndexSpec) -> Result<()> {
        match self.table_details.get_mut(&table_id) {
            None => {
                let s = format!("id={}", table_id);
                Err(Error::TableNotExists(SemiStr::new(&s)))
            }
            Some(table_details) => {
                // Update column attributes.
                for col_no in index.index_cols.iter().map(|c| c.col_no) {
                    table_details.columns[col_no as usize].attr |= ColumnAttributes::INDEX;
                }

                // Update index.
                table_details.indexes.push(TableIndex {
                    name: index.index_name,
                    columns: index
                        .index_cols
                        .iter()
                        .map(|c| table_details.columns[c.col_no as usize].clone())
                        .collect(),
                    index_attributes: index.index_attributes,
                });
                Ok(())
            }
        }
    }

    #[inline]
    fn drop_index(&mut self, table_id: TableID, index_name: &str) -> Result<()> {
        match self.table_details.get_mut(&table_id) {
            None => {
                let s = format!("id={}", table_id);
                Err(Error::TableNotExists(SemiStr::new(&s)))
            }
            Some(table_details) => {
                if let Some(idx) = table_details
                    .indexes
                    .iter()
                    .position(|i| i.name == index_name)
                {
                    table_details.indexes.remove(idx);
                    Ok(())
                } else {
                    Err(Error::IndexNotExists(SemiStr::new(index_name)))
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct TableDetails {
    table: Table,
    columns: Vec<Column>,
    indexes: Vec<TableIndex>,
}

#[derive(Debug)]
pub struct TableIndex {
    name: SemiStr,
    columns: Vec<Column>,
    index_attributes: IndexAttributes,
}

impl Catalog for MemCatalog {
    #[inline]
    fn all_schemas(&self) -> Vec<Schema> {
        let inner = self.inner.read();
        inner.all_schemas()
    }

    #[inline]
    fn exists_schema(&self, schema_name: &str) -> bool {
        let inner = self.inner.read();
        inner.exists_schema(schema_name)
    }

    #[inline]
    fn find_schema_by_name(&self, schema_name: &str) -> Option<Schema> {
        let inner = self.inner.read();
        inner.find_schema_by_name(schema_name)
    }

    #[inline]
    fn find_schema(&self, schema_id: SchemaID) -> Option<Schema> {
        let inner = self.inner.read();
        inner.find_schema(schema_id)
    }

    #[inline]
    fn all_tables_in_schema(&self, schema_id: SchemaID) -> Vec<Table> {
        let inner = self.inner.read();
        inner.all_tables_in_schema(schema_id)
    }

    #[inline]
    fn exists_table(&self, schema_id: SchemaID, table_name: &str) -> bool {
        let inner = self.inner.read();
        inner.exists_table(schema_id, table_name)
    }

    #[inline]
    fn find_table_by_name(&self, schema_id: SchemaID, table_name: &str) -> Option<Table> {
        let inner = self.inner.read();
        inner.find_table_by_name(schema_id, table_name)
    }

    #[inline]
    fn find_table(&self, table_id: TableID) -> Option<Table> {
        let inner = self.inner.read();
        inner.find_table(table_id)
    }

    #[inline]
    fn all_columns_in_table(&self, table_id: TableID) -> Vec<Column> {
        let inner = self.inner.read();
        inner.all_columns_in_table(table_id)
    }

    #[inline]
    fn exists_column(&self, table_id: TableID, column_name: &str) -> bool {
        let inner = self.inner.read();
        inner.exists_column(table_id, column_name)
    }

    #[inline]
    fn find_column_by_name(&self, table_id: TableID, column_name: &str) -> Option<Column> {
        let inner = self.inner.read();
        inner.find_column_by_name(table_id, column_name)
    }

    #[inline]
    fn create_schema(&self, schema: SchemaSpec) -> Result<SchemaID> {
        let mut inner = self.inner.write();
        inner.create_schema(schema)
    }

    #[inline]
    fn drop_schema(&self, schema_name: &str) -> Result<()> {
        let mut inner = self.inner.write();
        inner.drop_schema(schema_name)
    }

    #[inline]
    fn create_table(&self, schema_id: SchemaID, table_spec: TableSpec) -> Result<TableID> {
        let mut inner = self.inner.write();
        inner.create_table(schema_id, table_spec)
    }

    #[inline]
    fn drop_table(&self, schema_id: SchemaID, table_name: &str) -> Result<()> {
        let mut inner = self.inner.write();
        inner.drop_table(schema_id, table_name)
    }

    #[inline]
    fn find_keys(&self, table_id: TableID) -> Vec<Key> {
        let inner = self.inner.read();
        inner.find_keys(table_id)
    }

    #[inline]
    fn create_index(&self, table_id: TableID, index: IndexSpec) -> Result<()> {
        let mut inner = self.inner.write();
        inner.create_index(table_id, index)
    }

    #[inline]
    fn drop_index(&self, table_id: TableID, index_name: &str) -> Result<()> {
        let mut inner = self.inner.write();
        inner.drop_index(table_id, index_name)
    }
}
