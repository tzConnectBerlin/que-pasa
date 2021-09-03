use crate::sql::table::Table;
use crate::storage_structure::relational::{RelationalAST, RelationalEntry};
use std::collections::HashMap;

pub type TableMap = HashMap<String, Table>;

pub struct TableBuilder {
    pub tables: TableMap,
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TableBuilder {
    pub(crate) fn new() -> Self {
        Self {
            tables: TableMap::new(),
        }
    }

    fn add_column(&mut self, rel_entry: &RelationalEntry) {
        let mut table = self.get_table(rel_entry.table_name.clone());
        if rel_entry.is_index {
            table.add_index(rel_entry);
        } else {
            table.add_column(rel_entry);
        }
        self.store_table(table);
    }

    fn get_table(&self, name: String) -> Table {
        match self.tables.get(&name) {
            Some(x) => x.clone(),
            None => Table::new(name),
        }
    }

    fn store_table(&mut self, table: Table) {
        self.tables
            .insert(table.name.clone(), table);
    }

    pub(crate) fn populate(&mut self, rel_ast: &RelationalAST) {
        match rel_ast {
            RelationalAST::Pair {
                left_ast,
                right_ast,
            } => {
                self.populate(left_ast);
                self.populate(right_ast);
            }
            RelationalAST::Map {
                key_ast, value_ast, ..
            } => {
                self.populate(key_ast);
                self.populate(value_ast);
            }
            RelationalAST::BigMap {
                table,
                key_ast,
                value_ast,
                ..
            } => {
                self.populate(key_ast);
                self.populate(value_ast);
                let mut t = self.get_table(table.clone());
                t.tracks_changes();
                self.store_table(t);
            }
            RelationalAST::Option { elem_ast } => self.populate(elem_ast),
            RelationalAST::List {
                table,
                elems_unique,
                elems_ast,
            } => {
                self.populate(elems_ast);
                if !elems_unique {
                    let mut t = self.get_table(table.clone());
                    t.no_uniqueness();
                    self.store_table(t);
                }
            }
            RelationalAST::OrEnumeration {
                or_unfold,
                left_ast,
                right_ast,
                ..
            } => {
                self.add_column(or_unfold);
                self.populate(left_ast);
                self.populate(right_ast);
            }
            RelationalAST::Leaf { rel_entry } => self.add_column(rel_entry),
        }
    }
}
