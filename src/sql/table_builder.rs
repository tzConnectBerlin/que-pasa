use crate::sql::table::Table;
use crate::storage_structure::relational::{RelationalAST, RelationalEntry};
use crate::storage_structure::typing::{ExprTy, SimpleExprTy};
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
        let mut res = Self {
            tables: TableMap::new(),
        };
        res.add_column(&RelationalEntry {
            table_name: "storage".to_string(),
            column_name: "deleted".to_string(),
            column_type: ExprTy::SimpleExprTy(SimpleExprTy::Bool),
            value: None,
            is_index: false,
        });
        res
    }

    fn add_column(&mut self, rel_entry: &RelationalEntry) {
        let mut table = self.get_table(&rel_entry.table_name);
        if rel_entry.is_index {
            table.add_index(rel_entry);
        } else {
            table.add_column(rel_entry);
        }
        self.store_table(table);
    }

    fn touch_table(&mut self, name: &String) {
        self.store_table(self.get_table(name))
    }

    fn get_table(&self, name: &String) -> Table {
        match self.tables.get(name) {
            Some(x) => x.clone(),
            None => Table::new(name.clone()),
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
                let mut t = self.get_table(table);
                t.tracks_changes();
                t.add_column(&RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "deleted".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Bool),
                    value: None,
                    is_index: false,
                });
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
                    let mut t = self.get_table(table);
                    t.no_uniqueness();
                    self.store_table(t);
                }
            }
            RelationalAST::OrEnumeration {
                or_unfold,
                left_table,
                left_ast,
                right_table,
                right_ast,
            } => {
                self.add_column(or_unfold);

                self.touch_table(left_table);
                self.touch_table(right_table);

                self.populate(left_ast);
                self.populate(right_ast);
            }
            RelationalAST::Leaf { rel_entry } => self.add_column(rel_entry),
        }
    }
}
