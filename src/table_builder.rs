use crate::relational::{RelationalAST, RelationalEntry};
use crate::table::Table;
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
        let mut table = self.get_table(rel_entry);
        if rel_entry.is_index {
            table.add_index(rel_entry);
        } else {
            table.add_column(rel_entry);
        }
        self.store_table(table);
    }

    fn get_table(&self, rel_entry: &RelationalEntry) -> Table {
        let name = rel_entry.clone().table_name;
        match self.tables.get(&name) {
            Some(x) => x.clone(),
            None => Table::new(name),
        }
    }

    fn store_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    pub(crate) fn populate(&mut self, rel_ast: &RelationalAST) {
        match rel_ast {
            RelationalAST::Pair(left, right) => {
                self.populate(left);
                self.populate(right);
            }
            RelationalAST::Map(key, value) | RelationalAST::BigMap(_, key, value) => {
                self.populate(key);
                self.populate(value);
            }
            RelationalAST::List(elem) => self.populate(elem),
            RelationalAST::OrEnumeration(left, right) => {
                // TODO
                //self.add_column(rel_ast)
                self.populate(left);
                self.populate(right);
            }
            RelationalAST::Leaf(rel_entry) => self.add_column(rel_entry),
        }
    }
}
