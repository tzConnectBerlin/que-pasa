use crate::node::Node;
use crate::storage::{Expr, SimpleExpr};
use crate::table::{Column, Table};
use crate::table_builder;
use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct PostgresqlGenerator {}

impl PostgresqlGenerator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_sql(&mut self, column: Column) -> String {
        match column.expr {
            SimpleExpr::Address => self.create_address(&column.name),
            SimpleExpr::Int => self.int(&column.name),
            SimpleExpr::Nat => self.nat(&column.name),
            SimpleExpr::String => self.string(&column.name),
            SimpleExpr::Timestamp => self.timestamp(&column.name),
            SimpleExpr::Unit => self.unit(&column.name),
            _ => panic!("Unexpected type {:?}", column.expr),
        }
    }

    pub fn create_address(&mut self, name: &String) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub fn int(&mut self, name: &String) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub fn nat(&mut self, name: &String) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub fn string(&mut self, name: &String) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub fn timestamp(&mut self, name: &String) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub fn unit(&mut self, name: &String) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub fn start_table(&mut self, name: &String) -> String {
        format!(
            "CREATE TABLE \"{}\" (\n\
                \tid SERIAL PRIMARY KEY,\n\
                \t _level INTEGER NOT NULL,",
            name
        )
    }

    pub fn end_table(&mut self) -> String {
        format!(");\n")
    }

    pub fn create_columns(&mut self, table: &Table) -> Vec<String> {
        let mut cols: Vec<String> = vec![];
        for column in &table.columns {
            cols.push(self.create_sql(column.clone()));
        }
        cols
    }

    pub fn create_index(&mut self, table: &Table) -> String {
        format!(
            "CREATE UNIQUE INDEX ON \"{}\"(_level, {});\n",
            table.name,
            table.indices.join(", ")
        )
    }

    pub fn create_table_definition(&mut self, table: &Table) -> String {
        let mut v: Vec<String> = vec![];
        v.push(self.start_table(&table.name));
        let mut columns: Vec<String> = self.create_columns(table);
        v.push(columns.join(",\n\t"));
        v.push(self.end_table());
        v.push(self.create_index(table));
        v.join("\n")
    }
}
