use crate::storage::SimpleExpr;
use crate::table::{Column, Table};
use chrono::{DateTime, Utc};
use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct PostgresqlGenerator {}

impl PostgresqlGenerator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_sql(&mut self, column: Column) -> String {
        match column.expr {
            SimpleExpr::Address => self.address(&column.name),
            SimpleExpr::Bool => self.bool(&column.name),
            SimpleExpr::Bytes => self.bytes(&column.name),
            SimpleExpr::Int => self.int(&column.name),
            SimpleExpr::Mutez => self.numeric(&column.name),
            SimpleExpr::Nat => self.nat(&column.name),
            SimpleExpr::String => self.string(&column.name),
            SimpleExpr::Timestamp => self.timestamp(&column.name),
            SimpleExpr::Unit => self.unit(&column.name),
        }
    }

    pub fn address(&mut self, name: &String) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub fn bool(&mut self, name: &String) -> String {
        format!("{} BOOLEAN NULL", name)
    }

    pub fn bytes(&mut self, name: &String) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub fn int(&mut self, name: &String) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub fn nat(&mut self, name: &String) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub fn numeric(&mut self, name: &String) -> String {
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

    fn parent_name(name: &String) -> Option<String> {
        if let Some(pos) = name.rfind(".") {
            Some(name.as_str()[0..pos].to_string())
        } else {
            None
        }
    }

    fn create_foreign_key_constraint(&mut self, table: &Table) -> Option<String> {
        if let Some(parent) = Self::parent_name(&table.name) {
            Some(format!(
                "FOREIGN KEY {}_id REFERENCES {}(id)",
                parent, parent
            ))
        } else {
            None
        }
    }

    pub fn create_table_definition(&mut self, table: &Table) -> String {
        let mut v: Vec<String> = vec![];
        v.push(self.start_table(&table.name));
        let mut columns: Vec<String> = self.create_columns(table);
        columns[0] = format!("\t{}", columns[0]);
        if let Some(fk) = self.create_foreign_key_constraint(&table) {
            columns.push(fk);
        }
        v.push(columns.join(",\n\t"));
        v.push(self.end_table());
        v.push(self.create_index(table));
        v.join("\n")
    }

    fn escape(s: &String) -> String {
        s.clone()
    }

    fn quote(value: &crate::michelson::Value) -> String {
        match value {
            crate::michelson::Value::Address(s)
            | crate::michelson::Value::Bytes(s)
            | crate::michelson::Value::String(s)
            | crate::michelson::Value::Unit(Some(s)) => format!(r#"'{}'"#, Self::escape(&s)),
            crate::michelson::Value::Bool(val) => {
                if *val {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            crate::michelson::Value::Int(b)
            | crate::michelson::Value::Mutez(b)
            | crate::michelson::Value::Nat(b) => b.to_str_radix(10).to_string(),
            crate::michelson::Value::None => "NULL".to_string(),
            crate::michelson::Value::Timestamp(t) => {
                let date_time: chrono::DateTime<Utc> = chrono::DateTime::from(*t);
                date_time.to_rfc3339()
            }
            crate::michelson::Value::Elt(_, _)
            | crate::michelson::Value::Left(_)
            | crate::michelson::Value::List(_)
            | crate::michelson::Value::Pair(_, _)
            | crate::michelson::Value::Right(_)
            | crate::michelson::Value::Unit(None) => panic!("quote called with {:?}", value),
        }
    }

    pub fn build_insert(&mut self, insert: &crate::table::insert::Insert, level: u32) -> String {
        let mut columns: String = insert
            .columns
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>()
            .join(", ");
        let mut values: String = insert
            .columns
            .iter()
            .map(|x| Self::quote(&x.value))
            .collect::<Vec<String>>()
            .join(", ");
        if let Some(fk_id) = insert.fk_id {
            columns.push_str(&format!(
                ", {}_id",
                Self::parent_name(&insert.table_name).unwrap()
            ));
            values.push_str(&format!(", {}", fk_id));
        }
        columns.push_str(", _level");
        values.push_str(&format!(", {}", level));
        let sql = format!(
            r#"INSERT INTO "{}" (id, {}) VALUES ({}, {});"#,
            insert.table_name, columns, insert.id, values,
        );
        sql
    }
}
