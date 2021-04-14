use crate::storage::Expr;
use crate::table::Table;
use crate::table_builder;
use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct PostgresqlGenerator {
    last_index: u32,
    prefix: String,
}

impl PostgresqlGenerator {
    pub fn new() -> Self {
        Self {
            last_index: 0u32,
            prefix: String::from(""),
        }
    }

    pub fn create_sql(&mut self, expr: &Expr) -> String {
        match expr {
            Expr::Address(name) => self.create_address(name.clone()),
            Expr::Int(name) => self.int(name.clone()),
            Expr::Nat(name) => self.nat(name.clone()),
            Expr::String(name) => self.string(name.clone()),
            Expr::Timestamp(name) => self.timestamp(name.clone()),
            Expr::Unit(name) => self.unit(name.clone()),
            Expr::Option(name, expr) => self.create_sql(expr),
            _ => panic!("Unexpected type {:?}", expr),
        }
    }

    pub fn get_name(&mut self, name: &Option<String>) -> String {
        match name {
            Some(x) => x.clone(),
            None => {
                let name = format!("{}{}", self.prefix, self.last_index);
                self.last_index = self.last_index + 1;
                name
            }
        }
    }

    pub fn create_address(&mut self, name: Option<String>) -> String {
        format!("{} VARCHAR(128) NULL,", self.get_name(&name))
    }

    pub fn int(&mut self, name: Option<String>) -> String {
        format!("{} VARCHAR(128) NULL", self.get_name(&name))
    }

    pub fn nat(&mut self, name: Option<String>) -> String {
        format!("{} VARCHAR(128) NULL", self.get_name(&name))
    }

    pub fn string(&mut self, name: Option<String>) -> String {
        format!("{} VARCHAR(128) NULL", self.get_name(&name))
    }

    pub fn timestamp(&mut self, name: Option<String>) -> String {
        format!("{} VARCHAR(128) NULL", self.get_name(&name))
    }

    pub fn unit(&mut self, name: Option<String>) -> String {
        format!("{} VARCHAR(128) NULL", self.get_name(&name))
    }

    pub fn start_table(&mut self, name: Option<String>) -> String {
        format!(
            "CREATE TABLE {} (\
            id SERIAL PRIMARY KEY,",
            self.get_name(&name)
        )
    }

    pub fn end_table(&mut self) -> String {
        format!(")")
    }

    pub fn create_indices(&mut self, table: &Table, tables: &table_builder::Tables) -> Vec<String> {
        //let mut last_index: u32 = 0;
        self.prefix = String::from("idx");
        let mut t: Option<&Table> = Some(table);
        let mut sql: Vec<String> = vec![];
        loop {
            match t {
                None => break,
                _ => (),
            };
            let _t = t.unwrap();
            for idx in _t.indices.iter() {
                sql.push(self.create_sql(idx));
            }
            t = match &_t.parent_name {
                Some(s) => tables.get(&s.clone()),
                None => None,
            }
        }
        sql
    }

    pub fn create_columns(&mut self, table: &Table) -> Vec<String> {
        let mut cols: Vec<String> = vec![];
        self.prefix = String::from("col");
        self.last_index = 0;
        for column in table.columns.iter() {
            cols.push(self.create_sql(column));
        }
        cols
    }

    pub fn create_table_definition(
        &mut self,
        table: &Table,
        tables: &table_builder::Tables,
    ) -> String {
        let mut v: Vec<String> = vec![];
        v.push(self.start_table(Some(table.name.clone())));
        for index in self.create_indices(table, tables).iter() {
            v.push(index.clone());
        }
        for column in self.create_columns(table).iter() {
            v.push(column.clone());
        }
        v.push(self.end_table());
        v.join("\n")
    }
}
