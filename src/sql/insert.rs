use chrono::{DateTime, Utc};
use postgres::types::BorrowToSql;
use std::collections::BTreeMap;

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize,
)]
pub enum Value {
    String(String),
    Bool(bool),
    Numeric(String),
    Int(i32),
    BigInt(i64),
    Timestamp(DateTime<Utc>),
    Null,
}

impl Value {
    pub(crate) fn borrow_to_sql(&self) -> &dyn postgres::types::ToSql {
        match self {
            Value::String(s) => s.borrow_to_sql(),
            Value::Bool(b) => b.borrow_to_sql(),
            Value::Int(i) => i.borrow_to_sql(),
            Value::BigInt(i) => i.borrow_to_sql(),
            Value::Timestamp(t) => t.borrow_to_sql(),
            Value::Numeric(d) => d.borrow_to_sql(),
            Value::Null => "NULL".borrow_to_sql(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct InsertKey {
    pub table_name: String,
    pub id: u32,
}

impl std::cmp::Ord for InsertKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        format!("{}{}", other.table_name, other.id)
            .cmp(&format!("{}{}", self.table_name, self.id))
    }
}

impl PartialOrd for InsertKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

//Change name for more clarity?
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub value: Value,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Insert {
    pub table_name: String,
    pub id: u32,
    pub fk_id: Option<u32>,
    pub columns: Vec<Column>,
}

impl Insert {
    #[cfg(test)]
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|column| column.name == name)
    }
}

pub type Inserts = BTreeMap<InsertKey, Insert>;
