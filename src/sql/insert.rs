use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use pg_bigdecimal::PgNumeric;
use postgres::types::BorrowToSql;
use std::collections::HashMap;

use crate::sql::postgresql_generator::PostgresqlGenerator;

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize,
)]
pub enum Value {
    String(String),
    Bool(bool),
    Numeric(PgNumeric),
    Int(i32),
    BigInt(i64),
    Timestamp(Option<DateTime<Utc>>),
    Null,
}

impl Value {
    pub(crate) fn borrow_to_sql(&self) -> &dyn postgres::types::ToSql {
        match self {
            Value::String(s) => s.borrow_to_sql(),
            Value::Bool(b) => b.borrow_to_sql(),
            Value::Int(i) => i.borrow_to_sql(),
            Value::BigInt(i) => i.borrow_to_sql(),
            Value::Timestamp(Some(t)) => t.borrow_to_sql(),
            Value::Timestamp(None) => {
                postgres::types::Timestamp::<DateTime<Utc>>::PosInfinity
                    .borrow_to_sql()
            }
            Value::Numeric(n) => n.borrow_to_sql(),
            Value::Null => "NULL".borrow_to_sql(),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertKey {
    pub table_name: String,
    pub id: i64,
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
    pub id: i64,
    pub fk_id: Option<i64>,
    pub columns: Vec<Column>,
}

impl Insert {
    pub fn get_columns(&self) -> Result<Vec<Column>> {
        let mut res = self.columns.clone();

        res.push(Column {
            name: "id".to_string(),
            value: Value::BigInt(self.id),
        });
        if let Some(fk_id) = self.fk_id {
            let parent_name = PostgresqlGenerator::parent_name(
                &self.table_name,
            )
            .ok_or_else(|| {
                anyhow!(
                    "
                failed to get parent name from table={}",
                    self.table_name
                )
            })?;
            res.push(Column {
                name: PostgresqlGenerator::parent_ref(&parent_name),
                value: Value::BigInt(fk_id),
            });
        }
        Ok(res)
    }

    pub fn get_bigmap_id(&self) -> Result<Option<i32>> {
        match self.get_column("bigmap_id")? {
            None => Ok(None),
            Some(col) => match col.value {
                Value::Int(i) => Ok(Some(i)),
                _ => Err(anyhow!("bigmap_id column does not have i32 value")),
            },
        }
    }

    pub fn get_tx_context_id(&self) -> Result<i64> {
        match self.get_column("tx_context_id")? {
            None => Err(anyhow!("tx_context_id column is missing")),
            Some(col) => match col.value {
                Value::BigInt(i) => Ok(i),
                _ => {
                    Err(anyhow!("tx_context_id column does not have i64 value"))
                }
            },
        }
    }

    pub fn get_column(&self, name: &str) -> Result<Option<Column>> {
        let col = self.get_columns()?;
        Ok(col
            .iter()
            .find(|column| column.name == name)
            .cloned())
    }

    pub fn map_column<F>(&mut self, col_name: &str, f: F)
    where
        F: FnOnce(&Value) -> Value,
    {
        for col in self.columns.iter_mut() {
            if col.name == col_name {
                col.value = f(&col.value);
                break;
            }
        }
    }
}

pub type Inserts = HashMap<InsertKey, Insert>;

pub(crate) fn offset_inserts_ids(
    inserts: &Inserts,
    offset: i64,
) -> (Inserts, i64) {
    let mut res = Inserts::new();

    let mut max = offset;
    for (k, v) in inserts {
        let mut k = k.clone();
        let mut v = v.clone();

        v.map_column("tx_context_id", |v| match v {
            Value::BigInt(i) => Value::BigInt(i + offset),
            _ => panic!(".."),
        });

        k.id += offset;
        v.id += offset;
        v.fk_id = v.fk_id.map(|fk_id| fk_id + offset);
        max = vec![
            k.id,
            v.id,
            v.fk_id.unwrap_or(0),
            v.get_tx_context_id().unwrap(),
            max,
        ]
        .into_iter()
        .max()
        .unwrap();
        res.insert(k, v);
    }
    (res, max)
}
