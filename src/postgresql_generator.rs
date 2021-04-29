use crate::error::Res;
use crate::michelson::Level;
use crate::storage::SimpleExpr;
use crate::table::{Column, Table};
use chrono::Utc;
use postgres::{Client, NoTls, Transaction};
use std::error::Error;
use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct PostgresqlGenerator {}

pub fn connect() -> Res<Client> {
    let url = std::env::var(&"DATABASE_URL").unwrap();
    debug!("DATABASE_URL={}", url);
    Ok(Client::connect(&url, NoTls)?)
}

pub fn transaction(client: &mut Client) -> Result<Transaction, Box<dyn Error>> {
    Ok(client.transaction()?)
}

pub fn commit(transaction: Transaction) -> Result<(), Box<dyn Error>> {
    Ok(transaction.commit()?)
}

pub fn exec(transaction: &mut Transaction, sql: &String) -> Result<u64, Box<dyn Error>> {
    debug!("postgresql_generator::exec {}:", sql);
    match transaction.execute(sql.as_str(), &[]) {
        Ok(x) => Ok(x),
        Err(e) => Err(Box::new(crate::error::Error::new(&e.to_string()))),
    }
}

pub fn delete_everything(connection: &mut Client) -> Res<u64> {
    Ok(connection.execute("DELETE FROM levels", &[])?)
}

pub fn fill_in_levels(connection: &mut Client, from: u32, to: u32) -> Res<u64> {
    Ok(connection.execute(
        format!("INSERT INTO levels(_level, hash) SELECT g.level, NULL FROM GENERATE_SERIES({},{}) AS g(level) WHERE g.level NOT IN (SELECT _level FROM levels)", from, to).as_str(), &[])?)
}

pub fn get_head(connection: &mut Client) -> Res<Option<Level>> {
    let result = connection.query(
        "SELECT _level, hash, is_origination FROM levels ORDER BY _level DESC LIMIT 1",
        &[],
    )?;
    if result.len() == 0 {
        Ok(None)
    } else if result.len() == 1 {
        let _level: i32 = result[0].get(0);
        Ok(Some(Level {
            _level: _level as u32,
            hash: result[0].get(1),
        }))
    } else {
        Err(crate::error::Error::boxed("Too many results for get_head"))
    }
}

pub fn get_missing_levels(
    connection: &mut Client,
    origination: Option<u32>,
    end: u32,
) -> Res<Vec<u32>> {
    let start = match origination {
        Some(x) => x,
        None => 1,
    };
    let mut rows: Vec<i32> = vec![];
    for row in connection.query(
        format!("SELECT * from generate_series({},{}) s(i) WHERE NOT EXISTS (SELECT _level FROM levels WHERE _level = s.i)", start, end).as_str(), &[])? {
        rows.push(row.get(0));
    }
    rows.reverse();
    Ok(rows.iter().map(|x| *x as u32).collect::<Vec<u32>>())
}

pub fn get_max_id(connection: &mut Client) -> Res<i32> {
    let max_id: i32 = connection.query("SELECT max_id FROM max_id", &[])?[0].get(0);
    Ok(max_id + 1)
}

pub fn set_max_id(connection: &mut Transaction, max_id: i32) -> Res<()> {
    let updated = connection.execute("UPDATE max_id SET max_id=$1", &[&max_id])?;
    if updated == 1 {
        Ok(())
    } else {
        Err(crate::error::Error::boxed(
            &"Wrong number of rows in max_id table. Please fix manually. Sorry",
        ))
    }
}

/// get the origination of the contract, which is currently store in the levels (will change)
pub fn set_origination(transaction: &mut Transaction, level: u32) -> Res<()> {
    exec(
        transaction,
        &"UPDATE levels SET is_origination = FALSE WHERE is_origination = TRUE".to_string(),
    )?;
    exec(
        transaction,
        &format!(
            "UPDATE levels SET is_origination = TRUE where _level={}",
            level,
        ),
    )?;
    Ok(())
}

pub fn get_origination(connection: &mut Client) -> Res<Option<u32>> {
    let result = connection.query("SELECT _level FROM levels WHERE is_origination = TRUE", &[])?;
    if result.len() == 0 {
        Ok(None)
    } else if result.len() == 1 {
        let level: i32 = result[0].get(0);
        Ok(Some(level as u32))
    } else {
        Err(crate::error::Error::boxed(
            "Too many results for get_origination",
        ))
    }
}

pub fn save_level(transaction: &mut Transaction, level: &Level) -> Res<u64> {
    exec(
        transaction,
        &format!(
            "INSERT INTO levels(_level, hash) VALUES ({}, '{}')",
            level._level, level.hash
        ),
    )
}

pub fn delete_level(transaction: &mut Transaction, level: &Level) -> Res<u64> {
    exec(
        transaction,
        &format!("DELETE FROM levels where _level = {}", level._level),
    )
}

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
        format!("{} VARCHAR(127) NULL", name)
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
        format!("{} TIMESTAMP NULL", name)
    }

    pub fn unit(&mut self, name: &String) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub fn start_table(&mut self, name: &String) -> String {
        format!(include_str!("../sql/postgresql-table-header.sql"), name)
    }

    pub fn end_table(&mut self) -> String {
        include_str!("../sql/postgresql-table-footer.sql").to_string()
    }

    pub fn create_columns(&mut self, table: &Table) -> Vec<String> {
        let mut cols: Vec<String> = match Self::parent_name(&table.name) {
            Some(x) => vec![format!(r#""{}_id" BIGINT NOT NULL"#, x)],
            None => vec![],
        };
        for column in &table.columns {
            cols.push(self.create_sql(column.clone()));
        }
        cols
    }

    pub fn create_index(&mut self, table: &Table) -> String {
        format!(
            "CREATE INDEX ON \"{}\"({});\n",
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
                r#"FOREIGN KEY ("{}_id") REFERENCES "{}"(id)"#,
                parent, parent
            ))
        } else {
            None
        }
    }

    pub fn create_common_tables(&mut self) -> String {
        include_str!("../sql/postgresql-common-tables.sql").to_string()
    }

    pub fn create_table_definition(&mut self, table: &Table) -> String {
        let mut v: Vec<String> = vec![];
        v.push(self.start_table(&table.name));
        let mut columns: Vec<String> = self.create_columns(table);
        columns[0] = format!("\t{}", columns[0]);
        if let Some(fk) = self.create_foreign_key_constraint(&table) {
            columns.push(fk);
        }
        let mut s = columns.join(",\n\t");
        s.push_str(",\n\t");
        v.push(s);
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
            | crate::michelson::Value::String(s)
            | crate::michelson::Value::Unit(Some(s)) => format!(r#"'{}'"#, Self::escape(&s)),
            crate::michelson::Value::Bool(val) => {
                if *val {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            crate::michelson::Value::Bytes(s) => {
                format!(
                    "'{}'",
                    match crate::michelson::StorageParser::decode_address(&s) {
                        Ok(a) => a,
                        Err(_) => s.to_string(),
                    }
                )
            }
            crate::michelson::Value::Int(b)
            | crate::michelson::Value::Mutez(b)
            | crate::michelson::Value::Nat(b) => b.to_str_radix(10).to_string(),
            crate::michelson::Value::None => "NULL".to_string(),
            crate::michelson::Value::Timestamp(t) => {
                let date_time: chrono::DateTime<Utc> = chrono::DateTime::from(*t);
                format!("'{}'", date_time.to_rfc2822())
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
                r#", "{}_id""#,
                Self::parent_name(&insert.table_name).unwrap()
            ));
            values.push_str(&format!(", {}", fk_id));
        }
        columns.push_str(", _level");
        values.push_str(&format!(", {}", level));
        let sql = format!(
            r#"INSERT INTO "{}" (id, {}) VALUES ({}, {})"#,
            insert.table_name, columns, insert.id, values,
        );
        sql
    }
}
