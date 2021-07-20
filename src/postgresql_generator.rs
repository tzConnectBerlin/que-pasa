use crate::error::Res;
use crate::michelson::Level;
use crate::node::Context;
use crate::storage::SimpleExpr;
use crate::table::{Column, Table};
use chrono::{DateTime, Utc};
use native_tls::{Certificate, TlsConnector};
use postgres::{Client, NoTls, Transaction};
use postgres_native_tls::MakeTlsConnector;
use std::error::Error;
use std::fs;

use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct PostgresqlGenerator {}

impl Default for PostgresqlGenerator {
    fn default() -> Self {
        Self::new()
    }
}

pub fn connect(ssl: bool, ca_cert: Option<&str>) -> Res<Client> {
    let url = std::env::var(&"DATABASE_URL").unwrap();
    debug!("DATABASE_URL={}", url);

    if ssl {
        let mut builder = TlsConnector::builder();
        if let Some(ca_cert) = ca_cert {
            builder.add_root_certificate(Certificate::from_pem(&fs::read(ca_cert)?)?);
        }
        let connector = builder.build()?;
        let connector = MakeTlsConnector::new(connector);

        Ok(postgres::Client::connect(&url, connector)?)
    } else {
        Ok(Client::connect(&url, NoTls)?)
    }
}

pub fn transaction(client: &mut Client) -> Result<Transaction, Box<dyn Error>> {
    Ok(client.transaction()?)
}

pub fn commit(transaction: Transaction) -> Result<(), Box<dyn Error>> {
    Ok(transaction.commit()?)
}

pub fn exec(transaction: &mut Transaction, sql: &str) -> Result<u64, Box<dyn Error>> {
    debug!("postgresql_generator::exec {}:", sql);
    match transaction.execute(sql, &[]) {
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
        "SELECT _level, hash, is_origination, baked_at FROM levels ORDER BY _level DESC LIMIT 1",
        &[],
    )?;
    if result.is_empty() {
        Ok(None)
    } else if result.len() == 1 {
        let _level: i32 = result[0].get(0);
        let hash: Option<String> = result[0].get(1);
        let baked_at: Option<DateTime<Utc>> = result[0].get(3);
        Ok(Some(Level {
            _level: _level as u32,
            hash,
            baked_at,
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
    let start = origination.unwrap_or(1);
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
            "Wrong number of rows in max_id table. Please fix manually. Sorry",
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
    if result.is_empty() {
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
            "INSERT INTO levels(_level, hash, baked_at) VALUES ({}, {}, {})",
            level._level,
            match &level.hash {
                Some(hash) => format!("'{}'", hash),
                None => "NULL".to_string(),
            },
            match &level.baked_at {
                Some(baked_at) =>
                    PostgresqlGenerator::quote(&crate::michelson::Value::Timestamp(*baked_at)),
                None => "NULL".to_string(),
            }
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

    pub fn create_sql(&mut self, column: Column) -> Option<String> {
        let name = Self::quote_id(&column.name);
        match column.expr {
            SimpleExpr::Address => Some(self.address(&name)),
            SimpleExpr::Bool => Some(self.bool(&name)),
            SimpleExpr::Bytes => Some(self.bytes(&name)),
            SimpleExpr::Int => Some(self.int(&name)),
            SimpleExpr::KeyHash => Some(self.string(&name)),
            SimpleExpr::Mutez => Some(self.numeric(&name)),
            SimpleExpr::Nat => Some(self.nat(&name)),
            SimpleExpr::Stop => None,
            SimpleExpr::String => Some(self.string(&name)),
            SimpleExpr::Timestamp => Some(self.timestamp(&name)),
            SimpleExpr::Unit => Some(self.unit(&name)),
        }
    }

    fn quote_id(s: &str) -> String {
        format!("\"{}\"", s)
    }

    pub fn address(&mut self, name: &str) -> String {
        format!("{} VARCHAR(127) NULL", name)
    }

    pub fn bool(&mut self, name: &str) -> String {
        format!("{} BOOLEAN NULL", name)
    }

    pub fn bytes(&mut self, name: &str) -> String {
        format!("{} TEXT NULL", name)
    }

    pub fn int(&mut self, name: &str) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub fn nat(&mut self, name: &str) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub fn numeric(&mut self, name: &str) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub fn string(&mut self, name: &str) -> String {
        format!("{} TEXT NULL", name)
    }

    pub fn timestamp(&mut self, name: &str) -> String {
        format!("{} TIMESTAMP NULL", name)
    }

    pub fn unit(&mut self, name: &str) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub fn start_table(&mut self, name: &str) -> String {
        format!(include_str!("../sql/postgresql-table-header.sql"), name)
    }

    pub fn end_table(&mut self) -> String {
        include_str!("../sql/postgresql-table-footer.sql").to_string()
    }

    pub fn create_columns(&mut self, table: &Table) -> Res<Vec<String>> {
        let mut cols: Vec<String> = match Self::parent_name(&table.name) {
            Some(x) => vec![format!(r#""{}_id" BIGINT"#, x)],
            None => vec![],
        };
        for column in &table.columns {
            if let Some(val) = self.create_sql(column.clone()) {
                cols.push(val);
            }
        }
        Ok(cols)
    }

    fn indices(&self, table: &Table) -> Vec<String> {
        let mut indices = table.indices.clone();
        if let Some(parent_key) = self.parent_key(table) {
            indices.push(parent_key);
        }
        indices
    }

    pub fn create_index(&mut self, table: &Table) -> String {
        format!(
            "CREATE INDEX ON \"{}\"({});\n",
            table.name,
            self.indices(table).join(", ")
        )
    }

    fn parent_name(name: &str) -> Option<String> {
        name.rfind('.').map(|pos| name[0..pos].to_string())
    }

    fn parent_key(&self, table: &Table) -> Option<String> {
        Self::parent_name(&table.name).map(|parent| format!(r#""{}_id""#, parent))
    }

    fn create_foreign_key_constraint(&mut self, table: &Table) -> Option<String> {
        Self::parent_name(&table.name).map(|parent| {
            format!(
                r#"FOREIGN KEY ("{}_id") REFERENCES "{}"(id)"#,
                parent, parent
            )
        })
    }

    pub fn create_common_tables(&mut self) -> String {
        include_str!("../sql/postgresql-common-tables.sql").to_string()
    }

    pub fn fill_big_map_table(&mut self, table: &Table, big_map_names: Vec<String>) -> String {
        let columns = table
            .columns
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>()
            .join(",");
        let mut sql_commands = vec![];

        for name in big_map_names {
            let sql = format!(
                r#"INSERT INTO "{}" ({}) VALUES ({})"#,
                table.name, columns, name
            );
            sql_commands.push(sql);
        }
        sql_commands.join("\n")
    }

    pub fn create_big_map_table(&mut self, context: Context, tables_names: Vec<String>) -> String {
        let mut columns = vec![];

        let column = Column {
            name: "big_map_table_name".to_string(),
            expr: SimpleExpr::String,
        };

        columns.push(column);

        let table = Table {
            name: context.start_table("big_map".to_string()).table_name,
            indices: vec![],
            columns,
        };

        let big_map = self.fill_big_map_table(&table, tables_names);
        let mut table_definition = self.create_table_definition(&table).unwrap();
        table_definition.push_str(&big_map);
        table_definition
    }

    pub fn create_view_store_all(&mut self, tables_names: Vec<String>) -> String {
        let mut query = String::new();
        query.push_str("CREATE VIEW storage_all AS SELECT DISTINCT ON (l._level) l._level, ");
        query.push_str(
            &tables_names
                .iter()
                .map(|x| format!(r#""{}".id as "{}_id""#, x, x))
                .collect::<Vec<String>>()
                .join(", "),
        );
        query.push_str("\nFROM levels l\n");
        query.push_str(&tables_names.iter().map(|x|
            [
                "LEFT JOIN\n",
                format!(
                r#"(SELECT id, MAX(_level) AS max_level FROM "{}" GROUP BY id, _level ORDER BY max_level DESC) as "{}" ON l._level >= "{}".max_level"#,
                x, x, x
                ).as_str()
            ].concat()
        ).collect::<Vec<String>>().join("\n"));
        query.push_str("\nORDER BY _level DESC;\n");
        query
    }

    pub fn create_table_definition(&mut self, table: &Table) -> Res<String> {
        let mut v: Vec<String> = vec![self.start_table(&table.name)];
        let mut columns: Vec<String> = self.create_columns(table)?;
        columns[0] = format!("\t{}", columns[0]);
        if let Some(fk) = self.create_foreign_key_constraint(table) {
            columns.push(fk);
        }
        let mut s = columns.join(",\n\t");
        s.push_str(",\n\t");
        v.push(s);
        v.push(self.end_table());
        v.push(self.create_index(table));
        Ok(v.join("\n"))
    }

    pub fn create_view_definition(&mut self, table: &Table) -> String {
        if table.name == "storage" {
            return "".to_string();
        }
        let mut indices = self.indices(table);
        indices.remove(indices.iter().position(|x| *x == "_level").unwrap());
        format!(
            r#"
CREATE VIEW "{}_live" AS (
        SELECT t1.* FROM "{}" t1
        INNER JOIN (
                SELECT {}, MAX(_level) AS _level FROM "{}"
        GROUP BY {}) t2
        ON t1._level = t2._level);
"#,
            table.name,
            table.name,
            indices.join(", "),
            table.name,
            indices.join(", "),
            // indices
            //     .iter()
            //     .map(|x| format!(" AND t1.{} = t2.{}", x, x))
            //    .collect::<String>()
        )
    }

    fn escape(s: &str) -> String {
        s.to_string()
    }

    fn quote(value: &crate::michelson::Value) -> String {
        match value {
            crate::michelson::Value::Address(s)
            | crate::michelson::Value::KeyHash(s)
            | crate::michelson::Value::String(s)
            | crate::michelson::Value::Unit(Some(s)) => format!(r#"'{}'"#, Self::escape(s)),
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
                    match crate::michelson::StorageParser::decode_address(s) {
                        Ok(a) => a,
                        Err(_) => s.to_string(),
                    }
                )
            }
            crate::michelson::Value::Int(b)
            | crate::michelson::Value::Mutez(b)
            | crate::michelson::Value::Nat(b) => b.to_str_radix(10),
            crate::michelson::Value::None => "NULL".to_string(),
            crate::michelson::Value::Timestamp(t) => {
                format!("'{}'", t.to_rfc2822())
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
            .map(|x| Self::quote_id(&x.name))
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
            r#"INSERT INTO "{}"
(id, {})
VALUES
({}, {})"#,
            insert.table_name, columns, insert.id, values,
        );
        sql
    }
}
