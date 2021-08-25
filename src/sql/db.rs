use anyhow::{anyhow, Result};

use native_tls::{Certificate, TlsConnector};
use postgres::{Client, NoTls, Transaction};
use postgres_native_tls::MakeTlsConnector;
use std::fs;

use chrono::{DateTime, Utc};

use crate::octez::block::LevelMeta;
use crate::sql::insert::{Column, Insert, Value};
use crate::sql::postgresql_generator::PostgresqlGenerator;
use crate::storage_value::processor::TxContext;

pub struct DBClient {
    dbconn: postgres::Client,
}

impl DBClient {
    pub(crate) fn connect(
        url: &str,
        ssl: bool,
        ca_cert: Option<String>,
    ) -> Result<Self> {
        if ssl {
            let mut builder = TlsConnector::builder();
            if let Some(ca_cert) = ca_cert {
                builder.add_root_certificate(Certificate::from_pem(
                    &fs::read(ca_cert)?,
                )?);
            }
            let connector = builder.build()?;
            let connector = MakeTlsConnector::new(connector);

            Ok(DBClient {
                dbconn: postgres::Client::connect(url, connector)?,
            })
        } else {
            Ok(DBClient {
                dbconn: Client::connect(url, NoTls)?,
            })
        }
    }

    pub(crate) fn save_tx_contexts(
        transaction: &mut Transaction,
        tx_context_map: &[TxContext],
    ) -> Result<()> {
        let stmt = transaction.prepare("
INSERT INTO
tx_contexts(id, level, operation_group_number, operation_number, content_number, internal_number, operation_hash, source, destination, entrypoint) VALUES
($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)")?;
        for tx_context in tx_context_map {
            transaction.execute(
                &stmt,
                &[
                    &(tx_context
                        .id
                        .ok_or_else(|| anyhow!("Missing ID on TxContext"))?
                        as i32),
                    &(tx_context.level as i32),
                    &(tx_context.operation_group_number as i32),
                    &(tx_context.operation_number as i32),
                    &(tx_context.content_number as i32),
                    &(tx_context
                        .internal_number
                        .map(|n| n as i32)),
                    &tx_context.operation_hash,
                    &tx_context.source,
                    &tx_context.destination,
                    &tx_context.entrypoint,
                ],
            )?;
        }
        Ok(())
    }

    pub(crate) fn apply_insert(
        tx: &mut postgres::Transaction,
        insert: &Insert,
    ) -> Result<()> {
        let mut columns = insert.columns.clone();
        columns.push(Column {
            name: "id".to_string(),
            value: Value::Int(insert.id as i32),
        });
        if let Some(fk_id) = insert.fk_id {
            let parent_name =
                PostgresqlGenerator::parent_name(&insert.table_name)
                    .ok_or_else(|| {
                        anyhow!(
                            "
                failed to get parent name from table={}",
                            insert.table_name
                        )
                    })?;
            columns.push(Column {
                name: format!("{}_id", parent_name),
                value: Value::Int(fk_id as i32),
            });
        }

        let v_names: String = columns
            .iter()
            .map(|x| PostgresqlGenerator::quote_id(&x.name))
            .collect::<Vec<String>>()
            .join(", ");

        let v_refs = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let i = i + 1;
                match c.value {
                    Value::Numeric(_) => format!("${}::text", i.to_string()),
                    Value::Bool(_) => format!("${}::boolean", i.to_string()),
                    Value::String(_) => format!("${}::text", i.to_string()),
                    Value::Int(_) => format!("${}::integer", i.to_string()),
                    Value::BigInt(_) => format!("${}::bigint", i.to_string()),
                    Value::Timestamp(_) => {
                        format!("${}::timestamp with time zone", i.to_string())
                    }
                    Value::Null => format!("${}", i.to_string()),
                    //_ => format!("${}", i.to_string()),
                }
            })
            .collect::<Vec<String>>()
            .join(", ");

        let v_struct = (1..columns.len() + 1)
            .map(|i| format!("v{}", i.to_string()))
            .collect::<Vec<String>>()
            .join(", ");

        let v_select = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let i = i + 1;
                match c.value {
                    Value::Numeric(_) => {
                        format!("v.v{}::numeric", i.to_string())
                    }
                    Value::Bool(_) => format!("v.v{}::boolean", i.to_string()),
                    Value::String(_) => format!("v.v{}::text", i.to_string()),
                    Value::Int(_) => format!("v.v{}::integer", i.to_string()),
                    Value::BigInt(_) => format!("v.v{}::bigint", i.to_string()),
                    Value::Timestamp(_) => format!(
                        "v.v{}::timestamp with time zone",
                        i.to_string()
                    ),
                    Value::Null => format!("v.v{}", i.to_string()),
                }
            })
            .collect::<Vec<String>>()
            .join(", ");

        let qry = format!(
            r#"
INSERT INTO "{}" (
    {}
)
SELECT {}
FROM (
    VALUES ({})
) as v({})"#,
            insert.table_name, v_names, v_select, v_refs, v_struct
        );
        debug!(
            "qry: {}, values: {:?}",
            qry,
            columns
                .iter()
                .cloned()
                .map(|x| x.value)
                .collect::<Vec<Value>>()
        );
        let stmt = tx.prepare(qry.as_str())?;

        let values: Vec<&dyn postgres::types::ToSql> = columns
            .iter()
            .map(|x| x.value.borrow_to_sql())
            .collect();

        tx.query_raw(&stmt, values)?;
        Ok(())
    }

    pub(crate) fn transaction(&mut self) -> Result<Transaction> {
        Ok(self.dbconn.transaction()?)
    }

    pub(crate) fn delete_everything(&mut self) -> Result<u64> {
        Ok(self
            .dbconn
            .execute("DELETE FROM levels", &[])?)
    }

    pub(crate) fn fill_in_levels(&mut self) -> Result<u64> {
        Ok(self.dbconn.execute(
            "
INSERT INTO levels(_level, hash)
SELECT
    g.level,
    NULL
FROM GENERATE_SERIES(
    (SELECT MIN(_level) FROM levels),
    (SELECT MAX(_level) FROM levels)
) AS g(level)
WHERE g.level NOT IN (
    SELECT _level FROM levels
)",
            &[],
        )?)
    }

    pub(crate) fn get_head(&mut self) -> Result<Option<LevelMeta>> {
        let result = self.dbconn.query(
            "
SELECT _level, hash, is_origination, baked_at
FROM levels
ORDER BY _level
DESC LIMIT 1",
            &[],
        )?;
        if result.is_empty() {
            Ok(None)
        } else if result.len() == 1 {
            let _level: i32 = result[0].get(0);
            let hash: Option<String> = result[0].get(1);
            let baked_at: Option<DateTime<Utc>> = result[0].get(3);
            Ok(Some(LevelMeta {
                _level: _level as u32,
                hash,
                baked_at,
            }))
        } else {
            Err(anyhow!("Too many results for get_head"))
        }
    }

    pub(crate) fn get_missing_levels(
        &mut self,
        origination: Option<u32>,
        end: u32,
    ) -> Result<Vec<u32>> {
        let start = origination.unwrap_or(1);
        let mut rows: Vec<i32> = vec![];
        for row in self.dbconn.query(
            format!(
                "
SELECT
    *
FROM generate_series({},{}) s(i)
WHERE NOT EXISTS (
    SELECT
        _level
    FROM levels
    WHERE _level = s.i
)",
                start, end
            )
            .as_str(),
            &[],
        )? {
            rows.push(row.get(0));
        }
        rows.reverse();
        Ok(rows
            .iter()
            .map(|x| *x as u32)
            .collect::<Vec<u32>>())
    }

    pub(crate) fn get_max_id(&mut self) -> Result<i32> {
        let max_id: i32 = self
            .dbconn
            .query("SELECT max_id FROM max_id", &[])?[0]
            .get(0);
        Ok(max_id + 1)
    }

    pub(crate) fn set_max_id(tx: &mut Transaction, max_id: i32) -> Result<()> {
        let updated = tx.execute(
            "
UPDATE max_id
SET max_id = $1",
            &[&max_id],
        )?;
        if updated == 1 {
            Ok(())
        } else {
            Err(anyhow!(
            "Wrong number of rows in max_id table. Please fix manually. Sorry"
        ))
        }
    }

    /// get the origination of the contract, which is currently store in the levels (will change)
    pub(crate) fn set_origination(
        tx: &mut Transaction,
        level: u32,
    ) -> Result<()> {
        tx.execute(
            "
UPDATE levels
SET is_origination = FALSE
WHERE is_origination = TRUE",
            &[],
        )?;
        tx.execute(
            "
UPDATE levels
SET is_origination = TRUE
WHERE _level = $1",
            &[&(level as i32)],
        )?;
        Ok(())
    }

    pub(crate) fn get_origination(&mut self) -> Result<Option<u32>> {
        let result = self.dbconn.query(
            "
SELECT
    _level
FROM levels
WHERE is_origination = TRUE",
            &[],
        )?;
        if result.is_empty() {
            Ok(None)
        } else if result.len() == 1 {
            let level: i32 = result[0].get(0);
            Ok(Some(level as u32))
        } else {
            Err(anyhow!("Too many results for get_origination"))
        }
    }

    pub(crate) fn save_level(
        tx: &mut Transaction,
        level: &LevelMeta,
    ) -> Result<()> {
        tx.execute(
            "
INSERT INTO levels(
    _level, hash, baked_at
) VALUES ($1, $2, $3)
",
            &[&(level._level as i32), &level.hash, &level.baked_at],
        )?;
        Ok(())
    }

    pub(crate) fn delete_level(
        tx: &mut Transaction,
        level: &LevelMeta,
    ) -> Result<()> {
        tx.execute(
            "
DELETE FROM levels
WHERE _level = $1",
            &[&(level._level as i32)],
        )?;
        Ok(())
    }
}
