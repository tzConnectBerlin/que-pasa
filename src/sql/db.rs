use anyhow::{anyhow, Result};
use itertools::Itertools;
use std::collections::HashMap;

use native_tls::{Certificate, TlsConnector};
use postgres::types::BorrowToSql;
use postgres::{Client, NoTls, Transaction};
use postgres_native_tls::MakeTlsConnector;
use std::fs;

use chrono::{DateTime, Utc};

use crate::config::ContractID;
use crate::octez::block::LevelMeta;
use crate::octez::node::NodeClient;
use crate::sql::insert::{Column, Insert, Value};
use crate::sql::postgresql_generator::PostgresqlGenerator;
use crate::sql::table_builder::TableBuilder;
use crate::storage_structure::relational::RelationalAST;
use crate::storage_value::processor::TxContext;

pub struct DBClient {
    dbconn: postgres::Client,
}

impl DBClient {
    const INSERT_BATCH_SIZE: usize = 100;

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

    pub(crate) fn create_common_tables(&mut self) -> Result<()> {
        self.dbconn.simple_query(
            PostgresqlGenerator::create_common_tables().as_str(),
        )?;
        Ok(())
    }

    pub(crate) fn create_contract_schema(
        &mut self,
        contract_id: &ContractID,
        rel_ast: &RelationalAST,
    ) -> Result<bool> {
        if !self.contract_schema_defined(contract_id)? {
            info!("creating schema for contract {}", contract_id.name);
            // Generate the SQL schema for this contract
            let mut builder = TableBuilder::new();
            builder.populate(rel_ast);

            let generator = PostgresqlGenerator::new(contract_id);
            let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
            sorted_tables.sort_by_key(|a| a.0);

            let mut tx = self.transaction()?;
            tx.execute(
                "
INSERT INTO contracts (name, address)
VALUES ($1, $2)",
                &[&contract_id.name, &contract_id.address],
            )?;
            tx.simple_query(
                format!(
                    r#"
CREATE SCHEMA IF NOT EXISTS "{contract_schema}";
"#,
                    contract_schema = contract_id.name
                )
                .as_str(),
            )?;
            for (_name, table) in sorted_tables {
                let table_def = generator.create_table_definition(table)?;
                let views_def = generator.create_view_definition(table)?;

                tx.simple_query(table_def.as_str())?;
                tx.simple_query(views_def.as_str())?;
            }
            tx.commit()?;

            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn delete_contract_schema(
        tx: &mut Transaction,
        contract_id: &ContractID,
        rel_ast: &RelationalAST,
    ) -> Result<()> {
        info!("deleting schema for contract {}", contract_id.name);
        // Generate the SQL schema for this contract
        let mut builder = TableBuilder::new();
        builder.populate(rel_ast);

        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);
        sorted_tables.reverse();

        for (_name, table) in sorted_tables {
            if table.name != "bigmap_clears" {
                tx.simple_query(
                    format!(
                        r#"
DROP VIEW "{contract_schema}"."{table}_ordered";
DROP VIEW "{contract_schema}"."{table}_live";
"#,
                        contract_schema = contract_id.name,
                        table = table.name,
                    )
                    .as_str(),
                )?;
            }

            tx.simple_query(
                format!(
                    r#"
DROP TABLE "{contract_schema}"."{table}";
"#,
                    contract_schema = contract_id.name,
                    table = table.name,
                )
                .as_str(),
            )?;
        }
        Ok(())
    }

    fn contract_schema_defined(
        &mut self,
        contract_id: &ContractID,
    ) -> Result<bool> {
        let res = self.dbconn.query_opt(
            "
SELECT
    1
FROM contracts
WHERE name = $1
",
            &[&contract_id.name],
        )?;
        Ok(res.is_some())
    }

    pub(crate) fn save_tx_contexts(
        tx: &mut Transaction,
        tx_contexts: &[TxContext],
    ) -> Result<()> {
        struct TxContextPG {
            id: i32,
            pub level: i32,
            pub contract: String,
            pub operation_hash: String,
            pub operation_group_number: i32,
            pub operation_number: i32,
            pub content_number: i32,
            pub internal_number: Option<i32>,
            pub source: Option<String>,
            pub destination: Option<String>,
            pub entrypoint: Option<String>,
        }
        for chunk in tx_contexts.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 11;
            let v_refs = (1..(num_columns * chunk.len()) + 1)
                .map(|i| format!("${}", i.to_string()))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!("
INSERT INTO
tx_contexts(id, level, contract, operation_group_number, operation_number, content_number, internal_number, operation_hash, source, destination, entrypoint) VALUES ( {} )", v_refs))?;

            let tx_contexts_pg: Vec<TxContextPG> = chunk
                .iter()
                .map(|tx_context| TxContextPG {
                    id: tx_context
                        .id
                        .ok_or_else(|| anyhow!("Missing ID on TxContext"))
                        .unwrap() as i32,
                    level: tx_context.level as i32,
                    contract: tx_context.contract.clone(),
                    operation_group_number: tx_context.operation_group_number
                        as i32,
                    operation_number: tx_context.operation_number as i32,
                    content_number: tx_context.content_number as i32,
                    internal_number: tx_context
                        .internal_number
                        .map(|n| n as i32),
                    operation_hash: tx_context.operation_hash.clone(),
                    source: tx_context.source.clone(),
                    destination: tx_context.destination.clone(),
                    entrypoint: tx_context.entrypoint.clone(),
                })
                .collect();
            let values: Vec<&dyn postgres::types::ToSql> = tx_contexts_pg
                .iter()
                .flat_map(|tx_context| {
                    [
                        tx_context.id.borrow_to_sql(),
                        tx_context.level.borrow_to_sql(),
                        tx_context.contract.borrow_to_sql(),
                        tx_context
                            .operation_group_number
                            .borrow_to_sql(),
                        tx_context
                            .operation_number
                            .borrow_to_sql(),
                        tx_context
                            .content_number
                            .borrow_to_sql(),
                        tx_context
                            .internal_number
                            .borrow_to_sql(),
                        tx_context
                            .operation_hash
                            .borrow_to_sql(),
                        tx_context.source.borrow_to_sql(),
                        tx_context.destination.borrow_to_sql(),
                        tx_context.entrypoint.borrow_to_sql(),
                    ]
                })
                .collect();

            tx.query_raw(&stmt, values)?;
        }

        Ok(())
    }

    pub(crate) fn apply_inserts(
        tx: &mut postgres::Transaction,
        contract_id: &ContractID,
        inserts: &[Insert],
    ) -> Result<()> {
        let mut table_grouped: HashMap<(String, Vec<String>), Vec<Insert>> =
            HashMap::new();
        for insert in inserts {
            let key = &(
                insert.table_name.clone(),
                insert
                    .columns
                    .iter()
                    .map(|col| col.name.clone())
                    .collect::<Vec<String>>(),
            );
            if !table_grouped.contains_key(key) {
                table_grouped.insert(key.clone(), vec![]);
            }
            table_grouped
                .get_mut(key)
                .unwrap()
                .push(insert.clone());
        }
        let mut keys: Vec<&(String, Vec<String>)> =
            table_grouped.keys().collect();
        keys.sort();
        for k in keys {
            let table_inserts = table_grouped.get(k).unwrap();
            for chunk in table_inserts.chunks(Self::INSERT_BATCH_SIZE) {
                Self::apply_inserts_for_table(tx, contract_id, chunk)?;
            }
        }
        Ok(())
    }

    pub(crate) fn apply_inserts_for_table(
        tx: &mut postgres::Transaction,
        contract_id: &ContractID,
        inserts: &[Insert],
    ) -> Result<()> {
        let meta = &inserts[0];

        let columns = inserts[0].get_columns()?;

        let v_names: String = columns
            .iter()
            .map(|x| PostgresqlGenerator::quote_id(&x.name))
            .collect::<Vec<String>>()
            .join(", ");

        let v_refs = (1..(columns.len() * inserts.len()) + 1)
            .map(|i| format!("${}", i.to_string()))
            .collect::<Vec<String>>()
            .chunks(columns.len())
            .map(|x| x.join(", "))
            .join("), (");

        let qry = format!(
            r#"
INSERT INTO "{contract_schema}"."{table}" ( {v_names} )
VALUES ( {v_refs} )"#,
            contract_schema = contract_id.name,
            table = meta.table_name,
            v_names = v_names,
            v_refs = v_refs,
        );
        let stmt = tx.prepare(qry.as_str())?;

        let all_columns: Vec<Column> = inserts
            .iter()
            .map(|insert| insert.get_columns())
            .collect::<Result<Vec<_>>>()?
            .iter()
            .flatten()
            .cloned()
            .collect();

        debug!(
            "qry: {}, values: {:#?}",
            qry,
            all_columns
                .iter()
                .cloned()
                .map(|x| x.value)
                .collect::<Vec<Value>>()
        );

        let values: Vec<&dyn postgres::types::ToSql> = all_columns
            .iter()
            .map(|x| x.value.borrow_to_sql())
            .collect();
        tx.query_raw(&stmt, values)?;
        Ok(())
    }

    pub(crate) fn transaction(&mut self) -> Result<Transaction> {
        Ok(self.dbconn.transaction()?)
    }

    pub(crate) fn delete_everything<F>(
        &mut self,
        node_cli: &mut NodeClient,
        mut get_rel_ast: F,
    ) -> Result<()>
    where
        F: FnMut(&mut NodeClient, &str) -> Result<RelationalAST>,
    {
        let mut tx = self.transaction()?;
        let contracts_table = tx.query_opt(
            "
SELECT
    1
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name = 'contracts'
",
            &[],
        )?;
        if contracts_table.is_some() {
            for row in tx.query("SELECT name, address FROM contracts", &[])? {
                let contract_id = ContractID {
                    name: row.get(0),
                    address: row.get(1),
                };
                let rel_ast = get_rel_ast(node_cli, &contract_id.address)?;
                Self::delete_contract_schema(&mut tx, &contract_id, &rel_ast)?
            }
        }
        tx.simple_query(
            "
DROP TABLE IF EXISTS tx_contexts;
DROP TABLE IF EXISTS max_id;
DROP TABLE IF EXISTS contract_levels;
DROP TABLE IF EXISTS contracts;
DROP TABLE IF EXISTS levels;
",
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn fill_in_levels(
        &mut self,
        contract_id: &ContractID,
    ) -> Result<u64> {
        Ok(self.dbconn.execute(
            "
INSERT INTO contract_levels(contract, level)
SELECT
    $1,
    g.level
FROM GENERATE_SERIES(
    (SELECT MIN(level) FROM levels),
    (SELECT MAX(level) FROM levels)
) AS g(level)
WHERE g.level NOT IN (
    SELECT level FROM contract_levels WHERE contract = $1
)",
            &[&contract_id.name],
        )?)
    }

    pub(crate) fn get_head(&mut self) -> Result<Option<LevelMeta>> {
        let result = self.dbconn.query(
            "
SELECT level, hash, baked_at
FROM levels
ORDER BY level
DESC LIMIT 1",
            &[],
        )?;
        if result.is_empty() {
            Ok(None)
        } else if result.len() == 1 {
            let level: i32 = result[0].get(0);
            let hash: Option<String> = result[0].get(1);
            let baked_at: Option<DateTime<Utc>> = result[0].get(2);
            Ok(Some(LevelMeta {
                level: level as u32,
                hash,
                baked_at,
            }))
        } else {
            Err(anyhow!("Too many results for get_head"))
        }
    }

    pub(crate) fn get_missing_levels(
        &mut self,
        contracts: &[ContractID],
        end: u32,
    ) -> Result<Vec<u32>> {
        let mut rows: Vec<i32> = vec![];
        for contract_id in contracts {
            let origination = self.get_origination(contract_id)?;
            let start = origination.unwrap_or(1);
            for row in self.dbconn.query(
                format!(
                    "
SELECT
    s.i
FROM generate_series({},{}) s(i)
WHERE NOT EXISTS (
    SELECT
        1
    FROM contract_levels c
    WHERE contract = $1
      AND level = s.i
)
ORDER BY 1",
                    start, end
                )
                .as_str(),
                &[&contract_id.name],
            )? {
                rows.push(row.get(0));
            }
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

    pub(crate) fn save_level(
        tx: &mut Transaction,
        meta: &LevelMeta,
    ) -> Result<()> {
        tx.execute(
            "
INSERT INTO levels(
    level, hash, baked_at
) VALUES ($1, $2, $3)
",
            &[&(meta.level as i32), &meta.hash, &meta.baked_at],
        )?;
        Ok(())
    }

    pub(crate) fn delete_level(
        tx: &mut Transaction,
        meta: &LevelMeta,
    ) -> Result<()> {
        tx.execute(
            "
DELETE FROM contract_levels
WHERE level = $1",
            &[&(meta.level as i32)],
        )?;
        tx.execute(
            "
DELETE FROM levels
WHERE level = $1",
            &[&(meta.level as i32)],
        )?;
        Ok(())
    }

    pub(crate) fn save_contract_level(
        tx: &mut Transaction,
        contract_id: &ContractID,
        level: u32,
    ) -> Result<()> {
        tx.execute(
            "
INSERT INTO contract_levels(
    contract, level
) VALUES ($1, $2)
",
            &[&contract_id.name, &(level as i32)],
        )?;
        Ok(())
    }

    pub(crate) fn delete_contract_level(
        tx: &mut Transaction,
        contract_id: &ContractID,
        level: u32,
    ) -> Result<()> {
        tx.execute(
            "
DELETE FROM contract_levels
WHERE contract = $1
  AND level = $2",
            &[&contract_id.name, &(level as i32)],
        )?;
        Ok(())
    }

    /// get the origination of the contract, which is currently store in the levels (will change)
    pub(crate) fn set_origination(
        tx: &mut Transaction,
        contract_id: &ContractID,
        level: u32,
    ) -> Result<()> {
        tx.execute(
            "
UPDATE contract_levels
SET is_origination = FALSE
WHERE is_origination = TRUE
  AND contract = $1",
            &[&contract_id.name],
        )?;
        tx.execute(
            "
UPDATE contract_levels
SET is_origination = TRUE
WHERE contract = $1
  AND level = $2",
            &[&contract_id.name, &(level as i32)],
        )?;
        Ok(())
    }

    pub(crate) fn get_origination(
        &mut self,
        contract_id: &ContractID,
    ) -> Result<Option<u32>> {
        let result = self.dbconn.query(
            "
SELECT
    level
FROM contract_levels
WHERE contract = $1
  AND is_origination = TRUE",
            &[&contract_id.name],
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
}
