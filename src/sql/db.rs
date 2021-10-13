use anyhow::{anyhow, Result};
use askama::Template;
use itertools::Itertools;
use std::collections::HashMap;

use native_tls::{Certificate, TlsConnector};
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::{BorrowToSql, ToSql};
use postgres::{Client, NoTls, Transaction};
use postgres_native_tls::MakeTlsConnector;
use std::fs;

use chrono::{DateTime, Utc};

use crate::config::ContractID;
use crate::octez::block::LevelMeta;
use crate::octez::block::TxContext;
use crate::octez::node::NodeClient;
use crate::sql::insert::{Column, Insert, Value};
use crate::sql::postgresql_generator::PostgresqlGenerator;
use crate::sql::table::Table;
use crate::sql::table_builder::TableBuilder;
use crate::storage_structure::relational::RelationalAST;

#[derive(Template)]
#[template(path = "repopulate-snapshot-derived.sql", escape = "none")]
struct RepopulateSnapshotDerivedTmpl<'a> {
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
}
#[derive(Template)]
#[template(path = "repopulate-changes-derived.sql", escape = "none")]
struct RepopulateChangesDerivedTmpl<'a> {
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    indices: &'a [String],
}
#[derive(Template)]
#[template(path = "update-snapshot-derived.sql", escape = "none")]
struct UpdateSnapshotDerivedTmpl<'a> {
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    tx_context_ids: &'a [i64],
}
#[derive(Template)]
#[template(path = "update-changes-derived.sql", escape = "none")]
struct UpdateChangesDerivedTmpl<'a> {
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    indices: &'a [String],
    tx_context_ids: &'a [i64],
}

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
    pub(crate) fn common_tables_exist(&mut self) -> Result<bool> {
        let res = self.dbconn.query_opt(
            "
SELECT 1
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name = 'levels'
",
            &[],
        )?;
        Ok(res.is_some())
    }

    pub(crate) fn repopulate_derived_tables(
        &mut self,
        contract_id: &ContractID,
        rel_ast: &RelationalAST,
    ) -> Result<()> {
        let mut builder = TableBuilder::new();
        builder.populate(rel_ast);

        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);

        let mut tx = self.transaction()?;
        let noview_prefixes = builder.get_viewless_table_prefixes();
        for (_name, table) in sorted_tables {
            if !noview_prefixes
                .iter()
                .any(|prefix| table.name.starts_with(prefix))
            {
                DBClient::repopulate_derived_table(
                    &mut tx,
                    contract_id,
                    table,
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn repopulate_derived_table(
        tx: &mut Transaction,
        contract_id: &ContractID,
        table: &Table,
    ) -> Result<()> {
        let columns = PostgresqlGenerator::table_sql_columns(table, false)
            .iter()
            .cloned()
            .collect::<Vec<String>>();
        if table.contains_snapshots() {
            let tmpl = RepopulateSnapshotDerivedTmpl {
                contract_schema: &contract_id.name,
                table: &table.name,
                columns: &columns,
            };
            tx.simple_query(&tmpl.render()?)?;
        } else {
            let indices = PostgresqlGenerator::table_sql_indices(table, false)
                .iter()
                .cloned()
                .collect::<Vec<String>>();
            let tmpl = RepopulateChangesDerivedTmpl {
                contract_schema: &contract_id.name,
                table: &table.name,
                columns: &columns,
                indices: &indices,
            };
            tx.simple_query(&tmpl.render()?)?;
        };
        Ok(())
    }

    pub(crate) fn update_derived_tables(
        tx: &mut Transaction,
        contract_id: &ContractID,
        rel_ast: &RelationalAST,
        tx_contexts: &[TxContext],
    ) -> Result<()> {
        let mut builder = TableBuilder::new();
        builder.populate(rel_ast);

        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);

        let noview_prefixes = builder.get_viewless_table_prefixes();
        for (_name, table) in sorted_tables {
            if !noview_prefixes
                .iter()
                .any(|prefix| table.name.starts_with(prefix))
            {
                DBClient::update_derived_table(
                    tx,
                    contract_id,
                    table,
                    tx_contexts,
                )?;
            }
        }
        Ok(())
    }

    fn update_derived_table(
        tx: &mut Transaction,
        contract_id: &ContractID,
        table: &Table,
        tx_contexts: &[TxContext],
    ) -> Result<()> {
        let tx_context_ids: Vec<i64> = tx_contexts
            .iter()
            .map(|ctx| ctx.id.unwrap())
            .collect();
        let columns: Vec<String> =
            PostgresqlGenerator::table_sql_columns(table, false)
                .iter()
                .cloned()
                .collect();
        if table.contains_snapshots() {
            let tmpl = UpdateSnapshotDerivedTmpl {
                contract_schema: &contract_id.name,
                table: &table.name,
                columns: &columns,
                tx_context_ids: &tx_context_ids,
            };
            tx.simple_query(&tmpl.render()?)?;
        } else {
            let indices: Vec<String> =
                PostgresqlGenerator::table_sql_indices(table, false)
                    .iter()
                    .cloned()
                    .collect();
            let tmpl = UpdateChangesDerivedTmpl {
                contract_schema: &contract_id.name,
                table: &table.name,
                columns: &columns,
                tx_context_ids: &tx_context_ids,
                indices: &indices,
            };
            tx.simple_query(&tmpl.render()?)?;
        };
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

            let noview_prefixes = builder.get_viewless_table_prefixes();
            for (_name, table) in sorted_tables {
                let table_def = generator.create_table_definition(table)?;
                tx.simple_query(table_def.as_str())?;

                if !noview_prefixes
                    .iter()
                    .any(|prefix| table.name.starts_with(prefix))
                {
                    for derived_table_def in
                        generator.create_derived_table_definitions(table)?
                    {
                        tx.simple_query(derived_table_def.as_str())?;
                    }
                }
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

        let noview_prefixes = builder.get_viewless_table_prefixes();
        for (_name, table) in sorted_tables {
            if !noview_prefixes
                .iter()
                .any(|prefix| table.name.starts_with(prefix))
            {
                tx.simple_query(
                    format!(
                        r#"
DROP TABLE "{contract_schema}"."{table}_ordered";
DROP TABLE "{contract_schema}"."{table}_live";
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

    pub(crate) fn save_bigmap_keyhashes(
        tx: &mut Transaction,
        bigmap_keyhashes: Vec<(TxContext, i32, String, String)>,
    ) -> Result<()> {
        for chunk in bigmap_keyhashes.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 4;
            let v_refs = (1..(num_columns * chunk.len()) + 1)
                .map(|i| format!("${}", i.to_string()))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
INSERT INTO bigmap_keys (
    tx_context_id, bigmap_id, keyhash, key
)
Values ({})",
                v_refs
            ))?;

            let values: Vec<&dyn postgres::types::ToSql> = chunk
                .iter()
                .flat_map(|(tx_context, bigmap_id, keyhash, key)| {
                    [
                        tx_context.id.borrow_to_sql(),
                        bigmap_id.borrow_to_sql(),
                        keyhash.borrow_to_sql(),
                        key.borrow_to_sql(),
                    ]
                })
                .collect();

            tx.query_raw(&stmt, values)?;
        }
        Ok(())
    }

    pub(crate) fn save_tx_contexts(
        tx: &mut Transaction,
        tx_contexts: &[TxContext],
    ) -> Result<()> {
        struct TxContextPG {
            id: i64,
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
            let stmt = tx.prepare(&format!(
                "
INSERT INTO tx_contexts(
    id,
    level,
    contract,
    operation_group_number,
    operation_number,
    content_number,
    internal_number,
    operation_hash,
    source,
    destination,
    entrypoint
)
VALUES ( {} )",
                v_refs
            ))?;

            let tx_contexts_pg: Vec<TxContextPG> = chunk
                .iter()
                .map(|tx_context| TxContextPG {
                    id: tx_context
                        .id
                        .ok_or_else(|| anyhow!("Missing ID on TxContext"))
                        .unwrap(),
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

    pub(crate) fn get_config_deps(
        &mut self,
        config: &[ContractID],
    ) -> Result<Vec<ContractID>> {
        if config.is_empty() {
            return Ok(vec![]);
        }
        let v_refs = (0..config.len())
            .map(|i| format!("${}", (i + 1).to_string()))
            .collect::<Vec<String>>()
            .join(", ");

        let mut it = self.dbconn.query_raw(
            format!(
                "
SELECT DISTINCT
    src_contract
FROM contract_deps
WHERE dest_schema IN ({})
",
                v_refs
            )
            .as_str(),
            config
                .iter()
                .map(|c| c.name.borrow_to_sql())
                .collect::<Vec<&dyn ToSql>>(),
        )?;
        let mut res: Vec<ContractID> = vec![];
        while let Some(row) = it.next()? {
            res.push(ContractID {
                address: row.get(0),
                name: row.get(0),
            });
        }
        Ok(res
            .into_iter()
            .filter(|dep| {
                !config
                    .iter()
                    .any(|c| c.address == dep.address)
            })
            .collect())
    }

    pub(crate) fn get_dependent_levels(
        &mut self,
        config: &[&ContractID],
    ) -> Result<Vec<u32>> {
        if config.is_empty() {
            return Ok(vec![]);
        }

        let v_refs = (0..config.len())
            .map(|i| format!("${}", (i + 1).to_string()))
            .collect::<Vec<String>>()
            .join(", ");

        let mut it = self.dbconn.query_raw(
            format!(
                "
SELECT DISTINCT
    level
FROM contract_deps
WHERE dest_schema IN ({})
",
                v_refs
            )
            .as_str(),
            config
                .iter()
                .map(|c| c.name.borrow_to_sql())
                .collect::<Vec<&dyn ToSql>>(),
        )?;
        let mut res: Vec<i32> = vec![];
        while let Some(row) = it.next()? {
            res.push(row.get(0));
        }
        Ok(res
            .into_iter()
            .map(|x| x as u32)
            .collect())
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
DROP TABLE IF EXISTS bigmap_keys;
DROP TABLE IF EXISTS contract_deps;
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
    (SELECT MIN(level) FROM contract_levels WHERE contract = $1),
    (SELECT MAX(level) FROM contract_levels WHERE contract = $1)
) AS g(level)
WHERE g.level NOT IN (
    SELECT level FROM contract_levels WHERE contract = $1
)",
            &[&contract_id.name],
        )?)
    }

    pub(crate) fn get_head(&mut self) -> Result<Option<LevelMeta>> {
        self.get_level_internal(None)
    }

    pub(crate) fn get_level(
        &mut self,
        level: u32,
    ) -> Result<Option<LevelMeta>> {
        self.get_level_internal(Some(level as i32))
    }

    fn get_level_internal(
        &mut self,
        level: Option<i32>,
    ) -> Result<Option<LevelMeta>> {
        let result = self.dbconn.query_opt(
            "
SELECT
    level, hash, prev_hash, baked_at
FROM levels
WHERE ($1::INTEGER IS NULL AND level = (SELECT max(level) FROM levels)) OR level = $1",
            &[&level],
        )?;
        if result.is_none() {
            return Ok(None);
        }

        let row = result.unwrap();

        let level: i32 = row.get(0);
        let hash: Option<String> = row.get(1);
        let prev_hash: Option<String> = row.get(2);
        let baked_at: Option<DateTime<Utc>> = row.get(3);

        Ok(Some(LevelMeta {
            level: level as u32,
            hash,
            prev_hash,
            baked_at,
        }))
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
        rows.sort_unstable();
        rows.dedup();
        rows.reverse();
        Ok(rows
            .iter()
            .map(|x| *x as u32)
            .collect::<Vec<u32>>())
    }

    pub(crate) fn get_max_id(&mut self) -> Result<i64> {
        let max_id: i64 = self
            .dbconn
            .query("SELECT max_id FROM max_id", &[])?[0]
            .get(0);
        Ok(max_id + 1)
    }

    pub(crate) fn set_max_id(tx: &mut Transaction, max_id: i64) -> Result<()> {
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
    level, hash, prev_hash, baked_at
) VALUES ($1, $2, $3, $4)
",
            &[
                &(meta.level as i32),
                &meta.hash,
                &meta.prev_hash,
                &meta.baked_at,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn delete_level(tx: &mut Transaction, level: u32) -> Result<()> {
        tx.execute(
            "
DELETE FROM contract_deps
WHERE level = $1",
            &[&(level as i32)],
        )?;
        tx.execute(
            "
DELETE FROM contract_levels
WHERE level = $1",
            &[&(level as i32)],
        )?;
        tx.execute(
            "
DELETE FROM levels
WHERE level = $1",
            &[&(level as i32)],
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

    pub(crate) fn save_contract_deps(
        tx: &mut Transaction,
        level: u32,
        contract_id: &ContractID,
        deps: Vec<String>,
    ) -> Result<()> {
        for dep in deps {
            tx.execute(
                "
INSERT INTO contract_deps (level, src_contract, dest_schema)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING",
                &[&(level as i32), &dep, &contract_id.name],
            )?;
        }
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

pub(crate) trait BigmapKeysGetter {
    fn get(
        &mut self,
        level: u32,
        bigmap_id: i32,
    ) -> Result<Vec<(String, String)>>;
}

impl BigmapKeysGetter for DBClient {
    fn get(
        &mut self,
        level: u32,
        bigmap_id: i32,
    ) -> Result<Vec<(String, String)>> {
        let res = self.dbconn.query(
            "
SELECT
    keyhash,
    key
FROM bigmap_keys bigmap
JOIN tx_contexts ctx
  ON ctx.id = bigmap.tx_context_id
WHERE bigmap_id = $1
  AND ctx.level <= $2
",
            &[&bigmap_id, &(level as i32)],
        )?;
        Ok(res
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect::<Vec<(String, String)>>())
    }
}
