use anyhow::{anyhow, Result};
use askama::Template;
use itertools::Itertools;
use std::collections::HashMap;

use native_tls::{Certificate, TlsConnector};
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::{BorrowToSql, FromSql, ToSql};
use postgres::{Client, NoTls, Transaction};
use postgres_native_tls::MakeTlsConnector;
use std::fs;

use chrono::{DateTime, Utc};

use crate::config::ContractID;
use crate::octez::block::{LevelMeta, Tx, TxContext};
use crate::octez::node::NodeClient;
use crate::sql::insert::{Column, Insert, Value};
use crate::sql::postgresql_generator::PostgresqlGenerator;
use crate::sql::table::Table;
use crate::sql::table_builder::TableBuilder;
use crate::storage_structure::relational::RelationalAST;

#[derive(PartialEq, Eq, Debug, ToSql, FromSql)]
#[postgres(name = "indexer_mode")]
pub(crate) enum IndexerMode {
    Bootstrap,
    Head,
}

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

    url: String,
    ssl: bool,
    ca_cert: Option<String>,
}

impl Clone for DBClient {
    fn clone(&self) -> Self {
        self.reconnect().unwrap()
    }
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
            if let Some(ca_cert) = &ca_cert {
                builder.add_root_certificate(Certificate::from_pem(
                    &fs::read(ca_cert)?,
                )?);
            }
            let connector = builder.build()?;
            let connector = MakeTlsConnector::new(connector);

            Ok(DBClient {
                dbconn: postgres::Client::connect(url, connector)?,

                url: url.to_string(),
                ssl,
                ca_cert,
            })
        } else {
            Ok(DBClient {
                dbconn: Client::connect(url, NoTls)?,

                url: url.to_string(),
                ssl,
                ca_cert,
            })
        }
    }

    pub(crate) fn reconnect(&self) -> Result<Self> {
        Self::connect(&self.url, self.ssl, self.ca_cert.clone())
    }

    pub(crate) fn get_quepasa_version(&mut self) -> Result<String> {
        let version: String = self
            .dbconn
            .query_one(
                "
SELECT
    quepasa_version
FROM indexer_state
            ",
                &[],
            )?
            .get(0);
        Ok(version)
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
        for (i, (_name, table)) in sorted_tables.iter().enumerate() {
            if !noview_prefixes
                .iter()
                .any(|prefix| table.name.starts_with(prefix))
            {
                info!(
                    "repopulating {table} _live and _ordered ({contract} table {table_i}/~{table_total})",
                    contract = contract_id.name,
                    table = table.name,
                    table_i = i,
                    table_total = sorted_tables.len(),
                );
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
        let columns: Vec<String> =
            PostgresqlGenerator::table_sql_columns(table, false).to_vec();
        if table.contains_snapshots() {
            let tmpl = RepopulateSnapshotDerivedTmpl {
                contract_schema: &contract_id.name,
                table: &table.name,
                columns: &columns,
            };
            tx.simple_query(&tmpl.render()?)?;
        } else {
            let tmpl = RepopulateChangesDerivedTmpl {
                contract_schema: &contract_id.name,
                table: &table.name,
                columns: &columns,
                indices: &PostgresqlGenerator::table_sql_indices(table, false)
                    .to_vec(),
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
        if tx_contexts.is_empty() {
            return Ok(());
        }
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
            PostgresqlGenerator::table_sql_columns(table, false).to_vec();
        if table.contains_snapshots() {
            let tmpl = UpdateSnapshotDerivedTmpl {
                contract_schema: &contract_id.name,
                table: &table.name,
                columns: &columns,
                tx_context_ids: &tx_context_ids,
            };
            tx.simple_query(&tmpl.render()?)?;
        } else {
            let tmpl = UpdateChangesDerivedTmpl {
                contract_schema: &contract_id.name,
                table: &table.name,
                columns: &columns,
                tx_context_ids: &tx_context_ids,
                indices: &PostgresqlGenerator::table_sql_indices(table, false)
                    .to_vec(),
            };
            tx.simple_query(&tmpl.render()?)?;
        };
        Ok(())
    }

    pub(crate) fn create_contract_schemas(
        &mut self,
        contracts: &mut Vec<(ContractID, RelationalAST)>,
    ) -> Result<bool> {
        let mut tx = self.transaction()?;

        contracts.sort_by_key(|(cid, _)| cid.name.clone());

        let num_columns = 2;
        let v_refs = (1..(num_columns * contracts.len()) + 1)
            .map(|i| format!("${}", i.to_string()))
            .collect::<Vec<String>>()
            .chunks(num_columns)
            .map(|x| x.join(", "))
            .join("), (");
        let stmt = tx.prepare(&format!(
            "
INSERT INTO contracts (name, address)
VALUES ({})
ON CONFLICT DO NOTHING
RETURNING name",
            v_refs
        ))?;

        let values: Vec<&dyn postgres::types::ToSql> = contracts
            .iter()
            .flat_map(|(c, _)| {
                [c.name.borrow_to_sql(), c.address.borrow_to_sql()]
            })
            .collect();

        let new_contracts = tx
            .query_raw(&stmt, values)?
            .map(|x| x.try_get(0))
            .collect::<Vec<String>>()?;
        if new_contracts.is_empty() {
            tx.rollback()?;
            return Ok(false);
        }
        let mut stmnts: Vec<String> = vec![];
        for name in &new_contracts {
            let (contract_id, rel_ast) = contracts
                .iter()
                .find(|(c, _)| &c.name == name)
                .unwrap();

            // Generate the SQL schema for this contract
            let mut builder = TableBuilder::new();
            builder.populate(rel_ast);

            let generator = PostgresqlGenerator::new(contract_id);
            let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
            sorted_tables.sort_by_key(|a| a.0);

            stmnts.push(format!(
                r#"
CREATE SCHEMA IF NOT EXISTS "{contract_schema}";
"#,
                contract_schema = contract_id.name
            ));

            let noview_prefixes = builder.get_viewless_table_prefixes();
            for (_name, table) in &sorted_tables {
                let table_def = generator.create_table_definition(table)?;
                stmnts.push(table_def);

                if !noview_prefixes
                    .iter()
                    .any(|prefix| table.name.starts_with(prefix))
                {
                    for derived_table_def in
                        generator.create_derived_table_definitions(table)?
                    {
                        stmnts.push(derived_table_def);
                    }
                }
            }
        }
        tx.simple_query(stmnts.join("\n").as_str())?;
        tx.commit()?;

        Ok(true)
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

    pub(crate) fn save_bigmap_keyhashes(
        tx: &mut Transaction,
        bigmap_keyhashes: &[(TxContext, i32, String, String)],
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
            pub operation_group_number: i32,
            pub operation_number: i32,
            pub content_number: i32,
            pub internal_number: Option<i32>,
        }
        for chunk in tx_contexts.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 7;
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
    internal_number
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
                    ]
                })
                .collect();

            tx.query_raw(&stmt, values)?;
        }

        Ok(())
    }

    pub(crate) fn save_txs(tx: &mut Transaction, txs: &[Tx]) -> Result<()> {
        for txs_chunk in txs.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 11;
            let v_refs = (1..(num_columns * txs_chunk.len()) + 1)
                .map(|i| format!("${}", i.to_string()))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
INSERT INTO txs(
    tx_context_id,

    operation_hash,
    source,
    destination,
    entrypoint,

    fee,
    gas_limit,
    storage_limit,

    consumed_milligas,
    storage_size,
    paid_storage_size_diff
)
VALUES ( {} )",
                v_refs
            ))?;

            let values: Vec<&dyn postgres::types::ToSql> = txs_chunk
                .iter()
                .flat_map(|tx| {
                    [
                        tx.tx_context_id.borrow_to_sql(),
                        tx.operation_hash.borrow_to_sql(),
                        tx.source.borrow_to_sql(),
                        tx.destination.borrow_to_sql(),
                        tx.entrypoint.borrow_to_sql(),
                        tx.fee.borrow_to_sql(),
                        tx.gas_limit.borrow_to_sql(),
                        tx.storage_limit.borrow_to_sql(),
                        tx.consumed_milligas.borrow_to_sql(),
                        tx.storage_size.borrow_to_sql(),
                        tx.paid_storage_size_diff
                            .borrow_to_sql(),
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
        let mut table_grouped: HashMap<(String, Vec<String>), Vec<&Insert>> =
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
                .push(insert);
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
        config: &[ContractID],
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
        inserts: &[&Insert],
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
        node_cli: &NodeClient,
        get_rel_ast: F,
    ) -> Result<()>
    where
        F: Fn(&NodeClient, &str) -> Result<RelationalAST>,
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
DROP VIEW  IF EXISTS txs_ordered;
DROP TABLE IF EXISTS txs;
DROP TABLE IF EXISTS tx_contexts;
DROP TABLE IF EXISTS indexer_state;
DROP TYPE  IF EXISTS indexer_mode;
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
        Ok(rows
            .iter()
            .map(|x| *x as u32)
            .collect::<Vec<u32>>())
    }

    pub(crate) fn get_indexer_mode(&mut self) -> Result<IndexerMode> {
        let mode: IndexerMode = self
            .dbconn
            .query_one("select mode from indexer_state", &[])?
            .get(0);
        Ok(mode)
    }

    pub(crate) fn set_indexer_mode(&mut self, mode: IndexerMode) -> Result<()> {
        let updated = self.dbconn.execute(
            "
update indexer_state
set mode = $1",
            &[&mode],
        )?;
        if updated == 1 {
            Ok(())
        } else {
            Err(anyhow!(
                "wrong number of rows in indexer_state table. please fix manually. sorry"
            ))
        }
    }

    pub(crate) fn get_max_id(&mut self) -> Result<i64> {
        let max_id: i64 = self
            .dbconn
            .query_one("select max_id from indexer_state", &[])?
            .get(0);
        Ok(max_id)
    }

    pub(crate) fn set_max_id(tx: &mut Transaction, max_id: i64) -> Result<()> {
        let updated = tx.execute(
            "
update indexer_state
set max_id = $1",
            &[&max_id],
        )?;
        if updated == 1 {
            Ok(())
        } else {
            Err(anyhow!(
            "wrong number of rows in indexer_state table. please fix manually. sorry"
        ))
        }
    }

    pub(crate) fn get_fully_processed_levels(&mut self) -> Result<Vec<u32>> {
        let fully_processed: Vec<u32> = self
            .dbconn
            .query(
                "
SELECT
    level
FROM contract_levels
GROUP by 1
HAVING COUNT(1) = (SELECT COUNT(1) FROM contracts)
ORDER by 1",
                &[],
            )?
            .iter()
            .map(|row| row.get(0))
            .map(|lvl: i32| lvl as u32)
            .collect();
        Ok(fully_processed)
    }

    pub(crate) fn get_partial_processed_levels(&mut self) -> Result<Vec<u32>> {
        let partial_processed: Vec<u32> = self
            .dbconn
            .query(
                "
with all_levels as (
    select distinct
        level
    from contract_levels
)
select distinct
    lvl.level
from all_levels lvl, contracts c
left join contract_levels orig
  on  orig.contract = c.name
  and orig.is_origination
where lvl.level >= coalesce(orig.level, 0)
  and not exists (
    select 1
    from contract_levels clvl
    where clvl.level = lvl.level
      and clvl.contract = c.name
)
order by 1",
                &[],
            )?
            .iter()
            .map(|row| row.get(0))
            .map(|lvl: i32| lvl as u32)
            .collect();
        Ok(partial_processed)
    }

    pub(crate) fn save_levels(
        tx: &mut Transaction,
        levels: &[&LevelMeta],
    ) -> Result<()> {
        Self::delete_levels(
            tx,
            &levels
                .iter()
                .map(|meta| meta.level as i32)
                .collect::<Vec<i32>>(),
        )?;

        for lvls_chunk in levels.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 4;
            let v_refs = (1..(num_columns * lvls_chunk.len()) + 1)
                .map(|i| format!("${}", i.to_string()))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
INSERT INTO levels(
    level, hash, prev_hash, baked_at
)
VALUES ( {} )",
                v_refs
            ))?;

            #[allow(clippy::type_complexity)]
            let v_: Vec<(
                i32,
                Option<String>,
                Option<String>,
                Option<DateTime<Utc>>,
            )> = lvls_chunk
                .iter()
                .map(|m| {
                    (
                        m.level as i32,
                        m.hash.clone(),
                        m.prev_hash.clone(),
                        m.baked_at,
                    )
                })
                .collect();

            let values: Vec<&dyn postgres::types::ToSql> = v_
                .iter()
                .flat_map(|(lvl, hash, prev_hash, baked_at)| {
                    [
                        lvl.borrow_to_sql(),
                        hash.borrow_to_sql(),
                        prev_hash.borrow_to_sql(),
                        baked_at.borrow_to_sql(),
                    ]
                })
                .collect();

            tx.query_raw(&stmt, values)?;
        }
        Ok(())
    }

    pub(crate) fn delete_levels(
        tx: &mut Transaction,
        levels: &[i32],
    ) -> Result<()> {
        for lvls_chunk in levels.chunks(Self::INSERT_BATCH_SIZE) {
            let v_refs = (1..lvls_chunk.len() + 1)
                .map(|i| format!("${}", i.to_string()))
                .collect::<Vec<String>>()
                .join(", ");

            let values: Vec<&dyn postgres::types::ToSql> = lvls_chunk
                .iter()
                .map(|level| level.borrow_to_sql())
                .collect();
            let stmt = tx.prepare(&format!(
                "
DELETE FROM contract_deps
WHERE level IN ( {} )
",
                v_refs
            ))?;
            tx.query_raw(&stmt, values)?;

            let values: Vec<&dyn postgres::types::ToSql> = lvls_chunk
                .iter()
                .map(|level| level.borrow_to_sql())
                .collect();
            let stmt = tx.prepare(&format!(
                "
DELETE FROM contract_levels
WHERE level IN ( {} )
",
                v_refs
            ))?;
            tx.query_raw(&stmt, values)?;

            let values: Vec<&dyn postgres::types::ToSql> = lvls_chunk
                .iter()
                .map(|level| level.borrow_to_sql())
                .collect();
            let stmt = tx.prepare(&format!(
                "
DELETE FROM levels
WHERE level IN ( {} )
",
                v_refs
            ))?;
            tx.query_raw(&stmt, values)?;
        }
        Ok(())
    }

    pub(crate) fn save_contract_levels(
        tx: &mut Transaction,
        clvls: &[(ContractID, i32, bool)],
    ) -> Result<()> {
        for clvls_chunk in clvls.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 3;
            let v_refs = (1..(num_columns * clvls_chunk.len()) + 1)
                .map(|i| format!("${}", i.to_string()))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
INSERT INTO contract_levels(
    contract, level, is_origination
)
VALUES ( {} )",
                v_refs
            ))?;

            let values: Vec<&dyn postgres::types::ToSql> = clvls_chunk
                .iter()
                .flat_map(|(contract, level, is_origination)| {
                    [
                        contract.name.borrow_to_sql(),
                        level.borrow_to_sql(),
                        is_origination.borrow_to_sql(),
                    ]
                })
                .collect();

            tx.query_raw(&stmt, values)?;
        }
        Ok(())
    }

    pub(crate) fn save_contract_deps(
        tx: &mut Transaction,
        deps: &[(i32, String, ContractID)],
    ) -> Result<()> {
        for deps_chunk in deps.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 3;
            let v_refs = (1..(num_columns * deps_chunk.len()) + 1)
                .map(|i| format!("${}", i.to_string()))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
INSERT INTO contract_deps (level, src_contract, dest_schema)
VALUES ( {} )
ON CONFLICT DO NOTHING",
                v_refs
            ))?;

            let values: Vec<&dyn postgres::types::ToSql> = deps_chunk
                .iter()
                .flat_map(|(level, src_addr, dest)| {
                    [
                        level.borrow_to_sql(),
                        src_addr.borrow_to_sql(),
                        dest.name.borrow_to_sql(),
                    ]
                })
                .collect();

            tx.query_raw(&stmt, values)?;
        }
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
