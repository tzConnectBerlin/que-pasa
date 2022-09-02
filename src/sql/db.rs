use anyhow::{anyhow, Result};
use askama::Template;
use itertools::Itertools;
use std::collections::HashMap;
use std::time::Duration;

use postgres::fallible_iterator::FallibleIterator;
use postgres::types::{BorrowToSql, FromSql, ToSql};
use postgres::Transaction;

use chrono::{DateTime, Utc};

use crate::config::ContractID;
use crate::octez::block::{LevelMeta, Tx, TxContext};
use crate::octez::node::NodeClient;
use crate::sql::insert::{Column, Insert, Value};
use crate::sql::postgresql_generator::PostgresqlGenerator;
use crate::sql::table::Table;
use crate::sql::table_builder::TableBuilder;
use crate::sql::types::BigmapMetaAction;
use crate::storage_structure::relational;

use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};

#[derive(PartialEq, Eq, Debug, ToSql, FromSql)]
#[postgres(name = "indexer_mode")]
pub(crate) enum IndexerMode {
    Bootstrap,
    Head,
}

#[derive(Template)]
#[template(path = "repopulate-snapshot-derived.sql", escape = "none")]
struct RepopulateSnapshotDerivedTmpl<'a> {
    main_schema: &'a str,
    contract_schema: &'a str,
    table: &'a str,
    parent_table: &'a str,
    columns: &'a [String],
}
#[derive(Template)]
#[template(path = "repopulate-changes-derived.sql", escape = "none")]
struct RepopulateChangesDerivedTmpl<'a> {
    main_schema: &'a str,
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    indices: &'a [String],
}
#[derive(Template)]
#[template(path = "update-snapshot-derived.sql", escape = "none")]
struct UpdateSnapshotDerivedTmpl<'a> {
    main_schema: &'a str,
    contract_schema: &'a str,
    table: &'a str,
    parent_table: &'a str,
    columns: &'a [String],
    tx_context_ids: &'a [i64],
}
#[derive(Template)]
#[template(path = "update-changes-derived.sql", escape = "none")]
struct UpdateChangesDerivedTmpl<'a> {
    main_schema: &'a str,
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    indices: &'a [String],
    tx_context_ids: &'a [i64],
}

type DBPool = r2d2::Pool<PostgresConnectionManager<NoTls>>;
type DBPooledConn = r2d2::PooledConnection<PostgresConnectionManager<NoTls>>;

#[derive(Clone)]
pub struct DBClient {
    dbpool: DBPool,
    main_schema: String,
}

impl DBClient {
    const INSERT_BATCH_SIZE: usize = 100;

    pub(crate) fn connect(
        url: &str,
        main_schema: &str,
        conn_timeout: Duration,
        max_conn: u32,
    ) -> Result<Self> {
        let manager = PostgresConnectionManager::new(url.parse()?, NoTls);
        let dbpool = r2d2::Builder::new()
            .max_size(max_conn)
            .connection_timeout(conn_timeout)
            .build(manager)?;

        Ok(DBClient {
            dbpool,
            main_schema: main_schema.to_string(),
        })
    }

    pub(crate) fn dbconn(&self) -> Result<DBPooledConn> {
        let mut conn = self
            .dbpool
            .get()
            .map_err(|err| anyhow!("err: {}", err))?;
        conn.simple_query(
            format!(r#"SET SCHEMA '{}'"#, self.main_schema).as_str(),
        )?;
        Ok(conn)
    }

    pub(crate) fn get_quepasa_version(&mut self) -> Result<String> {
        let mut conn = self.dbconn()?;

        let version: String = conn
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
        let mut conn = self.dbconn()?;

        conn.simple_query(
            format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, self.main_schema)
                .as_str(),
        )?;
        conn.simple_query(
            PostgresqlGenerator::create_common_tables(&self.main_schema)
                .as_str(),
        )?;
        Ok(())
    }

    pub(crate) fn common_tables_exist(&mut self) -> Result<bool> {
        let mut conn = self.dbconn()?;

        let res = conn.query_opt(
            "
SELECT 1
FROM information_schema.tables
WHERE table_schema = $1
  AND table_name = 'levels'
",
            &[&self.main_schema],
        )?;
        Ok(res.is_some())
    }

    pub(crate) fn repopulate_derived_tables(
        &mut self,
        contract: &relational::Contract,
    ) -> Result<()> {
        let (mut tables, noview_prefixes, _): (
            Vec<Table>,
            Vec<String>,
            Vec<String>,
        ) = TableBuilder::tables_from_contract(contract);

        tables.sort_by_key(|t| t.name.clone());

        let mut conn = self.dbconn()?;
        let mut tx = conn.transaction()?;
        for (i, table) in tables.iter().enumerate() {
            if !noview_prefixes
                .iter()
                .any(|prefix| table.name.starts_with(prefix))
            {
                info!(
                    "repopulating {table} _live and _ordered ({contract} table {table_i}/~{table_total})",
                    contract = contract.cid.name,
                    table = table.name,
                    table_i = i,
                    table_total = tables.len(),
                );
                self.repopulate_derived_table(&mut tx, &contract.cid, table)?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn repopulate_derived_table(
        &self,
        tx: &mut Transaction,
        contract_id: &ContractID,
        table: &Table,
    ) -> Result<()> {
        let columns: Vec<String> =
            PostgresqlGenerator::table_sql_columns(table, false).to_vec();
        if table.contains_snapshots() {
            let parent_table: String =
                PostgresqlGenerator::table_parent_name(table)
                    .unwrap_or_else(|| table.name.clone());
            let tmpl = RepopulateSnapshotDerivedTmpl {
                main_schema: &self.main_schema,
                contract_schema: &contract_id.name,
                table: &table.name,
                parent_table: &parent_table,
                columns: &columns,
            };
            tx.simple_query(&tmpl.render()?)?;
        } else {
            let tmpl = RepopulateChangesDerivedTmpl {
                main_schema: &self.main_schema,
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
        &self,
        tx: &mut Transaction,
        contract: &relational::Contract,
        tx_contexts: &[TxContext],
    ) -> Result<()> {
        if tx_contexts.is_empty() {
            return Ok(());
        }

        let (mut tables, noview_prefixes, _): (
            Vec<Table>,
            Vec<String>,
            Vec<String>,
        ) = TableBuilder::tables_from_contract(contract);

        tables.sort_by_key(|t| t.name.clone());

        for table in &tables {
            if !noview_prefixes
                .iter()
                .any(|prefix| table.name.starts_with(prefix))
            {
                self.update_derived_table(
                    tx,
                    &contract.cid,
                    table,
                    tx_contexts,
                )?;
            }
        }
        Ok(())
    }

    fn update_derived_table(
        &self,
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
            let parent_table: String =
                PostgresqlGenerator::table_parent_name(table)
                    .unwrap_or_else(|| table.name.clone());
            let tmpl = UpdateSnapshotDerivedTmpl {
                main_schema: &self.main_schema,
                contract_schema: &contract_id.name,
                table: &table.name,
                parent_table: &parent_table,
                columns: &columns,
                tx_context_ids: &tx_context_ids,
            };
            tx.simple_query(&tmpl.render()?)?;
        } else {
            let tmpl = UpdateChangesDerivedTmpl {
                main_schema: &self.main_schema,
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
        contracts: &mut Vec<relational::Contract>,
    ) -> Result<bool> {
        let mut conn = self.dbconn()?;
        let mut tx = conn.transaction()?;

        contracts.sort_by_key(|c| c.cid.name.clone());

        let num_columns = 2;
        let v_refs = (1..(num_columns * contracts.len()) + 1)
            .map(|i| format!("${}", i))
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
            .flat_map(|c| {
                [c.cid.name.borrow_to_sql(), c.cid.address.borrow_to_sql()]
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
            let contract = contracts
                .iter()
                .find(|c| &c.cid.name == name)
                .unwrap();

            let (mut tables, noview_prefixes, nofunctions_prefixes): (
                Vec<Table>,
                Vec<String>,
                Vec<String>,
            ) = TableBuilder::tables_from_contract(contract);

            tables.sort_by_key(|t| t.name.clone());

            stmnts.push(format!(
                r#"
CREATE SCHEMA IF NOT EXISTS "{contract_schema}";
"#,
                contract_schema = contract.cid.name
            ));

            let generator = PostgresqlGenerator::new(
                self.main_schema.clone(),
                &contract.cid,
            );

            for table in &tables {
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

                if !nofunctions_prefixes
                    .iter()
                    .any(|prefix| table.name.starts_with(prefix))
                {
                    let function_def = generator
                        .create_table_functions(&contract.cid.name, table)?;
                    stmnts.extend(function_def);
                }
            }
        }
        for stmnt in stmnts {
            tx.simple_query(stmnt.as_str())?;
        }
        tx.commit()?;

        Ok(true)
    }

    pub(crate) fn delete_contract_schema(
        tx: &mut Transaction,
        contract: &relational::Contract,
    ) -> Result<()> {
        info!("deleting schema for contract {}", contract.cid.name);
        let (mut tables, noview_prefixes, nofunctions_prefixes): (
            Vec<Table>,
            Vec<String>,
            Vec<String>,
        ) = TableBuilder::tables_from_contract(contract);
        tables.sort_by_key(|t| t.name.clone());
        tables.reverse();

        for table in &tables {
            if !nofunctions_prefixes
                .iter()
                .any(|prefix| table.name.starts_with(prefix))
            {
                tx.simple_query(
                    format!(
                        r#"
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at_deref(INT, INT, INT, INT, INT)";
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at_deref(INT, INT, INT, INT)";
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at_deref(INT, INT, INT)";
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at_deref(INT, INT)";
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at_deref(INT)";

DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at(INT, INT, INT, INT, INT)";
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at(INT, INT, INT, INT)";
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at(INT, INT, INT)";
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at(INT, INT)";
DROP FUNCTION IF EXISTS "{contract_schema}"."{table}_at(INT)";
"#,
                        contract_schema = contract.cid.name,
                        table = table.name,
                    )
                    .as_str(),
                )?;
            }

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
                        contract_schema = contract.cid.name,
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
                    contract_schema = contract.cid.name,
                    table = table.name,
                )
                .as_str(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn save_bigmap_meta_actions(
        tx: &mut Transaction,
        actions: &[BigmapMetaAction],
    ) -> Result<()> {
        for chunk in actions.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 4;
            let v_refs = (1..(num_columns * chunk.len()) + 1)
                .map(|i| format!("${}", i))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
    INSERT INTO bigmap_meta_actions (
        tx_context_id, bigmap_id, action, value
    )
    Values ({})",
                v_refs
            ))?;

            let values: Vec<&dyn postgres::types::ToSql> = chunk
                .iter()
                .flat_map(|x| {
                    [
                        x.tx_context_id.borrow_to_sql(),
                        x.bigmap_id.borrow_to_sql(),
                        x.action.borrow_to_sql(),
                        x.value.borrow_to_sql(),
                    ]
                })
                .collect();

            tx.query_raw(&stmt, values)?;
        }
        Ok(())
    }

    pub(crate) fn save_bigmap_keyhashes(
        tx: &mut Transaction,
        bigmap_keyhashes: BigmapEntries,
    ) -> Result<()> {
        for chunk in bigmap_keyhashes
            .into_iter()
            .collect::<Vec<(
                (i32, TxContext, String),
                (serde_json::Value, Option<serde_json::Value>),
            )>>()
            .chunks(Self::INSERT_BATCH_SIZE)
        {
            let num_columns = 5;
            let v_refs = (1..(num_columns * chunk.len()) + 1)
                .map(|i| format!("${}", i))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
INSERT INTO bigmap_keys (
    tx_context_id, bigmap_id, keyhash, key, value
)
Values ({})",
                v_refs
            ))?;

            let values: Vec<&dyn postgres::types::ToSql> = chunk
                .iter()
                .flat_map(|((bigmap_id, tx_context, keyhash), (key, value))| {
                    [
                        tx_context.id.borrow_to_sql(),
                        bigmap_id.borrow_to_sql(),
                        keyhash.borrow_to_sql(),
                        key.borrow_to_sql(),
                        value.borrow_to_sql(),
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
                .map(|i| format!("${}", i))
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
            let num_columns = 12;
            let v_refs = (1..(num_columns * txs_chunk.len()) + 1)
                .map(|i| format!("${}", i))
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

    amount,
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
                        tx.amount.borrow_to_sql(),
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
            .map(|i| format!("${}", (i + 1)))
            .collect::<Vec<String>>()
            .join(", ");

        let mut conn = self.dbconn()?;

        let mut it = conn.query_raw(
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
            .map(|i| format!("${}", (i + 1)))
            .collect::<Vec<String>>()
            .join(", ");

        let mut conn = self.dbconn()?;

        let mut it = conn.query_raw(
            format!(
                "
SELECT DISTINCT
    level
FROM contract_deps
WHERE dest_schema IN ({})
  AND is_deep_copy
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
            .map(|i| format!("${}", i))
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

    pub(crate) fn delete_everything<F>(
        &mut self,
        node_cli: &NodeClient,
        get_contract_rel: F,
    ) -> Result<()>
    where
        F: Fn(&NodeClient, &ContractID) -> Result<relational::Contract>,
    {
        let mut conn = self.dbconn()?;

        let main_schema = self.main_schema.clone();
        let mut tx = conn.transaction()?;

        let contracts_table = tx.query_opt(
            "
SELECT
    1
FROM information_schema.tables
WHERE table_schema = $1
  AND table_name = 'contracts'
",
            &[&main_schema],
        )?;
        if contracts_table.is_some() {
            for row in tx.query("SELECT name, address FROM contracts", &[])? {
                let contract_id = ContractID {
                    name: row.get(0),
                    address: row.get(1),
                };
                let contract = get_contract_rel(node_cli, &contract_id)?;
                Self::delete_contract_schema(&mut tx, &contract)?
            }
        }
        tx.simple_query(
            "
DROP FUNCTION IF EXISTS last_context_at(INT, INT, INT, INT, INT);
DROP FUNCTION IF EXISTS last_context_at(INT, INT, INT, INT);
DROP FUNCTION IF EXISTS last_context_at(INT, INT, INT);
DROP FUNCTION IF EXISTS last_context_at(INT, INT);
DROP FUNCTION IF EXISTS last_context_at(INT);
DROP TABLE IF EXISTS bigmap_keys;
DROP TABLE IF EXISTS contract_deps;
DROP TABLE IF EXISTS bigmap_meta_actions;
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
        let mut conn = self.dbconn()?;

        Ok(conn.execute(
            "
INSERT INTO contract_levels(contract, level)
SELECT $1, q.level
FROM (
    SELECT
        g.level
    FROM GENERATE_SERIES(
        (SELECT MIN(level) FROM contract_levels WHERE contract = $1),
        (SELECT MAX(level) FROM contract_levels WHERE contract = $1)
    ) AS g(level)
    LEFT JOIN contract_levels clvl
      ON  clvl.contract = $1
      AND clvl.level = g.level
    WHERE clvl IS NULL
) q
",
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
        let mut conn = self.dbconn()?;

        let result = conn.query_opt(
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
        let mut conn = self.dbconn()?;

        let mut rows: Vec<i32> = vec![];
        for contract_id in contracts {
            info!(
                "querying db to check for any missing levels of {}..",
                contract_id.name
            );
            let origination = self.get_origination(contract_id)?;
            let start = origination.unwrap_or(1);
            for row in conn.query(
                format!(
                    "
SELECT
    s.i
FROM generate_series({},{}) s(i)
LEFT JOIN contract_levels clvl
  ON  clvl.contract = $1
  AND clvl.level = s.i
WHERE clvl IS NULL
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

    pub(crate) fn get_forked_levels(&mut self) -> Result<Vec<u32>> {
        let mut conn = self.dbconn()?;

        let mut rows: Vec<i32> = vec![];
        for row in conn.query(
            "
SELECT DISTINCT
  level
FROM (
  SELECT
    level,
    prev_hash AS chain_prev_hash,
    LAG(level) OVER w as db_prev_level,
    LAG(hash) OVER w AS db_prev_hash
  FROM levels
  WINDOW w AS (ORDER BY level)
) q
WHERE chain_prev_hash != db_prev_hash
  AND db_prev_level = level - 1",
            &[],
        )? {
            rows.push(row.get(0));
        }
        Ok(rows
            .iter()
            .map(|x| *x as u32)
            .collect::<Vec<u32>>())
    }

    pub(crate) fn get_indexing_mode_contracts(
        &mut self,
        contracts: &[ContractID],
    ) -> Result<HashMap<ContractID, IndexerMode>> {
        let mut conn = self.dbconn()?;

        let mut res: HashMap<ContractID, IndexerMode> = HashMap::new();
        for row in conn.query(
            "
SELECT name, address, mode
FROM contracts
WHERE name = ANY($1)
        ",
            &[&contracts
                .iter()
                .map(|c| &c.name)
                .collect::<Vec<&String>>()],
        )? {
            res.insert(
                ContractID {
                    name: row.get(0),
                    address: row.get(1),
                },
                row.get(2),
            );
        }

        Ok(res)
    }

    pub(crate) fn set_indexing_mode_contracts(
        &mut self,
        mode: IndexerMode,
        contract_names: &[String],
    ) -> Result<()> {
        let mut conn = self.dbconn()?;

        let updated = conn.execute(
            "
UPDATE contracts
SET mode = $1
WHERE name = ANY($2)",
            &[&mode, &contract_names],
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
        let mut conn = self.dbconn()?;

        let max_id: i64 = conn
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

    pub(crate) fn get_fully_processed_levels(
        &mut self,
        contracts: &[ContractID],
    ) -> Result<Vec<u32>> {
        let mut conn = self.dbconn()?;

        let fully_processed: Vec<u32> = conn
            .query(
                "
SELECT
    level
FROM contract_levels
WHERE contract = ANY($1)
GROUP by 1
HAVING COUNT(1) = array_length($1, 1)
ORDER by 1",
                &[&contracts
                    .iter()
                    .map(|c| &c.name)
                    .collect::<Vec<&String>>()],
            )?
            .iter()
            .map(|row| row.get(0))
            .map(|lvl: i32| lvl as u32)
            .collect();
        Ok(fully_processed)
    }

    pub(crate) fn get_partial_processed_levels(
        &mut self,
        contracts: &[ContractID],
    ) -> Result<Vec<u32>> {
        let mut conn = self.dbconn()?;

        let partial_processed: Vec<u32> = conn
            .query(
                "
with all_levels as (
    select distinct
        level
    from contract_levels
    where contract = any($1)
)
select distinct
    lvl.level
from all_levels lvl, contracts c
left join contract_levels orig
  on  orig.contract = c.name
  and orig.is_origination
where lvl.level >= coalesce(orig.level, 0)
  and c.name = any($1)
  and not exists (
    select 1
    from contract_levels clvl
    where clvl.level = lvl.level
      and clvl.contract = c.name
)
order by 1",
                &[&contracts
                    .iter()
                    .map(|c| &c.name)
                    .collect::<Vec<&String>>()],
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
                .map(|i| format!("${}", i))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
INSERT INTO levels(
    level, hash, prev_hash, baked_at
)
VALUES ( {} )
ON CONFLICT DO NOTHING",
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
                .map(|i| format!("${}", i))
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
                .map(|i| format!("${}", i))
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
        deps: &[(i32, String, ContractID, bool)],
    ) -> Result<()> {
        for deps_chunk in deps.chunks(Self::INSERT_BATCH_SIZE) {
            let num_columns = 4;
            let v_refs = (1..(num_columns * deps_chunk.len()) + 1)
                .map(|i| format!("${}", i))
                .collect::<Vec<String>>()
                .chunks(num_columns)
                .map(|x| x.join(", "))
                .join("), (");
            let stmt = tx.prepare(&format!(
                "
INSERT INTO contract_deps (level, src_contract, dest_schema, is_deep_copy)
VALUES ( {} )
ON CONFLICT DO NOTHING",
                v_refs
            ))?;

            let values: Vec<&dyn postgres::types::ToSql> = deps_chunk
                .iter()
                .flat_map(|(level, src_addr, dest, is_deep_copy)| {
                    [
                        level.borrow_to_sql(),
                        src_addr.borrow_to_sql(),
                        dest.name.borrow_to_sql(),
                        is_deep_copy.borrow_to_sql(),
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
        let mut conn = self.dbconn()?;

        let result = conn.query(
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

pub(crate) type BigmapEntries = HashMap<
    (i32, TxContext, String),
    (serde_json::Value, Option<serde_json::Value>),
>;
pub(crate) type BigmapEntry =
    (String, serde_json::Value, Option<serde_json::Value>);

pub(crate) trait BigmapKeysGetter {
    fn get(&mut self, level: u32, bigmap_id: i32) -> Result<Vec<BigmapEntry>>;
}

impl BigmapKeysGetter for DBClient {
    fn get(&mut self, level: u32, bigmap_id: i32) -> Result<Vec<BigmapEntry>> {
        let mut conn = self.dbconn()?;
        let res = conn.query(
            "
SELECT
    keyhash,
    key,
    value
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
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect::<Vec<BigmapEntry>>())
    }
}
