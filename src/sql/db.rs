use anyhow::{anyhow, Result};
use itertools::Itertools;
use std::collections::HashMap;

use native_tls::{Certificate, TlsConnector};
use postgres::fallible_iterator::FallibleIterator;
use postgres::row::Row;
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
use crate::sql::table_builder::TableBuilder;
use crate::storage_structure::relational::RelationalAST;
use crate::storage_update::bigmap::BigmapCopy;

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

    pub(crate) fn recreate_contract_views(
        &mut self,
        contract_id: &ContractID,
        rel_ast: &RelationalAST,
    ) -> Result<()> {
        let mut builder = TableBuilder::new();
        builder.populate(rel_ast);

        let generator = PostgresqlGenerator::new(contract_id);
        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);

        let mut tx = self.transaction()?;
        for (_, table) in sorted_tables {
            if table.name == "bigmap_clears" {
                continue;
            }
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
            let views_def = generator.create_view_definition(table)?;

            tx.simple_query(views_def.as_str())?;
        }
        tx.commit()?;
        Ok(())
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

    fn get_contract_schema(
        tx: &mut Transaction,
        contract_address: &str,
    ) -> Result<Option<String>> {
        let res = tx.query_opt(
            "
SELECT
    name
FROM contracts
WHERE address = $1
",
            &[&contract_address],
        )?;
        Ok(res.map(|row| row.get(0)))
    }

    fn get_bigmap_table(
        tx: &mut Transaction,
        contract_name: &str,
        bigmap_id: i32,
    ) -> Result<Option<String>> {
        let res = tx.query_opt(
            r#"
SELECT
    "table"
FROM bigmap_tables
WHERE contract = $1
  AND bigmap_id = $2
"#,
            &[&contract_name, &bigmap_id],
        )?;
        Ok(res.map(|row| row.get(0)))
    }

    fn get_table_columns(
        tx: &mut Transaction,
        contract_name: &str,
        table_name: &str,
    ) -> Result<Vec<String>> {
        let rows = tx.query(
            "
SELECT column_name
FROM information_schema.columns
WHERE table_schema = $1
  AND table_name = $2
",
            &[&contract_name, &table_name],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| row.get(0))
            .collect())
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
            let stmt = tx.prepare(&format!("
INSERT INTO
tx_contexts(id, level, contract, operation_group_number, operation_number, content_number, internal_number, operation_hash, source, destination, entrypoint) VALUES ( {} )", v_refs))?;

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

    pub(crate) fn apply_bigmap_deps(
        tx: &mut postgres::Transaction,
        contract_id: &ContractID,
        bigmap_deps: &[BigmapCopy],
        next_id: &mut i64,
    ) -> Result<()> {
        for dep in bigmap_deps {
            let ctx = format!("{}", dep.tx_context.id.unwrap());
            let src_id = format!("{}", dep.src_bigmap);
            let dest_id = format!("{}", dep.dest_bigmap);
            tx.execute(
                "
INSERT INTO bigmap_deps(
    tx_context_id, src_contract, src_bigmap, dest_schema, dest_table, dest_bigmap
)
SELECT
    *
FROM (
    SELECT
        x.ctx::integer as tx_context_id,
        COALESCE(c.address, x.src_contract)::text as src_contract,
        x.src_bigmap::integer,
        x.dest_schema::text,
        x.dest_table::text,
        x.dest_bigmap::integer
    FROM (VALUES ($1, $2, $3, $4, $5, $6)) x(ctx, src_contract, src_bigmap, dest_schema, dest_table, dest_bigmap)
    LEFT JOIN contracts c
      ON c.address = x.src_contract
) q
",
                &[
		    &ctx,
                    &dep.src_contract,
                    &src_id,
                    &contract_id.name,
		    &dep.dest_table,
		    &dest_id,
                ],
            )?;

            if let Some(src_schema) =
                Self::get_contract_schema(tx, &dep.src_contract)?
            {
                if let Some(src_table) =
                    Self::get_bigmap_table(tx, &src_schema, dep.src_bigmap)?
                {
                    let src_columns: Vec<String> =
                        Self::get_table_columns(tx, &src_schema, &src_table)?
                            .into_iter()
                            .filter(|x| {
                                x != "bigmap_id"
                                    && x != "tx_context_id"
                                    && x != "id"
                            })
                            .collect();

                    let column_mapping = Self::get_tables_column_mapping(
                        tx,
                        &src_schema,
                        &src_table,
                        &contract_id.name,
                        &dep.dest_table,
                    )?;

                    let dest_columns: Vec<String> = src_columns
                        .iter()
                        .map(|col| column_mapping[col].clone())
                        .collect();

                    let qry = format!(
                        r#"
WITH copy_rows AS (
    SELECT
        $1 + row_number() over ()::bigint as id,
        src.tx_context_id,
        {src_columns}
    FROM "{src_schema}"."{src_table}" as src
    JOIN tx_contexts ctx
      ON ctx.id = src.tx_context_id
    WHERE ctx.level < $2
      AND src.bigmap_id = $3
), insert_into_dest AS (
    INSERT INTO "{dest_schema}"."{dest_table}" (
        bigmap_id, tx_context_id, id, {dest_columns}
    )
    SELECT
        $4,
        $5,
        src.id,
        {src_columns}
    FROM copy_rows as src
)
INSERT INTO bigmap_copied_rows (
    src_tx_context_id, src_contract, dest_tx_context_id, dest_schema, dest_table, dest_row_id
)
SELECT
    src.tx_context_id,
    $6,
    $5,
    $7,
    $8,
    id
FROM copy_rows src
RETURNING dest_row_id"#,
                        src_schema = src_schema,
                        src_table = src_table,
                        src_columns = src_columns
                            .into_iter()
                            .map(|col| format!(r#"src."{}""#, col))
                            .join(", "),
                        dest_schema = contract_id.name,
                        dest_table = dep.dest_table,
                        dest_columns = dest_columns.join(", "),
                    );
                    for row in tx.query(
                        qry.as_str(),
                        &[
                            next_id,
                            &(dep.tx_context.level as i32),
                            &dep.src_bigmap,
                            &dep.dest_bigmap,
                            &dep.tx_context.id,
                            &dep.src_contract,
                            &contract_id.name,
                            &dep.dest_table,
                        ],
                    )? {
                        let inserted_id = row.get(0);
                        if inserted_id > *next_id {
                            *next_id = inserted_id;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_tables_column_mapping(
        tx: &mut postgres::Transaction,
        src_schema: &str,
        src_table: &str,
        dest_schema: &str,
        dest_table: &str,
    ) -> Result<HashMap<String, String>> {
        let src_columns: Vec<String> =
            Self::get_table_columns(tx, src_schema, src_table)?
                .into_iter()
                .collect();
        let dest_columns: Vec<String> =
            Self::get_table_columns(tx, dest_schema, dest_table)?
                .into_iter()
                .collect();
        let mut res: HashMap<String, String> = HashMap::new();
        for i in 0..src_columns.len() {
            res.insert(src_columns[i].clone(), dest_columns[i].clone());
        }
        Ok(res)
    }

    pub(crate) fn save_bigmap_table_locations(
        tx: &mut postgres::Transaction,
        contract_id: &ContractID,
        bigmap_locs: &[(i32, String)],
    ) -> Result<()> {
        for (bigmap_id, table) in bigmap_locs {
            tx.execute(
                r#"
INSERT INTO bigmap_tables (
    contract, "table", bigmap_id
) VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING
"#,
                &[&contract_id.name, table, bigmap_id],
            )?;
        }
        Ok(())
    }

    pub(crate) fn apply_inserts(
        tx: &mut postgres::Transaction,
        level: i32,
        contract_id: &ContractID,
        inserts: &[Insert],
        next_id: &mut i64,
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
        Self::populate_depending_bigmaps(
            tx,
            level,
            contract_id,
            inserts,
            next_id,
        )?;
        Ok(())
    }

    pub(crate) fn populate_depending_bigmaps(
        tx: &mut postgres::Transaction,
        level: i32,
        contract_id: &ContractID,
        inserts: &[Insert],
        next_id: &mut i64,
    ) -> Result<()> {
        let bigmap_inserts: Result<Vec<(i32, &Insert)>> = inserts
            .iter()
            .filter_map(|insert| {
                insert
                    .get_bigmap_id()
                    .map(|res_id| res_id.map(|id| (id, insert)))
            })
            .collect();
        let bigmap_inserts = bigmap_inserts?;
        if bigmap_inserts.is_empty() {
            return Ok(());
        }

        let mut bigmap_ids: HashMap<i32, ()> = HashMap::new();
        for (bigmap_id, _) in &bigmap_inserts {
            bigmap_ids.insert(*bigmap_id, ());
        }

        let bigmap_refs = (0..bigmap_ids.len())
            .map(|i| format!("${}", (i + 3).to_string()))
            .collect::<Vec<String>>()
            .join(", ");

        let mut args =
            vec![contract_id.address.borrow_to_sql(), level.borrow_to_sql()];
        args.extend::<Vec<&dyn ToSql>>(
            bigmap_ids
                .keys()
                .map(|k| k.borrow_to_sql())
                .collect(),
        );

        let mut rows: Vec<Row> = vec![];
        {
            let mut it = tx.query_raw(
                format!(
                    "
SELECT
    tx_context_id,
    src_bigmap,
    dest_schema,
    dest_table,
    dest_bigmap
FROM bigmap_deps dep
JOIN tx_contexts ctx
  ON ctx.id = dep.tx_context_id
WHERE src_contract = $1
  AND ctx.level > $2
  AND src_bigmap IN ({bigmap_refs})",
                    bigmap_refs = bigmap_refs
                )
                .as_str(),
                args,
            )?;

            while let Some(row) = it.next()? {
                rows.push(row);
            }
        }
        for row in rows {
            let ctx_id_at_copy: i64 = row.get(0);
            let src_bigmap: i32 = row.get(1);
            let dest_schema: String = row.get(2);
            let dest_table: String = row.get(3);
            let dest_bigmap: i32 = row.get(4);

            let mut dep_inserts: Vec<Insert> = vec![];

            let mut ids: Vec<(i64, i64)> = vec![];
            for (bigmap_id, insert) in &bigmap_inserts {
                if *bigmap_id != src_bigmap {
                    continue;
                }

                let src_tx_context_id = match insert.get_column("tx_context_id")
                {
                    Some(v) => match v.value {
                        Value::BigInt(i) => Ok(i),
                        _ => Err(anyhow!(
                            "insert has bad tx_context_id value type"
                        )),
                    },
                    None => Err(anyhow!("insert misses tx_context_id field")),
                }?;
                let column_mapping = Self::get_tables_column_mapping(
                    tx,
                    &contract_id.name,
                    &insert.table_name,
                    &dest_schema,
                    &dest_table,
                )?;

                let mut dep_columns: Vec<Column> = vec![];
                for insert in &insert.columns {
                    if insert.name == "bigmap_id"
                        || insert.name == "tx_context_id"
                        || insert.name == "id"
                    {
                        continue;
                    }
                    dep_columns.push(Column {
                        name: column_mapping[&insert.name].clone(),
                        value: insert.value.clone(),
                    });
                }
                let mut dep_insert = (*insert).clone();
                *next_id += 1;
                dep_insert.id = *next_id;
                dep_insert.table_name = dest_table.clone();

                dep_insert.columns = dep_columns;
                dep_insert.columns.push(Column {
                    name: "bigmap_id".to_string(),
                    value: Value::Int(dest_bigmap),
                });
                dep_insert.columns.push(Column {
                    name: "tx_context_id".to_string(),
                    value: Value::BigInt(ctx_id_at_copy),
                });

                ids.push((dep_insert.id, src_tx_context_id));
                dep_inserts.push(dep_insert);
            }

            Self::apply_inserts(
                tx,
                level,
                &ContractID {
                    name: dest_schema.clone(),
                    address: "".to_string(),
                },
                &dep_inserts,
                next_id,
            )?;

            let v_refs = (0..(ids.len() * 2))
                .map(|i| format!("${}::bigint", (i + 5).to_string()))
                .collect::<Vec<String>>()
                .chunks(2)
                .map(|x| x.join(", "))
                .join("), (");
            let mut args = vec![
                contract_id.address.borrow_to_sql(),
                ctx_id_at_copy.borrow_to_sql(),
                dest_schema.borrow_to_sql(),
                dest_table.borrow_to_sql(),
            ];
            for (id, src_tx_context_id) in &ids {
                args.push(id.borrow_to_sql());
                args.push(src_tx_context_id.borrow_to_sql());
            }

            let qry = format!(
                r#"
INSERT INTO bigmap_copied_rows (
    src_contract, src_tx_context_id, dest_tx_context_id, dest_schema, dest_table, dest_row_id
)
SELECT
    $1,
    src_tx_context_id,
    $2,
    $3,
    $4,
    id
FROM
(
    VALUES ({})
) q(id, src_tx_context_id)
"#,
                v_refs
            );
            tx.query_raw(qry.as_str(), args)?;
        }
        Ok(())
    }

    pub(crate) fn get_config_deps(
        &mut self,
        config: &[ContractID],
    ) -> Result<Vec<String>> {
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
FROM bigmap_deps
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
        let mut res: Vec<String> = vec![];
        while let Some(row) = it.next()? {
            res.push(row.get(0));
        }
        Ok(res
            .into_iter()
            .filter(|dep| !config.iter().any(|c| c.address == *dep))
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
DROP TABLE IF EXISTS bigmap_copied_rows;
DROP TABLE IF EXISTS bigmap_deps;
DROP TABLE IF EXISTS bigmap_tables;
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
SELECT level, hash, prev_hash, baked_at
FROM levels
WHERE ($1::INTEGER IS NULL AND level = (SELECT max(level) FROM levels)) OR level = $1
ORDER BY level DESC
LIMIT 1",
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
        Self::delete_bigmap_copied_rows(tx, level, None)?;

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

    fn delete_bigmap_copied_rows(
        tx: &mut Transaction,
        level: u32,
        contract_id: Option<&ContractID>, // if None: delete for all contracts
    ) -> Result<()> {
        for row in tx.query(
            "
DELETE FROM bigmap_copied_rows
WHERE ($1::text is null OR $1::text = src_contract)
  AND src_tx_context_id IN (
    SELECT id from tx_contexts WHERE level = $2
)
RETURNING
    dest_schema,
    dest_table,
    dest_row_id
",
            &[&contract_id.map(|c| &c.address), &(level as i32)],
        )? {
            let dest_schema: String = row.get(0);
            let dest_table: String = row.get(1);
            let dest_row_id: i64 = row.get(2);

            tx.execute(
                format!(
                    r#"
DELETE FROM "{}"."{}"
WHERE id = $1
"#,
                    dest_schema, dest_table
                )
                .as_str(),
                &[&dest_row_id],
            )?;
        }
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
        Self::delete_bigmap_copied_rows(tx, level, Some(contract_id))?;

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
