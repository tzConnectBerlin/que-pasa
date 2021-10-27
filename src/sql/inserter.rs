use anyhow::{Context, Result};
use postgres::Transaction;
use std::collections::HashMap;
use std::thread;

use crate::config::ContractID;
use crate::octez::block::{LevelMeta, Tx, TxContext};
use crate::sql::db::{DBClient, IndexerMode};
use crate::sql::insert::{offset_inserts_ids, Insert, Inserts};
use crate::stats::StatsLogger;
use crate::storage_structure::relational::RelationalAST;

pub(crate) struct DBInserter {
    dbcli: DBClient,

    // the number of processed blocks to collect before inserting into the db
    batch_size: usize,
    update_derived_tables: bool,
}

#[derive(Clone)]
pub(crate) struct ProcessedContractBlock {
    pub level: LevelMeta,
    pub contract_id: ContractID,
    pub rel_ast: RelationalAST,

    pub is_origination: bool,

    pub inserts: Inserts,
    pub tx_contexts: Vec<TxContext>,
    pub txs: Vec<Tx>,
    pub bigmap_contract_deps: Vec<String>,
    pub bigmap_keyhashes: Vec<(TxContext, i32, String, String)>,
}

pub(crate) type ProcessedBlock = Vec<ProcessedContractBlock>;

impl DBInserter {
    pub(crate) fn new(
        mut dbcli: DBClient,
        batch_size: usize,
        always_update_derived: bool,
    ) -> Result<Self> {
        let update_derived_tables = always_update_derived
            || dbcli.get_indexer_mode()? == IndexerMode::Head;

        Ok(Self {
            dbcli,
            update_derived_tables,
            batch_size,
        })
    }

    pub(crate) fn run(
        &self,
        recv_ch: flume::Receiver<Box<ProcessedBlock>>,
    ) -> Result<thread::JoinHandle<()>> {
        let batch_size = self.batch_size;
        let update_derived_tables = self.update_derived_tables;
        let mut dbcli = self.dbcli.reconnect()?;

        let thread_handle = thread::spawn(move || {
            Self::exec(dbcli, update_derived_tables, batch_size, recv_ch)
                .unwrap();
        });
        Ok(thread_handle)
    }

    fn exec(
        mut dbcli: DBClient,
        update_derived_tables: bool,
        batch_size: usize,
        recv_ch: flume::Receiver<Box<ProcessedBlock>>,
    ) -> Result<()> {
        let mut batch: Vec<ProcessedContractBlock> = vec![];
        let ch = recv_ch.clone();

        let stats = StatsLogger::new(
            "inserter".to_string(),
            std::time::Duration::new(60, 0),
        );
        stats.run();

        for processed in recv_ch {
            batch.extend(*processed);
            if batch.len() >= batch_size {
                insert_batch(
                    &mut dbcli,
                    Some(&stats),
                    update_derived_tables,
                    &batch
                        .drain(..)
                        .collect::<Vec<ProcessedContractBlock>>(),
                )?;
            }
        }
        Ok(())
    }
}

pub(crate) fn insert_batch(
    dbcli: &mut DBClient,
    stats: Option<&StatsLogger>,
    update_derived_tables: bool,
    batch: &[ProcessedContractBlock],
) -> Result<()> {
    let mut c_inserts: HashMap<ContractID, Inserts> = HashMap::new();
    let mut c_tx_contexts: HashMap<
        ContractID,
        (RelationalAST, Vec<TxContext>),
    > = HashMap::new();

    let mut tx_contexts: Vec<TxContext> = vec![];
    let mut txs: Vec<Tx> = vec![];
    let mut bigmap_keyhashes: Vec<(TxContext, i32, String, String)> = vec![];

    let mut max_id = dbcli.get_max_id()?;
    let mut db_tx: Transaction = dbcli.transaction()?;

    let mut levels: HashMap<u32, ()> = HashMap::new();
    for block in batch {
        max_id += 1;

        if levels
            .insert(block.level.level, ())
            .is_none()
        {
            DBClient::delete_level(&mut db_tx, block.level.level)?;
            DBClient::save_level(&mut db_tx, &block.level)?;
        }
        DBClient::delete_contract_level(
            &mut db_tx,
            &block.contract_id,
            block.level.level,
        )?;
        DBClient::save_contract_level(
            &mut db_tx,
            &block.contract_id,
            block.level.level,
        )?;
        DBClient::save_contract_deps(
            &mut db_tx,
            block.level.level,
            &block.contract_id,
            &block.bigmap_contract_deps,
        )?;

        if block.is_origination {
            DBClient::set_origination(&mut db_tx, &block.contract_id, block.level.level)
                    .with_context(|| {
                        format!(
                            "execute for level={} failed: could not mark level as contract origination in db",
                            block.level.level)
                    })?;
        }

        if !c_inserts.contains_key(&block.contract_id) {
            c_inserts.insert(block.contract_id.clone(), HashMap::new());
        }
        let inserts: &mut Inserts = c_inserts
            .get_mut(&block.contract_id)
            .unwrap();
        let (new_inserts, mut next_max_id) =
            offset_inserts_ids(&block.inserts, max_id);
        inserts.extend(new_inserts);

        if !c_tx_contexts.contains_key(&block.contract_id) {
            c_tx_contexts.insert(
                block.contract_id.clone(),
                (block.rel_ast.clone(), vec![]),
            );
        }
        let ctxs: &mut Vec<TxContext> = &mut c_tx_contexts
            .get_mut(&block.contract_id)
            .unwrap()
            .1;
        for ctx in &block.tx_contexts {
            let mut ctx = ctx.clone();

            let shifted = ctx.id.unwrap() + max_id;
            ctx.id = Some(shifted);
            next_max_id = std::cmp::max(shifted, next_max_id);

            ctxs.push(ctx.clone());
            tx_contexts.push(ctx);
        }

        for tx in &block.txs {
            let mut tx = tx.clone();
            tx.tx_context_id += max_id;
            next_max_id = std::cmp::max(tx.tx_context_id, next_max_id);
            txs.push(tx);
        }

        for keyhash in &block.bigmap_keyhashes {
            let mut keyhash = keyhash.clone();
            let shifted = keyhash.0.id.unwrap() + max_id;
            keyhash.0.id = Some(shifted);
            bigmap_keyhashes.push(keyhash);

            next_max_id = std::cmp::max(shifted, next_max_id);
        }

        max_id = next_max_id;
    }

    DBClient::save_tx_contexts(&mut db_tx, &tx_contexts)?;
    DBClient::save_txs(&mut db_tx, &txs)?;

    for (contract_id, inserts) in c_inserts {
        let num_rows = inserts.len();
        DBClient::apply_inserts(
            &mut db_tx,
            &contract_id,
            &inserts
                .into_values()
                .collect::<Vec<Insert>>(),
        )?;
        if let Some(stats) = stats {
            stats.add(contract_id.name.clone(), num_rows)?;
        }
    }
    DBClient::save_bigmap_keyhashes(&mut db_tx, &bigmap_keyhashes)?;

    if update_derived_tables {
        for (contract_id, (rel_ast, ctxs)) in c_tx_contexts {
            DBClient::update_derived_tables(
                &mut db_tx,
                &contract_id,
                &rel_ast,
                &ctxs,
            ).with_context(|| {
                format!(
                    "insert failed (levels={:?}, contract={}): could not update derived tables",
                    levels.keys(), contract_id.name,
                )})?;
        }
    }
    DBClient::set_max_id(&mut db_tx, max_id)?;

    db_tx.commit()?;

    if let Some(stats) = stats {
        stats.add("levels".to_string(), levels.len())?;
        stats.add("tx_contexts".to_string(), tx_contexts.len())?;
        stats.add("txs".to_string(), txs.len())?;
        stats.add("bigmap_keyhashes".to_string(), bigmap_keyhashes.len())?;
    }

    Ok(())
}
