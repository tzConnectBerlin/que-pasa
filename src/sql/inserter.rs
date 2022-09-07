use anyhow::{Context, Result};
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::thread;
use std::time::Instant;

use crate::config::ContractID;
use crate::octez::block::{LevelMeta, Tx, TxContext};
use crate::sql::db;
use crate::sql::db::{DBClient, IndexerMode};
use crate::sql::insert;
use crate::sql::insert::Insert;
use crate::sql::types::BigmapMetaAction;
use crate::stats::StatsLogger;
use crate::storage_structure::relational;
use crate::threading_utils::AtomicCondvar;

#[derive(Clone)]
pub(crate) struct DBInserter {
    dbcli: DBClient,

    // the number of processed blocks to collect before inserting into the db
    batch_size: usize,
}

pub(crate) enum InserterAction {
    InsertNow { notify_on_inserted: AtomicCondvar },
    AddToBatch { block: ProcessedBlock },
}

pub(crate) struct ProcessedBlock {
    pub trigger_insert: bool,
    pub level: LevelMeta,
    pub notify_on_inserted: Option<AtomicCondvar>,

    pub contracts: Vec<ProcessedContractBlock>,
}

impl DBInserter {
    pub(crate) fn new(dbcli: DBClient, batch_size: usize) -> Self {
        Self { dbcli, batch_size }
    }

    pub(crate) fn run(
        &self,
        stats: &StatsLogger,
        recv_ch: flume::Receiver<Box<InserterAction>>,
    ) -> Result<thread::JoinHandle<()>> {
        let batch_size = self.batch_size;
        let dbcli = self.dbcli.clone();
        let stats_cl = stats.clone();

        let thread_handle = thread::spawn(move || {
            Self::exec(dbcli, batch_size, &stats_cl, recv_ch).unwrap();
        });
        Ok(thread_handle)
    }

    fn exec(
        mut dbcli: DBClient,
        batch_size: usize,
        stats: &StatsLogger,
        recv_ch: flume::Receiver<Box<InserterAction>>,
    ) -> Result<()> {
        let update_derived = false;
        #[cfg(feature = "regression_force_update_derived")]
        let update_derived = true | update_derived;

        let mut batch = ProcessedBatch::new(dbcli.get_max_id()?);

        let mut accum_begin = Instant::now();
        for action in recv_ch {
            let force_insert;
            match *action {
                InserterAction::AddToBatch { block } => {
                    force_insert = block.trigger_insert;
                    batch.add(block);
                }
                InserterAction::InsertNow { notify_on_inserted } => {
                    force_insert = true;
                    batch
                        .inserted_listeners
                        .push(notify_on_inserted);
                }
            }

            if batch.len() >= batch_size || force_insert {
                let accum_elapsed = accum_begin.elapsed();

                let insert_begin = Instant::now();
                insert_batch(&mut dbcli, Some(stats), update_derived, &batch)?;
                let insert_elapsed = insert_begin.elapsed();

                stats.set(
                    "inserter",
                    "prev batch's accumulation time",
                    format!("{:?}", accum_elapsed),
                )?;
                stats.set(
                    "inserter",
                    "prev batch's insert time",
                    format!("{:?}", insert_elapsed),
                )?;
                batch.clear();
                accum_begin = Instant::now();
            }
        }
        insert_batch(&mut dbcli, Some(stats), update_derived, &batch)?;

        Ok(())
    }
}

fn insert_batch(
    dbcli: &mut DBClient,
    stats: Option<&StatsLogger>,
    force_update_derived_tables: bool,
    batch: &ProcessedBatch,
) -> Result<()> {
    debug!("insert batch for levels: {:?}", batch.levels.keys());
    if batch.levels.len() > 0 {
        let contract_modes = dbcli.get_indexing_mode_contracts(
            &batch
                .contract_tx_contexts
                .keys()
                .cloned()
                .collect::<Vec<ContractID>>(),
        )?;

        let mut conn = dbcli.dbconn()?;

        let mut db_tx = conn.transaction()?;

        DBClient::set_max_id(&mut db_tx, batch.get_max_id())?;
        DBClient::delete_contract_levels(&mut db_tx, &batch.contract_levels)?;
        DBClient::save_levels(
            &mut db_tx,
            &batch
                .levels
                .values()
                .collect::<Vec<&LevelMeta>>(),
        )?;
        DBClient::save_contract_levels(
            &mut db_tx,
            &batch.contract_levels,
            &batch.contract_deps,
        )?;

        DBClient::save_tx_contexts(&mut db_tx, &batch.tx_contexts)?;
        DBClient::save_txs(&mut db_tx, &batch.txs)?;

        for (contract_id, inserts) in &batch.contract_inserts {
            let num_rows = inserts.len();
            if let Some(stats) = stats {
                stats.add("inserter", "contract data rows", num_rows)?;
            }
            DBClient::apply_inserts(&mut db_tx, contract_id, inserts)?;
        }
        DBClient::save_bigmap_keyhashes(
            &mut db_tx,
            batch.bigmap_keyhashes.clone(),
        )?;
        DBClient::save_bigmap_meta_actions(
            &mut db_tx,
            &batch.bigmap_meta_actions,
        )?;

        for (contract_id, mode) in contract_modes.into_iter() {
            if mode == IndexerMode::Bootstrap && !force_update_derived_tables {
                continue;
            }
            let (rel_contract, ctxs) = &batch
                .contract_tx_contexts
                .get(&contract_id)
                .unwrap();
            debug!(
                "updating derived tables for {:?}, ctxs: {:?}",
                contract_id.name, ctxs
            );
            dbcli.update_derived_tables(
                &mut db_tx,
                rel_contract,
                ctxs,
            ).with_context(|| {
                format!(
                    "insert failed (levels={:?}, contract={}): could not update derived tables",
                    batch.levels.keys(), contract_id.name,
                )})?;
        }

        db_tx.commit()?;
    }

    for listener in &batch.inserted_listeners {
        listener.notify_all();
    }

    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct ProcessedContractBlock {
    pub contract: relational::Contract,

    pub is_origination: bool,

    pub inserts: Vec<Insert>,
    pub tx_contexts: Vec<TxContext>,
    pub txs: Vec<Tx>,
    pub bigmap_contract_deps: Vec<(String, i32, bool)>,
    pub bigmap_keyhashes: db::BigmapEntries,
    pub bigmap_meta_actions: Vec<BigmapMetaAction>,
}

impl ProcessedContractBlock {
    pub fn offset_ids(&mut self, offset: i64) -> i64 {
        let max_insert_id = self.offset_inserts(offset);
        let max_tx_id = self.offset_txs(offset);
        std::cmp::max(max_insert_id, max_tx_id)
    }

    fn offset_inserts(&mut self, offset: i64) -> i64 {
        let mut max = offset;
        for insert in self.inserts.iter_mut() {
            insert.map_column("tx_context_id", |v| match v {
                insert::Value::BigInt(i) => insert::Value::BigInt(i + offset),
                _ => panic!(".."),
            });

            insert.id += offset;
            insert.fk_id = insert.fk_id.map(|fk_id| fk_id + offset);
            max = vec![
                insert.id,
                insert.fk_id.unwrap_or(0),
                insert.get_tx_context_id().unwrap(),
                max,
            ]
            .into_iter()
            .max()
            .unwrap();
        }
        max
    }

    fn offset_txs(&mut self, offset: i64) -> i64 {
        let mut max = offset;
        for ctx in self.tx_contexts.iter_mut() {
            let shifted = ctx.id.unwrap() + offset;
            ctx.id = Some(shifted);
            max = std::cmp::max(shifted, max);
        }
        for tx in self.txs.iter_mut() {
            tx.tx_context_id += offset;
            max = std::cmp::max(tx.tx_context_id, max);
        }

        self.bigmap_keyhashes = self
            .bigmap_keyhashes
            .clone()
            .into_iter()
            .map(|(mut k, v)| {
                let shifted = k.1.id.unwrap() + offset;
                k.1.id = Some(shifted);
                max = std::cmp::max(shifted, max);
                (k, v)
            })
            .collect();

        for action in self.bigmap_meta_actions.iter_mut() {
            action.tx_context_id += offset;
            max = std::cmp::max(action.tx_context_id, max);
        }

        max
    }
}

struct ProcessedBatch {
    size: usize,

    pub levels: HashMap<i32, LevelMeta>,
    pub tx_contexts: Vec<TxContext>,
    pub txs: Vec<Tx>,
    pub bigmap_keyhashes: db::BigmapEntries,
    pub bigmap_meta_actions: Vec<BigmapMetaAction>,

    pub contract_levels: Vec<(ContractID, i32, bool)>,
    pub contract_inserts: HashMap<ContractID, Vec<Insert>>,
    pub contract_deps: Vec<(i32, String, ContractID, bool)>,
    pub contract_tx_contexts:
        HashMap<ContractID, (relational::Contract, Vec<TxContext>)>,

    pub inserted_listeners: Vec<AtomicCondvar>,

    max_id: i64,
}

impl ProcessedBatch {
    pub fn new(max_id: i64) -> Self {
        Self {
            size: 0,

            levels: HashMap::new(),
            tx_contexts: vec![],
            txs: vec![],
            bigmap_keyhashes: HashMap::new(),
            bigmap_meta_actions: vec![],

            contract_levels: vec![],
            contract_inserts: HashMap::new(),
            contract_deps: vec![],
            contract_tx_contexts: HashMap::new(),

            inserted_listeners: vec![],

            max_id,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn get_max_id(&self) -> i64 {
        self.max_id
    }

    pub fn clear(&mut self) {
        self.levels.clear();
        self.tx_contexts.clear();
        self.txs.clear();
        self.bigmap_keyhashes.clear();
        self.bigmap_meta_actions.clear();
        self.contract_levels.clear();
        self.contract_inserts.clear();
        self.contract_deps.clear();
        self.inserted_listeners.clear();
        self.contract_tx_contexts.clear();

        self.size = 0;
    }

    pub fn add(&mut self, processed_block: ProcessedBlock) {
        let block_level: i32 = processed_block.level.level as i32;
        if let Vacant(e) = self.levels.entry(block_level) {
            e.insert(processed_block.level.clone());
        }

        for mut cres in processed_block.contracts.into_iter() {
            self.max_id = cres.offset_ids(self.max_id);
            self.add_cres(block_level, cres);
        }

        if let Some(listener) = processed_block.notify_on_inserted {
            self.inserted_listeners.push(listener);
        }

        self.size += 1;
    }

    fn add_cres(&mut self, block_level: i32, cres: ProcessedContractBlock) {
        self.tx_contexts
            .extend(cres.tx_contexts.clone());
        self.txs.extend(cres.txs.clone());

        if !self
            .contract_tx_contexts
            .contains_key(&cres.contract.cid)
        {
            self.contract_tx_contexts.insert(
                cres.contract.cid.clone(),
                (cres.contract.clone(), vec![]),
            );
        }
        let contract_ctxs: &mut Vec<TxContext> = &mut self
            .contract_tx_contexts
            .get_mut(&cres.contract.cid)
            .unwrap()
            .1;
        contract_ctxs.extend(cres.tx_contexts.clone());

        self.contract_levels.push((
            cres.contract.cid.clone(),
            block_level,
            cres.is_origination,
        ));

        if !self
            .contract_inserts
            .contains_key(&cres.contract.cid)
        {
            self.contract_inserts
                .insert(cres.contract.cid.clone(), vec![]);
        }
        let inserts: &mut Vec<Insert> = self
            .contract_inserts
            .get_mut(&cres.contract.cid)
            .unwrap();
        inserts.extend(cres.inserts.clone());

        self.contract_deps
            .extend(
                cres.bigmap_contract_deps
                    .iter()
                    .map(|dep| {
                        (
                            block_level,
                            dep.0.clone(),
                            cres.contract.cid.clone(),
                            dep.2,
                        )
                    }),
            );

        self.bigmap_keyhashes
            .extend(cres.bigmap_keyhashes);

        self.bigmap_meta_actions
            .extend(cres.bigmap_meta_actions);
    }
}
