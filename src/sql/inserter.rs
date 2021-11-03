use anyhow::{Context, Result};
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::thread;
use std::time::Instant;

use crate::config::ContractID;
use crate::octez::block::{LevelMeta, Tx, TxContext};
use crate::sql::db::DBClient;
use crate::sql::insert::Insert;
use crate::stats::StatsLogger;
use crate::storage_structure::relational::RelationalAST;

pub(crate) struct DBInserter {
    dbcli: DBClient,

    // the number of processed blocks to collect before inserting into the db
    batch_size: usize,
}

pub(crate) type ProcessedBlock = Vec<ProcessedContractBlock>;

impl DBInserter {
    pub(crate) fn new(dbcli: DBClient, batch_size: usize) -> Self {
        Self { dbcli, batch_size }
    }

    pub(crate) fn run(
        &self,
        stats: &StatsLogger,
        recv_ch: flume::Receiver<Box<ProcessedBlock>>,
    ) -> Result<thread::JoinHandle<()>> {
        let batch_size = self.batch_size;
        let dbcli = self.dbcli.reconnect()?;
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
        recv_ch: flume::Receiver<Box<ProcessedBlock>>,
    ) -> Result<()> {
        let update_derived = false;
        #[cfg(feature = "regression_force_update_derived")]
        let update_derived = true | update_derived;

        let mut batch = ProcessedBatch::new(dbcli.get_max_id()?);

        let mut accum_begin = Instant::now();
        for processed_block in recv_ch {
            batch.add(*processed_block);

            if batch.len() >= batch_size {
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

pub(crate) fn insert_processed(
    dbcli: &mut DBClient,
    update_derived_tables: bool,
    processed: ProcessedBlock,
) -> Result<()> {
    let mut batch = ProcessedBatch::new(dbcli.get_max_id()?);
    batch.add(processed);

    insert_batch(dbcli, None, update_derived_tables, &batch)
}

fn insert_batch(
    dbcli: &mut DBClient,
    stats: Option<&StatsLogger>,
    update_derived_tables: bool,
    batch: &ProcessedBatch,
) -> Result<()> {
    let sql_gen = &dbcli.sql_gen.clone();
    let mut db_tx = dbcli.transaction()?;

    DBClient::set_max_id(&mut db_tx, batch.get_max_id())?;
    DBClient::save_levels(
        &mut db_tx,
        &batch
            .levels
            .values()
            .collect::<Vec<&LevelMeta>>(),
    )?;
    DBClient::save_contract_deps(&mut db_tx, &batch.contract_deps)?;
    DBClient::save_contract_levels(&mut db_tx, &batch.contract_levels)?;

    DBClient::save_tx_contexts(&mut db_tx, &batch.tx_contexts)?;
    DBClient::save_txs(&mut db_tx, &batch.txs)?;

    for (contract_id, inserts) in &batch.contract_inserts {
        let num_rows = inserts.len();
        if let Some(stats) = stats {
            stats.add("inserter", "contract data rows", num_rows)?;
        }
        DBClient::apply_inserts(&mut db_tx, sql_gen, contract_id, inserts)?;
    }
    DBClient::save_bigmap_keyhashes(&mut db_tx, &batch.bigmap_keyhashes)?;

    if update_derived_tables {
        for (contract_id, (rel_ast, ctxs)) in &batch.contract_tx_contexts {
            DBClient::update_derived_tables(
                &mut db_tx,
                sql_gen,
                contract_id,
                rel_ast,
                ctxs,
            ).with_context(|| {
                format!(
                    "insert failed (levels={:?}, contract={}): could not update derived tables",
                    batch.levels.keys(), contract_id.name,
                )})?;
        }
    }

    db_tx.commit()?;

    Ok(())
}

#[derive(Clone)]
pub(crate) struct ProcessedContractBlock {
    pub level: LevelMeta,
    pub contract_id: ContractID,
    pub rel_ast: RelationalAST,

    pub is_origination: bool,

    pub inserts: Vec<Insert>,
    pub tx_contexts: Vec<TxContext>,
    pub txs: Vec<Tx>,
    pub bigmap_contract_deps: Vec<String>,
    pub bigmap_keyhashes: Vec<(TxContext, i32, String, String)>,
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
            insert.tx_context_id += offset;

            insert.id += offset;
            insert.fk_id = insert.fk_id.map(|fk_id| fk_id + offset);
            max = vec![
                insert.id,
                insert.fk_id.unwrap_or(0),
                insert.tx_context_id,
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
        }
        for keyhash in self.bigmap_keyhashes.iter_mut() {
            let shifted = keyhash.0.id.unwrap() + offset;
            keyhash.0.id = Some(shifted);
        }
        max
    }
}

struct ProcessedBatch {
    size: usize,

    pub levels: HashMap<i32, LevelMeta>,
    pub tx_contexts: Vec<TxContext>,
    pub txs: Vec<Tx>,
    pub bigmap_keyhashes: Vec<(TxContext, i32, String, String)>,

    pub contract_levels: Vec<(ContractID, i32, bool)>,
    pub contract_inserts: HashMap<ContractID, Vec<Insert>>,
    pub contract_deps: Vec<(i32, String, ContractID)>,
    pub contract_tx_contexts:
        HashMap<ContractID, (RelationalAST, Vec<TxContext>)>,

    max_id: i64,
}

impl ProcessedBatch {
    pub fn new(max_id: i64) -> Self {
        Self {
            size: 0,

            levels: HashMap::new(),
            tx_contexts: vec![],
            txs: vec![],
            bigmap_keyhashes: vec![],

            contract_levels: vec![],
            contract_inserts: HashMap::new(),
            contract_deps: vec![],
            contract_tx_contexts: HashMap::new(),

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
        self.contract_levels.clear();
        self.contract_inserts.clear();
        self.contract_deps.clear();

        self.size = 0;
    }

    pub fn add(&mut self, processed_block: ProcessedBlock) {
        for mut cres in processed_block.into_iter() {
            self.max_id = cres.offset_ids(self.max_id);
            self.add_cres(cres);
        }
        self.size += 1;
    }

    fn add_cres(&mut self, cres: ProcessedContractBlock) {
        let level: i32 = cres.level.level as i32;
        if let Vacant(e) = self.levels.entry(level) {
            e.insert(cres.level.clone());
        }
        self.tx_contexts
            .extend(cres.tx_contexts.clone());
        self.txs.extend(cres.txs.clone());

        if !self
            .contract_tx_contexts
            .contains_key(&cres.contract_id)
        {
            self.contract_tx_contexts.insert(
                cres.contract_id.clone(),
                (cres.rel_ast.clone(), vec![]),
            );
        }
        let contract_ctxs: &mut Vec<TxContext> = &mut self
            .contract_tx_contexts
            .get_mut(&cres.contract_id)
            .unwrap()
            .1;
        contract_ctxs.extend(cres.tx_contexts.clone());

        self.contract_levels.push((
            cres.contract_id.clone(),
            cres.level.level as i32,
            cres.is_origination,
        ));

        if !self
            .contract_inserts
            .contains_key(&cres.contract_id)
        {
            self.contract_inserts
                .insert(cres.contract_id.clone(), vec![]);
        }
        let inserts: &mut Vec<Insert> = self
            .contract_inserts
            .get_mut(&cres.contract_id)
            .unwrap();
        inserts.extend(cres.inserts.clone());

        self.contract_deps.extend(
            cres.bigmap_contract_deps
                .iter()
                .map(|dep| (level, dep.clone(), cres.contract_id.clone())),
        );

        self.bigmap_keyhashes
            .extend(cres.bigmap_keyhashes);
    }
}
