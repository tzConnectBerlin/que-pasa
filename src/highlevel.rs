use anyhow::{anyhow, ensure, Context, Result};
use chrono::Duration;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;

#[cfg(test)]
use pretty_assertions::assert_eq;

use crate::config::ContractID;
use crate::debug;
use crate::octez::bcd;
use crate::octez::block::{get_implicit_origination_level, Block, LevelMeta};
use crate::octez::block_getter::ConcurrentBlockGetter;
use crate::octez::node::NodeClient;
use crate::relational::RelationalAST;
use crate::sql::db::{DBClient, IndexerMode};
use crate::sql::inserter::{
    DBInserter, InserterAction, ProcessedBlock, ProcessedContractBlock,
};
use crate::stats::StatsLogger;
use crate::storage_structure::relational;
use crate::storage_structure::typing;
use crate::storage_update::bigmap::IntraBlockBigmapDiffsProcessor;
use crate::storage_update::processor::StorageProcessor;

use crate::threading_utils::AtomicCondvar;

pub struct SaveLevelResult {
    pub level: u32,
    pub hash: String,
    pub contract_id: ContractID,
    pub is_origination: bool,
    pub tx_count: usize,
}

impl SaveLevelResult {
    pub(crate) fn from_processed_block(
        processed_block: &ProcessedContractBlock,
    ) -> Self {
        Self {
            level: 0, // TODO: fix processed_block.level.level,
            hash: "todo".to_string(),
            // hash: processed_block
            //     .level
            //     .hash
            //     .clone()
            //     .unwrap(),
            contract_id: processed_block.contract.cid.clone(),
            is_origination: processed_block.is_origination,
            tx_count: processed_block.tx_contexts.len(),
        }
    }
}

#[derive(Clone)]
pub struct Executor {
    node_cli: NodeClient,
    dbcli: DBClient,

    all_contracts: bool,

    mutexed_state: MutexedState,
    // threads: Vec<thread::JoinHandle<()>>,
    out_processed: flume::Sender<Box<InserterAction>>,
    stats: StatsLogger,
    inserter: DBInserter,
}

impl Executor {
    pub fn new(
        node_cli: NodeClient,
        dbcli: DBClient,
        reports_interval: usize,
    ) -> Result<Self> {
        let batch_size = 10;
        let (processed_send, processed_recv) =
            flume::bounded::<Box<InserterAction>>(batch_size * 2);
        let inserter = DBInserter::new(dbcli.clone(), batch_size);
        let stats = StatsLogger::new(std::time::Duration::new(
            reports_interval as u64,
            0,
        ));
        let threads = vec![inserter.run(&stats, processed_recv)?, stats.run()];

        Ok(Self {
            node_cli,
            dbcli,
            all_contracts: false,
            out_processed: processed_send,
            inserter,
            // threads,
            mutexed_state: MutexedState::new(),
            stats,
        })
    }

    fn update_level_floor(&mut self) -> Result<()> {
        if self.all_contracts {
            return Ok(());
        }

        self.mutexed_state.set_level_floor()
    }

    pub fn index_all_contracts(&mut self) {
        self.all_contracts = true
    }

    pub fn add_contract(&mut self, contract_id: &ContractID) -> Result<bool> {
        debug!(
            "getting the storage definition for contract={}..",
            contract_id.name
        );
        let mut contract = get_contract_rel(&self.node_cli, contract_id)?;

        contract.level_floor = self
            .dbcli
            .get_origination(contract_id)?;

        debug!("interpreted contract definition: {:#?}", contract);

        self.mutexed_state
            .add_contract(contract)
    }

    pub fn dynamic_loader(&mut self) {}

    pub fn add_missing_contracts(
        &mut self,
        contracts: &[ContractID],
    ) -> Result<()> {
        let mut l: Vec<relational::Contract> = vec![];

        for contract_id in contracts {
            l.push(get_contract_rel(&self.node_cli, contract_id)?);
        }

        self.dbcli
            .create_contract_schemas(&mut l)?;

        for mut contract in l {
            contract.level_floor = self
                .dbcli
                .get_origination(&contract.cid)?;

            let new_contract = self
                .mutexed_state
                .add_contract(contract)?;

            if new_contract && self.all_contracts {
                self.stats
                    .add("processor", "unique contracts", 1)?;
            }
        }
        Ok(())
    }

    pub fn create_contract_schemas(&mut self) -> Result<Vec<ContractID>> {
        let mut new_contracts: Vec<ContractID> = vec![];
        for contract in self
            .mutexed_state
            .get_contracts()?
            .into_values()
        {
            if self
                .dbcli
                .create_contract_schemas(&mut vec![contract.clone()])?
            {
                new_contracts.push(contract.cid.clone());
            }
        }
        Ok(new_contracts)
    }

    pub fn get_config(&self) -> Result<Vec<ContractID>> {
        Ok(self
            .mutexed_state
            .get_contracts()?
            .values()
            .into_iter()
            .map(|rel| rel.cid.clone())
            .collect::<Vec<ContractID>>())
    }

    pub fn get_config_sorted(&self) -> Result<Vec<ContractID>> {
        let mut res = self.get_config()?;
        res.sort_by_key(|elem| elem.name.clone());
        Ok(res)
    }

    pub fn add_dependency_contracts(&mut self) -> Result<()> {
        let deps = self
            .dbcli
            .get_config_deps(&self.get_config()?)
            .unwrap();

        for dep in &deps {
            self.add_contract(dep)?;
        }

        Ok(())
    }

    pub fn exec_dependents(&mut self) -> Result<Vec<u32>> {
        let mut levels = self
            .dbcli
            .get_dependent_levels(&self.get_config()?)?;
        if levels.is_empty() {
            return Ok(vec![]);
        }
        levels.sort_unstable();

        info!("reprocessing following levels, they have bigmap copies whose keys are now fully known: {:?}", levels);
        self.exec_levels(1, 1, levels)
    }

    pub fn exec_continuous(&mut self) -> Result<()> {
        // Executes blocks monotically, from old to new, continues from the heighest block present
        // in the db
        self.finalize_bootstrapping_contracts(true)?;

        fn wait(first_wait: &mut bool) {
            if *first_wait {
                print!("waiting for the next block");
            } else {
                print!(".");
            }
            *first_wait = false;
            io::stdout().flush().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
        fn wait_done(first_wait: &mut bool) {
            if !*first_wait {
                println!();
                *first_wait = false;
            }
        }
        let mut first_wait = true;
        loop {
            let chain_head = self.node_cli.head()?;
            let db_head = match self.dbcli.get_head()? {
                Some(head) => Ok(head),
                None => {
                    if self.all_contracts {
                        Self::print_status(
                            chain_head.level,
                            &self.exec_level(chain_head.level)?,
                        );
                        continue;
                    }
                    Err(anyhow!(
                        "cannot run in continuous mode: DB is empty, expected at least 1 block present to continue from"
                    ))
                }
            }?;
            debug!("db: {} chain: {}", db_head.level, chain_head.level);
            match chain_head.level.cmp(&db_head.level) {
                Ordering::Greater => {
                    wait_done(&mut first_wait);
                    for level in (db_head.level + 1)..=chain_head.level {
                        Self::print_status(level, &self.exec_level(level)?);
                    }
                    first_wait = true;
                    continue;
                }
                Ordering::Less => {
                    wait(&mut first_wait);
                    continue;
                }
                Ordering::Equal => {
                    // they are equal, so we will just check that the hashes match.
                    if db_head.hash != chain_head.hash {
                        wait_done(&mut first_wait);
                        warn!(
                            "Hashes don't match at level={:?}: {:?} (db) <> {:?} (chain)",
                            db_head.level, db_head.hash, chain_head.hash
                        );
                        warn!(
                            "reprocessing following forked levels: {:?}",
                            vec![db_head.level],
                        );

                        let mut conn = self.dbcli.dbconn()?;
                        let mut tx = conn.transaction()?;
                        DBClient::delete_levels(
                            &mut tx,
                            &[db_head.level as i32],
                        )?;
                        tx.commit()?;
                    }
                    wait(&mut first_wait);
                }
            }
        }
    }

    pub fn reprocess_forked_levels(
        &mut self,
        num_getters: usize,
        num_processors: usize,
    ) -> Result<Vec<u32>> {
        info!("checking if any levels need to be reprocessed due to forks..");
        match self.dbcli.get_head()? {
            Some(db_head) => {
                let mut forked_levels = self.dbcli.get_forked_levels()?;
                for lvl in forked_levels.clone() {
                    forked_levels.push(lvl - 1);
                }

                let db_head_verify: LevelMeta = self
                    .node_cli
                    .level_json(db_head.level)?
                    .0;
                if db_head_verify.hash != db_head.hash {
                    forked_levels.push(db_head.level);
                }

                let db_head_verify_backwards: LevelMeta = self
                    .node_cli
                    .level_json(db_head.level - 1)?
                    .0;

                let forked_pre_head_levels = self.ensure_level_hash(
                    db_head_verify_backwards.level,
                    db_head_verify_backwards
                        .hash
                        .as_ref()
                        .unwrap(),
                    db_head_verify_backwards
                        .prev_hash
                        .as_ref()
                        .unwrap(),
                )?;

                forked_levels.extend(forked_pre_head_levels);
                forked_levels.sort_unstable();
                forked_levels.dedup();

                if forked_levels.is_empty() {
                    info!("no forked levels, nothing needs to be reprocessed");
                    return Ok(vec![]);
                }

                warn!(
                    "reprocessing following forked levels: {:?}",
                    forked_levels
                );

                let mut conn = self.dbcli.dbconn()?;
                let mut tx = conn.transaction()?;
                DBClient::delete_levels(
                    &mut tx,
                    &forked_levels
                        .iter()
                        .map(|lvl| *lvl as i32)
                        .collect::<Vec<i32>>(),
                )?;
                tx.commit()?;

                self.exec_levels(num_getters, num_processors, forked_levels)
            }
            None => {
                info!("no forked levels, nothing needs to be reprocessed");

                Ok(vec![])
            }
        }
    }

    pub fn exec_levels(
        &mut self,
        num_getters: usize,
        num_processors: usize,
        levels: Vec<u32>,
    ) -> Result<Vec<u32>> {
        if levels.is_empty() {
            return Ok(vec![]);
        }

        let st = self.mutexed_state.clone();
        let have_floor = !self.all_contracts;
        let processed_levels = self.exec_parallel(
            num_getters,
            num_processors,
            move |height_chan| {
                for l in levels {
                    if have_floor && l < st.get_level_floor().unwrap() {
                        continue;
                    }
                    height_chan.send(l).unwrap();
                }
            },
        )?;
        Ok(processed_levels)
    }

    pub fn exec_new_contracts_historically(
        &mut self,
        bcd_settings: &Option<(String, String)>,
        num_getters: usize,
        num_processors: usize,
        acceptable_head_offset: Duration,
    ) -> Result<Vec<ContractID>> {
        let mut res: Vec<ContractID> = vec![];
        loop {
            self.add_dependency_contracts().unwrap();
            let new_contracts = self.create_contract_schemas().unwrap();

            if new_contracts.is_empty() {
                break;
            }
            res.extend(new_contracts.clone());

            info!(
                "initializing the following contracts historically: {:#?}",
                new_contracts
            );

            self.exec_missing_levels(
                bcd_settings,
                num_getters,
                num_processors,
                acceptable_head_offset,
                false,
            )
            .unwrap();
        }
        if !res.is_empty() {
            self.exec_dependents().unwrap();
        }
        Ok(res)
    }

    fn exec_partially_processed(
        &mut self,
        num_getters: usize,
        num_processors: usize,
    ) -> Result<()> {
        let partial_processed: Vec<u32> = self
            .dbcli
            .get_partial_processed_levels(&self.get_config()?)?;
        if partial_processed.is_empty() {
            return Ok(());
        }

        info!("re-processing {} levels that were not initialized for some contracts", partial_processed.len());
        self.exec_levels(num_getters, num_processors, partial_processed)?;
        Ok(())
    }

    pub fn exec_missing_levels(
        &mut self,
        bcd_settings: &Option<(String, String)>,
        num_getters: usize,
        num_processors: usize,
        acceptable_head_offset: Duration,
        exec_dependent_levels: bool,
    ) -> Result<()> {
        loop {
            let latest_level: LevelMeta = self.node_cli.head()?;

            let mut missing_levels: Vec<u32> = self
                .dbcli
                .get_missing_levels(&self.get_config()?, latest_level.level)?;
            if missing_levels.is_empty() {
                break;
            }
            let has_gaps = missing_levels
                .windows(2)
                .any(|w| w[0] != w[1] - 1);

            let first_missing: LevelMeta = self
                .node_cli
                .level_json(missing_levels[0])?
                .0;

            if !has_gaps
                && latest_level.baked_at.unwrap()
                    - first_missing.baked_at.unwrap()
                    < acceptable_head_offset
            {
                break;
            }

            info!("missing levels: {:?}", missing_levels.len());
            if missing_levels.len() > 1000 && bcd_settings.is_some() {
                let (bcd_url, network) = bcd_settings.as_ref().unwrap();
                let config = &self.get_config_sorted()?;

                let mut exclude_levels: Vec<u32> = self
                    .dbcli
                    .get_fully_processed_levels(config)?;
                for contract_id in config {
                    info!("Indexing missing levels for {}..", contract_id.name);
                    let bcd_cli = bcd::BCDClient::new(
                        bcd_url.clone(),
                        network.clone(),
                        contract_id.clone(),
                    );

                    let excl = exclude_levels.clone();
                    let stats = self.stats.clone();
                    let node_cli = self.node_cli.clone();
                    let processed_levels = self
                        .exec_parallel(
                            num_getters,
                            num_processors,
                            move |height_chan| {
                                bcd_cli
                                    .populate_levels_chan(
                                        || Ok(node_cli.head()?.level),
                                        &stats,
                                        &height_chan,
                                        &excl,
                                    )
                                    .unwrap()
                            },
                        )
                        .unwrap();
                    exclude_levels.extend(processed_levels);

                    if let Some(l) =
                        get_implicit_origination_level(&contract_id.address)
                    {
                        self.exec_level(l).unwrap();
                    }

                    self.fill_in_levels(contract_id)
                        .unwrap();

                    info!("contract {} initialized.", contract_id.name)
                }
                self.exec_partially_processed(num_getters, num_processors)?;
            } else {
                missing_levels.reverse();
                info!("processing {} missing levels", missing_levels.len());
                self.exec_levels(num_getters, num_processors, missing_levels)?;
            }
        }
        if exec_dependent_levels {
            self.exec_dependents()?;
        }
        Ok(())
    }

    pub fn exec_parallel<F>(
        &mut self,
        num_getters: usize,
        num_processors: usize,
        levels_selector: F,
    ) -> Result<Vec<u32>>
    where
        F: FnOnce(flume::Sender<u32>) + Send + 'static,
    {
        // a parallel exec has the consequence that we need to re-derive the
        // _live and _ordered tables when done. therefore we change the mode to
        // "bootstrap" here
        self.dbcli.set_indexing_mode_contracts(
            IndexerMode::Bootstrap,
            &self
                .get_config()?
                .into_iter()
                .map(|c| c.name)
                .collect::<Vec<String>>(),
        )?;

        let (height_send, height_recv) = flume::bounded::<u32>(num_getters);
        let (block_send, block_recv) =
            flume::bounded::<Box<(LevelMeta, Block)>>(num_getters * 5);

        let block_getter =
            ConcurrentBlockGetter::new(self.node_cli.clone(), num_getters);
        let mut threads = block_getter.run(height_recv, block_send);

        threads.push(thread::spawn(|| levels_selector(height_send)));

        let processed_results: Arc<Mutex<(Vec<u32>, Vec<u32>)>> =
            Arc::new(Mutex::new((vec![], vec![])));

        if num_processors <= 1 {
            let (processed, reprocess) = self.read_block_chan(block_recv)?;
            let mut res = processed_results.lock().unwrap();
            res.0.extend(processed);
            res.1.extend(reprocess);
        } else {
            info!("starting {} concurrent processors", num_processors);
            for _ in 0..num_processors {
                let mut exec = self.clone();
                let w_recv_ch = block_recv.clone();
                let res_arc = processed_results.clone();
                threads.push(thread::spawn(move || {
                    let (processed, reprocess) =
                        exec.read_block_chan(w_recv_ch).unwrap();

                    let mut res = res_arc.lock().unwrap();
                    res.0.extend(processed);
                    res.1.extend(reprocess);
                }));
            }
        }

        for t in threads {
            t.join().map_err(|e| {
                anyhow!("parallel execution thread failed with err: {:?}", e)
            })?;
        }

        let (mut processed_levels, reprocess_levels) =
            Arc::try_unwrap(processed_results)
                .map_err(|e| anyhow!("{:?}", e))?
                .into_inner()?;

        if !reprocess_levels.is_empty() {
            warn!(
                "reprocessing following forked levels: {:?}",
                reprocess_levels
            );
            let reprocessed_levels = self.exec_levels(
                num_getters,
                num_processors,
                reprocess_levels,
            )?;
            processed_levels.extend(reprocessed_levels);
        }

        info!("exiting exec_parallel");

        Ok(processed_levels)
    }

    pub(crate) fn finalize_bootstrapping_contracts(
        &mut self,
        ensure_sane_input_state: bool,
    ) -> Result<()> {
        info!(
            "re-populating derived tables (_live, _ordered). may take a while (expect minutes-hours, not seconds-minutes)."
        );

        if ensure_sane_input_state {
            let latest_level: LevelMeta = self.node_cli.head()?;
            let missing_levels: Vec<u32> = self
                .dbcli
                .get_missing_levels(&self.get_config()?, latest_level.level)?;
            let has_gaps = missing_levels
                .windows(2)
                .any(|w| w[0] != w[1] - 1);
            ensure!(
                !has_gaps,
                anyhow!("cannot re-populate derived tables, there are gaps in the processed levels")
            );
            ensure!(
                self.dbcli
                    .get_partial_processed_levels(&self.get_config()?)?
                    .is_empty(),
                anyhow!("cannot re-populate derived tables, some levels are only partially processed (not processed for some contracts)")
            );
        }

        let contract_modes = self
            .dbcli
            .get_indexing_mode_contracts(&self.get_config()?)?;

        for (contract_name, mode) in contract_modes {
            if mode == IndexerMode::Bootstrap {
                self.dbcli.repopulate_derived_tables(
                    &self
                        .mutexed_state
                        .get_contract(&contract_name)?
                        .unwrap(),
                )?;
                self.dbcli.set_indexing_mode_contracts(
                    IndexerMode::Head,
                    &[contract_name],
                )?;
            }
        }

        Ok(())
    }

    pub fn fill_in_levels(&mut self, contract_id: &ContractID) -> Result<()> {
        // fills in all levels in db as empty that are missing between min and max
        // level present

        self.dbcli.fill_in_levels(contract_id)
            .with_context(|| {
                "failed to mark levels unrelated to the contract as empty in the db"
            })?;
        Ok(())
    }

    fn read_block_chan(
        &mut self,
        block_ch: flume::Receiver<Box<(LevelMeta, Block)>>,
    ) -> Result<(Vec<u32>, Vec<u32>)> {
        let in_ch = block_ch.clone();

        let mut processed_levels: Vec<u32> = vec![];
        let mut reprocess_levels: Vec<u32> = vec![];
        for b in block_ch {
            let (meta, block) = *b;

            let (processed_block, forked_lvls) = self
                .exec_for_block(&meta, &block)
                .with_context(|| {
                    anyhow!(
                        "execute for level={} failed: could not process",
                        meta.level
                    )
                })?;
            reprocess_levels.extend(forked_lvls);

            for cres in &processed_block.contracts {
                if self.all_contracts {
                    self.stats.add(
                        "processor",
                        "contract calls",
                        cres.tx_contexts.len(),
                    )?;
                } else {
                    self.stats.add(
                        "processor",
                        &cres.contract.cid.name,
                        cres.tx_contexts.len(),
                    )?;
                }
            }
            self.stats.set(
                "processor",
                "channel sizes (input - output)",
                format!(
                    "{}/{} - {}/{}",
                    in_ch.len(),
                    in_ch.capacity().unwrap(),
                    self.out_processed.len(),
                    self.out_processed.capacity().unwrap()
                ),
            )?;

            self.out_processed
                .send(Box::new(InserterAction::AddToBatch {
                    block: processed_block,
                }))?;
            self.stats
                .add("processor", "levels", 1)?;
            self.stats.set(
                "processor",
                "last processed level",
                format!("{} ({:?})", meta.level, meta.baked_at.unwrap()),
            )?;

            processed_levels.push(meta.level);
        }

        let listener = AtomicCondvar::new();
        self.out_processed
            .send(Box::new(InserterAction::InsertNow {
                notify_on_inserted: listener.clone(),
            }))?;
        listener.wait();

        Ok((processed_levels, reprocess_levels))
    }

    fn print_status(level: u32, contract_results: &[SaveLevelResult]) {
        let mut contract_statuses: String = contract_results
            .iter()
            .filter_map(|c| match c.tx_count {
                0 => None,
                1 => Some(format!(
                    "\n\t1 contract call for {}",
                    c.contract_id.name
                )),
                _ => Some(format!(
                    "\n\t{} contract calls for {}",
                    c.tx_count, c.contract_id.name
                )),
            })
            .collect::<Vec<String>>()
            .join(",");
        let hash_msg = if contract_results.is_empty() {
            "".to_string()
        } else {
            format!("\nblock has hash: {}", contract_results[0].hash)
        };

        if contract_statuses.is_empty() {
            contract_statuses = "0 txs for us".to_string();
        }
        info!("level {}: {}{}", level, contract_statuses, hash_msg);
    }

    fn get_storage_processor(
        &self,
    ) -> Result<StorageProcessor<NodeClient, DBClient>> {
        Ok(StorageProcessor::new(
            1,
            self.node_cli.clone(),
            self.dbcli.clone(),
        ))
    }

    pub(crate) fn exec_level(
        &mut self,
        level_height: u32,
    ) -> Result<Vec<SaveLevelResult>> {
        let (meta, block) = self
            .node_cli
            .level_json(level_height)
            .with_context(|| {
                format!(
                    "execute for level={} failed: could not get block json",
                    level_height
                )
            })?;

        let mut res: Vec<SaveLevelResult> = vec![];
        let (mut processed_block, forked_lvls) = self
            .exec_for_block(&meta, &block)
            .with_context(|| {
                anyhow!(
                    "execute for level={} failed: could not process",
                    level_height
                )
            })?;
        if !forked_lvls.is_empty() {
            warn!("reprocessing following forked levels: {:?}", forked_lvls);

            let mut conn = self.dbcli.dbconn()?;
            let mut tx = conn.transaction()?;
            DBClient::delete_levels(
                &mut tx,
                &forked_lvls
                    .iter()
                    .map(|lvl| *lvl as i32)
                    .collect::<Vec<i32>>(),
            )?;
            tx.commit()?;

            for lvl in forked_lvls {
                Self::print_status(lvl, &self.exec_level(lvl)?);
            }
        }

        for cres in &processed_block.contracts {
            res.push(SaveLevelResult::from_processed_block(cres));
        }

        let listener = AtomicCondvar::new();

        processed_block.notify_on_inserted = Some(listener.clone());
        processed_block.trigger_insert = true;
        self.out_processed
            .send(Box::new(InserterAction::AddToBatch {
                block: processed_block,
            }))?;

        listener.wait();

        Ok(res)
    }

    fn ensure_level_hash(
        &mut self,
        level: u32,
        hash: &str,
        prev_hash: &str,
    ) -> Result<Vec<u32>> {
        let mut forked_lvls: Vec<u32> = vec![];

        if level != 0 {
            let prev = self.dbcli.get_level(level - 1)?;
            if let Some(db_prev_hash) = prev
                .as_ref()
                .and_then(|l| l.hash.as_ref())
            {
                if db_prev_hash != prev_hash {
                    let forked_lvl = prev.as_ref().unwrap().level;
                    warn!("Hashes don't match at level={:?}: {:?} (db) <> {:?} (chain)",
                      forked_lvl, db_prev_hash, prev_hash);

                    forked_lvls.push(forked_lvl);
                }
            }
        }

        let next = self.dbcli.get_level(level + 1)?;
        if let Some(db_next_prev_hash) = next
            .as_ref()
            .and_then(|l| l.prev_hash.as_ref())
        {
            if db_next_prev_hash != hash {
                let forked_lvl = next.as_ref().unwrap().level;
                warn!("Previous hashes don't match at level={:?}: {:?} (db) <> {:?} (chain)",
                      forked_lvl, db_next_prev_hash, hash);

                forked_lvls.push(forked_lvl);
            }
        }

        Ok(forked_lvls)
    }

    fn exec_for_block(
        &mut self,
        level: &LevelMeta,
        block: &Block,
    ) -> Result<(ProcessedBlock, Vec<u32>)> {
        info!("exec_for_block: {:?}", level.level);
        // note: we expect level's values to all be set (no None values in its fields)
        let forked_lvls = self.ensure_level_hash(
            level.level,
            level.hash.as_ref().unwrap(),
            level.prev_hash.as_ref().unwrap(),
        )?;

        let process_contracts = if self.all_contracts {
            let active_contracts: Vec<ContractID> = block
                .active_contracts()
                .iter()
                .map(|address| ContractID {
                    name: address.clone(),
                    address: address.clone(),
                })
                .collect();
            let new_contracts: Vec<ContractID> = self
                .mutexed_state
                .get_missing_contracts(&active_contracts)?;

            if !new_contracts.is_empty() {
                debug!(
                    "level {}, analyzing contracts: {:#?}..",
                    block.header.level,
                    new_contracts
                        .iter()
                        .map(|x| &x.address)
                        .collect::<Vec<&String>>()
                );
                self.add_missing_contracts(&new_contracts)?;
            }
            active_contracts
        } else {
            self.get_config()?
        };
        let mut contract_results: Vec<ProcessedContractBlock> = vec![];

        let diffs = IntraBlockBigmapDiffsProcessor::from_block(block)?;
        for contract_id in &process_contracts {
            let contract = self
                .mutexed_state
                .get_contract(&contract_id.name)?
                .unwrap();
            contract_results.push(
                self.exec_for_block_contract(level, block, &diffs, &contract)
                    .with_context(|| {
                        anyhow!(
                            "err on processing contract={}",
                            contract_id.name
                        )
                    })?,
            );
        }
        for cres in &contract_results {
            if cres.is_origination {
                self.update_contract_floor(&cres.contract.cid, level.level)?;
            }
        }
        Ok((
            ProcessedBlock {
                trigger_insert: false,
                level: level.clone(),

                contracts: contract_results,

                notify_on_inserted: None,
            },
            forked_lvls,
        ))
    }

    fn update_contract_floor(
        &mut self,
        contract_id: &ContractID,
        level: u32,
    ) -> Result<()> {
        self.mutexed_state
            .update_contract_floor(contract_id, level)?;
        self.update_level_floor()
    }

    fn exec_for_block_contract(
        &self,
        meta: &LevelMeta,
        block: &Block,
        diffs: &IntraBlockBigmapDiffsProcessor,
        contract: &relational::Contract,
    ) -> Result<ProcessedContractBlock> {
        let is_origination =
            block.has_contract_origination(&contract.cid.address);

        if !is_origination && !block.is_contract_active(&contract.cid.address) {
            return Ok(ProcessedContractBlock {
                contract: contract.clone(),

                inserts: vec![],
                tx_contexts: vec![],
                txs: vec![],
                bigmap_contract_deps: vec![],
                bigmap_keyhashes: HashMap::new(),
                bigmap_meta_actions: vec![],
                is_origination: false,
            });
        }

        let mut storage_processor = self.get_storage_processor()?;
        storage_processor.set_stats_logger(self.stats.clone());
        storage_processor
            .process_block(block, diffs, contract)
            .with_context(|| {
                format!(
                    "execute failed (level={}, contract={}): could not process block",
                    meta.level, contract.cid.name,
                )
            })?;

        let inserts = storage_processor.drain_inserts();
        let (tx_contexts, txs) = storage_processor.drain_txs();
        let bigmap_contract_deps =
            storage_processor.drain_bigmap_contract_dependencies();
        let bigmap_meta_actions = storage_processor.drain_bigmap_meta_actions();

        Ok(ProcessedContractBlock {
            contract: contract.clone(),

            inserts: inserts.values().cloned().collect(),
            tx_contexts,
            txs,
            bigmap_contract_deps,
            bigmap_keyhashes: storage_processor.get_bigmap_keyhashes(),
            is_origination,
            bigmap_meta_actions,
        })
    }
}

#[derive(Clone)]
struct MutexedState {
    #[allow(clippy::type_complexity)]
    contracts: Arc<Mutex<HashMap<String, relational::Contract>>>,

    // Everything below this level has nothing to do with what we are indexing
    level_floor: Arc<Mutex<u32>>,
}

impl MutexedState {
    pub fn new() -> Self {
        Self {
            contracts: Arc::new(Mutex::new(HashMap::new())),
            level_floor: Arc::new(Mutex::new(0)),
        }
    }

    pub fn set_level_floor(&self) -> Result<()> {
        let contracts = self
            .contracts
            .lock()
            .map_err(|_| anyhow!("failed to lock contracts mutex"))?;
        let mut level_floor = self
            .level_floor
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;

        *level_floor = contracts
            .values()
            .map(|c| c.level_floor.unwrap_or(0))
            .min()
            .unwrap_or(0);
        Ok(())
    }

    pub fn get_level_floor(&self) -> Result<u32> {
        let level_floor = self
            .level_floor
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;
        Ok(*level_floor)
    }

    pub fn add_contract(&self, contract: relational::Contract) -> Result<bool> {
        let mut contracts = self
            .contracts
            .lock()
            .map_err(|_| anyhow!("failed to lock contracts mutex"))?;

        if contracts.contains_key(&contract.cid.name) {
            return Ok(false);
        }

        contracts.insert(contract.cid.name.clone(), contract);
        Ok(true)
    }

    pub fn update_contract_floor(
        &self,
        contract_id: &ContractID,
        level: u32,
    ) -> Result<()> {
        let mut contracts = self
            .contracts
            .lock()
            .map_err(|_| anyhow!("failed to lock contracts mutex"))?;

        let mut v = contracts
            .get_mut(&contract_id.name)
            .unwrap();
        v.level_floor = Some(level);
        Ok(())
    }

    pub fn get_contract(
        &self,
        contract_name: &str,
    ) -> Result<Option<relational::Contract>> {
        let contracts = self
            .contracts
            .lock()
            .map_err(|_| anyhow!("failed to lock contracts mutex"))?;
        Ok(contracts.get(contract_name).cloned())
    }

    pub fn get_contracts(
        &self,
    ) -> Result<HashMap<String, relational::Contract>> {
        let contracts = self
            .contracts
            .lock()
            .map_err(|_| anyhow!("failed to lock contracts mutex"))?;
        Ok(contracts.clone())
    }

    pub fn get_missing_contracts(
        &self,
        l: &[ContractID],
    ) -> Result<Vec<ContractID>> {
        let contracts = self
            .contracts
            .lock()
            .map_err(|_| anyhow!("failed to lock contracts mutex"))?;

        Ok(l.iter()
            .filter(|contract_id| !contracts.contains_key(&contract_id.name))
            .cloned()
            .collect::<Vec<ContractID>>())
    }
}

pub(crate) fn get_contract_rel(
    node_cli: &NodeClient,
    cid: &ContractID,
) -> Result<relational::Contract> {
    let (storage_def, _) =
        &node_cli.get_contract_storage_definition(&cid.address, None)?;
    let type_ast = typing::type_ast_from_json(storage_def)
        .with_context(|| {
            "failed to derive a storage type from the storage definition"
        })
        .with_context(|| anyhow!("contract address={}", cid.address))?;
    debug!("storage definition retrieved, and type derived");
    debug!("type_ast: {:#?}", type_ast);

    debug!(
        "storage_def: {}, type_ast: {}",
        debug::pp_depth(6, &storage_def),
        debug::pp_depth(6, &type_ast),
    );

    // Build the internal representation from the storage defition
    let ctx = relational::Context::init("storage");
    let storage_ast = relational::ASTBuilder::new()
        .build_relational_ast(&ctx, &type_ast)
        .with_context(|| {
            "failed to build a relational AST from the storage type"
        })
        .with_context(|| anyhow!("contract address={}", cid.address))?;
    debug!("rel_ast: {:#?}", storage_ast);

    let entrypoint_defs =
        &node_cli.get_contract_entrypoint_definitions(&cid.address, None)?;

    let mut entrypoint_asts: HashMap<String, RelationalAST> = HashMap::new();
    for (entrypoint, entrypoint_def) in entrypoint_defs {
        let type_ast = typing::type_ast_from_json(entrypoint_def)
            .with_context(|| "failed to derive an entrypoint type ast")
            .with_context(|| {
                anyhow!(
                    "contract address={}, entrypoint={}",
                    cid.address,
                    entrypoint
                )
            })?;

        // Build the internal representation from the storage defition
        let ctx =
            relational::Context::init(format!("entry.{}", entrypoint).as_str());
        let rel_ast = relational::ASTBuilder::new()
            .memoryless_bigmaps()
            .build_relational_ast(&ctx, &type_ast)
            .with_context(|| {
                "failed to build a relational AST from the entrypoint type"
            })
            .with_context(|| {
                anyhow!(
                    "contract address={}, entrypoint={}",
                    cid.address,
                    entrypoint
                )
            })?;

        entrypoint_asts.insert(entrypoint.clone(), rel_ast);
    }

    Ok(relational::Contract {
        cid: cid.clone(),
        level_floor: None,

        storage_ast,
        entrypoint_asts,
    })
}

#[test]
fn test_generate() {
    use crate::sql::postgresql_generator::PostgresqlGenerator;
    use crate::storage_structure::relational::ASTBuilder;
    use crate::storage_structure::typing;

    use ron::ser::{to_string_pretty, PrettyConfig};
    use std::fs::File;
    use std::io::BufReader;
    use std::path::Path;
    use std::str::FromStr;

    let json = serde_json::Value::from_str(&debug::load_test(
        "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.script",
    ))
    .unwrap();
    let storage_definition = &json["code"][1]["args"][0];
    let type_ast =
        typing::type_ast_from_json(&storage_definition.clone()).unwrap();
    println!("{:#?}", type_ast);

    use crate::relational::Context;
    let context = Context::init("storage");

    let rel_ast = ASTBuilder::new()
        .build_relational_ast(&context, &type_ast)
        .unwrap();
    println!("{:#?}", rel_ast);
    let generator = PostgresqlGenerator::new(
        "some_main_schema".to_string(),
        &ContractID {
            name: "testcontract".to_string(),
            address: "".to_string(),
        },
    );
    let mut builder = crate::sql::table_builder::TableBuilder::new("storage");
    builder.populate(&rel_ast);
    let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
    sorted_tables.sort_by_key(|a| a.0);
    let mut tables: Vec<crate::sql::table::Table> = vec![];
    for (_name, table) in sorted_tables {
        print!(
            "{}",
            generator
                .create_table_definition(table)
                .unwrap()
        );
        tables.push(table.clone());
        println!();
    }

    let filename = "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.tables.json";
    println!("cat > {} <<ENDOFJSON", filename);
    println!(
        "{}",
        to_string_pretty(&tables, PrettyConfig::new()).unwrap()
    );
    println!(
        "ENDOFJSON
    "
    );

    let p = Path::new(filename);
    let file = File::open(p).unwrap();
    let reader = BufReader::new(file);

    let v: Vec<crate::sql::table::Table> =
        ron::de::from_reader(reader).unwrap();
    assert_eq!(v.len(), tables.len());
    //test doesn't verify view exist
    for i in 0..v.len() {
        assert_eq!(v[i], tables[i]);
    }
}

#[test]
fn test_get_origination_operations_from_block() {
    use crate::octez::block::Block;
    let test_file =
        "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.originations_test.json";
    let contract_id = "KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq";
    let block: Block =
        serde_json::from_str(&debug::load_test(test_file)).unwrap();
    assert!(block.has_contract_origination(&contract_id));

    for level in vec![
        132343, 123318, 123327, 123339, 128201, 132201, 132211, 132219, 132222,
        132240, 132242, 132259, 132262, 132278, 132282, 132285, 132298, 132300,
        132343, 132367, 132383, 132384, 132388, 132390, 135501, 138208, 149127,
    ] {
        let filename = format!(
            "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.level-{}.json",
            level
        );
        println!("testing {}", filename);
        let level_block: Block =
            serde_json::from_str(&debug::load_test(&filename)).unwrap();

        assert!(!level_block.has_contract_origination(&contract_id));
    }
}

#[test]
fn test_storage() {}
