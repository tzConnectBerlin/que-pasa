use crate::octez::block::{Block, LevelMeta};
use crate::octez::block_getter::ConcurrentBlockGetter;
use crate::octez::node::NodeClient;
use crate::relational::RelationalAST;
use crate::sql::db::DBClient;
use crate::sql::insert::{InsertKey, Inserts};
use crate::storage_value::processor::{StorageProcessor, TxContext};
use anyhow::{anyhow, Context, Result};
use std::cmp::Ordering;
use std::thread;

#[cfg(test)]
use pretty_assertions::assert_eq;

pub struct SaveLevelResult {
    pub level: u32,
    pub is_origination: bool,
    pub tx_count: u32,
}

pub struct Executor {
    node_cli: NodeClient,
    rel_ast: RelationalAST,
    contract_id: String,
    dbcli: DBClient,
}

impl Executor {
    pub fn new(
        node_cli: NodeClient,
        rel_ast: RelationalAST,
        contract_id: String,
        dbcli: DBClient,
    ) -> Self {
        Self {
            node_cli,
            rel_ast,
            contract_id,
            dbcli,
        }
    }

    pub fn exec_continuous(&mut self) -> Result<()> {
        // Executes blocks monotically, from old to new, continues from the heighest block present
        // in the db

        let mut storage_processor = self.get_storage_processor()?;
        let is_tty = stdout_is_tty();

        loop {
            let _spinner;

            if is_tty {
                _spinner =
                    spinners::Spinner::new(spinners::Spinners::Line, "".into());
                //print!("Waiting for first block");
            }

            let chain_head = self.node_cli.head()?;
            let db_head = match self.dbcli.get_head()? {
                Some(head) => Ok(head),
                None => Err(anyhow!(
                    "cannot run in continuous mode: DB is empty, expected at least 1 block present to continue from"
                )),
            }?;
            debug!("db: {} chain: {}", db_head._level, chain_head._level);
            match chain_head._level.cmp(&db_head._level) {
                Ordering::Greater => {
                    for level in (db_head._level + 1)..=chain_head._level {
                        let result =
                            self.exec_level(level, &mut storage_processor)?;
                        Self::print_status(&result);
                    }
                    continue;
                }
                Ordering::Less => {
                    return Err(anyhow!(
                        "More levels in DB than chain, bailing!"
                    ))
                }
                Ordering::Equal => {
                    // they are equal, so we will just check that the hashes match.
                    if db_head.hash == chain_head.hash {
                        // if they match, nothing to do.
                    } else {
                        warn!(
                            "Hashes don't match: {:?} (db) <> {:?} (chain)",
                            db_head.hash, chain_head.hash
                        );
                        let mut tx = self.dbcli.transaction()?;
                        DBClient::delete_level(&mut tx, &db_head)?;
                        tx.commit()?;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1500));
                }
            }
        }
    }

    pub fn exec_levels(
        &mut self,
        num_getters: usize,
        levels: Vec<u32>,
    ) -> Result<()> {
        self.exec_parallel(num_getters, move |height_chan| {
            for l in levels {
                height_chan.send(l).unwrap();
            }
        })
    }

    pub fn exec_missing_levels(&mut self, num_getters: usize) -> Result<()> {
        loop {
            let origination_level = self.dbcli.get_origination()?;

            let latest_level = self.node_cli.head()?._level;

            let missing_levels: Vec<u32> = self
                .dbcli
                .get_missing_levels(origination_level, latest_level)?;
            if missing_levels.is_empty() {
                // finally through them
                return Ok(());
            }

            self.exec_levels(num_getters, missing_levels)?;
        }
    }

    pub fn exec_parallel<F>(
        &mut self,
        num_getters: usize,
        levels_selector: F,
    ) -> Result<()>
    where
        F: FnOnce(flume::Sender<u32>) + Send + 'static,
    {
        // Fetches block data in parallel, processes each block sequentially

        let (height_send, height_recv) = flume::bounded::<u32>(num_getters);
        let (block_send, block_recv) =
            flume::bounded::<Box<(LevelMeta, Block)>>(num_getters);

        let block_getter =
            ConcurrentBlockGetter::new(self.node_cli.clone(), num_getters);
        let mut threads = block_getter.run(height_recv, block_send);

        threads.push(thread::spawn(|| levels_selector(height_send)));

        self.read_block_chan(block_recv)?;

        for t in threads {
            t.join().unwrap();
        }
        Ok(())
    }

    pub fn fill_in_levels(&mut self) -> Result<()> {
        // fills in all levels in db as empty that are missing between min and max
        // level present

        self.dbcli.fill_in_levels()
            .with_context(|| {
                "failed to mark levels unrelated to the contract as empty in the db"
            })?;
        Ok(())
    }

    fn read_block_chan(
        &mut self,
        block_recv: flume::Receiver<Box<(LevelMeta, Block)>>,
    ) -> Result<()> {
        let mut storage_processor = self.get_storage_processor()?;
        for b in block_recv {
            let (level, block) = *b;
            let res =
                self.exec_for_block(&level, &block, &mut storage_processor)?;
            Self::print_status(&res);
        }

        Ok(())
    }

    fn print_status(result: &SaveLevelResult) {
        info!("{}", level_text(result));
    }

    fn get_storage_processor(&mut self) -> Result<StorageProcessor> {
        let id = self
            .dbcli
            .get_max_id()
            .with_context(|| {
                "could not initialize storage processor from the db state"
            })?;
        Ok(StorageProcessor::new(id as u32))
    }

    fn exec_level(
        &mut self,
        level_height: u32,
        storage_processor: &mut StorageProcessor,
    ) -> Result<SaveLevelResult> {
        let (_json, level, block) = self
            .node_cli
            .level_json(level_height)
            .with_context(|| {
                format!(
                    "execute for level={} failed: could not get block json",
                    level_height
                )
            })?;

        self.exec_for_block(&level, &block, storage_processor)
    }

    fn exec_for_block(
        &mut self,
        level: &LevelMeta,
        block: &Block,
        storage_processor: &mut StorageProcessor,
    ) -> Result<SaveLevelResult> {
        if block.has_contract_origination(&self.contract_id) {
            self.mark_level_contract_origination(level)
            .with_context(|| {
                format!(
                    "execute for level={} failed: could not mark level as contract origination in db",
                    level._level)
            })?;
            return Ok(SaveLevelResult {
                level: level._level,
                is_origination: true,
                tx_count: 0,
            });
        }

        if !block.is_contract_active(&self.contract_id) {
            self.mark_level_empty(level)
            .with_context(|| {
                format!(
                    "execute for level={} failed: could not mark level as empty in db",
                    level._level)
            })?;
            return Ok(SaveLevelResult {
                level: level._level,
                is_origination: false,
                tx_count: 0,
            });
        }

        let (inserts, tx_contexts) = storage_processor
            .process_block(block, &self.rel_ast, &self.contract_id)
            .with_context(|| {
                format!(
                    "execute for level={} failed: could not process block",
                    level._level
                )
            })?;
        let tx_count = tx_contexts.len() as u32;

        self.save_level_processed_contract(
            level,
            &inserts,
            tx_contexts,
            (storage_processor.get_id_value() + 1) as i32,
        )
        .with_context(|| {
            format!(
                "execute for level={} failed: could not save processed block",
                level._level
            )
        })?;
        Ok(SaveLevelResult {
            level: level._level,
            is_origination: false,
            tx_count,
        })
    }

    fn mark_level_contract_origination(
        &mut self,
        level: &LevelMeta,
    ) -> Result<()> {
        let mut tx = self.dbcli.transaction()?;
        DBClient::delete_level(&mut tx, level)?;
        DBClient::save_level(&mut tx, level)?;
        DBClient::set_origination(&mut tx, level._level)?;
        tx.commit()?;
        Ok(())
    }

    fn mark_level_empty(&mut self, level: &LevelMeta) -> Result<()> {
        let mut tx = self.dbcli.transaction()?;
        DBClient::delete_level(&mut tx, level)?;
        DBClient::save_level(&mut tx, level)?;
        tx.commit()?;
        Ok(())
    }

    fn save_level_processed_contract(
        &mut self,
        level: &LevelMeta,
        inserts: &Inserts,
        tx_contexts: Vec<TxContext>,
        next_id: i32,
    ) -> Result<()> {
        let mut tx = self.dbcli.transaction()?;
        DBClient::delete_level(&mut tx, level)?;
        DBClient::save_level(&mut tx, level)?;

        DBClient::save_tx_contexts(&mut tx, &tx_contexts)?;
        let mut keys = inserts
            .keys()
            .collect::<Vec<&InsertKey>>();
        keys.sort_by_key(|a| a.id);
        for key in keys.iter() {
            DBClient::apply_insert(
                &mut tx,
                inserts
                    .get(key)
                    .ok_or_else(|| anyhow!("no insert for key"))?,
            )?;
        }
        DBClient::set_max_id(&mut tx, next_id)?;
        tx.commit()?;
        Ok(())
    }
}

fn level_text(result: &SaveLevelResult) -> String {
    match result {
        SaveLevelResult {
            is_origination: true,
            ..
        } => format!("level {}: contract origination", result.level),
        SaveLevelResult { tx_count: 1, .. } => {
            format!("level {}: {} tx for us", result.level, result.tx_count)
        }
        _ => format!("level {}: {} txs for us", result.level, result.tx_count),
    }
}

fn stdout_is_tty() -> bool {
    atty::is(atty::Stream::Stdout)
}

/// Load from the ../test directory, only for testing
#[allow(dead_code)]
fn load_test(name: &str) -> String {
    //println!("{}", name);
    std::fs::read_to_string(std::path::Path::new(name)).unwrap()
}

#[test]
fn test_generate() {
    use crate::storage_structure::relational::build_relational_ast;
    use crate::storage_structure::typing;

    use std::fs::File;
    use std::io::BufReader;
    use std::path::Path;
    let json = json::parse(&load_test(
        "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.script",
    ))
    .unwrap();
    let storage_definition = &json["code"][1]["args"][0];
    let type_ast =
        typing::storage_ast_from_json(&storage_definition.clone()).unwrap();
    println!("{:#?}", type_ast);

    use crate::relational::Context;
    let context = Context::init();

    use crate::relational::Indexes;
    let rel_ast =
        build_relational_ast(&context.clone(), &type_ast, &mut Indexes::new())
            .unwrap();
    println!("{:#?}", rel_ast);
    let generator = crate::postgresql_generator::PostgresqlGenerator::new();
    let mut builder = crate::sql::table_builder::TableBuilder::new();
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
    println!("{}", serde_json::to_string(&tables).unwrap());
    println!(
        "ENDOFJSON
    "
    );

    let p = Path::new(filename);
    let file = File::open(p).unwrap();
    let reader = BufReader::new(file);
    let v: Vec<crate::sql::table::Table> =
        serde_json::from_reader(reader).unwrap();
    assert_eq!(v.len(), tables.len());
    //test doesn't verify view exist
    for i in 0..v.len() {
        assert_eq!(v[i], tables[i]);
    }
}

#[test]
fn test_block() {
    // this tests the generated table structures against known good ones.
    // if it fails for a good reason, the output can be used to repopulate the
    // test files. To do this, execute script/generate_test_output.bash
    use crate::octez::block::Block;
    use crate::sql::insert;
    use crate::sql::insert::Insert;
    use crate::sql::table_builder::{TableBuilder, TableMap};
    use crate::storage_structure::relational::{build_relational_ast, Indexes};
    use crate::storage_structure::typing;
    use json::JsonValue;
    use ron::ser::{to_string_pretty, PrettyConfig};

    env_logger::init();

    fn get_rel_ast_from_script_json(
        json: &JsonValue,
        indexes: &mut Indexes,
    ) -> Result<RelationalAST> {
        let storage_definition = json["code"][1]["args"][0].clone();
        debug!("{}", storage_definition.to_string());
        let type_ast = typing::storage_ast_from_json(&storage_definition)?;
        let rel_ast = build_relational_ast(
            &crate::relational::Context::init(),
            &type_ast,
            indexes,
        )
        .unwrap();
        Ok(rel_ast)
    }

    #[derive(Debug)]
    struct Contract<'a> {
        id: &'a str,
        levels: Vec<u32>,
    }

    let contracts: [Contract; 4] = [
        Contract {
            id: "KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq",
            levels: vec![
                132343, 123318, 123327, 123339, 128201, 132091, 132201, 132211,
                132219, 132222, 132240, 132242, 132259, 132262, 132278, 132282,
                132285, 132298, 132300, 132367, 132383, 132384, 132388, 132390,
                135501, 138208, 149127,
            ],
        },
        Contract {
            id: "KT1McJxUCT8qAybMfS6n5kjaESsi7cFbfck8",
            levels: vec![
                228459, 228460, 228461, 228462, 228463, 228464, 228465, 228466,
                228467, 228468, 228490, 228505, 228506, 228507, 228508, 228509,
                228510, 228511, 228512, 228516, 228521, 228522, 228523, 228524,
                228525, 228526, 228527,
            ],
        },
        Contract {
            id: "KT1LYbgNsG2GYMfChaVCXunjECqY59UJRWBf",
            levels: vec![
                147806, 147807, 147808, 147809, 147810, 147811, 147812, 147813,
                147814, 147815, 147816,
            ],
        },
        Contract {
            // Hic et Nunc hDAO contract (has "set" type in storage)
            id: "KT1QxLqukyfohPV5kPkw97Rs6cw1DDDvYgbB",
            levels: vec![1443112],
        },
    ];

    fn sort_inserts(tables: &TableMap, inserts: &mut Vec<Insert>) {
        inserts.sort_by_key(|insert| {
            let mut res: Vec<insert::Value> = tables[&insert.table_name]
                .indices
                .iter()
                .map(|idx| {
                    insert
                        .get_column(idx)
                        .map_or(insert::Value::Null, |col| col.value.clone())
                })
                .collect();
            res.insert(0, insert::Value::String(insert.table_name.clone()));
            res
        });
    }

    let mut results: Vec<(&str, u32, Vec<Insert>)> = vec![];
    let mut expected: Vec<(&str, u32, Vec<Insert>)> = vec![];
    for contract in &contracts {
        let mut storage_processor = StorageProcessor::new(1);

        // verify that the test case is sane
        let mut unique_levels = contract.levels.clone();
        unique_levels.sort();
        unique_levels.dedup();
        assert_eq!(contract.levels.len(), unique_levels.len());

        let script_json =
            json::parse(&load_test(&format!("test/{}.script", contract.id)))
                .unwrap();
        let rel_ast =
            get_rel_ast_from_script_json(&script_json, &mut Indexes::new())
                .unwrap();

        // having the table layout is useful for sorting the test results and
        // expected results in deterministic order (we'll use the table's index)
        let mut builder = TableBuilder::new();
        builder.populate(&rel_ast);
        let tables = &builder.tables;

        for level in &contract.levels {
            println!("contract={}, level={}", contract.id, level);

            let block: Block = serde_json::from_str(&load_test(&format!(
                "test/{}.level-{}.json",
                contract.id, level
            )))
            .unwrap();

            let (inserts, _) = storage_processor
                .process_block(&block, &rel_ast, contract.id)
                .unwrap();

            let filename =
                format!("test/{}-{}-inserts.json", contract.id, level);
            println!("cat > {} <<ENDOFJSON", filename);
            println!(
                "{}",
                to_string_pretty(&inserts, PrettyConfig::new()).unwrap()
            );
            println!(
                "ENDOFJSON
    "
            );

            let mut result: Vec<Insert> = inserts.values().cloned().collect();
            sort_inserts(tables, &mut result);
            results.push((contract.id, *level, result));

            use std::path::Path;
            let p = Path::new(&filename);

            use std::fs::File;
            if let Ok(file) = File::open(p) {
                use std::io::BufReader;
                let reader = BufReader::new(file);
                println!("filename: {}", filename);
                let v: Inserts = ron::de::from_reader(reader).unwrap();

                let mut expected_result: Vec<Insert> =
                    v.values().cloned().collect();
                sort_inserts(tables, &mut expected_result);

                expected.push((contract.id, *level, expected_result));
            }
        }
    }
    assert_eq!(expected, results);
}

#[test]
fn test_get_origination_operations_from_block() {
    use crate::octez::block::Block;
    let test_file =
        "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.level-132091.json";
    let contract_id = "KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq";
    let block: Block = serde_json::from_str(&load_test(test_file)).unwrap();
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
            serde_json::from_str(&load_test(&filename)).unwrap();

        assert!(!level_block.has_contract_origination(&contract_id));
    }
}

#[test]
fn test_storage() {}
