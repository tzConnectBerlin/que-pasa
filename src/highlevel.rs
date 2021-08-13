use crate::error::Res;
use crate::octez::node::NodeClient;
use crate::p;
use crate::relational::RelationalAST;
use crate::sql::postgresql_generator;
use crate::sql::postgresql_generator::PostgresqlGenerator;
use crate::storage_value::processor::StorageProcessor;
use std::cmp::Ordering;

#[cfg(test)]
use pretty_assertions::assert_eq;

pub(crate) fn get_origination(
    _contract_id: &str,
    dbconn: &mut postgres::Client,
) -> Res<Option<u32>> {
    postgresql_generator::get_origination(dbconn)
}

pub struct SaveLevelResult {
    pub level: u32,
    pub is_origination: bool,
    pub tx_count: u32,
}

pub(crate) fn get_storage_processor(
    _contract_id: &str,
    dbconn: &mut postgres::Client,
) -> Res<StorageProcessor> {
    let id = crate::postgresql_generator::get_max_id(dbconn)? as u32;
    Ok(StorageProcessor::new(id))
}

pub(crate) fn execute_continuous(
    node_cli: &NodeClient,
    rel_ast: &RelationalAST,
    contract_id: &str,
    storage_processor: &mut StorageProcessor,
    dbconn: &mut postgres::Client,
) -> Res<SaveLevelResult> {
    let is_tty = stdout_is_tty();
    let print_status = |result: &SaveLevelResult| {
        p!("{}", level_text(result));
    };

    loop {
        let _spinner;

        if is_tty {
            _spinner = spinners::Spinner::new(spinners::Spinners::Line, "".into());
            //print!("Waiting for first block");
        }

        let chain_head = node_cli.head().unwrap();
        let db_head = postgresql_generator::get_head(dbconn).unwrap().unwrap();
        debug!("db: {} chain: {}", db_head._level, chain_head._level);
        match chain_head._level.cmp(&db_head._level) {
            Ordering::Greater => {
                for level in (db_head._level + 1)..=chain_head._level {
                    let result = execute_for_level(
                        node_cli,
                        rel_ast,
                        contract_id,
                        level,
                        storage_processor,
                        dbconn,
                    )?;
                    print_status(&result);
                }
                continue;
            }
            Ordering::Less => {
                return Err("More levels in DB than chain, bailing!".into());
            }
            Ordering::Equal => {
                // they are equal, so we will just check that the hashes match.
                if db_head.hash == chain_head.hash {
                    // if they match, nothing to do.
                } else {
                    p!("");
                    p!(
                        "Hashes don't match: {:?} (db) <> {:?} (chain)",
                        db_head.hash,
                        chain_head.hash
                    );
                    let mut transaction = dbconn.transaction()?;
                    postgresql_generator::delete_level(&mut transaction, &db_head)?;
                    transaction.commit()?;
                }
                std::thread::sleep(std::time::Duration::from_millis(1500));
            }
        }
    }
}

pub(crate) fn execute_missing_levels(
    node_cli: &NodeClient,
    rel_ast: &RelationalAST,
    contract_id: &str,
    storage_processor: &mut StorageProcessor,
    dbconn: &mut postgres::Client,
) -> Res<()> {
    loop {
        let origination_level = get_origination(contract_id, dbconn).unwrap();

        let mut missing_levels: Vec<u32> = postgresql_generator::get_missing_levels(
            dbconn,
            origination_level,
            node_cli.head().unwrap()._level,
        )
        .unwrap();
        missing_levels.reverse();

        if missing_levels.is_empty() {
            // finally through them
            return Ok(());
        }

        while let Some(level) = missing_levels.pop() {
            let store_result = loop {
                match execute_for_level(
                    node_cli,
                    rel_ast,
                    contract_id,
                    level as u32,
                    storage_processor,
                    dbconn,
                ) {
                    Ok(x) => break x,
                    Err(e) => {
                        warn!("Error contacting node: {:?}", e);
                        std::thread::sleep(std::time::Duration::from_millis(1500));
                    }
                };
            };

            if store_result.is_origination {
                p!(
                    "Found new origination level {}",
                    get_origination(contract_id, dbconn).unwrap().unwrap()
                );
                break;
            }
            p!(
                " {} transactions for us, {} remaining",
                store_result.tx_count,
                missing_levels.len()
            );
        }
    }
}

pub(crate) fn execute_for_levels(
    node_cli: &NodeClient,
    rel_ast: &RelationalAST,
    contract_id: &str,
    levels: &[u32],
    storage_processor: &mut StorageProcessor,
    dbconn: &mut postgres::Client,
) -> Res<Vec<SaveLevelResult>> {
    let mut res: Vec<SaveLevelResult> = vec![];
    for level in levels {
        let level_res = execute_for_level(
            node_cli,
            rel_ast,
            contract_id,
            *level,
            storage_processor,
            dbconn,
        )?;
        p!("{}", level_text(&level_res));
        res.push(level_res);
    }
    Ok(res)
}

pub(crate) fn execute_for_level(
    node_cli: &NodeClient,
    rel_ast: &RelationalAST,
    contract_id: &str,
    level: u32,
    storage_processor: &mut StorageProcessor,
    dbconn: &mut postgres::Client,
) -> Res<SaveLevelResult> {
    let generator = PostgresqlGenerator::new();
    let mut transaction = postgresql_generator::transaction(dbconn)?;
    let (_json, block) = node_cli.level_json(level)?;

    if block.has_contract_origination(contract_id) {
        postgresql_generator::delete_level(&mut transaction, &node_cli.level(level)?)?;
        postgresql_generator::save_level(&mut transaction, &node_cli.level(level)?)?;
        postgresql_generator::set_origination(&mut transaction, level)?;
        transaction.commit()?;
        return Ok(SaveLevelResult {
            level,
            is_origination: true,
            tx_count: 0,
        });
    }

    if !block.is_contract_active(contract_id) {
        postgresql_generator::delete_level(&mut transaction, &node_cli.level(level)?)?;
        postgresql_generator::save_level(&mut transaction, &node_cli.level(level)?)?;
        transaction.commit()?; // TODO: think about this
        return Ok(SaveLevelResult {
            level,
            is_origination: false,
            tx_count: 0,
        });
    }

    let (inserts, tx_contexts) = storage_processor.process_block(block, rel_ast, contract_id)?;

    postgresql_generator::delete_level(&mut transaction, &node_cli.level(level)?)?;
    postgresql_generator::save_level(&mut transaction, &node_cli.level(level)?)?;

    postgresql_generator::save_tx_contexts(&mut transaction, &tx_contexts)?;
    let mut keys = inserts
        .keys()
        .collect::<Vec<&crate::table::insert::InsertKey>>();
    keys.sort_by_key(|a| a.id);
    for key in keys.iter() {
        postgresql_generator::exec(
            &mut transaction,
            &generator.build_insert(
                inserts
                    .get(key)
                    .ok_or_else(|| crate::error::Error::boxed("No insert for key"))?,
            ),
        )?;
    }
    postgresql_generator::set_max_id(
        &mut transaction,
        (storage_processor.get_id_value() + 1) as i32,
    )?;
    transaction.commit()?;
    Ok(SaveLevelResult {
        level,
        is_origination: false,
        tx_count: keys.len() as u32,
    })
}

fn level_text(result: &SaveLevelResult) -> String {
    format!(
        "level {} {} transactions for us, origination={}",
        result.level, result.tx_count, result.is_origination
    )
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
    let type_ast = typing::storage_ast_from_json(&storage_definition.clone()).unwrap();
    println!("{:#?}", type_ast);

    use crate::relational::Context;
    let context = Context::init();

    use crate::relational::Indexes;
    let rel_ast = build_relational_ast(&context.clone(), &type_ast, &mut Indexes::new());
    println!("{:#?}", rel_ast);
    let generator = crate::postgresql_generator::PostgresqlGenerator::new();
    let mut builder = crate::table_builder::TableBuilder::new();
    builder.populate(&rel_ast);
    let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
    sorted_tables.sort_by_key(|a| a.0);
    let mut tables: Vec<crate::table::Table> = vec![];
    for (_name, table) in sorted_tables {
        print!("{}", generator.create_table_definition(table).unwrap());
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
    let v: Vec<crate::table::Table> = serde_json::from_reader(reader).unwrap();
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
    use crate::sql::postgresql_generator::PostgresqlGenerator;
    use crate::sql::table_builder::{TableBuilder, TableMap};
    use crate::storage_structure::relational::{build_relational_ast, Indexes};
    use crate::storage_structure::typing;
    use json::JsonValue;
    use ron::ser::{to_string_pretty, PrettyConfig};

    env_logger::init();

    fn get_rel_ast_from_script_json(json: &JsonValue, indexes: &mut Indexes) -> Res<RelationalAST> {
        let storage_definition = json["code"][1]["args"][0].clone();
        debug!("{}", storage_definition.to_string());
        let type_ast = typing::storage_ast_from_json(&storage_definition)?;
        let rel_ast = build_relational_ast(&crate::relational::Context::init(), &type_ast, indexes);
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
                132343, 123318, 123327, 123339, 128201, 132091, 132201, 132211, 132219, 132222,
                132240, 132242, 132259, 132262, 132278, 132282, 132285, 132298, 132300, 132367,
                132383, 132384, 132388, 132390, 135501, 138208, 149127,
            ],
        },
        Contract {
            id: "KT1McJxUCT8qAybMfS6n5kjaESsi7cFbfck8",
            levels: vec![
                228459, 228460, 228461, 228462, 228463, 228464, 228465, 228466, 228467, 228468,
                228490, 228505, 228506, 228507, 228508, 228509, 228510, 228511, 228512, 228516,
                228521, 228522, 228523, 228524, 228525, 228526, 228527,
            ],
        },
        Contract {
            id: "KT1LYbgNsG2GYMfChaVCXunjECqY59UJRWBf",
            levels: vec![
                147806, 147807, 147808, 147809, 147810, 147811, 147812, 147813, 147814, 147815,
                147816,
            ],
        },
        Contract {
            // Hic et Nunc hDAO contract (has "set" type in storage)
            id: "KT1QxLqukyfohPV5kPkw97Rs6cw1DDDvYgbB",
            levels: vec![1443112],
        },
    ];

    fn sort_inserts(tables: &TableMap, inserts: &mut Vec<crate::table::insert::Insert>) {
        inserts.sort_by_key(|x| {
            tables[&x.table_name]
                .indices
                .iter()
                .map(|index| {
                    PostgresqlGenerator::sql_value(
                        x.get_column(index)
                            .map_or(&crate::storage_value::parser::Value::None, |col| &col.value),
                    )
                })
                .collect::<Vec<String>>()
                .insert(0, x.table_name.clone())
        });
    }

    let mut results: Vec<(&str, u32, Vec<crate::table::insert::Insert>)> = vec![];
    let mut expected: Vec<(&str, u32, Vec<crate::table::insert::Insert>)> = vec![];
    for contract in &contracts {
        let mut storage_processor = StorageProcessor::new(1);

        // verify that the test case is sane
        let mut unique_levels = contract.levels.clone();
        unique_levels.sort();
        unique_levels.dedup();
        assert_eq!(contract.levels.len(), unique_levels.len());

        let script_json = json::parse(&load_test(&format!("test/{}.script", contract.id))).unwrap();
        let rel_ast = get_rel_ast_from_script_json(&script_json, &mut Indexes::new()).unwrap();

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
                .process_block(block, &rel_ast, contract.id)
                .unwrap();

            let filename = format!("test/{}-{}-inserts.json", contract.id, level);
            println!("cat > {} <<ENDOFJSON", filename);
            println!(
                "{}",
                to_string_pretty(&inserts, PrettyConfig::new()).unwrap()
            );
            println!(
                "ENDOFJSON
    "
            );

            let mut result: Vec<crate::table::insert::Insert> = inserts.values().cloned().collect();
            sort_inserts(tables, &mut result);
            results.push((contract.id, *level, result));

            use std::path::Path;
            let p = Path::new(&filename);

            use std::fs::File;
            if let Ok(file) = File::open(p) {
                use std::io::BufReader;
                let reader = BufReader::new(file);
                println!("filename: {}", filename);
                let v: crate::table::insert::Inserts = ron::de::from_reader(reader).unwrap();

                let mut expected_result: Vec<crate::table::insert::Insert> =
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
    let test_file = "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.level-132091.json";
    let contract_id = "KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq";
    let block: Block = serde_json::from_str(&load_test(test_file)).unwrap();
    assert!(block.has_contract_origination(&contract_id));

    for level in vec![
        132343, 123318, 123327, 123339, 128201, 132201, 132211, 132219, 132222, 132240, 132242,
        132259, 132262, 132278, 132282, 132285, 132298, 132300, 132343, 132367, 132383, 132384,
        132388, 132390, 135501, 138208, 149127,
    ] {
        let filename = format!(
            "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.level-{}.json",
            level
        );
        println!("testing {}", filename);
        let level_block: Block = serde_json::from_str(&load_test(&filename)).unwrap();

        assert!(!level_block.has_contract_origination(&contract_id));
    }
}

#[test]
fn test_storage() {}
