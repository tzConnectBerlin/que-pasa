use crate::error::Res;
use crate::michelson::StorageParser;
use crate::postgresql_generator;
use crate::postgresql_generator::PostgresqlGenerator;
use crate::relational::RelationalAST;
#[cfg(test)]
use pretty_assertions::assert_eq;

pub(crate) fn get_origination(
    _contract_id: &str,
    connection: &mut postgres::Client,
) -> Res<Option<u32>> {
    postgresql_generator::get_origination(connection)
}

pub struct SaveLevelResult {
    pub is_origination: bool,
    pub tx_count: u32,
}

pub(crate) fn get_storage_parser(
    _contract_id: &str,
    connection: &mut postgres::Client,
) -> Res<StorageParser> {
    let id = crate::postgresql_generator::get_max_id(connection)? as u32;
    Ok(StorageParser::new(id))
}

fn load_from_block(
    rel_ast: &RelationalAST,
    block: crate::block::Block,
    contract_id: &str,
    storage_parser: &mut crate::michelson::StorageParser,
) -> Res<()> {
    let mut storages: Vec<(crate::michelson::TxContext, serde_json::Value)> = vec![];
    let mut big_map_diffs: Vec<(crate::michelson::TxContext, crate::block::BigMapDiff)> = vec![];
    let operations = block.operations();
    debug!("operations: {} {:#?}", operations.len(), operations);
    storage_parser.clear_inserts();

    let mut operation_group_number = 0u32;
    for operation_group in operations {
        operation_group_number += 1;
        let mut operation_number = 0u32;
        for operation in operation_group {
            operation_number += 1;
            storages.extend(storage_parser.get_storage_from_operation(
                block.header.level,
                operation_group_number,
                operation_number,
                &operation,
                contract_id,
            )?);
            big_map_diffs.extend(storage_parser.get_big_map_diffs_from_operation(
                block.header.level,
                operation_group_number,
                operation_number,
                &operation,
            )?);
        }
    }

    for storage in storages {
        debug!(
            "storage is

{:?}",
            storage
        );
        let tx_context = storage.0;
        let store = storage.1;
        let storage_json = serde_json::to_string(&store)?;
        debug!("storage_json: {}", storage_json);
        let parsed_storage = storage_parser.parse(storage_json)?;

        debug!("parsed_storage: {:?}", parsed_storage);
        storage_parser.read_storage(&parsed_storage, rel_ast, &tx_context)?;
    }

    for big_map_diff in big_map_diffs {
        let tx_content = big_map_diff.0;
        let diff = big_map_diff.1;
        storage_parser.process_big_map_diff(&diff, &tx_content)?;
    }
    Ok(())
}

pub(crate) fn load_and_store_level(
    rel_ast: &RelationalAST,
    contract_id: &str,
    level: u32,
    storage_parser: &mut crate::michelson::StorageParser,
    connection: &mut postgres::Client,
) -> Res<SaveLevelResult> {
    let generator = PostgresqlGenerator::new();
    let mut transaction = postgresql_generator::transaction(connection)?;
    let (_json, block) = StorageParser::level_json(level)?;

    if StorageParser::block_has_contract_origination(&block, contract_id)? {
        debug!("Setting origination to true");
        postgresql_generator::delete_level(&mut transaction, &StorageParser::level(level)?)?;
        postgresql_generator::save_level(&mut transaction, &StorageParser::level(level)?)?;
        postgresql_generator::set_origination(&mut transaction, level)?;
        transaction.commit()?;
        return Ok(SaveLevelResult {
            is_origination: true,
            tx_count: 0,
        });
    }

    if !StorageParser::block_has_tx_for_us(&block, contract_id)? {
        postgresql_generator::delete_level(&mut transaction, &StorageParser::level(level)?)?;
        postgresql_generator::save_level(&mut transaction, &StorageParser::level(level)?)?;
        transaction.commit()?; // TODO: think about this
        return Ok(SaveLevelResult {
            is_origination: false,
            tx_count: 0,
        });
    }

    load_from_block(rel_ast, block, contract_id, storage_parser)?;

    postgresql_generator::save_tx_contexts(&mut transaction, &storage_parser.tx_contexts)?;

    let inserts = storage_parser.get_inserts();
    let mut keys = inserts
        .keys()
        .collect::<Vec<&crate::table::insert::InsertKey>>();
    keys.sort_by_key(|a| a.id);
    debug!("keys: {:?}", keys);
    postgresql_generator::delete_level(&mut transaction, &StorageParser::level(level)?)?;
    postgresql_generator::save_level(&mut transaction, &StorageParser::level(level)?)?;
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
        storage_parser.id_generator.get_id() as i32,
    )?;
    transaction.commit()?;
    Ok(SaveLevelResult {
        is_origination: false,
        tx_count: keys.len() as u32,
    })
}

/// Load from the ../test directory, only for testing
#[allow(dead_code)]
fn load_test(name: &str) -> String {
    //println!("{}", name);
    std::fs::read_to_string(std::path::Path::new(name)).unwrap()
}

#[test]
fn test_generate() {
    use crate::relational::build_relational_ast;
    use ron::ser::{to_string_pretty, PrettyConfig};

    use std::fs::File;
    use std::io::BufReader;
    use std::path::Path;
    let json = json::parse(&load_test(
        "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.script",
    ))
    .unwrap();
    let storage_definition = &json["code"][1]["args"][0];
    let type_ast = crate::storage::storage_ast_from_json(&storage_definition.clone()).unwrap();
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
    println!("{}", serde_json::to_string(&tables).unwrap());

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
    use crate::postgresql_generator::PostgresqlGenerator;
    use crate::relational::{build_relational_ast, Indexes};
    use crate::table_builder::{TableBuilder, TableMap};
    use json::JsonValue;
    use ron::ser::{to_string_pretty, PrettyConfig};

    env_logger::init();

    fn get_rel_ast_from_script_json(json: &JsonValue, indexes: &mut Indexes) -> Res<RelationalAST> {
        let storage_definition = json["code"][1]["args"][0].clone();
        debug!("{}", storage_definition.to_string());
        let type_ast = crate::storage::storage_ast_from_json(&storage_definition)?;
        // println!("{:#?}", type_ast);
        // panic!("stop.");
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
                            .map_or(&crate::michelson::Value::None, |col| &col.value),
                    )
                })
                .collect::<Vec<String>>()
                .insert(0, x.table_name.clone())
        });
    }

    let mut results: Vec<(&str, u32, Vec<crate::table::insert::Insert>)> = vec![];
    let mut expected: Vec<(&str, u32, Vec<crate::table::insert::Insert>)> = vec![];
    for contract in &contracts {
        let mut storage_parser = StorageParser::new(1);

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

            let block: crate::block::Block = serde_json::from_str(&load_test(&format!(
                "test/{}.level-{}.json",
                contract.id, level
            )))
            .unwrap();

            load_from_block(&rel_ast, block, contract.id, &mut storage_parser).unwrap();
            let inserts = storage_parser.get_inserts();

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
    let test_file = "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.level-132091.json";
    let contract_id = "KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq";
    let block: crate::block::Block = serde_json::from_str(&load_test(test_file)).unwrap();
    assert!(StorageParser::block_has_contract_origination(&block, &contract_id).unwrap());

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
        let level_block: crate::block::Block = serde_json::from_str(&load_test(&filename)).unwrap();

        assert!(
            !StorageParser::block_has_contract_origination(&level_block, &contract_id).unwrap()
        );
    }
}

#[test]
fn test_storage() {}
