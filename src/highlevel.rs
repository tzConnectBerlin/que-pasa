use json::JsonValue;

use crate::michelson;
use crate::node;
use crate::postgresql_generator;
use crate::storage;
use crate::table_builder;

use std::error::Error;

pub fn get_node_from_script_json(json: &JsonValue) -> Result<node::Node, Box<dyn Error>> {
    let storage_definition = json["code"][1]["args"][0].clone();
    debug!("{}", storage_definition.to_string());
    let ast = storage::storage_from_json(storage_definition);
    let node = node::Node::build(node::Context::init(), ast);
    Ok(node)
}

pub fn get_tables_from_node(node: &node::Node) -> Result<table_builder::TableMap, Box<dyn Error>> {
    let mut builder = table_builder::TableBuilder::new();
    builder.populate(&node);
    Ok(builder.tables)
}

/// Load from the ../test directory, only for testing
#[allow(dead_code)]
fn load_test(name: &str) -> String {
    std::fs::read_to_string(std::path::Path::new(name)).unwrap()
}
pub fn get_operations_from_node(
    contract_id: &str,
    level: Option<u32>,
) -> Result<Vec<JsonValue>, Box<dyn Error>> {
    let level = match level {
        Some(x) => format!("{}", x),
        None => "head".to_string(),
    };
    let url = format!(
        "https://testnet-tezos.giganode.io/chains/main/blocks/{}",
        level
    );
    let json = michelson::StorageParser::load(&url)?;
    get_operations_from_block_json(contract_id, &json)
}

pub fn get_operations_from_block_json(
    contract_id: &str,
    json: &JsonValue,
) -> Result<Vec<JsonValue>, Box<dyn Error>> {
    if let JsonValue::Array(operations) = &json["operations"][3] {
        let mut result = vec![];
        for operation in operations {
            if let JsonValue::String(id) = &operation["contents"][0]["destination"] {
                if id == contract_id {
                    result.push(operation.clone());
                } else {
                    println!("{} Didn't match!", id);
                }
            }
        }
        Ok(result)
    } else {
        let err: Box<dyn Error> = String::from("No operations section found in block").into();
        Err(err)
    }
}

#[test]
fn test_generate() {
    let json = json::parse(&load_test(
        "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.script",
    ))
    .unwrap();
    let storage_definition = &json["code"][1]["args"][0];
    let ast = crate::storage::storage_from_json(storage_definition.clone());
    let node = crate::node::Node::build(crate::node::Context::init(), ast);
    let _tables = get_tables_from_node(&node);
}

#[test]
fn test_block() {
    let script_json = json::parse(&load_test(
        "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.script",
    ))
    .unwrap();
    let node = get_node_from_script_json(&script_json).unwrap();
    for level in vec![
        132343, 123318, 123327, 123339, 128201, 132091, 132201, 132211, 132219, 132222, 132240,
        132242, 132259, 132262, 132278, 132282, 132285, 132298, 132300, 132343, 132367, 132383,
        132384, 132388, 132390, 135501, 138208, 149127,
    ] {
        let level_json = json::parse(&load_test(&format!(
            "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.level-{}.json",
            level
        )))
        .unwrap();
        let operations =
            get_operations_from_block_json("KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq", &level_json)
                .unwrap();

        println!("Operations count: {}", operations.len());

        for operation in &operations {
            println!("level {}, {}", level, operation.to_string());
            let storage_json =
                operation["contents"][0]["metadata"]["operation_result"]["storage"].clone();

            println!("storage_json: {:?}", storage_json);

            let mut storage_parser = michelson::StorageParser::new();

            let preparsed_storage = storage_parser.preparse_storage(&storage_json);
            let parsed_storage = storage_parser.parse_storage(&preparsed_storage);
            storage_parser.update(&parsed_storage, &node);

            let big_map_diff_json =
                operation["contents"][0]["metadata"]["operation_result"]["big_map_diff"].clone();
            println!("big map diff: {}", big_map_diff_json.to_string());
            if let JsonValue::Array(a) = big_map_diff_json {
                for big_map_diff in a {
                    storage_parser.process_big_map(&big_map_diff);
                }
            }
            let inserts = crate::table::insert::get_inserts();
            let mut generator = postgresql_generator::PostgresqlGenerator::new();
            for (key, value) in &inserts {
                println!("{}", generator.build_insert(inserts.get(&key).unwrap()));
            }
        }
    }
}

#[test]
fn test_storage() {}
