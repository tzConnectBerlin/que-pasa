use json::JsonValue;

use crate::michelson::StorageParser;
use crate::node;
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
    let mut generator = crate::postgresql_generator::PostgresqlGenerator::new();
    let mut builder = table_builder::TableBuilder::new();
    builder.populate(&node);
    let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
    sorted_tables.sort_by_key(|a| a.0);
    for (_name, table) in sorted_tables {
        print!("{}", generator.create_table_definition(table));
        println!();
    }
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
        let contract_id = "KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq";
        let level_json = json::parse(&load_test(&format!(
            "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.level-{}.json",
            level
        )))
        .unwrap();

        let operations = StorageParser::get_operations_from_block_json(
            "KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq",
            &level_json,
        )
        .unwrap();

        for operation in &operations {
            debug!("level {}, {}", level, operation.to_string());
            let storage_json = StorageParser::get_storage_from_operation(&operation).unwrap();

            debug!("storage_json: {:?}", storage_json);

            let mut storage_parser = StorageParser::new();

            let preparsed_storage = storage_parser.preparse_storage(&storage_json);
            let parsed_storage = storage_parser.parse_storage(&preparsed_storage);
            storage_parser.read_storage(&parsed_storage, &node);

            for operation in &operations {
                let big_map_ops =
                    StorageParser::get_big_map_operations_from_operations(operation).unwrap();
                for big_map_op in big_map_ops {
                    storage_parser.process_big_map(&big_map_op);
                }
            }
        }
        let inserts = crate::table::insert::get_inserts();
        let mut generator = crate::postgresql_generator::PostgresqlGenerator::new();
        let keys: Vec<_> = inserts.keys().collect();
        for (key, value) in &inserts {
            println!(
                "{}",
                generator.build_insert(inserts.get(&key).unwrap(), level)
            );
        }
        println!("");
        crate::table::insert::clear_inserts();
    }
}

#[test]
fn test_storage() {}
