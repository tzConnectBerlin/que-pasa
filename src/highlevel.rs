use json::JsonValue;

use crate::error::Res;
use crate::michelson::StorageParser;
use crate::node::{Context, Node};
use crate::postgresql_generator;
use crate::postgresql_generator::PostgresqlGenerator;
use crate::storage;
use crate::table_builder;

use std::error::Error;

pub fn init() -> Res<()> {
    crate::michelson::set_id(
        postgresql_generator::get_max_id(&mut postgresql_generator::connect()?)? as u32,
    );
    Ok(())
}

pub fn get_node_from_script_json(json: &JsonValue) -> Res<Node> {
    let storage_definition = json["code"][1]["args"][0].clone();
    debug!("{}", storage_definition.to_string());
    let ast = storage::storage_from_json(storage_definition);
    let node = Node::build(Context::init(), ast);
    Ok(node)
}

pub fn get_tables_from_node(node: &Node) -> Result<table_builder::TableMap, Box<dyn Error>> {
    let mut builder = table_builder::TableBuilder::new();
    builder.populate(&node);
    Ok(builder.tables)
}

pub fn save_level(node: &Node, contract_id: &str, level: u32) -> Res<()> {
    let mut storage_parser = StorageParser::new();
    let json = storage_parser
        .get_storage(&contract_id.to_string(), level)
        .unwrap();
    let v = storage_parser.preparse_storage(&json);
    let result = storage_parser.parse_storage(&v)?;
    debug!("storage: {:#?}", result);
    let result = storage_parser.read_storage(&result, &node)?;
    debug!("{:#?}", result);

    let operations = StorageParser::get_operations_from_node(contract_id, Some(level))?;
    for operation in operations {
        let big_map_ops = StorageParser::get_big_map_operations_from_operations(&operation)?;
        for big_map_op in big_map_ops {
            storage_parser.process_big_map(&big_map_op)?;
        }
    }
    let inserts = crate::table::insert::get_inserts().clone();
    let mut keys = inserts
        .keys()
        .collect::<Vec<&crate::table::insert::InsertKey>>();
    keys.sort_by_key(|a| a.id);
    debug!("keys: {:?}", keys);
    let mut generator = PostgresqlGenerator::new();
    let mut connection = postgresql_generator::connect()?;
    let mut transaction = postgresql_generator::transaction(&mut connection)?;
    postgresql_generator::delete_level(&mut transaction, &StorageParser::level(level)?)?;
    postgresql_generator::save_level(&mut transaction, &StorageParser::level(level)?)?;
    for key in keys.iter() {
        postgresql_generator::exec(
            &mut transaction,
            &generator.build_insert(
                inserts
                    .get(key)
                    .ok_or(crate::error::Error::boxed("No insert for key"))?,
                level,
            ),
        )
        .unwrap();
    }
    println!("");
    postgresql_generator::set_max_id(&mut transaction, crate::michelson::get_id() as i32)?;
    transaction.commit().unwrap();
    crate::table::insert::clear_inserts();
    Ok(())
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
    let node = Node::build(Context::init(), ast);
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
            let parsed_storage = storage_parser.parse_storage(&preparsed_storage).unwrap();
            storage_parser.read_storage(&parsed_storage, &node).unwrap();

            for operation in &operations {
                let big_map_ops =
                    StorageParser::get_big_map_operations_from_operations(operation).unwrap();
                for big_map_op in big_map_ops {
                    storage_parser.process_big_map(&big_map_op).unwrap();
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
