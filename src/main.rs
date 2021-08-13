#![feature(format_args_capture)]
use postgresql_generator::PostgresqlGenerator;

extern crate atty;
extern crate backtrace;
extern crate bs58;
extern crate chrono;
extern crate clap;
extern crate curl;
extern crate dotenv;
extern crate hex;
extern crate indicatif;
extern crate itertools;
#[macro_use]
extern crate json;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate native_tls;
extern crate postgres;
extern crate postgres_native_tls;
extern crate regex;
extern crate ron;
#[macro_use]
extern crate serde;
extern crate serde_json;
extern crate spinners;
extern crate termion;

pub mod config;
pub mod error;
pub mod highlevel;
pub mod octez;
pub mod sql;
pub mod storage_structure;
pub mod storage_value;

use config::CONFIG;
use octez::node;
use sql::postgresql_generator;
use sql::table;
use sql::table_builder;
use storage_structure::relational;
use storage_structure::typing;

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    let contract_id = &CONFIG.contract_id;
    let node_cli = &node::NodeClient::new(CONFIG.node_url.clone(), "main".to_string());

    // init by grabbing the contract data.
    let json = node_cli.get_contract_script(contract_id, None).unwrap();
    let storage_definition = &json["code"][1]["args"][0];
    let type_ast = typing::storage_ast_from_json(storage_definition).unwrap();

    // Build the internal representation from the storage defition
    let ctx = relational::Context::init();
    let mut indexes = relational::Indexes::new();
    let rel_ast = &relational::build_relational_ast(&ctx, &type_ast, &mut indexes);

    // Generate the SQL schema for this contract
    let mut builder = table_builder::TableBuilder::new();
    builder.populate(rel_ast);

    // If generate-sql command is given, just output SQL and quit.
    if CONFIG.generate_sql {
        let generator = PostgresqlGenerator::new();
        println!("{}", generator.create_common_tables());
        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);
        for (_name, table) in sorted_tables {
            print!("{}", generator.create_table_definition(table).unwrap());
            println!();
            print!("{}", generator.create_view_definition(table).unwrap());
            println!();
        }
        return;
    }

    let mut dbconn =
        postgresql_generator::connect(&CONFIG.database_url, CONFIG.ssl, CONFIG.ca_cert.clone())
            .unwrap();

    if CONFIG.init {
        p!("Initialising--all data in DB will be destroyed. Interrupt within 5 seconds to abort");
        std::thread::sleep(std::time::Duration::from_millis(5000));
        postgresql_generator::delete_everything(&mut dbconn).unwrap();
    }

    let mut storage_processor =
        crate::highlevel::get_storage_processor(contract_id, &mut dbconn).unwrap();

    let head = node_cli.head().unwrap();
    let mut first = head._level;

    if CONFIG.levels.len() > 0 {
        let levels_res = highlevel::execute_for_levels(
            node_cli,
            rel_ast,
            contract_id,
            &CONFIG.levels,
            &mut storage_processor,
            &mut dbconn,
        )
        .unwrap();
        let max_level_processed = levels_res.iter().max_by_key(|res| res.level).unwrap().level;
        if max_level_processed > first {
            first = max_level_processed;
        }
    }

    if CONFIG.init {
        postgresql_generator::fill_in_levels(&mut dbconn, first, head._level).unwrap();
        return;
    }

    // No args so we will first load missing levels
    highlevel::execute_missing_levels(
        node_cli,
        rel_ast,
        contract_id,
        &mut storage_processor,
        &mut dbconn,
    )
    .unwrap();

    // At last, normal operation.
    crate::highlevel::execute_continuous(
        node_cli,
        rel_ast,
        contract_id,
        &mut storage_processor,
        &mut dbconn,
    )
    .unwrap();
}
