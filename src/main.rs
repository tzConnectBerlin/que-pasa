#![feature(format_args_capture)]
#![feature(map_try_insert)]
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
pub mod highlevel;
pub mod octez;
pub mod sql;
pub mod storage_structure;
pub mod storage_value;

use anyhow::Context;
use config::CONFIG;
use env_logger::Env;
use octez::bcd;
use octez::node;
use sql::db::DBClient;
use sql::postgresql_generator;
use sql::table_builder;
use std::panic;
use std::process;
use std::thread;
use storage_structure::relational;
use storage_structure::typing;

fn main() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        process::exit(1);
    }));

    dotenv::dotenv().ok();
    let env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let contract_id = &CONFIG.contract_id;
    let node_cli =
        &node::NodeClient::new(CONFIG.node_url.clone(), "main".to_string());

    // init by grabbing the contract data.
    info!(
        "getting the storage definition for contract={}..",
        contract_id.name
    );
    let storage_def = &node_cli
        .get_contract_storage_definition(&contract_id.address, None)
        .unwrap();
    let type_ast = typing::storage_ast_from_json(storage_def)
        .with_context(|| {
            "failed to derive a storage type from the storage definition"
        })
        .unwrap();
    info!("storage definition retrieved, and type derived");

    // Build the internal representation from the storage defition
    let ctx = relational::Context::init();
    let mut indexes = relational::Indexes::new();
    let rel_ast =
        &relational::build_relational_ast(&ctx, &type_ast, &mut indexes)
            .with_context(|| {
                "failed to build a relational AST from the storage type"
            })
            .unwrap();

    // Generate the SQL schema for this contract
    let mut builder = table_builder::TableBuilder::new();
    builder.populate(rel_ast);

    // If generate-sql command is given, just output SQL and quit.
    if CONFIG.generate_sql {
        let generator = PostgresqlGenerator::new(contract_id);
        println!("BEGIN;\n");
        println!("{}", generator.create_common_tables());
        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);
        for (_name, table) in sorted_tables {
            print!(
                "{}",
                generator
                    .create_table_definition(table)
                    .unwrap()
            );
            println!();
            print!(
                "{}",
                generator
                    .create_view_definition(table)
                    .unwrap()
            );
            println!();
        }
        println!("\nCOMMIT;");
        return;
    }

    let mut dbcli = DBClient::connect(
        &CONFIG.database_url,
        CONFIG.ssl,
        CONFIG.ca_cert.clone(),
    )
    .with_context(|| "failed to connect to the db")
    .unwrap();

    if CONFIG.init {
        println!(
            "Initialising--all data in DB will be destroyed. \
            Interrupt within 5 seconds to abort"
        );
        thread::sleep(std::time::Duration::from_millis(5000));
        dbcli
            .delete_everything()
            .with_context(|| "failed to delete the db's content")
            .unwrap();
    }

    let mut executor = highlevel::Executor::new(node_cli.clone(), dbcli);
    executor.add_contract(contract_id, rel_ast);

    let num_getters = CONFIG.workers_cap;
    if CONFIG.init {
        match CONFIG.bcd_url.clone() {
            Some(bcd_url) => {
                let bcd_cli = bcd::BCDClient::new(
                    bcd_url,
                    CONFIG.network.clone(),
                    contract_id.address.clone(),
                );

                executor
                    .exec_parallel(num_getters, move |height_chan| {
                        bcd_cli
                            .populate_levels_chan(height_chan)
                            .unwrap()
                    })
                    .unwrap()
            }
            None => {
                executor
                    .exec_levels(num_getters, CONFIG.levels.clone())
                    .unwrap();
            }
        };

        executor
            .fill_in_levels(contract_id)
            .unwrap();
        return;
    }

    if !CONFIG.levels.is_empty() {
        executor
            .exec_levels(num_getters, CONFIG.levels.clone())
            .unwrap();
        return;
    }

    // No args so we will first load missing levels
    executor
        .exec_missing_levels(num_getters)
        .unwrap();

    // At last, normal operation.
    executor.exec_continuous().unwrap();
}
