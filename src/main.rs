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
pub mod highlevel;
pub mod octez;
pub mod sql;
pub mod storage_structure;
pub mod storage_value;

use anyhow::Context;
use config::CONFIG;
use env_logger::Env;
use octez::bcd;
use octez::block::{Block, LevelMeta};
use octez::block_producer::BlockProducer;
use octez::node;
use sql::postgresql_generator;
use sql::table;
use sql::table_builder;
use std::iter;
use std::thread;
use storage_structure::relational;
use storage_structure::typing;

fn main() {
    dotenv::dotenv().ok();
    let env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let contract_id = &CONFIG.contract_id;
    let node_cli =
        &node::NodeClient::new(CONFIG.node_url.clone(), "main".to_string());

    // init by grabbing the contract data.
    let json = node_cli
        .get_contract_script(contract_id, None)
        .unwrap();
    let storage_definition = &json["code"][1]["args"][0];
    let type_ast = typing::storage_ast_from_json(storage_definition)
        .with_context(|| {
            "failed to derive a storage type from the storage definition"
        })
        .unwrap();

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
        let generator = PostgresqlGenerator::new();
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
        return;
    }

    let mut dbconn = postgresql_generator::connect(
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
        postgresql_generator::delete_everything(&mut dbconn)
            .with_context(|| "failed to delete the db's content")
            .unwrap();
    }

    let mut storage_processor =
        crate::highlevel::get_storage_processor(contract_id, &mut dbconn)
            .with_context(|| {
                "could not initialize storage processor from the db state"
            })
            .unwrap();

    if !CONFIG.levels.is_empty() {
        highlevel::execute_for_levels(
            node_cli,
            rel_ast,
            contract_id,
            &CONFIG.levels,
            &mut storage_processor,
            &mut dbconn,
        )
        .unwrap();
    }

    if CONFIG.init {
        let bcd = CONFIG.bcd_url.clone().map(|bcd_url| {
            bcd::BCDClient::new(bcd_url, CONFIG.network.clone())
        });

        let cli_count = 1000;

        let (height_send, height_recv) = flume::bounded::<u32>(cli_count);
        let (block_send, block_recv) =
            flume::bounded::<Box<(LevelMeta, Block)>>(cli_count);

        let producers = iter::repeat(BlockProducer::new(node_cli));
        let mut producer_threads = vec![];
        for producer in producers.take(cli_count) {
            let in_ch = height_recv.clone();
            let out_ch = block_send.clone();
            producer_threads.push(thread::spawn(move || {
                producer.run(in_ch, out_ch).unwrap();
            }));
        }

        thread::spawn(move || {
            if let Some(bcd_cli) = bcd {
                let mut last_id = None;
                loop {
                    let (levels, new_last_id) = bcd_cli
                        .get_levels_with_contract(
                            contract_id.to_string(),
                            last_id,
                        )
                        .unwrap();
                    if levels.is_empty() {
                        break;
                    }
                    last_id = Some(new_last_id);

                    for level in levels {
                        height_send.send(level).unwrap();
                    }
                }
            }
        });

        let rel_ast_cl = rel_ast.clone();
        thread::spawn(move || {
            for b in block_recv {
                let (level, block) = *b;
                highlevel::execute_for_block(
                    &rel_ast_cl,
                    contract_id,
                    &level,
                    &block,
                    &mut storage_processor,
                    &mut dbconn,
                )
                .unwrap();
            }
        });

        for thread in producer_threads {
            thread.join();
        }

        // let first = CONFIG.levels.iter().min().unwrap();
        // let last = CONFIG.levels.iter().max().unwrap();
        // postgresql_generator::fill_in_levels(&mut dbconn, *first, *last)
        //     .with_context(|| {
        //         "failed to mark levels unrelated to the contract as empty in the db"
        //     })
        //     .unwrap();
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
