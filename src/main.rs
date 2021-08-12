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

use std::cmp::Ordering;

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

fn stdout_is_tty() -> bool {
    atty::is(atty::Stream::Stdout)
}

#[macro_export]
macro_rules! p {
    ( $( $a:expr) , + ) => {
        if stdout_is_tty() {
            println!( $( $a, )* );
        } else {
            info!( $( $a, )* );
        }
    };
}

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    let contract_id = &CONFIG.contract_id;
    let node_cli = &node::NodeClient::new(CONFIG.node_url.clone());

    // init by grabbing the contract data.
    let json = node_cli.get_contract_script(contract_id, None).unwrap();
    let storage_definition = &json["code"][1]["args"][0];

    let type_ast = typing::storage_ast_from_json(storage_definition).unwrap();

    // Build the internal representation from the storage defition
    let ctx = relational::Context::init();
    let mut indexes = relational::Indexes::new();

    let rel_ast = &relational::build_relational_ast(&ctx, &type_ast, &mut indexes);

    // Make a SQL-compatible representation
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

    let mut dbconn = postgresql_generator::connect(CONFIG.ssl, CONFIG.ca_cert.clone()).unwrap();

    if CONFIG.init {
        p!("Initialising--all data in DB will be destroyed. Interrupt within 5 seconds to abort");
        std::thread::sleep(std::time::Duration::from_millis(5000));
        postgresql_generator::delete_everything(&mut dbconn).unwrap();
    }

    let mut storage_processor =
        crate::highlevel::get_storage_processor(contract_id, &mut dbconn).unwrap();

    let head = node_cli.head().unwrap();
    let mut first = head._level;

    for level in &CONFIG.levels {
        let result = crate::highlevel::load_and_store_level(
            node_cli,
            rel_ast,
            contract_id,
            *level,
            &mut storage_processor,
            &mut dbconn,
        )
        .unwrap();
        p!("{}", level_text(*level, &result));
        if *level < first {
            first = *level;
        }
    }

    if CONFIG.init {
        postgresql_generator::fill_in_levels(&mut dbconn, first, head._level).unwrap();
        return;
    }

    // No args so we will first load missing levels

    loop {
        let origination_level = highlevel::get_origination(contract_id, &mut dbconn).unwrap();

        let mut missing_levels: Vec<u32> = postgresql_generator::get_missing_levels(
            &mut dbconn,
            origination_level,
            node_cli.head().unwrap()._level,
        )
        .unwrap();
        missing_levels.reverse();

        if missing_levels.is_empty() {
            // finally through them
            break;
        }

        while let Some(level) = missing_levels.pop() {
            let store_result = loop {
                match crate::highlevel::load_and_store_level(
                    node_cli,
                    rel_ast,
                    contract_id,
                    level as u32,
                    &mut storage_processor,
                    &mut dbconn,
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
                    highlevel::get_origination(contract_id, &mut dbconn)
                        .unwrap()
                        .unwrap()
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

    let is_tty = stdout_is_tty();

    let print_status = |level: u32, result: &crate::highlevel::SaveLevelResult| {
        p!("{}", level_text(level, result));
    };

    // At last, normal operation.
    loop {
        let _spinner;

        if is_tty {
            _spinner = spinners::Spinner::new(spinners::Spinners::Line, "".into());
            //print!("Waiting for first block");
        }

        let chain_head = node_cli.head().unwrap();
        let db_head = postgresql_generator::get_head(&mut dbconn)
            .unwrap()
            .unwrap();
        debug!("db: {} chain: {}", db_head._level, chain_head._level);
        match chain_head._level.cmp(&db_head._level) {
            Ordering::Greater => {
                for level in (db_head._level + 1)..=chain_head._level {
                    let result = highlevel::load_and_store_level(
                        node_cli,
                        rel_ast,
                        contract_id,
                        level,
                        &mut storage_processor,
                        &mut dbconn,
                    )
                    .unwrap();
                    print_status(level, &result);
                }
                continue;
            }
            Ordering::Less => {
                p!("More levels in DB than chain, bailing!");
                return;
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
                    let mut transaction = dbconn.transaction().unwrap();
                    postgresql_generator::delete_level(&mut transaction, &db_head).unwrap();
                    transaction.commit().unwrap();
                }
                std::thread::sleep(std::time::Duration::from_millis(1500));
            }
        }
    }
}

fn level_text(level: u32, result: &crate::highlevel::SaveLevelResult) -> String {
    format!(
        "level {} {} transactions for us, origination={}",
        level, result.tx_count, result.is_origination
    )
}
