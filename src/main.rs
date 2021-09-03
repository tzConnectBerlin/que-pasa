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
use std::panic;
use std::process;
use std::thread;
use storage_structure::relational;

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

    let node_cli =
        &node::NodeClient::new(CONFIG.node_url.clone(), "main".to_string());

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
            .delete_everything(&CONFIG.contracts)
            .with_context(|| "failed to delete the db's content")
            .unwrap();
        dbcli.create_common_tables().unwrap();
    }

    let mut executor = highlevel::Executor::new(node_cli.clone(), dbcli);
    for contract_id in &CONFIG.contracts {
        executor
            .add_contract(contract_id)
            .unwrap();
    }
    let new_contracts = executor
        .create_contract_schemas()
        .unwrap();

    let num_getters = CONFIG.workers_cap;
    if !CONFIG.levels.is_empty() {
        executor
            .exec_levels(num_getters, CONFIG.levels.clone())
            .unwrap();
        return;
    }

    if let Some(bcd_url) = &CONFIG.bcd_url {
        for contract_id in &new_contracts {
            info!("Initializing contract {}..", contract_id.name);
            let bcd_cli = bcd::BCDClient::new(
                bcd_url.clone(),
                CONFIG.network.clone(),
                contract_id.address.clone(),
            );

            executor
                .exec_parallel(num_getters, move |height_chan| {
                    bcd_cli
                        .populate_levels_chan(height_chan)
                        .unwrap()
                })
                .unwrap();

            executor
                .fill_in_levels(contract_id)
                .unwrap();

            info!("contract {} initialized.", contract_id.name)
        }
    }

    // No args so we will first load missing levels
    executor
        .exec_missing_levels(num_getters)
        .unwrap();

    // At last, normal operation.
    executor.exec_continuous().unwrap();
}
