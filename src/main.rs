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

pub mod config;
pub mod contract_denylist;
pub mod debug;
pub mod highlevel;
pub mod octez;
pub mod sql;
pub mod storage_structure;
pub mod storage_update;
pub mod storage_value;

use anyhow::Context;
use config::CONFIG;
use env_logger::Env;
use octez::bcd;
use octez::node;
use sql::db::DBClient;
use std::collections::HashMap;
use std::panic;
use std::process;
use std::thread;

use config::ContractID;
use contract_denylist::is_contract_denylisted;
use octez::block::get_implicit_origination_level;
use storage_structure::relational;

fn main() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        // wait for a bit to give time to the root error's thread to print
        // its error
        thread::sleep(std::time::Duration::from_millis(500));
        process::exit(1);
    }));

    dotenv::dotenv().ok();
    let env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let config = CONFIG.as_ref().unwrap();

    let node_cli =
        &node::NodeClient::new(config.node_url.clone(), "main".to_string());

    let mut dbcli = DBClient::connect(
        &config.database_url,
        config.ssl,
        config.ca_cert.clone(),
    )
    .with_context(|| "failed to connect to the db")
    .unwrap();

    let setup_db = config.reinit || !dbcli.common_tables_exist().unwrap();
    if config.reinit {
        println!(
            "Re-initializing -- all data in DB related to ever set-up contracts, including those set-up in prior runs (!), will be destroyed. \
            Interrupt within 15 seconds to abort"
        );
        thread::sleep(std::time::Duration::from_millis(15000));
        dbcli
            .delete_everything(&mut node_cli.clone(), highlevel::get_rel_ast)
            .with_context(|| "failed to delete the db's content")
            .unwrap();
    }
    if setup_db {
        dbcli.create_common_tables().unwrap();
        info!("Common tables set up in db");
    }

    let mut executor = highlevel::Executor::new(
        node_cli.clone(),
        dbcli,
        &config.database_url,
        config.ssl,
        config.ca_cert.clone(),
    );
    let num_getters = config.workers_cap;
    if config.all_contracts {
        executor.index_all_contracts();
    } else {
        for contract_id in &config.contracts {
            executor
                .add_contract(contract_id)
                .unwrap();
        }
        let mut any_bootstrapped = false;
        loop {
            executor
                .add_dependency_contracts()
                .unwrap();

            let contracts = executor.get_config();
            assert_contracts_ok(&contracts);
            info!("running for contracts: {:#?}", contracts);

            if config.recreate_views {
                executor.recreate_views().unwrap();
            }

            let new_contracts = executor
                .create_contract_schemas()
                .unwrap();

            if new_contracts.is_empty() {
                break;
            }
            any_bootstrapped = true;

            info!(
                "initializing following contracts' historically: {:#?}",
                new_contracts
            );

            if let Some(bcd_url) = &config.bcd_url {
                let mut exclude_levels: Vec<u32> = vec![];
                for contract_id in &new_contracts {
                    info!("Initializing contract {}..", contract_id.name);
                    let bcd_cli = bcd::BCDClient::new(
                        bcd_url.clone(),
                        config.network.clone(),
                        contract_id.address.clone(),
                        &exclude_levels,
                    );

                    let processed_levels = executor
                        .exec_parallel(num_getters, move |height_chan| {
                            bcd_cli
                                .populate_levels_chan(height_chan)
                                .unwrap()
                        })
                        .unwrap();
                    exclude_levels.extend(processed_levels);

                    if let Some(l) =
                        get_implicit_origination_level(&contract_id.address)
                    {
                        executor.exec_level(l).unwrap();
                    }

                    executor
                        .fill_in_levels(contract_id)
                        .unwrap();

                    info!("contract {} initialized.", contract_id.name)
                }
            } else if !config.levels.is_empty() {
                executor
                    .exec_levels(num_getters, config.levels.clone())
                    .unwrap();
            } else {
                executor
                    .exec_missing_levels(num_getters)
                    .unwrap();
            }
        }
        if any_bootstrapped {
            executor.exec_dependents().unwrap();
            info!("all contracts historically bootstrapped. restart to begin normal continuous processing mode.");
            return;
        }
    }

    if !config.levels.is_empty() {
        executor
            .exec_levels(num_getters, config.levels.clone())
            .unwrap();
        executor.exec_dependents().unwrap();
        return;
    }

    // We will first load missing levels (if any)
    info!("processing missing levels");
    executor
        .exec_missing_levels(num_getters)
        .unwrap();

    // At last, normal operation.
    info!("processing blocks at the chain head");
    executor.exec_continuous().unwrap();
}

fn assert_contracts_ok(contracts: &[ContractID]) {
    if contracts.is_empty() {
        panic!("zero contracts to index..");
    }

    let mut names: HashMap<String, ()> = HashMap::new();
    for contract_id in contracts {
        if names.contains_key(&contract_id.name) {
            panic!("bad contract settings provided: name clash (multiple contracts assigned to name '{}'", contract_id.name);
        }
        if is_contract_denylisted(&contract_id.address) {
            panic!("bad contract settings provided: denylisted contract cannot be indexed ({})", contract_id.name);
        }
        names.insert(contract_id.name.clone(), ());
    }
}
