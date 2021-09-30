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
use octez::node;
use sql::db::DBClient;
use std::collections::HashMap;
use std::panic;
use std::process;
use std::thread;

use config::ContractID;
use contract_denylist::is_contract_denylisted;
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

    let node_cli =
        &node::NodeClient::new(CONFIG.node_url.clone(), "main".to_string());

    let mut dbcli = DBClient::connect(
        &CONFIG.database_url,
        CONFIG.ssl,
        CONFIG.ca_cert.clone(),
    )
    .with_context(|| "failed to connect to the db")
    .unwrap();

    let setup_db = CONFIG.reinit || !dbcli.common_tables_exist().unwrap();
    if CONFIG.reinit {
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
        &CONFIG.database_url,
        CONFIG.ssl,
        CONFIG.ca_cert.clone(),
    );
    let num_getters = CONFIG.workers_cap;
    if CONFIG.all_contracts {
        executor.index_all_contracts();
        if !CONFIG.levels.is_empty() {
            executor
                .exec_levels(num_getters, CONFIG.levels.clone())
                .unwrap();
        } else {
            info!("processing missing levels");
            executor
                .exec_missing_levels(num_getters)
                .unwrap();

            info!("processing blocks at the chain head");
            executor.exec_continuous().unwrap();
        }
        return;
    }

    for contract_id in &CONFIG.contracts {
        executor
            .add_contract(contract_id)
            .unwrap();
    }
    let contracts = executor.get_config();
    assert_contracts_ok(&contracts);

    if CONFIG.recreate_views {
        executor.recreate_views().unwrap();
        return;
    }

    if !CONFIG.levels.is_empty() {
        executor
            .add_dependency_contracts()
            .unwrap();
        executor
            .create_contract_schemas()
            .unwrap();
        executor
            .exec_levels(num_getters, CONFIG.levels.clone())
            .unwrap();
        return;
    }

    let new_initialized = executor
        .exec_new_contracts_historically(
            CONFIG
                .bcd_url
                .as_ref()
                .map(|url| (url.clone(), CONFIG.network.clone())),
            num_getters,
        )
        .unwrap();
    if !new_initialized.is_empty() {
        info!("all contracts historically bootstrapped. restart to begin normal continuous processing mode.");
        return;
    }

    info!("running for contracts: {:#?}", contracts);
    if !CONFIG.levels.is_empty() {
        executor
            .exec_levels(num_getters, CONFIG.levels.clone())
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
