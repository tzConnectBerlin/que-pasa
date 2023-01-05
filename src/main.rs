extern crate itertools;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

pub mod config;
pub mod contract_denylist;
pub mod debug;
pub mod highlevel;
pub mod octez;
pub mod sql;
pub mod stats;
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

    let env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let config = CONFIG.as_ref().unwrap();

    let node_cli = &node::NodeClient::new(
        config.node_urls.clone(),
        "main".to_string(),
        config.node_comm_retries,
    );

    let mut dbcli = DBClient::connect(
        &config.database_url,
        &config.main_schema,
        std::time::Duration::from_millis(5 * 60 * 1000),
        10,
    )
    .with_context(|| "failed to connect to the db")
    .unwrap();

    let setup_db = config.reinit || !dbcli.common_tables_exist().unwrap();
    if config.reinit {
        assert_sane_db(&mut dbcli);
        if !confirm_request("
Re-initializing -- all data in DB related to ever set-up contracts, including those set-up in prior runs (!), will be destroyed. Continue?") {
            process::exit(1);
        }
        dbcli
            .delete_everything(node_cli, highlevel::get_contract_rel)
            .with_context(|| "failed to delete the db's content")
            .unwrap();
    }
    if setup_db {
        dbcli.create_common_tables().unwrap();
        info!("Common tables set up in db");
    } else {
        assert_sane_db(&mut dbcli);
    }

    let bcd_settings = config
        .bcd_url
        .as_ref()
        .map(|url| (url.clone(), config.bcd_network.clone()));

    let mut executor = highlevel::Executor::new(
        node_cli.clone(),
        dbcli,
        config.reports_interval,
    );
    if config.all_contracts {
        index_all_contracts(config, &bcd_settings, executor);
        return;
    }

    for contract_id in &config.contracts {
        executor
            .add_contract(contract_id)
            .unwrap();
    }

    if config.only_migrate {
        executor
            .add_dependency_contracts()
            .unwrap();
        executor
            .create_contract_schemas()
            .unwrap();
        return;
    }

    let contracts = executor.get_config().unwrap();
    assert_contracts_ok(&contracts);

    let num_getters = config.getters_cap;
    let num_processors = config.workers_cap;
    if !config.levels.is_empty() {
        executor
            .add_dependency_contracts()
            .unwrap();
        executor
            .create_contract_schemas()
            .unwrap();
        executor
            .exec_levels(num_getters, num_processors, config.levels.clone())
            .unwrap();
        return;
    }

    let new_initialized = executor
        .exec_new_contracts_historically(
            &bcd_settings,
            num_getters,
            num_processors,
            config.allowed_unbootstrapped_offset,
        )
        .unwrap();
    if !new_initialized.is_empty() {
        info!("all contracts historically bootstrapped.");
    }

    info!("running for contracts: {:#?}", contracts);
    if !config.levels.is_empty() {
        executor
            .exec_levels(num_getters, num_processors, config.levels.clone())
            .unwrap();
        executor.exec_dependents().unwrap();
        return;
    }

    // We will first load missing levels (if any)
    executor
        .exec_missing_levels(
            &bcd_settings,
            num_getters,
            num_processors,
            config.allowed_unbootstrapped_offset,
            true,
        )
        .unwrap();

    executor
        .reprocess_forked_levels(num_getters, num_processors)
        .unwrap();

    // At last, normal operation.
    info!("processing blocks at the chain head");
    executor.exec_continuous().unwrap();
}

fn index_all_contracts(
    config: &config::Config,
    bcd_settings: &Option<(String, String)>,
    mut executor: highlevel::Executor,
) {
    executor.index_all_contracts();
    if !config.levels.is_empty() {
        executor
            .exec_levels(
                config.getters_cap,
                config.workers_cap,
                config.levels.clone(),
            )
            .unwrap();
        #[cfg(feature = "regression_force_update_derived")]
        if true {
            info!("skipping re-populating of derived tables, always_update_derived enabled");
            return;
        }
        executor
            .repopulate_derived_tables(false)
            .unwrap();
    } else {
        info!("processing missing levels");
        executor
            .exec_missing_levels(
                bcd_settings,
                config.getters_cap,
                config.workers_cap,
                config.allowed_unbootstrapped_offset,
                false,
            )
            .unwrap();

        info!("processing blocks at the chain head");
        executor.exec_continuous().unwrap();
    }
}

fn assert_contracts_ok(contracts: &[ContractID]) {
    if contracts.is_empty() {
        exit_with_err("zero contracts to index..");
    }

    let mut names: HashMap<String, ()> = HashMap::new();
    for contract_id in contracts {
        if names.contains_key(&contract_id.name) {
            exit_with_err(format!("bad contract settings provided: name clash (multiple contracts assigned to name '{}'", contract_id.name).as_str());
        }
        if is_contract_denylisted(&contract_id.address) {
            exit_with_err(format!("bad contract settings provided: denylisted contract cannot be indexed ({})", contract_id.name).as_str());
        }
        names.insert(contract_id.name.clone(), ());
    }
}

fn schema_version(v: &str) -> String {
    match v {
        // The first versions of Que Pasa didn't follow the semantics of using
        // minor versioning for non-db schema related changes only
        "1.0.0" | "1.0.1" | "1.0.2" | "1.0.3" | "1.0.4" | "1.0.5" => {
            v.to_string()
        }
        _ => {
            // Minor version bumps (_._.x) have same db schemas
            v.to_string()
                .rsplit_once('.')
                .map(|(db_ver, _)| db_ver.to_string())
                .unwrap_or_else(|| "".to_string())
        }
    }
}

fn assert_sane_db(dbcli: &mut DBClient) {
    let db_version = dbcli.get_quepasa_version().unwrap();
    if schema_version(&db_version)
        != schema_version(crate::config::QUEPASA_VERSION)
    {
        exit_with_err(
            format!(
                "
Cannot target a database that was initialized with an incompatible quepasa version.
This database was initialized with Que Pasa {}, currently running Que Pasa {}.
Either drop the old database namespace or keep it and target a different one.",
                db_version,
                crate::config::QUEPASA_VERSION,
            )
            .as_str(),
        );
    }
}

fn confirm_request(msg: &str) -> bool {
    // returns true if user confirmed, otherwise false.

    if CONFIG.as_ref().unwrap().always_yes {
        info!(
            "{}  -- skipping confirm request. running with always_yes enabled",
            msg
        );
        return true;
    }

    loop {
        info!("{} [y]es or [n]o", msg);
        let mut buf = String::new();
        std::io::stdin()
            .read_line(&mut buf)
            .unwrap();
        match buf.as_str().trim_end() {
            "n" | "no" => return false,
            "y" | "yes" => return true,
            _ => {}
        };
    }
}

fn exit_with_err(msg: &str) {
    error!("{}", msg);
    process::exit(1);
}
