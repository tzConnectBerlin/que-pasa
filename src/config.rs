use anyhow::Result;
use clap::{App, Arg};
use serde_yaml;
use std::fs;

#[derive(Clone, Default, Debug)]
pub struct Config {
    pub main_schema: String,

    pub contracts: Vec<ContractID>,
    pub all_contracts: bool,
    pub database_url: String,

    pub reinit: bool,
    pub only_migrate: bool,

    pub levels: Vec<u32>,
    pub node_urls: Vec<String>, // allowing multiple urls, HEAD is the primary node, any subsequent is a fallback node
    pub node_comm_retries: i32,

    pub bcd_url: Option<String>,
    pub bcd_network: String,

    pub getters_cap: usize,
    pub workers_cap: usize,
    pub always_yes: bool,
    pub reports_interval: usize, // in seconds
}

#[derive(
    Hash, Eq, PartialEq, Clone, Default, Debug, Serialize, Deserialize,
)]
pub struct ContractID {
    pub address: String,
    pub name: String,
}

lazy_static! {
    pub static ref CONFIG: Result<Config> = init_config();
}
pub const QUEPASA_VERSION: &str = env!("CARGO_PKG_VERSION");

// init config and return it also.
pub fn init_config() -> Result<Config> {
    let mut config: Config = Default::default();
    let matches = App::new("Tezos Contract Baby Indexer")
        .version(QUEPASA_VERSION)
        .author("Rick Klomp <rick.klomp@tzconect.com>")
        .about("An indexer for specific contracts")
        .arg(
            Arg::with_name("main_schema")
                .short("s")
                .long("main-schema")
                .value_name("MAIN_SCHEMA")
                .env("MAIN_SCHEMA")
                .default_value("que_pasa")
                .help("schema to use for global tables (eg levels table)")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("contract_settings")
                .short("c")
                .long("contract-settings")
                .value_name("CONTRACT_SETTINGS")
                .env("CONTRACT_SETTINGS")
                .help("path to the settings yaml (for contract settings)")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("contracts")
                .long("contracts")
                .value_name("CONTRACTS")
                .help("set of additional contract settings (in syntax: <name>=<address>)")
                .multiple(true)
                .takes_value(true)
        )
        .arg(
            Arg::with_name("index_all_contracts")
                .long("index-all-contracts")
                .value_name("INDEX_ALL_CONTRACTS")
                .help("index *all* contracts")
                .takes_value(false)
        )
        .arg(
            Arg::with_name("database_url")
                .short("d")
                .long("database-url")
                .env("DATABASE_URL")
                .default_value("host=localhost port=5432 user=test password=test dbname=test")
                .value_name("DATABASE_URL")
                .help("The URL of the database")
                .takes_value(true))
        .arg(
            Arg::with_name("node_url")
                .short("n")
                .long("node-url")
                .env("NODE_URL")
                .default_value("http://localhost:8732")
                .value_name("NODE_URL")
                .help("The URL of the Tezos node, optionally accepts more than 1 (comma separated) for fallback nodes in case of non-transcient communication issues with the primary node")
                .takes_value(true))
        .arg(
            Arg::with_name("node_comm_retries")
                .long("node-comm-retries")
                .env("NODE_COMM_RETRIES")
                .default_value("3")
                .value_name("NODE_COMM_RETRIES")
                .help("The number of times to retry a node RPC call on any error, set to smaller than 0 for infinite")
                .takes_value(true))
        .arg(
            Arg::with_name("bcd_enable")
                .long("bcd-enable")
                .value_name("BCD_ENABLE")
                .help("enable usage of better-call.dev api (helps increase bootstrapping low-activity contracts)")
                .takes_value(false))
        .arg(
            Arg::with_name("bcd_url")
                .long("bcd-url")
                .value_name("BCD_URL")
                .default_value("https://api.better-call.dev/v1")
                .takes_value(true))
        .arg(
            Arg::with_name("bcd_network")
                .long("bcd-network")
                .value_name("BCD_NETWORK")
                .env("BCD_NETWORK")
                .possible_values(&["mainnet", "ithacanet", "hangzhou2net", "hangzhounet", "granadanet", "florencenet"])
                .default_value("mainnet")
                .help("For better-call.dev: name of the Tezos network to target")
                .takes_value(true))
        .arg(
            Arg::with_name("getters_cap")
                .long("getters-cap")
                .value_name("GETTERS_CAP")
                .env("GETTERS_CAP")
                .default_value("2")
                .help("max number of processes used to concurrently fetch block data from the node (for faster bootstrap)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("workers_cap")
                .long("workers-cap")
                .value_name("WORKERS_CAP")
                .env("WORKERS_CAP")
                .default_value("4")
                .help("max number of processes used to concurrently process block data (for faster bootstrap)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("levels")
                .short("l")
                .long("levels")
                .value_name("LEVELS")
                .env("LEVELS")
                .help("command the indexer to process an exact set of levels (format: single number, or a range with format from-to)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("reports_interval")
                .short("i")
                .long("reports-interval")
                .value_name("REPORTS_INTERVAL")
                .env("REPORTS_INTERVAL")
                .default_value("10")
                .help("set the frequency of progress reports during bootstrap (unit: seconds). set to 0 to disable reports.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("reinit")
                .long("reinit")
                .value_name("REINIT")
                .help("If set, clear the DB out and recreate global tables")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("only_migrate")
                .long("only-migrate")
                .value_name("ONLY_MIGRATE")
                .help("If set, apply migrations (if any applicable) and then quit without processing levels.")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("always_yes")
                .long("always-yes")
                .short("y")
                .value_name("ALWAYS_YES")
                .help("If set, never prompt for confirmations, always default to 'yes'")
                .takes_value(false));
    let matches = matches.get_matches();

    config.main_schema = matches
        .value_of("main_schema")
        .unwrap()
        .to_string();

    let maybe_fpath = matches.value_of("contract_settings");
    if let Some(fpath) = maybe_fpath {
        info!("loading contract settings from {}", fpath);
        config.contracts = parse_contract_settings_file(fpath).unwrap();
    }
    if matches.is_present("contracts") {
        config.contracts.extend(
            matches.values_of("contracts").unwrap().map(|s| {
                match s.split_once("=") {
                    Some((name, address)) => ContractID {
                        name: name.to_string(),
                        address: address.to_string(),
                    },
                    None => panic!("bad contract arg format (expected: <name>=<address>, got {}", s),
                }
            }).collect::<Vec<ContractID>>(),
        );
    }

    config.database_url = matches
        .value_of("database_url")
        .unwrap()
        .to_string();

    config.reinit = matches.is_present("reinit");
    config.only_migrate = matches.is_present("only_migrate");
    config.all_contracts = matches.is_present("index_all_contracts");
    config.always_yes = matches.is_present("always_yes");

    config.levels = matches
        .value_of("levels")
        .map_or_else(Vec::new, range);

    config.node_urls = matches
        .value_of("node_url")
        .unwrap()
        .split(',')
        .map(|s| s.to_string())
        .collect();

    config.node_comm_retries = matches
        .value_of("node_comm_retries")
        .unwrap()
        .parse::<i32>()?;

    if matches.is_present("bcd_enable") {
        config.bcd_url = matches
            .value_of("bcd_url")
            .map(String::from);
        config.bcd_network = matches
            .value_of("bcd_network")
            .unwrap()
            .to_string();
    }

    config.reports_interval = matches
        .value_of("reports_interval")
        .unwrap()
        .parse::<usize>()?;

    config.getters_cap = matches
        .value_of("getters_cap")
        .unwrap()
        .parse::<usize>()?;
    if config.getters_cap == 0 {
        warn!(
            "set getters_cap ({}) is invalid. defaulting to 1",
            config.getters_cap
        );
        config.getters_cap = 1;
    }
    config.workers_cap = matches
        .value_of("workers_cap")
        .unwrap()
        .parse::<usize>()?;

    if config.workers_cap == 0 {
        warn!(
            "set workers_cap ({}) is invalid. defaulting to 1",
            config.workers_cap
        );
        config.workers_cap = 1;
    }

    debug!("Config={:#?}", config);
    Ok(config)
}

// get range of args in the form 1,2,3 or 1-3. All ranges inclusive.
fn range(arg: &str) -> Vec<u32> {
    let mut result = vec![];
    for h in arg.split(',') {
        let s = String::from(h);
        match s.find('-') {
            Some(_) => {
                let fromto: Vec<String> =
                    s.split('-').map(String::from).collect();
                for i in fromto[0].parse::<u32>().unwrap()
                    ..fromto[1].parse::<u32>().unwrap() + 1
                {
                    result.push(i);
                }
            }
            None => {
                result.push(s.parse::<u32>().unwrap());
            }
        }
    }
    result.sort_unstable();
    result
}

fn parse_contract_settings_file(fpath: &str) -> Result<Vec<ContractID>> {
    let content = fs::read_to_string(fpath)?;
    #[derive(Serialize, Deserialize)]
    struct ParseType {
        contracts: Vec<ContractID>,
    }
    let res: ParseType = serde_yaml::from_str(&content)?;
    Ok(res.contracts)
}
