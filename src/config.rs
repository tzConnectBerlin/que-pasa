use anyhow::Result;
use clap::{App, Arg};
use serde_yaml;
use std::fs;

#[derive(Clone, Default, Debug)]
pub struct Config {
    pub contracts: Vec<ContractID>,
    pub all_contracts: bool,
    pub database_url: String,
    pub ssl: bool,
    pub ca_cert: Option<String>,
    pub reinit: bool,
    pub levels: Vec<u32>,
    pub node_url: String,

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
            Arg::with_name("ssl")
                .short("S")
                .long("ssl")
                .help("Use SSL for postgres connection")
                .takes_value(false)
        )
        .arg(
            Arg::with_name("ca-cert")
                .short("C")
                .env("CA_CERT")
                .long("ca-cert")
                .help("CA Cert for SSL postgres connection")
                .takes_value(true))
        .arg(
            Arg::with_name("node_url")
                .short("n")
                .long("node-url")
                .env("NODE_URL")
                .default_value("http://localhost:8732")
                .value_name("NODE_URL")
                .help("The URL of the Tezos node")
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
                .possible_values(&["mainnet", "hangzhounet", "granadanet", "florencenet"])
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
            Arg::with_name("always_yes")
                .long("always-yes")
                .short("y")
                .value_name("ALWAYS_YES")
                .help("If set, never prompt for confirmations, always default to 'yes'")
                .takes_value(false));
    let matches = matches.get_matches();

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

    if matches.is_present("ssl") {
        config.ssl = true;
        config.ca_cert = matches
            .value_of("ssl-cert")
            .map(String::from);
    } else {
        config.ssl = false;
        config.ca_cert = None;
    }

    config.reinit = matches.is_present("reinit");
    config.all_contracts = matches.is_present("index_all_contracts");
    config.always_yes = matches.is_present("always_yes");

    config.levels = matches
        .value_of("levels")
        .map_or_else(Vec::new, |x| range(x));

    config.node_url = matches
        .value_of("node_url")
        .unwrap()
        .to_string();

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
