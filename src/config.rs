use anyhow::{anyhow, Result};
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
    pub generate_sql: bool,
    pub init: bool,
    pub levels: Vec<u32>,
    pub node_url: String,
    pub network: String,
    pub bcd_url: Option<String>,
    pub workers_cap: usize,
}

#[derive(
    Hash, Eq, PartialEq, Clone, Default, Debug, Serialize, Deserialize,
)]
pub struct ContractID {
    pub address: String,
    pub name: String,
}

lazy_static! {
    pub static ref CONFIG: Config = init_config().unwrap();
}

// init config and return it also.
pub fn init_config() -> Result<Config> {
    let mut config: Config = Default::default();
    let matches = App::new("Tezos Contract Baby Indexer")
        .version("0.0")
        .author("john newby <john.newby@tzconect.com>")
        .about("Indexes a single contract")
        .arg(
            Arg::with_name("contract_settings")
                .short("c")
                .long("contract-settings")
                .value_name("CONTRACT_SETTINGS")
                .help("path to the settings yaml (for contract settings)")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("contracts")
                .long("contracts")
                .value_name("CONTRACTS")
                .help("set of additional contract settings")
                .multiple(true)
                .takes_value(true)
        )
        .arg(
            Arg::with_name("index_all_contracts")
                .long("index-all-contracts")
                .value_name("INDEX_ALL_CONTRACTS")
                .help("if set, *all* active contracts are indexed")
                .takes_value(false)
        )
        .arg(
            Arg::with_name("database_url")
                .short("d")
                .long("database-url")
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
                .long("ca-cert")
                .help("CA Cert for SSL postgres connection")
                .takes_value(true))
        .arg(
            Arg::with_name("node_url")
                .short("n")
                .long("node-url")
                .value_name("NODE_URL")
                .help("The URL of the Tezos node")
                .takes_value(true))
        .arg(
            Arg::with_name("network")
                .long("network")
                .value_name("NETWORK")
                .help("Name of the Tezos network to target (eg 'main', 'granadanet', ..)")
                .takes_value(true))
        .arg(
            Arg::with_name("bcd_url")
                .long("bcd-url")
                .value_name("BCD_URL")
                .help("Optional: better-call.dev api url (enables fast bootstrap)")
                .takes_value(true))
        .arg(
            Arg::with_name("workers_cap")
                .long("workers-cap")
                .value_name("WORKERS_CAP")
                .help("max number of workers used to concurrently fetch block data from the node (only applies during bootstrap)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("levels")
                .short("l")
                .long("levels")
                .value_name("LEVELS")
                .help("Gives the set of levels to load")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("init")
                .short("i")
                .long("init")
                .value_name("INIT")
                .help("If present, clear the DB out, load the levels, and set the in-between levels as already loaded")
                .takes_value(false),
        )
        .get_matches();

    let maybe_fpath = matches
        .value_of("contract_settings")
        .map_or_else(
            || std::env::var("CONTRACT_SETTINGS"),
            |s| Ok(s.to_string()),
        )
        .ok();
    if let Some(fpath) = maybe_fpath {
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

    config.database_url = match matches
            .value_of("database_url")
            .map_or_else(|| std::env::var("DATABASE_URL"), |s| Ok(s.to_string())) {
        Ok(x) => x,
        Err(_) => {
            return Err(anyhow!(
                "Database URL must be set either on the command line or in the environment"
            ))
        }
    };

    if matches.is_present("ssl") {
        config.ssl = true;
        config.ca_cert = matches
            .value_of("ssl-cert")
            .map(String::from);
    } else {
        config.ssl = false;
        config.ca_cert = None;
    }

    config.init = matches.is_present("init");
    config.all_contracts = matches.is_present("index_all_contracts");

    config.levels = matches
        .value_of("levels")
        .map_or_else(Vec::new, |x| range(x));

    config.node_url = match matches
        .value_of("node_url")
        .map_or_else(|| std::env::var("NODE_URL"), |s| Ok(s.to_string()))
    {
        Ok(x) => x,
        Err(_) => {
            return Err(anyhow!(
                "Node URL must be set either on the command line or in the environment"
            ))
        }
    };
    config.bcd_url = matches
        .value_of("bcd_url")
        .map(String::from);
    config.network = matches
        .value_of("network")
        .map_or_else(|| std::env::var("NETWORK"), |s| Ok(s.to_string()))
        .unwrap_or_else(|_| "mainnet".to_string());

    let workers_cap = match matches.value_of("workers_cap") {
        Some(s) => s.to_string(),
        None => {
            std::env::var("WORKERS_CAP").unwrap_or_else(|_| "10".to_string())
        }
    };
    config.workers_cap = workers_cap.parse::<usize>()?;
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

fn parse_contract_settings_file(fpath: String) -> Result<Vec<ContractID>> {
    let content = fs::read_to_string(fpath)?;
    #[derive(Serialize, Deserialize)]
    struct ParseType {
        contracts: Vec<ContractID>,
    }
    let res: ParseType = serde_yaml::from_str(&content)?;
    Ok(res.contracts)
}
