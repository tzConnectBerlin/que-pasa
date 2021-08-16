use clap::{App, Arg, SubCommand};

use crate::err;
use crate::error::Res;

#[derive(Clone, Default, Debug)]
pub struct Config {
    pub contract_id: String,
    pub database_url: String,
    pub ssl: bool,
    pub ca_cert: Option<String>,
    pub generate_sql: bool,
    pub init: bool,
    pub levels: Vec<u32>,
    pub node_url: String,
}

lazy_static! {
    pub static ref CONFIG: Config = init_config().unwrap();
}

// init config and return it also.
pub fn init_config() -> Res<Config> {
    let mut config: Config = Default::default();
    let matches = App::new("Tezos Contract Baby Indexer")
        .version("0.0")
        .author("john newby <john.newby@tzconect.com>")
        .about("Indexes a single contract")
        .arg(
            Arg::with_name("contract_id")
                .short("c")
                .long("contract-id")
                .value_name("CONTRACT_ID")
                .help("Sets the id of the contract to use")
                .takes_value(true),
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
            Arg::with_name("generate_sql")
                .short("g")
                .long("generate-sql")
                .help("Generate SQL")
                .takes_value(false))
        .arg(
            Arg::with_name("node_url")
                .short("n")
                .long("node-url")
                .value_name("NODE_URL")
                .help("The URL of the Tezos node")
                .takes_value(true))
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
        .subcommand(
            SubCommand::with_name("generate-sql")
                .about("Generated table definitions")
                .version("0.0"),
        )
        .get_matches();

    config.contract_id = matches
        .value_of("contract_id")
        .map_or_else(|| std::env::var("CONTRACT_ID"), |s| Ok(s.to_string()))
        .map(|s| s.to_string())
        .unwrap();

    config.generate_sql = match matches.subcommand() {
        ("generate-sql", _) => true,
        _ => matches.is_present("generate_sql"),
    };

    if !config.generate_sql {
        config.database_url = match matches
            .value_of("database_url")
            .map_or_else(|| std::env::var("DATABASE_URL"), |s| Ok(s.to_string()))
        {
            Ok(x) => x,
            Err(_) => {
                return Err(err!(
                    "Database URL must be set either on the command line or in the environment"
                ))
            }
        };
        println!("db url: \"{}\"", config.database_url);
    }

    if matches.is_present("ssl") {
        config.ssl = true;
        config.ca_cert = matches.value_of("ssl-cert").map(String::from);
    } else {
        config.ssl = false;
        config.ca_cert = None;
    }

    config.init = matches.is_present("init");

    config.levels = matches
        .value_of("levels")
        .map_or_else(Vec::new, |x| range(x));

    config.node_url = match matches
        .value_of("node_url")
        .map_or_else(|| std::env::var("NODE_URL"), |s| Ok(s.to_string()))
    {
        Ok(x) => x,
        Err(_) => {
            return Err(err!(
                "Node URL must be set either on the command line or in the environment"
            ))
        }
    };
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
                let fromto: Vec<String> = s.split('-').map(String::from).collect();
                for i in fromto[0].parse::<u32>().unwrap()..fromto[1].parse::<u32>().unwrap() + 1 {
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
