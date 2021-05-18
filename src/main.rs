use postgresql_generator::PostgresqlGenerator;

extern crate bs58;
extern crate chrono;
extern crate clap;
extern crate curl;
extern crate dotenv;
extern crate hex;
extern crate indicatif;
#[macro_use]
extern crate json;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate postgres;
extern crate regex;
extern crate ron;
#[macro_use]
extern crate serde;
extern crate serde_json;

use clap::{App, Arg, SubCommand};

pub mod error;
pub mod highlevel;
pub mod michelson;
pub mod node;
pub mod postgresql_generator;
pub mod storage;
pub mod table;
pub mod table_builder;

use indicatif::ProgressBar;

use michelson::StorageParser;

fn main() {
    //dotenv::dotenv().ok();
    env_logger::init();
    let matches = App::new("Tezos Contract Baby Indexer")
        .version("0.0")
        .author("john newby <john.newby@tzconect.com>")
        .about("Indexes a single contract")
        .arg(
            Arg::with_name("contract_id")
                .short("c")
                .long("contract_id")
                .value_name("CONTRACT_ID")
                .help("Sets the id of the contract to use")
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
        .subcommand(
            SubCommand::with_name("generate-sql")
                .about("Generated table definitions")
                .version("0.0"),
        )
        .get_matches();

    let contract_id = matches
        .value_of("contract_id")
        .expect("contract_id is required");

    // init by grabbing the contract data.
    let json = StorageParser::get_everything(contract_id, None).unwrap();
    let storage_definition = json["code"][1]["args"][0].clone();
    debug!("{}", storage_definition.to_string());
    let ast = storage::storage_from_json(storage_definition).unwrap();

    // Build the internal representation from the node storage defition
    let context = node::Context::init();
    //use a reference here for more efficient memory management
    let node = node::Node::build(context.clone(), ast);
    //debug!("{:#?}", node);

    // Make a SQL-compatible representation
    let mut builder = table_builder::TableBuilder::new();
    let big_map_tables_names = builder.populate(&node);

    // If generate-sql command is given, just output SQL and quit.
    if matches.is_present("generate-sql") {
        let mut generator = PostgresqlGenerator::new();
        println!("{}", generator.create_common_tables());
        println!("{}", generator.create_big_map_table(context.clone(), big_map_tables_names.unwrap()));
        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);
        for (_name, table) in sorted_tables {
            print!("{}", generator.create_table_definition(table).unwrap());
            println!();
            print!("{}", generator.create_view_definition(table));
            println!();
        }
        return;
    }

    let init = matches.is_present("init");
    if init {
        println!(
            "Initialising--all data in DB will be destroyed. Interrupt within 5 seconds to abort"
        );
        std::thread::sleep(std::time::Duration::from_millis(5000));
        postgresql_generator::delete_everything(&mut postgresql_generator::connect().unwrap())
            .unwrap();
    }

    if let Some(levels) = matches.value_of("levels") {
        let levels = range(&levels.to_string());
        for level in &levels {
            let result =
                crate::highlevel::load_and_store_level(&node, contract_id, *level).unwrap();
            println!("{}", level_text(*level, &result));
        }

        if init {
            let first: u32 = *levels.iter().min().unwrap();
            let head = michelson::StorageParser::head().unwrap();
            postgresql_generator::fill_in_levels(
                &mut postgresql_generator::connect().unwrap(),
                first,
                head._level,
            )
            .unwrap();
        }

        return;
    }

    // No args so we will first load missing levels

    loop {
        let origination_level = highlevel::get_origination(&contract_id).unwrap();

        let mut missing_levels: Vec<u32> = postgresql_generator::get_missing_levels(
            &mut postgresql_generator::connect().unwrap(),
            origination_level,
            michelson::StorageParser::head().unwrap()._level,
        )
        .unwrap();
        missing_levels.reverse();

        if missing_levels.is_empty() {
            // finally through them
            break;
        }

        while let Some(level) = missing_levels.pop() {
            let store_result =
                crate::highlevel::load_and_store_level(&node, contract_id, level as u32).unwrap();
            if store_result.is_origination {
                println!(
                    "Found new origination level {}",
                    highlevel::get_origination(&contract_id).unwrap().unwrap()
                );
                break;
            }
            println!(
                " {} transactions for us, {} remaining",
                store_result.tx_count,
                missing_levels.len()
            );
            debug!("Inserts now {:?}", crate::table::insert::get_inserts());
        }
    }

    // At last, normal operation.

    let progress_bar = ProgressBar::new_spinner();

    loop {
        let chain_head = michelson::StorageParser::head().unwrap();
        let db_head = postgresql_generator::get_head(&mut postgresql_generator::connect().unwrap())
            .unwrap()
            .unwrap();
        debug!("db: {} chain: {}", db_head._level, chain_head._level);
        if chain_head._level > db_head._level {
            for level in (db_head._level + 1)..=chain_head._level {
                let result = highlevel::load_and_store_level(&node, contract_id, level).unwrap();
                println!("{}", level_text(level, &result));
            }
            continue;
        } else if db_head._level > chain_head._level {
            println!("More levels in DB than chain, bailing!");
            return;
        } else {
            // they are equal, so we will just check that the hashes match.
            if db_head.hash == chain_head.hash {
                // if they match, nothing to do.
                progress_bar.tick();
            } else {
                println!();
                println!(
                    "Hashes don't match: {:?} (db) <> {:?} (chain)",
                    db_head.hash, chain_head.hash
                );
                let mut connection = postgresql_generator::connect().unwrap();
                let mut transaction = connection.transaction().unwrap();
                postgresql_generator::delete_level(&mut transaction, &db_head).unwrap();
                transaction.commit().unwrap();
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

// get range of args in the form 1,2,3 or 1-3. All ranges inclusive.
fn range(arg: &String) -> Vec<u32> {
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
    result.reverse();
    result
}
