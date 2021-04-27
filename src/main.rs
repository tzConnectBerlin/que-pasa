use postgresql_generator::PostgresqlGenerator;

extern crate bs58;
extern crate chrono;
extern crate clap;
extern crate curl;
#[macro_use]
extern crate dotenv_codegen;
extern crate dotenv;
extern crate hex;
#[macro_use]
extern crate json;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate postgres;
extern crate regex;

use clap::{App, Arg, SubCommand};

pub mod error;
pub mod highlevel;
pub mod michelson;
pub mod node;
pub mod postgresql_generator;
pub mod storage;
pub mod table;
pub mod table_builder;

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
    let ast = storage::storage_from_json(storage_definition);

    // Build the internal representation from the node storage defition
    let node = node::Node::build(node::Context::init(), ast);
    debug!("{:#?}", node);

    // Make a SQL-compatible representation
    let mut builder = table_builder::TableBuilder::new();
    let _tables = builder.populate(&node);

    // If generate-sql command is given, just output SQL and quit.
    if matches.is_present("generate-sql") {
        let mut generator = PostgresqlGenerator::new();
        println!("{}", generator.create_common_tables());
        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);
        for (_name, table) in sorted_tables {
            print!("{}", generator.create_table_definition(table));
            println!();
        }
        return;
    }

    // set the ID in the DB.
    highlevel::init().unwrap();

    if let Some(levels) = matches.value_of("levels") {
        let levels = range(&levels.to_string());
        print!("Loading levels");
        for level in levels {
            print!("level {}", level);
            crate::highlevel::save_level(&node, contract_id, level).unwrap();
            debug!("Inserts now {:?}", crate::table::insert::get_inserts());
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

        if missing_levels.len() == 0 {
            // finally through them
            break;
        }

        while let Some(level) = missing_levels.pop() {
            print!("level {}", level);
            let found_origination =
                crate::highlevel::save_level(&node, contract_id, level as u32).unwrap();
            if found_origination {
                println!(
                    "Found new origination level {}",
                    highlevel::get_origination(&contract_id).unwrap().unwrap()
                );
                break;
            }
            println!(" {} remaining", missing_levels.len());
            debug!("Inserts now {:?}", crate::table::insert::get_inserts());
        }
    }
}

// get range of args in the form 1,2,3 or 1-3. All ranges inclusive.
fn range(arg: &String) -> Vec<u32> {
    let mut result = vec![];
    for h in arg.split(',') {
        let s = String::from(h);
        match s.find("-") {
            Some(_) => {
                let fromto: Vec<String> = s.split('-').map(|x| String::from(x)).collect();
                for i in fromto[0].parse::<u32>().unwrap()..fromto[1].parse::<u32>().unwrap() + 1 {
                    result.push(i);
                }
            }
            None => {
                result.push(s.parse::<u32>().unwrap());
            }
        }
    }
    result.sort();
    result.reverse();
    result
}
