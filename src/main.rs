use postgresql_generator::PostgresqlGenerator;

extern crate chrono;
extern crate clap;
extern crate curl;
#[macro_use]
extern crate dotenv_codegen;
extern crate dotenv;
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

    if let Some(levels) = matches.value_of("levels") {
        let levels = range(&levels.to_string());
        print!("Loading levels");
        for level in levels {
            print!("level {}", level);
            crate::highlevel::save_level(&node, contract_id, level);
            debug!("Inserts now {:?}", crate::table::insert::get_inserts());
        }
        return;
    }

    // No args so we will just start at the beginning.

    let head = StorageParser::head().unwrap();
    println!("Head is block {}. starting there.", head._level);
    let mut level = head._level + 1;
    loop {
        level -= 1;
        print!("{} ", level);
        let mut storage_parser = StorageParser::new();
        let operations = StorageParser::get_operations_from_node(contract_id, Some(level)).unwrap();

        if operations.len() == 0 {
            println!("");
            continue;
        }

        let json = storage_parser
            .get_storage(&contract_id.to_string(), level)
            .unwrap();
        print!(".");
        let v = storage_parser.preparse_storage(&json);
        let result = storage_parser.parse_storage(&v).unwrap();
        debug!("storage: {:#?}", result);
        let result = storage_parser.read_storage(&result, &node).unwrap();
        debug!("{:#?}", result);
        print!(".");
        let inserts = crate::table::insert::get_inserts().clone();
        let mut keys = inserts
            .keys()
            .collect::<Vec<&crate::table::insert::InsertKey>>();
        keys.sort_by_key(|a| a.id);
        let mut generator = PostgresqlGenerator::new();
        for key in keys.iter() {
            debug!(
                "{}",
                generator.build_insert(inserts.get(key).unwrap(), level)
            );
        }
        crate::table::insert::get_inserts().clear();
    }
}

// takes args of the form X,Y-Z,A and returns a vector of the individual numbers
// ranges in the form X-Y are INCLUSIVE
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
    result
}
