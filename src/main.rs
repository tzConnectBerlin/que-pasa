use postgresql_generator::PostgresqlGenerator;

extern crate chrono;
extern crate clap;
extern crate curl;
#[macro_use]
extern crate json;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate substring;

use clap::{App, Arg, SubCommand};

pub mod highlevel;
pub mod michelson;
pub mod node;
pub mod postgresql_generator;
pub mod storage;
pub mod table;
pub mod table_builder;

fn main() {
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

    let contract_id = matches.value_of("contract_id").unwrap();

    let json = michelson::StorageParser::get_everything(contract_id, None).unwrap();
    let storage_definition = json["code"][1]["args"][0].clone();
    debug!("{}", storage_definition.to_string());
    let ast = storage::storage_from_json(storage_definition);
    //debug!("{:#?}", ast);

    let node = node::Node::build(node::Context::init(), ast);
    debug!("{:#?}", node);

    let mut builder = table_builder::TableBuilder::new();
    let _tables = builder.populate(&node);

    // If generate-sql command is given, just output SQL and quit.
    if matches.is_present("generate-sql") {
        let mut generator = PostgresqlGenerator::new();
        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);
        for (_name, table) in sorted_tables {
            print!("{}", generator.create_table_definition(table));
            println!();
        }
        return;
    }

    if let Some(levels) = matches.value_of("levels") {
        let levels = levels
            .to_string()
            .split(",")
            .map(|x| x.to_string().parse().expect("Couldn't parse level as u32"))
            .collect::<Vec<u32>>();
        print!("Loading level");
        for level in levels {
            print!(" {}", level);
            let mut storage_parser = michelson::StorageParser::new();
            let json = storage_parser
                .get_storage(&contract_id.to_string(), level)
                .unwrap();
            let v = storage_parser.preparse_storage(&json);
            let result = storage_parser.parse_storage(&v);
            debug!("storage: {:#?}", result);
            let result = storage_parser.update(&result, &node);
            debug!("{:#?}", result);
        }
        println!("");
        let inserts = crate::table::insert::get_inserts();
        let mut keys = inserts
            .keys()
            .collect::<Vec<&crate::table::insert::InsertKey>>();
        keys.sort_by_key(|a| a.id);
        let mut generator = PostgresqlGenerator::new();
        println!("keys len: {}", keys.len());
        for key in keys.iter() {
            println!("{}", generator.build_insert(inserts.get(key).unwrap()));
        }
        println!("");
    }
}
