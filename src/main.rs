use postgresql_generator::PostgresqlGenerator;

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
        .subcommand(
            SubCommand::with_name("generate-sql")
                .about("Generated table definitions")
                .version("0.0"),
        )
        .get_matches();

    let contract_id = matches.value_of("contract_id").unwrap();

    let json = michelson::get_everything(contract_id).unwrap();
    let storage_definition = json["code"][1]["args"][0].clone();
    debug!("{}", storage_definition.to_string());
    let ast = storage::storage_from_json(storage_definition);
    //debug!("{:#?}", ast);

    let node = node::Node::build(node::Context::init(), ast);
    debug!("{:#?}", node);

    let mut builder = table_builder::TableBuilder::new();
    let _tables = builder.populate(&node);
    //debug!("{:#?}", builder.tables);
    if matches.is_present("generate-sql") {
        let mut generator = PostgresqlGenerator::new();
        let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
        sorted_tables.sort_by_key(|a| a.0);
        for (_name, table) in sorted_tables {
            print!("{}", generator.create_table_definition(table));
        }
        return;
    }

    let storage = &json["storage"];
    let v = michelson::preparse_storage(storage);
    let result = michelson::parse_storage(&v);
    debug!("storage: {:#?}", result);
    michelson::update(&result, &node);
}
