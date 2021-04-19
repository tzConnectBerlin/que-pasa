use postgresql_generator::PostgresqlGenerator;

extern crate curl;
#[macro_use]
extern crate json;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate substring;

pub mod michelson;
pub mod node;
pub mod postgresql_generator;
pub mod storage;
pub mod table;
pub mod table_builder;

fn main() {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let contract_id = args[1].clone();

    let json = michelson::get_everything(contract_id.as_str()).unwrap();
    let storage_definition = json["code"][1]["args"][0].clone();
    debug!("{}", storage_definition.to_string());
    let ast = storage::storage_from_json(storage_definition);
    //debug!("{:#?}", ast);

    let node = node::Node::build(node::Context::init(), ast);
    debug!("{:#?}", node);

    let mut builder = table_builder::TableBuilder::new();
    let _tables = builder.populate(&node);
    //debug!("{:#?}", builder.tables);
    let mut _generator = PostgresqlGenerator::new();
    let mut _sorted_tables: Vec<_> = builder.tables.iter().collect();
    _sorted_tables.sort_by_key(|a| a.0);
    for (_name, _table) in _sorted_tables {
        //debug!("{}", generator.create_table_definition(table));
    }

    let storage = &json["storage"];
    let v = michelson::preparse_storage(storage);
    let result = michelson::parse_storage(&v);
    debug!("storage: {:#?}", result);
    michelson::update(&result, &node);
}
