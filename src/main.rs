use postgresql_generator::PostgresqlGenerator;

extern crate curl;
extern crate peg;
#[macro_use]
extern crate lazy_static;
extern crate serde_json;

pub mod michelson;
pub mod node;
pub mod postgresql_generator;
pub mod storage;
pub mod table;
pub mod table_builder;

use curl::easy::Easy;

//fn load_storage(id: &String) ->

fn main() {
    let store = include_str!("../test/store1.json");
    let v = michelson::store::value(store).unwrap();
    println!("{:#?}", v);

    // let s = include_str!("../test/storage1.tz");
    // let ast = storage::storage::expr(s).unwrap();
    // //println!("{:?}", ast);
    // let node = node::Node::build(node::Context::init(), ast);
    // //println!("{:#?}", node);
    // let mut builder = table_builder::TableBuilder::new();
    // let tables = builder.populate(&node);
    // //println!("{:#?}", builder.tables);
    // let mut generator = PostgresqlGenerator::new();
    // let mut sorted_tables: Vec<_> = builder.tables.iter().collect();
    // sorted_tables.sort_by_key(|a| a.0);
    // for (name, table) in sorted_tables {
    //     println!("{}", generator.create_table_definition(table));
    // }
}
