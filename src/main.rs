use postgresql_generator::PostgresqlGenerator;

extern crate peg;
#[macro_use]
extern crate lazy_static;
pub mod node;
pub mod postgresql_generator;
pub mod storage;
pub mod table;
pub mod table_builder;

fn main() {
    let s = include_str!("../test/storage1.tz");
    let ast = storage::storage::expr(s).unwrap();
    //println!("{:?}", ast);
    let node = node::Node::build(node::Context::init(), ast);
    //println!("{:#?}", node);
    let mut builder = table_builder::TableBuilder::new();
    let tables = builder.populate(&node);
    //println!("{:#?}", builder.tables);
    let mut generator = PostgresqlGenerator::new();
    for table in builder.tables.keys() {
        println!(
            "{}",
            generator.create_table_definition(builder.tables.get(table).unwrap())
        );
    }
}
