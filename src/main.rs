extern crate peg;

pub mod storage;
pub mod table;
pub mod table_builder;

fn main() {
    let s = include_str!("../test/storage1.tz");
    let mut builder = table_builder::TableBuilder::new();
    let _ast = match storage::storage::expr(s) {
        Ok(ast) => builder.start_table(None, Some("storage".to_string()), None, ast),
        Err(e) => println!("{:?}", e),
    };
    println!("{:?}", builder.tables);
}
