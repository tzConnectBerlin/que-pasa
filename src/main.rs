extern crate peg;

pub mod node;
pub mod storage;
pub mod table;
pub mod table_builder;

use table_builder::{TableBuilder, Tables};

fn main() {
    let s = include_str!("../test/storage1.tz");
    let ast = storage::storage::expr(s).unwrap();
    //println!("{:?}", ast);
    let mut node = node::Node::build(ast);
    //println!("{:?}", node);
    let mut builder = TableBuilder::new(
        Box::new(Tables::new()),
        "".to_string(),
        "storage".to_string(),
    );
    let node = builder.node(&mut Box::new(node));
    println!("Final result: {:#?}", node);
}
