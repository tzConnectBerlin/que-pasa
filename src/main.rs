extern crate peg;

pub mod node;
pub mod storage;
pub mod table;
pub mod table_builder;

fn main() {
    let s = include_str!("../test/storage1.tz");
    let ast = storage::storage::expr(s).unwrap();
    println!("{:?}", ast);
    let node = node::Node::build(ast);
    println!("{:?}", node);
}
