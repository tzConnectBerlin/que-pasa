extern crate peg;

pub mod storage;

use crate::storage::Expr;
use std::string::String;

fn print(s: &str, depth: u32) {
    print!("{}", "    ".to_string().repeat(depth as usize));
    println!("{}", s);
}

fn label(s: Option<String>) -> String {
    match s {
        Some(s) => s,
        None => "**anonymous**".to_string(),
    }
}

fn print_expr(e: &Expr, depth: u32) {
    print(&format!("{:?}", e), depth);
}

fn address(s: Option<String>, depth: u32) {
    print(&format!("address : {}", label(s)), depth);
}

fn print_nat(s: Option<String>, depth: u32) {
    print(&format!("nat : {}", label(s)), depth);
}

fn map(map_type: String, s: Option<String>, key: Expr, value: Expr, depth: u32) {
    print(
        "========================================================================",
        depth,
    );
    print(&format!("{} {} : from", map_type, label(s)), depth);
    print_ast(key, depth);
    print(&format!("======to======"), depth);
    print_ast(value, depth);
    print(
        "========================================================================",
        depth,
    );
}

fn pair(left: Box<Expr>, right: Box<Expr>, depth: u32) {
    print_ast(*left, depth);
    print_ast(*right, depth);
}

fn print_ast(ast: storage::Expr, depth: u32) {
    match ast {
        Expr::Address(l) => address(l, depth),
        Expr::BigMap(l, key, value) => map("big_map".to_string(), l, *key, *value, depth + 1),
        Expr::Map(l, key, value) => map("map".to_string(), l, *key, *value, depth + 1),
        Expr::Int(_) => print_expr(&ast, depth),
        Expr::Nat(l) => print_nat(l, depth),
        Expr::Pair(_l, left, right) => pair(left, right, depth),
        Expr::String(_) => print_expr(&ast, depth),
        Expr::Timestamp(_) => print_expr(&ast, depth),
        Expr::Unit(_) => print_expr(&ast, depth),
        Expr::Option_(_, _) => print_expr(&ast, depth),
        Expr::Or(_, _, _) => print_expr(&ast, depth),
    };
}

fn main() {
    let s = include_str!("../test/storage1.tz");
    let _ast = match storage::storage::expr(s) {
        Ok(ast) => print_ast(ast, 0),
        Err(e) => println!("{:?}", e),
    };
}
