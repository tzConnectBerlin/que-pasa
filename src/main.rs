extern crate peg;

pub mod storage;
pub mod table;

use crate::storage::Expr;
use std::string::String;
use std::vec::Vec;

type TableVec<'a> = Vec<table::Table<'a>>;

fn label(s: Option<String>) -> String {
    match s {
        Some(s) => s,
        None => "**anonymous**".to_string(),
    }
}

fn flatten2<'a>(
    tables: &'a mut Vec<table::Table<'a>>,
    current_table: &table::Table<'a>,
    expr: &Expr,
    vec: &mut Vec<Expr>,
) {
    match expr {
        Expr::Map(l, key, value) | Expr::BigMap(l, key, value) => start_table(
            tables,
            Some(current_table.clone()),
            l.clone(),
            Some((**key).clone()),
            (**value).clone(),
        ),

        Expr::Pair(_l, left, right) => {
            flatten2(tables, current_table, left, vec);
            flatten2(tables, current_table, right, vec);
        }
        _ => {
            vec.push(expr.clone());
        }
    }
}

fn flatten<'a>(
    tables: &'a mut Vec<table::Table<'a>>,
    current_table: &table::Table<'a>,
    expr: &Expr,
) -> Vec<Expr> {
    let mut vec: Vec<Expr> = vec![];
    flatten2(tables, current_table, expr, &mut vec);
    vec
}

fn sql_name(table: table::Table) -> String {
    let mut name = table.name;
    let mut x;
    loop {
        x = table.parent;
        match x {
            None => break,
            Some(x) => {
                name.extend("_".chars());
                name.extend(x.name.chars());
            }
        }
    }
    name
}

fn start_table<'a>(
    tables: &'a mut TableVec<'a>,
    parent: Option<table::Table<'a>>,
    name: Option<String>,
    indices: Option<storage::Expr>,
    columns: storage::Expr,
) {
    let name = match name {
        Some(x) => x,
        None => "storage".to_string(),
    };
    let parent: Option<&'a table::Table> = match parent {
        Some(x) => Some(&x.clone()),
        None => None,
    };
    let mut table: table::Table = table::Table::new(parent.clone(), name);
    match indices {
        Some(indices) => table.set_indices(flatten(tables, &table, &indices)),
        None => (),
    }
    table.set_columns(flatten(tables, &table, &columns));
    tables.push(table.clone());
}

fn main() {
    let s = include_str!("../test/storage1.tz");
    let mut tables: TableVec = vec![];
    let _ast = match storage::storage::expr(s) {
        Ok(ast) => start_table(&mut tables, None, Some("storage".to_string()), None, ast),
        Err(e) => println!("{:?}", e),
    };
}
