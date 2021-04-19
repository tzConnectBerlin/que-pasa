use crate::node::Node;
use crate::storage::{Expr, SimpleExpr};

use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub expr: SimpleExpr,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub indices: Vec<String>,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String) -> Self {
        let new_table = Self {
            name,
            indices: vec![],
            columns: vec![],
        };
        new_table
    }

    pub fn add_index(&mut self, node: &Node) {
        let node = node.clone();
        let name = node.name.unwrap();
        let e = node.expr.clone();
        match e {
            Expr::SimpleExpr(e) => {
                self.indices.push(name.clone());
                self.columns.push(Column {
                    name,
                    expr: e.clone(),
                });
            }
            Expr::ComplexExpr(e) => panic!("add_index called with ComplexExpr {:#?}", e),
        }
    }

    pub fn add_column(&mut self, node: &Node) {
        let node: Node = node.clone();
        let name = node.name.unwrap();
        match &node.expr {
            Expr::SimpleExpr(e) => {
                self.columns.push(Column {
                    name: name,
                    expr: e.clone(),
                });
            }
            _ => panic!("add_column called with ComplexExpr {:?}", &node.expr),
        }
    }
}
