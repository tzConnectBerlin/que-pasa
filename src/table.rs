use crate::node::Node;
use crate::storage::{ComplexExpr, Expr, SimpleExpr};

use std::vec::Vec;

#[derive(Clone, Debug)]
struct Column {
    pub name: String,
    pub expr: SimpleExpr,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub parent_name: Option<String>,
    pub name: String,
    pub indices: Vec<String>,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(parent_name: Option<String>, name: String) -> Self {
        Self {
            parent_name,
            name,
            indices: vec![],
            columns: vec![],
        }
    }

    pub fn add_index(&mut self, node: &mut Node) {
        match node.expr {
            ComplexExpr => panic!("add_index called with ComplexExpr"),
            SimpleExpr => {
                self.indices.push(node.name.unwrap().clone());
                self.add_column(node);
            }
        };
    }

    pub fn add_column(&mut self, node: &mut Node) {}
}
