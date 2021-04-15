use crate::node::Node;
use crate::storage::{ComplexExpr, Expr, SimpleExpr};

use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct Column {
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
        println!("New table parent_name: {:?} name: {:?}", parent_name, name);
        let new_table = Self {
            parent_name,
            name,
            indices: vec![],
            columns: vec![],
        };
        println!("new table {:?}", new_table);
        new_table
    }

    fn name(&mut self, kind: &str, node: &mut Node) -> String {
        let name = match &node.name {
            Some(x) => x.clone(),
            None => format!("{}_{}{}", self.name, kind, self.columns.len()),
        };
        node.column_name = Some(name.clone());
        name
    }

    fn column_name(&mut self, node: &mut Node) -> String {
        self.name("col", node)
    }

    fn index_name(&mut self, node: &mut Node) -> String {
        self.name("idx", node)
    }

    pub fn add_index(&mut self, node: &mut Node) -> String {
        let e = node.expr.clone();
        match e {
            Expr::SimpleExpr(e) => {
                let name = match &node.name {
                    Some(x) => x.clone(),
                    None => self.index_name(node),
                };
                self.indices.push(name.clone());
                self.columns.push(Column {
                    name: name.clone(),
                    expr: e.clone(),
                });
                node.column_name = Some(name.clone());
                return name.clone();
            }
            Expr::ComplexExpr(_) => panic!("add_index called with ComplexExpr"),
        };
    }

    pub fn add_column(&mut self, node: &mut Node) -> String {
        match &node.expr {
            Expr::SimpleExpr(e) => {
                let expr = e.clone();
                let name = self.column_name(node);
                self.columns.push(Column {
                    name: name.clone(),
                    expr,
                });
                name
            }
            _ => panic!("add_column called with ComplexExpr {:?}", &node.expr),
        }
    }
}
