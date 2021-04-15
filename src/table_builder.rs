use crate::node::Node;
use crate::storage::{ComplexExpr, Expr, SimpleExpr};
use crate::table::Table;

use std::collections::HashMap;

pub type Tables = HashMap<String, Table>;

pub struct TableBuilder<'a> {
    pub tables: &'a Tables,
    pub table: Table,
    pub name: String,
    pub inherited_indices: Vec<SimpleExpr>,
}

impl<'a> TableBuilder<'a> {
    pub fn new(tables: &'a mut Tables, parent_name: String, name: String) -> Self {
        Self {
            inherited_indices: vec![],
            name: if parent_name.len() != 0 {
                format!("{}.{}", parent_name, name)
            } else {
                name
            },
            table: Table::new(Some(parent_name), name),
            tables: tables,
        }
    }

    pub fn node(&mut self, node: &mut Box<Node>) {
        match node.expr {
            Expr::ComplexExpr(e) => {
                match e {
                    ComplexExpr::BigMap(left, right) => {
                        self.big_map(node.name.unwrap().clone(), &mut node.left, &mut node.right)
                    }
                    ComplexExpr::Map(left, right) => {
                        self.map(node.name.unwrap().clone(), &mut node.left, &mut node.right)
                    }
                    ComplexExpr::Or(_, _) => (),
                    ComplexExpr::Option(_) => (),
                    ComplexExpr::Pair(_, _) => {
                        self.node(&mut node.left.unwrap());
                        self.node(&mut node.right.unwrap());
                    }
                };
            }
            Expr::SimpleExpr(_) => (),
        }
    }

    pub fn big_map(
        &mut self,
        name: String,
        left: &mut Option<Box<Node>>,
        right: &mut Option<Box<Node>>,
    ) {
        let new_table = Self::new(&mut self.tables, self.name, name);
    }

    pub fn map(
        &mut self,
        name: String,
        left: &mut Option<Box<Node>>,
        right: &mut Option<Box<Node>>,
    ) {
        let new_table = Self::new(&mut self.tables, self.name, name);
    }
}
