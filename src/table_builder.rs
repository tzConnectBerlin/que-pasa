use crate::node::Node;
use crate::storage::{ComplexExpr, Expr, SimpleExpr};
use crate::table::Table;

use std::collections::HashMap;

pub type Tables = HashMap<String, Table>;

pub struct TableBuilder {
    pub tables: Box<Tables>,
    pub table: Table,
    pub name: String,
    pub inherited_indices: Vec<SimpleExpr>,
}

impl TableBuilder {
    pub fn new(tables: Box<Tables>, parent_name: String, name: String) -> Self {
        println!("New table! {}", name);
        let inherited_indices = match tables.get(&parent_name) {
            Some(x) => x.indices.clone(),
            None => vec![],
        };
        Self {
            inherited_indices: vec![],
            name: if parent_name.len() > 0 {
                format!("{}.{}", parent_name, name)
            } else {
                name.clone()
            },
            table: Table::new(Some(parent_name), name),
            tables: tables,
        }
    }

    pub fn node(&mut self, mut boxed_node: &mut Box<Node>) -> Box<Node> {
        let mut node = *(boxed_node.clone());
        println!("Processing {:#?}", node);
        println!("Tables: {:#?}", self.tables);
        node.table_name = Some(self.table.name.clone());
        match &node.expr {
            Expr::ComplexExpr(e) => {
                match e {
                    ComplexExpr::BigMap(left, right) | ComplexExpr::Map(left, right) => {
                        self.map(&boxed_node);
                    }
                    ComplexExpr::Or(_, _) => (),
                    ComplexExpr::Option(_) => (),
                    ComplexExpr::Pair(_, _) => {
                        let n = node.clone();
                        self.node(&mut n.left.unwrap());
                        self.node(&mut n.right.unwrap());
                    }
                };
            }
            Expr::SimpleExpr(e) => self.simple_expr(&mut node),
        }
        self.tables
            .insert(self.table.name.clone(), self.table.clone());
        **boxed_node = node.clone();
        Box::new(node)
    }

    pub fn insert_current_table(&mut self) {
        self.tables
            .insert(self.table.name.clone(), self.table.clone());
    }

    pub fn map(&mut self, mut boxed_node: &Box<Node>) {
        println!("MAP****************************");
        let node = &*boxed_node;
        println!("map: {:#?}", node);
        let name = match &node.name {
            Some(x) => x.clone(),
            None => "??????".to_string(),
        };
        let indices = match &node.map_key {
            Some(x) => Node::flatten_indices(x.clone()),
            None => vec![],
        };
        for mut index in indices {
            self.table.add_index(&mut index);
        }
        let mut new_builder = Self::new(self.tables.clone(), self.table.name.clone(), name);
        new_builder.node(&mut node.clone());
        new_builder.insert_current_table();
    }

    pub fn simple_expr(&mut self, node: &mut Node) {
        let column_name = match &node.expr {
            Expr::SimpleExpr(e) => self.table.add_column(node),
            _ => {
                panic!("Simple Expression expected, got {:?}", &node.expr);
                "".to_string()
            }
        };
        node.column_name = Some(column_name);
    }
}
