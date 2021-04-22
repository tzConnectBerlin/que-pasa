use crate::node::{Node, Type};
use crate::storage::{ComplexExpr, Expr};
use crate::table::Table;

use std::collections::HashMap;

pub type TableMap = HashMap<String, Table>;

macro_rules! add_column {
    ($self: expr, $node:expr) => {{
        let mut table = $self.get_table(&$node);
        table.add_column(&$node);
        $self.store_table(table);
    }};
}

pub struct TableBuilder {
    pub tables: TableMap,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            tables: TableMap::new(),
        }
    }

    fn get_table(&self, node: &Node) -> Table {
        let name = node.clone().table_name.unwrap();
        match self.tables.get(&name) {
            Some(x) => x.clone(),
            None => Table::new(name),
        }
    }

    fn store_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    pub fn populate(&mut self, node: &Node) {
        let foo = node.clone();
        let node = node.clone();
        match node._type {
            Type::Pair => {
                self.populate(&node.left.expect(&format!("got pair {:#?}", foo)).clone());
                self.populate(&node.right.unwrap().clone());
            }
            Type::Table => {
                self.populate(&node.left.expect(&format!("got pair {:#?}", foo)).clone());
                self.populate(&node.right.unwrap().clone());
            }
            Type::Column => add_column!(self, node),
            Type::OrEnumeration => (),
            Type::Unit => add_column!(self, node),
            Type::TableIndex => match node.expr {
                Expr::SimpleExpr(_) => add_column!(self, node),
                Expr::ComplexExpr(ref expr) => match expr {
                    ComplexExpr::Pair(_, _) => {
                        self.populate(&node.left.unwrap());
                        self.populate(&node.right.unwrap());
                    }
                    _ => panic!("Found unexpected structure in index: {:#?}", expr),
                },
            },
        }
    }
}
