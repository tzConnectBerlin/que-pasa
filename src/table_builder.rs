use crate::err;
use crate::error::Res;
use crate::node::{Node, Type};
use crate::storage::{ComplexExpr, Expr};
use crate::table::Table;
use std::collections::HashMap;

pub type TableMap = HashMap<String, Table>;

pub struct TableBuilder {
    pub tables: TableMap,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            tables: TableMap::new(),
        }
    }

    fn add_column(&mut self, node: &Node) {
        let mut table = self.get_table(node);
        table.add_column(node);
        self.store_table(table);
    }

    fn add_index(&mut self, node: &Node) {
        let mut table = self.get_table(node);
        table.add_index(node);
        self.store_table(table);
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

    pub fn populate(&mut self, node: &Node) -> Res<()> {
        let node = node.clone();
        match &node._type {
            Type::Pair => {
                let left = node.left.clone();
                self.populate(&*left.ok_or(err!("Left is None, node is {:?}", &node))?)?;
                self.populate(&*node.right.ok_or(err!("Right is None"))?)?;
            }
            Type::Table => {
                if let Some(left) = node.left {
                    self.populate(&left)?;
                }
                if let Some(right) = node.right {
                    self.populate(&right)?;
                }
            }
            Type::Column => self.add_column(&node),
            Type::OrEnumeration => {
                self.add_column(&node);
                if let Some(left) = node.left {
                    self.populate(&*left)?;
                }
                if let Some(right) = node.right {
                    self.populate(&*right)?;
                }
            }
            Type::Unit => (),
            Type::TableIndex => match node.expr {
                Expr::SimpleExpr(_) => self.add_index(&node),
                Expr::ComplexExpr(ref expr) => match expr {
                    ComplexExpr::Pair(_, _) => {
                        self.populate(&*node.left.ok_or(err!("Left is None"))?)?;
                        self.populate(&*node.right.ok_or(err!("Right is None"))?)?;
                    }
                    _ => panic!("Found unexpected structure in index: {:#?}", expr),
                },
            },
        }
        Ok(())
    }
}
