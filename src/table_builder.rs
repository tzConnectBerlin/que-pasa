use crate::err;
use crate::error::Res;
use crate::relational::{RelationalAST, Type};
use crate::storage::{ComplexExpr, Expr};
use crate::table::Table;
use std::collections::HashMap;

pub type TableMap = HashMap<String, Table>;

pub struct TableBuilder {
    pub tables: TableMap,
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TableBuilder {
    pub(crate) fn new() -> Self {
        Self {
            tables: TableMap::new(),
        }
    }

    fn add_column(&mut self, rel_ast: &RelationalAST) {
        let mut table = self.get_table(rel_ast);
        table.add_column(rel_ast);
        self.store_table(table);
    }

    fn add_index(&mut self, rel_ast: &RelationalAST) {
        let mut table = self.get_table(rel_ast);
        table.add_index(rel_ast);
        self.store_table(table);
    }

    fn get_table(&self, rel_ast: &RelationalAST) -> Table {
        let name = rel_ast.clone().table_name.unwrap();
        match self.tables.get(&name) {
            Some(x) => x.clone(),
            None => Table::new(name),
        }
    }

    fn store_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    pub(crate) fn populate(&mut self, rel_ast: &RelationalAST) -> Res<()> {
        let rel_ast = rel_ast.clone();
        match &rel_ast._type {
            Type::Pair => {
                let left = rel_ast.left.clone();
                self.populate(
                    &*left.ok_or_else(|| err!("Left is None, rel_ast is {:?}", &rel_ast))?,
                )?;
                self.populate(&*rel_ast.right.ok_or_else(|| err!("Right is None"))?)?;
            }
            Type::Table => {
                //if the table is a bigmap the name is used to be inserted in the database
                if let Some(left) = rel_ast.left {
                    self.populate(&left)?;
                }
                if let Some(right) = rel_ast.right {
                    self.populate(&right)?;
                }
            }
            Type::Column => self.add_column(&rel_ast),
            Type::OrEnumeration => {
                self.add_column(&rel_ast);
                if let Some(left) = rel_ast.left {
                    self.populate(&*left)?;
                }
                if let Some(right) = rel_ast.right {
                    self.populate(&*right)?;
                }
            }
            Type::Unit => (),
            Type::TableIndex => match rel_ast.expr {
                Expr::SimpleExpr(_) => self.add_index(&rel_ast),
                Expr::ComplexExpr(ref expr) => match expr {
                    ComplexExpr::Pair(_, _) => {
                        self.populate(&*rel_ast.left.ok_or_else(|| err!("Left is None"))?)?;
                        self.populate(&*rel_ast.right.ok_or_else(|| err!("Right is None"))?)?;
                    }
                    _ => panic!("Found unexpected structure in index: {:#?}", expr),
                },
            },
        }
        Ok(())
    }
}
