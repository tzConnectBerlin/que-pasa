use crate::storage_structure::relational::RelationalEntry;
use crate::storage_structure::typing::{ComplexExprTy, ExprTy, SimpleExprTy};
use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: SimpleExprTy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub indices: Vec<String>,
    pub columns: Vec<Column>,
    unique: bool,
}

impl Table {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            indices: vec!["tx_context_id".to_string()],
            columns: vec![],
            unique: true,
        }
    }

    pub(crate) fn add_index(&mut self, rel_entry: &RelationalEntry) {
        let name = rel_entry.column_name.clone();
        match &rel_entry.column_type {
            ExprTy::SimpleExprTy(e) => {
                self.indices.push(name.clone());
                self.columns.push(Column {
                    name,
                    column_type: *e,
                });
            }
            ExprTy::ComplexExprTy(e) => {
                panic!("add_index called with ComplexExprTy {:#?}", e)
            }
        }
    }

    pub(crate) fn has_uniqueness(&self) -> bool {
        self.unique
    }

    pub(crate) fn no_uniqueness(&mut self) {
        self.unique = false
    }

    pub(crate) fn add_column(&mut self, rel_entry: &RelationalEntry) {
        let name = rel_entry.column_name.clone();
        if self
            .columns
            .iter()
            .any(|column| column.name == name)
        {
            return;
        }
        match &rel_entry.column_type {
            ExprTy::SimpleExprTy(e) => {
                self.columns.push(Column {
                    name,
                    column_type: *e,
                });
            }
            ExprTy::ComplexExprTy(ce) => match ce {
                ComplexExprTy::OrEnumeration(_, _) => {
                    self.columns.push(Column {
                        name,
                        column_type: SimpleExprTy::Unit, // What will ultimately go in is a Unit
                    })
                }
                _ => panic!(
                    "add_column called with ComplexExprTy {:?}",
                    &rel_entry.column_type
                ),
            },
        }
    }
}
