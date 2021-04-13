use crate::storage::Expr;

use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct Table {
    pub parent_name: Option<String>,
    pub name: String,
    pub indices: Vec<Expr>,
    pub columns: Vec<Expr>,
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

    pub fn set_indices(&mut self, indices: Vec<Expr>) {
        self.indices = indices;
    }

    pub fn set_columns(&mut self, columns: Vec<Expr>) {
        self.columns = columns;
    }

    pub fn add_index(&mut self, index: &Expr) {
        self.columns.push(index.clone());
        self.indices.push(index.clone());
    }

    pub fn add_column(&mut self, column: &Expr) {
        self.columns.push(column.clone());
    }
}
