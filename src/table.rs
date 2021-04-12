use crate::storage::Expr;

use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct Table<'a> {
    pub parent: Option<&'a Table<'a>>,
    pub name: String,
    pub indices: Vec<Expr>,
    pub columns: Vec<Expr>,
}

impl<'a> Table<'a> {
    pub fn new(parent: Option<&'a Table<'a>>, name: String) -> Self {
        Self {
            parent,
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
