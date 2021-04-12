use crate::storage::Expr;

use std::vec::Vec;

pub struct Table<'a> {
    parent: Option<&'a Table<'a>>,
    name: String,
    indices: Vec<Expr>,
    columns: Vec<Expr>,
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
}
