use crate::storage_structure::typing::{ComplexExprTy, ExprTy, SimpleExprTy};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: SimpleExprTy,
    pub is_keyword: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub indices: Vec<String>,
    pub columns: HashMap<String, Column>,
    unique: bool,
    snapshots: bool,
}

impl Table {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            indices: vec![],
            columns: HashMap::new(),
            unique: true,
            snapshots: true,
        }
    }

    pub(crate) fn has_uniqueness(&self) -> bool {
        self.unique
    }

    pub(crate) fn no_uniqueness(&mut self) {
        self.unique = false
    }

    pub(crate) fn tracks_changes(&mut self) {
        self.snapshots = false
    }

    pub(crate) fn contains_snapshots(&self) -> bool {
        self.snapshots
    }

    fn resolve_keyword_conflict(
        &mut self,
        is_keyword: bool,
        name: &mut String,
    ) -> bool {
        if let Some(mut other) = self.columns.get(name).cloned() {
            if other.is_keyword == is_keyword {
                return false;
            }

            let non_keyword_name = format!(".{}", name);
            if !is_keyword {
                *name = non_keyword_name;
            } else {
                other.name = non_keyword_name.clone();
                self.columns
                    .insert(non_keyword_name, other);
            }
        }
        true
    }

    pub(crate) fn add_column(
        &mut self,
        is_keyword: bool,
        column_name: &str,
        column_type: &ExprTy,
    ) {
        let mut name = column_name.to_string();
        let is_new = self.resolve_keyword_conflict(is_keyword, &mut name);
        if !is_new {
            return;
        }

        match column_type {
            ExprTy::SimpleExprTy(e) => {
                self.columns.insert(
                    name.clone(),
                    Column {
                        name,
                        column_type: *e,
                        is_keyword,
                    },
                );
            }
            ExprTy::ComplexExprTy(ce) => match ce {
                ComplexExprTy::OrEnumeration(_, _) => {
                    self.columns.insert(
                        name.clone(),
                        Column {
                            name,
                            column_type: SimpleExprTy::Unit, // What will ultimately go in is a Unit
                            is_keyword,
                        },
                    );
                }
                _ => panic!(
                    "add_column called with ComplexExprTy {:?}",
                    column_type
                ),
            },
        }
    }

    pub(crate) fn add_index(
        &mut self,
        is_keyword: bool,
        column_name: &str,
        column_type: &ExprTy,
    ) {
        let mut name = column_name.to_string();
        let is_new = self.resolve_keyword_conflict(is_keyword, &mut name);
        if !is_new {
            return;
        }

        match column_type {
            ExprTy::SimpleExprTy(e) => {
                self.indices.push(name.clone());
                self.columns.insert(
                    name.clone(),
                    Column {
                        name,
                        column_type: *e,
                        is_keyword,
                    },
                );
            }
            ExprTy::ComplexExprTy(e) => {
                panic!("add_index called with ComplexExprTy {:#?}", e)
            }
        }
    }

    pub(crate) fn get_columns(&self) -> Vec<&Column> {
        self.columns.values().collect()
    }
}
