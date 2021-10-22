use crate::storage_structure::typing::{ComplexExprTy, ExprTy, SimpleExprTy};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: SimpleExprTy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub indices: Vec<String>,
    pub columns: HashMap<String, Column>,
    pub fk: HashMap<(String, String, String), ()>,
    pub id_unique: bool,
    keys: Vec<String>,
    unique: bool,
    snapshots: bool,
}

impl Table {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            indices: vec![],
            columns: HashMap::new(),
            keys: vec![],
            unique: true,
            snapshots: true,
            fk: HashMap::new(),
            id_unique: true,
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

    pub(crate) fn add_fk(
        &mut self,
        column_name: String,
        ref_table: String,
        ref_col: String,
    ) {
        self.fk
            .insert((column_name, ref_table, ref_col), ());
    }

    pub(crate) fn add_column(
        &mut self,
        column_name: &str,
        column_type: &ExprTy,
    ) {
        if self.columns.contains_key(column_name) {
            return;
        }

        let name = column_name.to_string();
        match column_type {
            ExprTy::SimpleExprTy(e) => {
                if !self.columns.contains_key(&name) {
                    self.keys.push(name.clone());
                }
                self.columns.insert(
                    name.clone(),
                    Column {
                        name,
                        column_type: *e,
                    },
                );
            }
            ExprTy::ComplexExprTy(ce) => match ce {
                ComplexExprTy::OrEnumeration(_, _) => {
                    if !self.columns.contains_key(&name) {
                        self.keys.push(name.clone());
                    }
                    self.columns.insert(
                        name.clone(),
                        Column {
                            name,
                            column_type: SimpleExprTy::Unit, // What will ultimately go in is a Unit
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
        column_name: &str,
        column_type: &ExprTy,
    ) {
        if self.columns.contains_key(column_name) {
            return;
        }

        let name = column_name.to_string();
        match column_type {
            ExprTy::SimpleExprTy(e) => {
                if !self.columns.contains_key(&name) {
                    self.indices.push(name.clone());
                    self.keys.push(name.clone());
                }
                self.columns.insert(
                    name.clone(),
                    Column {
                        name,
                        column_type: *e,
                    },
                );
            }
            ExprTy::ComplexExprTy(e) => {
                panic!("add_index called with ComplexExprTy {:#?}", e)
            }
        }
    }

    pub(crate) fn get_columns(&self) -> Vec<&Column> {
        let mut res: Vec<&Column> = vec![];
        for k in &self.keys {
            res.push(&self.columns[k]);
        }
        res
    }

    pub(crate) fn drop_column(&mut self, name: &str) {
        if self.columns.remove(name).is_some() {
            self.keys = self
                .keys
                .clone()
                .into_iter()
                .filter(|k| k != name)
                .collect::<Vec<String>>();

            self.drop_index(name);
        }
    }

    pub(crate) fn drop_index(&mut self, name: &str) {
        self.indices = self
            .indices
            .clone()
            .into_iter()
            .filter(|k| k != name)
            .collect::<Vec<String>>();
    }

    pub(crate) fn keywords(&self) -> Vec<String> {
        let mut res = vec!["id".to_string(), "tx_context_id".to_string()];
        if !self.contains_snapshots() {
            res.push("deleted".to_string());
            res.push("bigmap_id".to_string());
        }
        res
    }
}
