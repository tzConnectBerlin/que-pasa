use crate::michelson::Value;
use crate::relational::RelationalEntry;
use crate::storage::{ComplexExprTy, ExprTy, SimpleExprTy};
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
}

impl Table {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            indices: vec!["tx_context_id".to_string()],
            columns: vec![],
        }
    }

    pub(crate) fn add_index(&mut self, rel_entry: &RelationalEntry) {
        let name = rel_entry.column_name.clone();
        match &rel_entry.column_type {
            ExprTy::SimpleExprTy(e) => {
                self.indices.push(name.clone());
                self.columns.push(Column {
                    name,
                    column_type: e.clone(),
                });
            }
            ExprTy::ComplexExprTy(e) => panic!("add_index called with ComplexExprTy {:#?}", e),
        }
    }

    pub(crate) fn add_column(&mut self, rel_entry: &RelationalEntry) {
        let name = rel_entry.column_name.clone();
        if self.columns.iter().any(|column| column.name == name) {
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

pub mod insert {
    use crate::table::Value;
    use std::collections::BTreeMap;

    #[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
    pub struct InsertKey {
        pub table_name: String,
        pub id: u32,
    }

    impl std::cmp::Ord for InsertKey {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            format!("{}{}", other.table_name, other.id)
                .cmp(&format!("{}{}", self.table_name, self.id))
        }
    }

    impl PartialOrd for InsertKey {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    //Change name for more clarity?
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct Column {
        pub name: String,
        pub value: Value,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct Insert {
        pub table_name: String,
        pub id: u32,
        pub fk_id: Option<u32>,
        pub columns: Vec<Column>,
    }

    impl Insert {
        #[cfg(test)]
        pub fn get_column(&self, name: &str) -> Option<&Column> {
            self.columns.iter().find(|column| column.name == name)
        }
    }

    pub type Inserts = BTreeMap<InsertKey, Insert>;
}
