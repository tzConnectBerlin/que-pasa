use crate::michelson::Value;
use crate::node::Node;
use crate::storage::{ComplexExpr, Expr, SimpleExpr};
use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub expr: SimpleExpr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub indices: Vec<String>,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String) -> Self {
        let new_table = Self {
            name,
            indices: vec!["_level".to_string()],
            columns: vec![],
        };
        new_table
    }

    pub fn add_index(&mut self, node: &Node) {
        let node = node.clone();
        let name = node.name.unwrap();
        let e = node.expr.clone();
        match e {
            Expr::SimpleExpr(e) => {
                self.indices.push(name.clone());
                self.columns.push(Column {
                    name,
                    expr: e.clone(),
                });
            }
            Expr::ComplexExpr(e) => panic!("add_index called with ComplexExpr {:#?}", e),
        }
    }

    pub fn add_column(&mut self, node: &Node) {
        let node: Node = node.clone();
        let name = node.name.unwrap();
        for column in self.columns.iter() {
            if column.name == name {
                return;
            }
        }
        match &node.expr {
            Expr::SimpleExpr(e) => {
                self.columns.push(Column {
                    name,
                    expr: e.clone(),
                });
            }
            Expr::ComplexExpr(ce) => match ce {
                ComplexExpr::OrEnumeration(_, _) => {
                    self.columns.push(Column {
                        name,
                        expr: SimpleExpr::Unit, // What will ultimately go in is a Unit
                    })
                }
                _ => panic!("add_column called with ComplexExpr {:?}", &node.expr),
            },
        }
    }
}

pub mod insert {
    use crate::table::Value;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

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

    pub type Inserts = BTreeMap<InsertKey, Insert>;

    lazy_static! { // TODO: clean this up.
        static ref INSERTS: Mutex<Inserts> = Mutex::new(BTreeMap::new());
    }

    pub fn add_insert(table_name: String, id: u32, fk_id: Option<u32>, columns: Vec<Column>) {
        debug!(
            "table::add_insert {}, {}, {:?}, {:?}",
            table_name, id, fk_id, columns
        );
        let inserts: &mut Inserts = &mut *INSERTS.lock().unwrap();
        inserts.insert(
            InsertKey {
                table_name: table_name.clone(),
                id,
            },
            Insert {
                table_name,
                id,
                fk_id,
                columns,
            },
        );
    }

    pub fn clear_inserts() {
        let inserts: &mut Inserts = &mut *INSERTS.lock().unwrap();
        inserts.clear();
    }

    pub fn add_column(
        table_name: String,
        id: u32,
        fk_id: Option<u32>,
        column_name: String,
        value: Value,
    ) {
        debug!(
            "add_column {}, {}, {:?}, {}, {:?}",
            table_name, id, fk_id, column_name, value
        );

        let mut insert = match get_insert(table_name.clone(), id, fk_id) {
            Some(x) => x,
            None => Insert {
                table_name: table_name.clone(),
                id,
                fk_id,
                columns: vec![],
            },
        };
        insert.columns.push(Column {
            name: column_name,
            value,
        });
        add_insert(table_name, id, fk_id, insert.columns.clone());
    }

    pub fn get_insert(table_name: String, id: u32, fk_id: Option<u32>) -> Option<Insert> {
        match (*INSERTS.lock().unwrap()).get(&InsertKey { table_name, id }) {
            Some(e) => {
                assert!(e.fk_id == fk_id);
                Some((*e).clone())
            }
            None => None,
        }
    }

    pub fn get_inserts() -> Inserts {
        (*INSERTS.lock().unwrap()).clone()
    }
}
