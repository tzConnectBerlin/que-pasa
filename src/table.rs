use crate::michelson::Value;
use crate::node::Node;
use crate::storage::{Expr, SimpleExpr};

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub expr: SimpleExpr,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub indices: Vec<String>,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String) -> Self {
        let new_table = Self {
            name,
            indices: vec![],
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
        match &node.expr {
            Expr::SimpleExpr(e) => {
                self.columns.push(Column {
                    name: name,
                    expr: e.clone(),
                });
            }
            _ => panic!("add_column called with ComplexExpr {:?}", &node.expr),
        }
    }
}

pub mod insert {
    use crate::table::Value;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    pub struct InsertKey {
        pub table_name: String,
        pub id: u32,
    }

    #[derive(Clone, Debug)]
    pub struct Column {
        pub name: String,
        pub value: Value,
    }

    #[derive(Clone, Debug)]
    pub struct Insert {
        pub table_name: String,
        pub id: u32,
        pub fk_id: Option<u32>,
        pub columns: Vec<Column>,
    }

    pub type Inserts = HashMap<InsertKey, Insert>;

    lazy_static! {
        static ref INSERTS: Mutex<Inserts> = Mutex::new(HashMap::new());
    }

    pub fn add_insert(table_name: String, id: u32, fk_id: Option<u32>, columns: Vec<Column>) {
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

    pub fn add_column(
        table_name: String,
        id: u32,
        fk_id: Option<u32>,
        column_name: String,
        value: Value,
    ) {
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
