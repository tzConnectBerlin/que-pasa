use crate::storage::{Ele, Expr};

#[derive(Clone, Debug)]
pub struct Node {
    name: Option<String>,
    table_name: Option<String>,
    column_name: Option<String>,
    map_key: Option<Box<Node>>,
    map_value: Option<Box<Node>>,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
    expr: Expr,
}

impl Node {
    pub fn new(name: Option<String>, expr: Expr) -> Self {
        Self {
            name,
            expr,
            table_name: None,
            column_name: None,
            map_key: None,
            map_value: None,
            left: None,
            right: None,
        }
    }

    pub fn build(ele: Ele) -> Node {
        let expr = ele.expr.clone();
        let name = ele.name.clone();
        let node: Node = match ele.expr {
            Expr::BigMap(key, value) | Expr::Map(key, value) => {
                let mut n = Self::new(name, expr);
                n.map_key = Some(Box::new(Self::build(*key)));
                n.map_value = Some(Box::new(Self::build(*value)));
                n
            }
            Expr::Pair(left, right) => {
                let mut n = Self::new(name, expr);
                n.left = Some(Box::new(Self::build(*left)));
                n.right = Some(Box::new(Self::build(*right)));
                n
            }
            Expr::Option(_inner_expr) => Self::new(name, expr),
            Expr::Or(_this, _that) => Self::new(name, expr),
            _ => Self::new(name, expr),
        };
        node
    }
}
