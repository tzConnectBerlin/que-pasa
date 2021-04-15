use crate::storage::{ComplexExpr, Ele, Expr, SimpleExpr};

#[derive(Clone, Debug)]
pub struct Node {
    pub name: Option<String>,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub map_key: Option<Box<Node>>,
    pub map_value: Option<Box<Node>>,
    pub left: Option<Box<Node>>,
    pub right: Option<Box<Node>>,
    pub expr: Expr,
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

    pub fn flatten_indices(node: Box<Node>) -> Vec<Box<Node>> {
        let mut v: Vec<Box<Node>> = vec![];
        Self::flatten_indices2(node, &mut v);
        println!("Indices: {:?}", v);
        v
    }

    pub fn flatten_indices2(boxed_node: Box<Node>, v: &mut Vec<Box<Node>>) {
        let node = *boxed_node;
        match &node.expr {
            Expr::SimpleExpr(e) => v.push(Box::new(node)),
            Expr::ComplexExpr(e) => match e {
                ComplexExpr::Pair(_, _) => {
                    Self::flatten_indices2(node.left.unwrap(), v);
                    Self::flatten_indices2(node.right.unwrap(), v);
                }
                _ => panic!("Complex expr {:?} passed into flatten_indices()"),
            },
        }
    }

    pub fn build(ele: Ele) -> Node {
        let expr = ele.expr.clone();
        let name = ele.name.clone();
        let node: Node = match ele.expr {
            Expr::ComplexExpr(e) => match e {
                ComplexExpr::BigMap(key, value) | ComplexExpr::Map(key, value) => {
                    let mut n = Self::new(name, expr);
                    n.map_key = Some(Box::new(Self::build(*key)));
                    n.map_value = Some(Box::new(Self::build(*value)));
                    n
                }
                ComplexExpr::Pair(left, right) => {
                    let mut n = Self::new(name, expr);
                    n.left = Some(Box::new(Self::build(*left)));
                    n.right = Some(Box::new(Self::build(*right)));
                    n
                }
                ComplexExpr::Option(_inner_expr) => Self::new(name, expr),
                ComplexExpr::Or(_this, _that) => Self::new(name, expr),
            },
            Expr::SimpleExpr(_) => Self::new(name, expr),
        };
        node
    }
}
