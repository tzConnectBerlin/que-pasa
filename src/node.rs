use crate::storage::{ComplexExpr, Ele, Expr};
use std::collections::HashMap;
use std::sync::Mutex;

type Indexes = HashMap<String, u32>;

lazy_static! {
    static ref INDEXES: Mutex<Indexes> = Mutex::new(HashMap::new());
}

fn get_index(table_name: &String) -> u32 {
    let indexes: &mut Indexes = &mut *INDEXES.lock().unwrap();
    let x: u32 = match indexes.get(table_name) {
        Some(x) => *x,
        None => 0,
    };
    indexes.insert(table_name.clone(), x + 1);
    x
}

#[derive(Clone, Copy, Debug)]
pub enum Type {
    Pair,
    Table,
    TableIndex,
    Column,
}

#[derive(Clone, Debug)]
pub struct Context {
    column_index: u32,
    table_name: String,
    _type: Type,
}

impl Context {
    pub fn init() -> Self {
        Context {
            column_index: 0,
            table_name: "storage".to_string(),
            _type: Type::Table,
        }
    }

    pub fn name(&self) -> String {
        let index = get_index(&self.table_name);
        match self._type {
            Type::TableIndex => format!("idx{}", index),
            _ => format!("col{}", index),
        }
    }

    pub fn next(&self) -> Self {
        let mut ctx = self.clone();
        ctx.column_index += 1;
        ctx
    }

    pub fn next_with_state(&self, new_state: Type) -> Self {
        let mut c = self.next();
        c._type = new_state;
        c
    }

    pub fn start_table(&self, name: String) -> Self {
        let mut c = self.next_with_state(Type::Table);
        c.table_name = format!("{}.{}", self.table_name, name);
        c.column_index = 0;
        c
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub name: Option<String>,
    pub _type: Type,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub left: Option<Box<Node>>,
    pub right: Option<Box<Node>>,
    pub expr: Expr,
}

impl Node {
    pub fn new(c: &Context, ele: &Ele) -> Self {
        let name = match c._type {
            Type::Pair => None,
            _ => match &ele.name {
                Some(e) => Some(e.clone()),
                None => Some(c.name()),
            },
        };
        Self {
            name: name.clone(),
            _type: c._type,
            table_name: Some(c.table_name.clone()),
            column_name: name,
            left: None,
            right: None,
            expr: ele.expr.clone(),
        }
    }

    pub fn build(mut context: Context, ele: Ele) -> Node {
        let name = ele.name.clone();
        let node: Node = match ele.expr {
            Expr::ComplexExpr(ref e) => match e {
                ComplexExpr::BigMap(key, value) | ComplexExpr::Map(key, value) => {
                    let context = context.start_table(name.unwrap());
                    let mut n = Self::new(&context, &ele);
                    n.left = Some(Box::new(Self::build_index(
                        context.next_with_state(Type::TableIndex),
                        (**key).clone(),
                    )));
                    n.right = Some(Box::new(Self::build(context, (**value).clone())));
                    n
                }
                ComplexExpr::Pair(left, right) => {
                    context._type = Type::Pair;
                    let mut n = Self::new(&context, &ele);
                    n.left = Some(Box::new(Self::build(context.next(), (**left).clone())));
                    n.right = Some(Box::new(Self::build(context.next(), (**right).clone()))); // TODO: Make the count work
                    n
                }
                ComplexExpr::Option(_inner_expr) => Self::build(context, (**_inner_expr).clone()),
                ComplexExpr::Or(_this, _that) => Self::build_or(&mut context, &ele),
            },
            Expr::SimpleExpr(_) => {
                context._type = Type::Column;
                Self::new(&context, &ele)
            }
        };
        node
    }

    pub fn build_or(context: &mut Context, ele: &Ele) -> Node {
        match ele.expr {
            Expr::SimpleExpr(_) => {
                context._type = Type::Column;
                Self::new(context, ele)
            }
            Expr::ComplexExpr(ref e) => match e {
                ComplexExpr::Or(_this, _that) => Self::build_or(context, _this),
                _ => panic!("Complicated or! {:#?}", ele),
            },
        }
    }

    pub fn build_index(mut context: Context, ele: Ele) -> Node {
        let node: Node = match ele.expr {
            Expr::ComplexExpr(ref e) => match e {
                ComplexExpr::BigMap(_, _) | ComplexExpr::Map(_, _) => {
                    panic!("Got a map where I expected an index");
                }
                ComplexExpr::Pair(left, right) => {
                    let mut n = Self::new(&context, &ele);
                    n.left = Some(Box::new(Self::build_index(
                        context.next(),
                        (**left).clone(),
                    )));
                    n.right = Some(Box::new(Self::build_index(
                        context.next(),
                        (**right).clone(),
                    )));
                    n
                }
                _ => panic!("Unexpected input to index"),
            },
            Expr::SimpleExpr(_) => {
                context._type = Type::TableIndex;
                Self::new(&context, &ele)
            }
        };
        node
    }
}
