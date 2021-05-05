use crate::storage::{ComplexExpr, Ele, Expr, SimpleExpr};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

type Indexes = HashMap<String, u32>;

thread_local! {
    static INDEXES: RefCell<Indexes> = RefCell::new(HashMap::new());
}

fn get_index(_table_name: &String) -> u32 {
    let table_name = &String::from("foo"); // all tables have the same number space
    INDEXES.with(|indexes| {
        let x: u32 = match indexes.borrow_mut().get(table_name) {
            Some(x) => *x,
            None => 0,
        };
        indexes.borrow_mut().insert(table_name.clone(), x + 1);
        x
    })
}

fn get_table_name(name: Option<String>) -> String {
    match name {
        Some(s) => s,
        None => format!("table{}", get_index(&"table_names".to_string())),
    }
}

fn get_column_name(expr: &Expr) -> &str {
    match expr {
        Expr::ComplexExpr(_) => "",
        Expr::SimpleExpr(e) => match e {
            SimpleExpr::Address => "address",
            SimpleExpr::Bool => "bool",
            SimpleExpr::Bytes => "bytes",
            SimpleExpr::Int => "int",
            SimpleExpr::Nat => "nat",
            SimpleExpr::String => "string",
            SimpleExpr::Timestamp => "timestamp",
            SimpleExpr::Unit => "unit",
            _ => panic!("Unexpected type {:?}", e),
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Type {
    Pair,
    Table,
    TableIndex,
    Column,
    Unit,
    OrEnumeration,
}

#[derive(Clone, Debug)]
pub struct Context {
    table_name: String,
    _type: Type,
}

impl Context {
    pub fn init() -> Self {
        Context {
            table_name: "storage".to_string(),
            _type: Type::Table,
        }
    }

    pub fn name(&self, ele: &Ele) -> String {
        let index = get_index(&self.table_name);
        let name = get_column_name(&ele.expr);
        match self._type {
            Type::TableIndex => format!("idx_{}_{}", name, index),
            _ => format!("{}_{}", name, index),
        }
    }

    pub fn next(&self) -> Self {
        let ctx = self.clone();
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
        c
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Node {
    pub name: Option<String>,
    pub _type: Type,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub value: Option<String>,
    pub left: Option<Box<Node>>,
    pub right: Option<Box<Node>>,
    pub expr: Expr,
}

impl fmt::Debug for Node {
    // to stop it recursing into the Expr type
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("name", &self.name)
            .field("_type", &self._type)
            .field("table_name", &self.table_name)
            .field("column_name", &self.column_name)
            .field("value", &self.value)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl Node {
    pub fn new(c: &Context, ele: &Ele) -> Self {
        let name = match &ele.name {
            Some(e) => Some(e.clone()),
            None => Some(c.name(ele)),
        };
        Self {
            name: name.clone(),
            _type: c._type,
            table_name: Some(c.table_name.clone()),
            column_name: name,
            value: None,
            left: None,
            right: None,
            expr: ele.expr.clone(),
        }
    }

    pub fn build(mut context: Context, ele: Ele) -> Node {
        let name = match &ele.name {
            Some(x) => x.clone(),
            None => "noname".to_string(),
        };
        let node: Node = match ele.expr {
            Expr::ComplexExpr(ref e) => match e {
                ComplexExpr::BigMap(key, value) | ComplexExpr::Map(key, value) => {
                    let context = context.start_table(get_table_name(Some(name)));
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
                    n.right = Some(Box::new(Self::build(context.next(), (**right).clone())));
                    n
                }
                ComplexExpr::Option(_inner_expr) => Self::build(context, (**_inner_expr).clone()),
                ComplexExpr::OrEnumeration(_this, _that) => {
                    context._type = Type::OrEnumeration;
                    Self::build_enumeration_or(&mut context, &ele, &name)
                }
            },
            Expr::SimpleExpr(_) => {
                context._type = Type::Column;
                Self::new(&context, &ele)
            }
        };
        node
    }

    pub fn build_enumeration_or(context: &mut Context, ele: &Ele, column_name: &String) -> Node {
        let mut node = Self::new(context, ele);
        node.name = Some(column_name.clone());
        node.column_name = Some(column_name.clone());
        match ele.expr {
            Expr::SimpleExpr(e) => match e {
                SimpleExpr::Unit => {
                    debug!("Unit match: {:?}", e);
                    context._type = Type::Unit;
                    node.value = ele.name.clone();
                }
                _ => panic!("Wrong type {:?}", e),
            },
            Expr::ComplexExpr(ref e) => match e {
                ComplexExpr::OrEnumeration(this, that) => {
                    node._type = Type::OrEnumeration;
                    node.left = Some(Box::new(Self::build_enumeration_or(
                        context,
                        this,
                        column_name,
                    )));
                    node.right = Some(Box::new(Self::build_enumeration_or(
                        context,
                        that,
                        column_name,
                    )));
                }
                _ => panic!("Complicated or! {:#?}", ele),
            },
        }
        node
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
