use crate::storage::{ComplexExpr, Ele, Expr, SimpleExpr};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

type Indexes = HashMap<String, u32>;

thread_local! {
    static INDEXES: RefCell<Indexes> = RefCell::new(HashMap::new());
} // thread_local so unit tests can run in ||el

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
            SimpleExpr::Mutez => "int",
            SimpleExpr::Nat => "nat",
            SimpleExpr::String => "string",
            SimpleExpr::KeyHash => "string", // TODO: check this with the data
            SimpleExpr::Timestamp => "timestamp",
            SimpleExpr::Unit => "unit",
            SimpleExpr::Stop => "stop",
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
    pub table_name: String,
    prefix: String,
    _type: Type,
}

impl Context {
    pub fn init() -> Self {
        Context {
            table_name: "storage".to_string(),
            prefix: "".to_string(),
            _type: Type::Table,
        }
    }

    pub fn name(&self, ele: &Ele) -> String {
        let name = match &ele.name {
            Some(x) => x.to_string(),
            None => format!(
                "{}_{}",
                get_column_name(&ele.expr),
                get_index(&self.table_name),
            ),
        };
        let initial = format!(
            "{}{}{}",
            self.prefix,
            if self.prefix.len() == 0 { "" } else { "_" },
            name,
        );
        match self._type {
            Type::TableIndex => format!("idx_{}", initial),
            _ => format!("{}", initial),
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

    pub fn next_with_prefix(&self, prefix: Option<String>) -> Self {
        let mut c = self.next();
        if let Some(prefix) = prefix {
            c.prefix = prefix;
            // let sep = if self.prefix.len() == 0 { "" } else { "." };
            // c.prefix = format!("{}{}{}", self.prefix, sep, prefix);
        }
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
    pub fn new(ctx: &Context, ele: &Ele) -> Self {
        let name = ctx.name(ele);
        Self {
            name: Some(name.clone()),
            _type: ctx._type,
            table_name: Some(ctx.table_name.clone()),
            column_name: Some(name),
            value: None,
            left: None,
            right: None,
            expr: ele.expr.clone(),
        }
    }

    pub fn build(mut context: Context, ele: Ele, big_map_names: &mut Vec<String>) -> Node {
        let name = match &ele.name {
            Some(x) => x.clone(),
            None => "noname".to_string(),
        };
        let node: Node = match ele.expr {
            Expr::ComplexExpr(ref e) => match e {
                ComplexExpr::BigMap(key, value) | ComplexExpr::Map(key, value) => {
                    let context = context.start_table(get_table_name(Some(name.clone())));
                    let mut n = Self::new(&context, &ele);
                    n.left = Some(Box::new(Self::build_index(
                        context.next_with_state(Type::TableIndex),
                        (**key).clone(),
                    )));
                    n.right = Some(Box::new(Self::build(context, (**value).clone(), big_map_names)));
                    let table_name = n.table_name.clone();
                    match e {
                        ComplexExpr::BigMap(key, value) => {big_map_names.push(table_name.unwrap());}
                        _ => {}
                    }
                    n
                }
                ComplexExpr::Pair(left, right) => {
                    let mut n = Self::new(&context, &ele);
                    let mut context = context.next_with_prefix(ele.name);
                    context._type = Type::Pair;
                    n.left = Some(Box::new(Self::build(context.clone(), (**left).clone(), big_map_names)));
                    n.right = Some(Box::new(Self::build(context, (**right).clone(), big_map_names)));
                    n
                }
                ComplexExpr::Option(_inner_expr) => {
                    Self::build(context, Self::ele_with_annot(_inner_expr, ele.name), big_map_names)
                }
                ComplexExpr::OrEnumeration(_this, _that) => {
                    context._type = Type::OrEnumeration;
                    Self::build_enumeration_or(&mut context, &ele, &name, big_map_names)
                }
            },
            Expr::SimpleExpr(_) => {
                context._type = Type::Column;
                Self::new(&context, &ele)
            }
        };
        node
    }

    pub fn build_enumeration_or(context: &mut Context, ele: &Ele, column_name: &String, big_map_names: &mut Vec<String>) -> Node {
        let mut node = Self::new(context, ele);
        node.name = Some(column_name.clone());
        node.column_name = Some(column_name.clone());
        match ele.expr {
            Expr::SimpleExpr(SimpleExpr::Unit) => {
                context._type = Type::Column;
                node.value = ele.name.clone();
            }
            Expr::SimpleExpr(_) => {
                return Self::build(context.start_table(ele.name.clone().unwrap()), ele.clone(), big_map_names);
            }
            Expr::ComplexExpr(ref e) => match e {
                ComplexExpr::OrEnumeration(this, that) => {
                    node._type = Type::OrEnumeration;
                    node.left = Some(Box::new(Self::build_enumeration_or(
                        context,
                        this,
                        column_name,
                        big_map_names
                    )));
                    node.right = Some(Box::new(Self::build_enumeration_or(
                        context,
                        that,
                        column_name,
                        big_map_names
                    )));
                }
                _ => {
                    debug!("Starting table from OR with ele {:?}", ele);
                    return Self::build(
                        context.start_table(ele.name.clone().unwrap()),
                        ele.clone(),
                        big_map_names
                    );
                }
            },
        }
        node
    }

    fn ele_with_annot(ele: &Ele, annot: Option<String>) -> Ele {
        match &ele.name {
            Some(_) => ele.clone(),
            None => {
                let mut e = ele.clone();
                e.name = annot;
                e
            }
        }
    }

    pub fn build_index(mut context: Context, ele: Ele) -> Node {
        let node: Node = match ele.expr {
            Expr::ComplexExpr(ref e) => match e {
                ComplexExpr::BigMap(_, _) | ComplexExpr::Map(_, _) => {
                    panic!("Got a map where I expected an index");
                }
                ComplexExpr::Pair(left, right) => {
                    let ctx = context.next_with_prefix(ele.name.clone());
                    let mut n = Self::new(&context, &ele);
                    n.left = Some(Box::new(Self::build_index(ctx.next(), (**left).clone())));
                    n.right = Some(Box::new(Self::build_index(ctx, (**right).clone())));
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
    }}
