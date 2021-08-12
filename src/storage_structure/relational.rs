use crate::storage_structure::typing::{ComplexExprTy, Ele, ExprTy, SimpleExprTy};
use std::collections::HashMap;

pub type Indexes = HashMap<String, u32>;

fn get_index(indexes: &mut Indexes, _table_name: &str) -> u32 {
    let table_name = &String::from("foo"); // all tables have the same number space
    let x: u32 = match indexes.get(table_name) {
        Some(x) => *x,
        None => 0,
    };
    indexes.insert(table_name.clone(), x + 1);
    debug!("x={}", x);
    x
}

fn get_table_name(indexes: &mut Indexes, name: Option<String>) -> String {
    match name {
        Some(s) => s,
        None => format!("table{}", get_index(indexes, &"table_names".to_string())),
    }
}

fn get_column_name(expr: &ExprTy) -> &str {
    match expr {
        ExprTy::ComplexExprTy(_) => "",
        ExprTy::SimpleExprTy(e) => match e {
            SimpleExprTy::Address => "address",
            SimpleExprTy::Bool => "bool",
            SimpleExprTy::Bytes => "bytes",
            SimpleExprTy::Int => "int",
            SimpleExprTy::Mutez => "int",
            SimpleExprTy::Nat => "nat",
            SimpleExprTy::String => "string",
            SimpleExprTy::KeyHash => "string", // TODO: check this with the data
            SimpleExprTy::Timestamp => "timestamp",
            SimpleExprTy::Unit => "unit",
            SimpleExprTy::Stop => "stop",
        },
    }
}

#[derive(Clone, Debug)]
pub struct Context {
    pub table_name: String,
    prefix: String,
}

impl Context {
    pub(crate) fn init() -> Self {
        Context {
            table_name: "storage".to_string(),
            prefix: "".to_string(),
        }
    }

    pub(crate) fn name(&self, ele: &Ele, indexes: &mut Indexes, is_index: bool) -> String {
        let name = match &ele.name {
            Some(x) => x.to_string(),
            None => format!(
                "{}_{}",
                get_column_name(&ele.expr_type),
                get_index(indexes, &self.table_name),
            ),
        };
        let initial = format!(
            "{}{}{}",
            self.prefix,
            if self.prefix.is_empty() { "" } else { "_" },
            name,
        );
        if is_index {
            return format!("idx_{}", initial);
        }
        initial
    }

    pub(crate) fn next(&self) -> Self {
        self.clone()
    }

    pub(crate) fn next_with_prefix(&self, prefix: Option<String>) -> Self {
        let mut c = self.next();
        if let Some(pre) = prefix {
            c.prefix = pre;
        }
        c
    }

    pub(crate) fn start_table(&self, name: String) -> Self {
        let mut c = self.next();
        c.table_name = format!("{}.{}", self.table_name, name);
        c
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum RelationalAST {
    Pair {
        left_ast: Box<RelationalAST>,
        right_ast: Box<RelationalAST>,
    },
    OrEnumeration {
        or_unfold: RelationalEntry,
        left_table: String,
        left_ast: Box<RelationalAST>,
        right_table: String,
        right_ast: Box<RelationalAST>,
    },
    Map {
        table: String,
        key_ast: Box<RelationalAST>,
        value_ast: Box<RelationalAST>,
    },
    BigMap {
        table: String,
        key_ast: Box<RelationalAST>,
        value_ast: Box<RelationalAST>,
    },
    List {
        table: String,
        elems_ast: Box<RelationalAST>,
    },
    Leaf {
        rel_entry: RelationalEntry,
    },
}

impl RelationalAST {
    pub fn table_entry(&self) -> Option<String> {
        match self {
            RelationalAST::BigMap { table, .. }
            | RelationalAST::Map { table, .. }
            | RelationalAST::List { table, .. } => Some(table.clone()),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RelationalEntry {
    pub table_name: String,
    pub column_name: String,
    pub column_type: ExprTy,
    pub value: Option<String>,
    pub is_index: bool,
}

pub(crate) fn build_relational_ast(
    ctx: &Context,
    ele: &Ele,
    indexes: &mut Indexes,
) -> RelationalAST {
    let name = match &ele.name {
        Some(x) => x.clone(),
        None => "noname".to_string(),
    };
    match ele.expr_type {
        ExprTy::ComplexExprTy(ref expr_type) => match expr_type {
            ComplexExprTy::Pair(left_type, right_type) => {
                let ctx = &ctx.next_with_prefix(ele.name.clone());
                let left = build_relational_ast(ctx, left_type, indexes);
                let right = build_relational_ast(ctx, right_type, indexes);
                RelationalAST::Pair {
                    left_ast: Box::new(left),
                    right_ast: Box::new(right),
                }
            }
            ComplexExprTy::List(elems_type) => {
                let ctx = &ctx.start_table(get_table_name(indexes, Some(name)));
                let elems_ast = build_relational_ast(ctx, elems_type, indexes);
                RelationalAST::List {
                    table: ctx.table_name.clone(),
                    elems_ast: Box::new(elems_ast),
                }
            }
            ComplexExprTy::BigMap(key_type, value_type) => {
                let ctx = &ctx.start_table(get_table_name(indexes, Some(name)));
                let key_ast = build_index(ctx, key_type, indexes);
                let value_ast = build_relational_ast(ctx, value_type, indexes);
                RelationalAST::BigMap {
                    table: ctx.table_name.clone(),
                    key_ast: Box::new(key_ast),
                    value_ast: Box::new(value_ast),
                }
            }
            ComplexExprTy::Map(key_type, value_type) => {
                let ctx = &ctx.start_table(get_table_name(indexes, Some(name)));
                let key_ast = build_index(ctx, key_type, indexes);
                let value_ast = build_relational_ast(ctx, value_type, indexes);
                RelationalAST::Map {
                    table: ctx.table_name.clone(),
                    key_ast: Box::new(key_ast),
                    value_ast: Box::new(value_ast),
                }
            }
            ComplexExprTy::Option(expr_type) => {
                build_relational_ast(ctx, &ele_with_annot(expr_type, ele.name.clone()), indexes)
            }
            ComplexExprTy::OrEnumeration(_, _) => build_enumeration_or(ctx, ele, &name, indexes).0,
        },
        ExprTy::SimpleExprTy(_) => RelationalAST::Leaf {
            rel_entry: RelationalEntry {
                table_name: ctx.table_name.clone(),
                column_name: ctx.name(ele, indexes, false),
                column_type: ele.expr_type.clone(),
                value: None,
                is_index: false,
            },
        },
    }
}

fn build_enumeration_or(
    ctx: &Context,
    ele: &Ele,
    column_name: &str,
    indexes: &mut Indexes,
) -> (RelationalAST, String) {
    match &ele.expr_type {
        ExprTy::ComplexExprTy(ComplexExprTy::OrEnumeration(left_type, right_type)) => {
            let (left_ast, left_table) = build_enumeration_or(ctx, left_type, column_name, indexes);
            let (right_ast, right_table) =
                build_enumeration_or(ctx, right_type, column_name, indexes);
            let rel_entry = RelationalEntry {
                table_name: ctx.table_name.clone(),
                column_name: column_name.to_string(),
                column_type: ele.expr_type.clone(),
                is_index: false,
                value: None,
            };
            (
                RelationalAST::OrEnumeration {
                    or_unfold: rel_entry,
                    left_table,
                    left_ast: Box::new(left_ast),
                    right_table,
                    right_ast: Box::new(right_ast),
                },
                ctx.table_name.clone(),
            )
        }
        ExprTy::SimpleExprTy(SimpleExprTy::Unit) => (
            RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: ctx.table_name.clone(),
                    column_name: column_name.to_string(),
                    column_type: ele.expr_type.clone(),
                    value: ele.name.clone(),
                    is_index: false,
                },
            },
            ctx.table_name.clone(),
        ),
        _ => {
            let ctx = ctx.start_table(ele.name.clone().unwrap());
            (
                build_relational_ast(&ctx, ele, indexes),
                ctx.table_name.clone(),
            )
        }
    }
}

fn build_index(ctx: &Context, ele: &Ele, indexes: &mut Indexes) -> RelationalAST {
    match ele.expr_type {
        ExprTy::ComplexExprTy(ref ety) => match ety {
            ComplexExprTy::Pair(left_type, right_type) => {
                let ctx = ctx.next_with_prefix(ele.name.clone());
                let left = build_index(&ctx.next(), left_type, indexes);
                let right = build_index(&ctx, right_type, indexes);
                RelationalAST::Pair {
                    left_ast: Box::new(left),
                    right_ast: Box::new(right),
                }
            }
            _ => panic!("Unexpected input type to index"),
        },
        ExprTy::SimpleExprTy(_) => RelationalAST::Leaf {
            rel_entry: RelationalEntry {
                table_name: ctx.table_name.clone(),
                column_name: ctx.name(ele, indexes, true),
                column_type: ele.expr_type.clone(),
                value: None,
                is_index: true,
            },
        },
    }
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
