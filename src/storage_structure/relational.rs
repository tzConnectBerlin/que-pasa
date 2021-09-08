use crate::storage_structure::typing::{
    ComplexExprTy, Ele, ExprTy, SimpleExprTy,
};
use anyhow::{anyhow, Result};
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
        None => {
            format!("table{}", get_index(indexes, &"table_names".to_string()))
        }
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
            SimpleExprTy::Nat => "nat",
            SimpleExprTy::Mutez => "mutez",
            SimpleExprTy::String => "string",
            SimpleExprTy::KeyHash => "string", // TODO: check this with the data
            SimpleExprTy::Timestamp => "timestamp",
            SimpleExprTy::Unit => "unit",
            SimpleExprTy::Stop => "stop",
        },
    }
}

pub struct Noname {
    counter: u32,
}

impl Noname {
    pub(crate) fn new() -> Noname {
        Self { counter: 0 }
    }

    pub(crate) fn next(&mut self) -> String {
        let noname_ident = format!("noname_{}", self.counter);
        self.counter += 1;
        noname_ident
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

    pub(crate) fn apply_prefix(&self, name: &str, is_index: bool) -> String {
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

    pub(crate) fn name(
        &self,
        ele: &Ele,
        indexes: &mut Indexes,
        is_index: bool,
    ) -> String {
        let name = match &ele.name {
            Some(x) => x.to_string(),
            None => format!(
                "{}_{}",
                get_column_name(&ele.expr_type),
                get_index(indexes, &self.table_name),
            ),
        };
        self.apply_prefix(&name, is_index)
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
    Option {
        elem_ast: Box<RelationalAST>,
    },
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
        elems_unique: bool,
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
    noname: &mut Noname,
) -> Result<RelationalAST> {
    let name = match &ele.name {
        Some(x) => x.clone(),
        None => noname.next(),
    };
    match ele.expr_type {
        ExprTy::ComplexExprTy(ref expr_type) => match expr_type {
            ComplexExprTy::Pair(left_type, right_type) => {
                let ctx = &ctx.next_with_prefix(ele.name.clone());
                let left =
                    build_relational_ast(ctx, left_type, indexes, noname)?;
                let right =
                    build_relational_ast(ctx, right_type, indexes, noname)?;
                Ok(RelationalAST::Pair {
                    left_ast: Box::new(left),
                    right_ast: Box::new(right),
                })
            }
            ComplexExprTy::List(elems_unique, elems_type) => {
                let ctx = &ctx.start_table(get_table_name(indexes, Some(name)));
                let elems_ast = match elems_unique {
                    true => build_index(ctx, elems_type, indexes, noname)?,
                    false => {
                        build_relational_ast(ctx, elems_type, indexes, noname)?
                    }
                };
                Ok(RelationalAST::List {
                    table: ctx.table_name.clone(),
                    elems_ast: Box::new(elems_ast),
                    elems_unique: *elems_unique,
                })
            }
            ComplexExprTy::BigMap(key_type, value_type) => {
                let ctx = &ctx.start_table(get_table_name(indexes, Some(name)));
                let key_ast = build_index(ctx, key_type, indexes, noname)?;
                let value_ast =
                    build_relational_ast(ctx, value_type, indexes, noname)?;
                Ok(RelationalAST::BigMap {
                    table: ctx.table_name.clone(),
                    key_ast: Box::new(key_ast),
                    value_ast: Box::new(value_ast),
                })
            }
            ComplexExprTy::Map(key_type, value_type) => {
                let ctx = &ctx.start_table(get_table_name(indexes, Some(name)));
                let key_ast = build_index(ctx, key_type, indexes, noname)?;
                let value_ast =
                    build_relational_ast(ctx, value_type, indexes, noname)?;
                Ok(RelationalAST::Map {
                    table: ctx.table_name.clone(),
                    key_ast: Box::new(key_ast),
                    value_ast: Box::new(value_ast),
                })
            }
            ComplexExprTy::Option(expr_type) => {
                let elem_ast = build_relational_ast(
                    &ctx,
                    &ele_with_annot(expr_type, ele.name.clone()),
                    indexes,
                    noname,
                )?;
                Ok(RelationalAST::Option {
                    elem_ast: Box::new(elem_ast),
                })
            }
            ComplexExprTy::OrEnumeration(_, _) => {
                Ok(build_enumeration_or(&ctx, ele, &name, indexes, noname)?.0)
            }
        },
        ExprTy::SimpleExprTy(_) => Ok(RelationalAST::Leaf {
            rel_entry: RelationalEntry {
                table_name: ctx.table_name.clone(),
                column_name: ctx.name(ele, indexes, false),
                column_type: ele.expr_type.clone(),
                value: None,
                is_index: false,
            },
        }),
    }
}

fn build_enumeration_or(
    ctx: &Context,
    ele: &Ele,
    column_name: &str,
    indexes: &mut Indexes,
    noname: &mut Noname,
) -> Result<(RelationalAST, String)> {
    match &ele.expr_type {
        ExprTy::ComplexExprTy(ComplexExprTy::OrEnumeration(
            left_type,
            right_type,
        )) => {
            let (left_ast, left_table) = build_enumeration_or(
                ctx,
                left_type,
                column_name,
                indexes,
                noname,
            )?;
            let (right_ast, right_table) = build_enumeration_or(
                ctx,
                right_type,
                column_name,
                indexes,
                noname,
            )?;
            let rel_entry = RelationalEntry {
                table_name: ctx.table_name.clone(),
                column_name: column_name.to_string(),
                column_type: ele.expr_type.clone(),
                is_index: false,
                value: None,
            };
            Ok((
                RelationalAST::OrEnumeration {
                    or_unfold: rel_entry,
                    left_table,
                    left_ast: Box::new(left_ast),
                    right_table,
                    right_ast: Box::new(right_ast),
                },
                ctx.table_name.clone(),
            ))
        }
        ExprTy::SimpleExprTy(SimpleExprTy::Unit) => {
            //let ctx = &ctx.start_table(get_table_name(indexes, Some(name)));
            Ok((
                RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: ctx.table_name.clone(),
                        column_name: ctx.apply_prefix(column_name, false),
                        column_type: ele.expr_type.clone(),
                        value: ele.name.clone(),
                        is_index: false,
                    },
                },
                ctx.table_name.clone(),
            ))
        }
        _ => {
            let ctx = ctx.start_table(ele.name.clone().unwrap());
            Ok((
                build_relational_ast(&ctx, ele, indexes, noname)?,
                ctx.table_name.clone(),
            ))
        }
    }
}

fn build_index(
    ctx: &Context,
    ele: &Ele,
    indexes: &mut Indexes,
    noname: &mut Noname,
) -> Result<RelationalAST> {
    match ele.expr_type {
        ExprTy::ComplexExprTy(ref ety) => match ety {
            ComplexExprTy::Pair(left_type, right_type) => {
                let ctx = ctx.next_with_prefix(ele.name.clone());
                let left =
                    build_index(&ctx.next(), left_type, indexes, noname)?;
                let right = build_index(&ctx, right_type, indexes, noname)?;
                Ok(RelationalAST::Pair {
                    left_ast: Box::new(left),
                    right_ast: Box::new(right),
                })
            }
            _ => Err(anyhow!("unexpected input type to index")),
        },
        ExprTy::SimpleExprTy(_) => Ok(RelationalAST::Leaf {
            rel_entry: RelationalEntry {
                table_name: ctx.table_name.clone(),
                column_name: ctx.name(ele, indexes, true),
                column_type: ele.expr_type.clone(),
                value: None,
                is_index: true,
            },
        }),
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
