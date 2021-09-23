use crate::storage_structure::typing::{
    ComplexExprTy, Ele, ExprTy, SimpleExprTy,
};
use anyhow::{anyhow, Result};
use std::collections::HashMap;

pub type Indexes = HashMap<String, u32>;

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
            SimpleExprTy::KeyHash => "keyhash",
            SimpleExprTy::Signature => "signature",
            SimpleExprTy::Contract => "contract",
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

    pub(crate) fn apply_prefix(&self, name: &str) -> String {
        let mut res = format!(
            "{}{}{}",
            self.prefix,
            if self.prefix.is_empty() { "" } else { "_" },
            name,
        );
        if res == "level" || res == "level_timestamp" {
            // always reserve these column names for the _live and _ordered view defintion
            res = format!(".{}", res);
        }
        res
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

pub struct ASTBuilder {
    table_names: HashMap<String, u32>,
    column_names: HashMap<(String, String), u32>,
}

impl ASTBuilder {
    pub(crate) fn new() -> Self {
        Self {
            table_names: HashMap::new(),
            column_names: HashMap::new(),
        }
    }

    fn start_table(&mut self, ctx: &Context, ele: &Ele) -> Context {
        let name = match &ele.name {
            Some(s) => s.clone(),
            None => "noname".to_string(),
        };

        let mut c = 0;
        if self.table_names.contains_key(&name) {
            c = self.table_names[&name] + 1;
            while self
                .table_names
                .contains_key(&format!("{}_{}", name, c))
            {
                c += 1;
            }
        }
        self.table_names.insert(name.clone(), c);
        let name = if c == 0 {
            name
        } else {
            format!("{}_{}", name, c)
        };

        ctx.start_table(name)
    }

    fn column_name(
        &mut self,
        ctx: &Context,
        ele: &Ele,
        is_index: bool,
    ) -> String {
        let name = match &ele.name {
            Some(x) => x.clone(),
            None => get_column_name(&ele.expr_type).to_string(),
        };
        let mut name = ctx.apply_prefix(&name);
        if is_index {
            name = format!("idx_{}", name);
        }

        let table = ctx.table_name.clone();
        let mut c = 0;
        if self
            .column_names
            .contains_key(&(table.clone(), name.clone()))
        {
            c = self.column_names[&(table.clone(), name.clone())] + 1;
            while self.column_names.contains_key(&(
                table.clone(),
                format!("{}_{}", name.clone(), c),
            )) {
                c += 1;
            }
        }
        self.column_names
            .insert((table, name.clone()), c);
        if c == 0 {
            name
        } else {
            format!("{}_{}", name, c)
        }
    }

    pub(crate) fn build_relational_ast(
        &mut self,
        ctx: &Context,
        ele: &Ele,
    ) -> Result<RelationalAST> {
        match ele.expr_type {
            ExprTy::ComplexExprTy(ref expr_type) => match expr_type {
                ComplexExprTy::Pair(left_type, right_type) => {
                    let ctx = &ctx.next_with_prefix(ele.name.clone());
                    let left = self.build_relational_ast(ctx, left_type)?;
                    let right = self.build_relational_ast(ctx, right_type)?;
                    Ok(RelationalAST::Pair {
                        left_ast: Box::new(left),
                        right_ast: Box::new(right),
                    })
                }
                ComplexExprTy::List(elems_unique, elems_type) => {
                    let ctx = &self.start_table(ctx, ele);
                    let elems_ast = match elems_unique {
                        true => self.build_index(ctx, elems_type)?,
                        false => self.build_relational_ast(ctx, elems_type)?,
                    };
                    Ok(RelationalAST::List {
                        table: ctx.table_name.clone(),
                        elems_ast: Box::new(elems_ast),
                        elems_unique: *elems_unique,
                    })
                }
                ComplexExprTy::BigMap(key_type, value_type) => {
                    let ctx = &self.start_table(ctx, ele);
                    let key_ast = self.build_index(ctx, key_type)?;
                    let value_ast =
                        self.build_relational_ast(ctx, value_type)?;
                    Ok(RelationalAST::BigMap {
                        table: ctx.table_name.clone(),
                        key_ast: Box::new(key_ast),
                        value_ast: Box::new(value_ast),
                    })
                }
                ComplexExprTy::Map(key_type, value_type) => {
                    let ctx = &self.start_table(ctx, ele);
                    let key_ast = self.build_index(ctx, key_type)?;
                    let value_ast =
                        self.build_relational_ast(ctx, value_type)?;
                    Ok(RelationalAST::Map {
                        table: ctx.table_name.clone(),
                        key_ast: Box::new(key_ast),
                        value_ast: Box::new(value_ast),
                    })
                }
                ComplexExprTy::Option(expr_type) => {
                    let elem_ast = self.build_relational_ast(
                        ctx,
                        &ele_with_annot(expr_type, ele.name.clone()),
                    )?;
                    Ok(RelationalAST::Option {
                        elem_ast: Box::new(elem_ast),
                    })
                }
                ComplexExprTy::OrEnumeration(_, _) => {
                    let name = match &ele.name {
                        Some(s) => s.clone(),
                        None => "noname".to_string(),
                    };
                    Ok(self
                        .build_enumeration_or(ctx, ele, &name)?
                        .0)
                }
            },
            ExprTy::SimpleExprTy(_) => Ok(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: ctx.table_name.clone(),
                    column_name: self.column_name(ctx, ele, false),
                    column_type: ele.expr_type.clone(),
                    value: None,
                    is_index: false,
                },
            }),
        }
    }

    fn build_enumeration_or(
        &mut self,
        ctx: &Context,
        ele: &Ele,
        column_name: &str,
    ) -> Result<(RelationalAST, String)> {
        match &ele.expr_type {
            ExprTy::ComplexExprTy(ComplexExprTy::OrEnumeration(
                left_type,
                right_type,
            )) => {
                let (left_ast, left_table) =
                    self.build_enumeration_or(ctx, left_type, column_name)?;
                let (right_ast, right_table) =
                    self.build_enumeration_or(ctx, right_type, column_name)?;
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
            ExprTy::SimpleExprTy(SimpleExprTy::Unit) => Ok((
                RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: ctx.table_name.clone(),
                        column_name: self.column_name(
                            ctx,
                            &ele_set_annot(ele, Some(column_name.to_string())),
                            false,
                        ),
                        column_type: ele.expr_type.clone(),
                        value: ele.name.clone(),
                        is_index: false,
                    },
                },
                ctx.table_name.clone(),
            )),
            _ => {
                let ctx = ctx.start_table(ele.name.clone().unwrap());
                Ok((
                    self.build_relational_ast(&ctx, ele)?,
                    ctx.table_name.clone(),
                ))
            }
        }
    }

    fn build_index(
        &mut self,
        ctx: &Context,
        ele: &Ele,
    ) -> Result<RelationalAST> {
        match ele.expr_type {
            ExprTy::ComplexExprTy(ref ety) => match ety {
                ComplexExprTy::Pair(left_type, right_type) => {
                    let ctx = ctx.next_with_prefix(ele.name.clone());
                    let left = self.build_index(&ctx.next(), left_type)?;
                    let right = self.build_index(&ctx, right_type)?;
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
                    column_name: self.column_name(ctx, ele, true),
                    column_type: ele.expr_type.clone(),
                    value: None,
                    is_index: true,
                },
            }),
        }
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

fn ele_set_annot(ele: &Ele, annot: Option<String>) -> Ele {
    let mut e = ele.clone();
    e.name = annot;
    e
}
