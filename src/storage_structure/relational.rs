use crate::storage_structure::typing::{
    ComplexExprTy, Ele, ExprTy, SimpleExprTy,
};
use anyhow::{anyhow, Result};
use std::collections::HashMap;

#[cfg(test)]
use pretty_assertions::assert_eq;

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
        format!(
            "{}{}{}",
            self.prefix,
            if self.prefix.is_empty() { "" } else { "_" },
            name,
        )
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

    pub(crate) fn start_table(&self, name: &str) -> Self {
        let mut c = self.next();
        c.table_name = format!("{}.{}", self.table_name, name);
        c
    }

    pub(crate) fn table_leaf_name(&self) -> String {
        self.table_name
            .rfind('.')
            .map(|pos| {
                self.table_name[pos + 1..self.table_name.len()].to_string()
            })
            .unwrap_or_else(|| self.table_name.clone())
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
        or_unfold: Option<RelationalEntry>,
        left_table: Option<String>,
        left_ast: Box<RelationalAST>,
        right_table: Option<String>,
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

lazy_static! {
    static ref RESERVED: Vec<String> = vec![
        "id".to_string(),
        "tx_context_id".to_string(),
        "level".to_string(),
        "level_timestamp".to_string()
    ];
    static ref RESERVED_BIGMAP: Vec<String> =
        vec!["bigmap_id".to_string(), "deleted".to_string()];
}

impl ASTBuilder {
    pub(crate) fn new() -> Self {
        let mut res = Self {
            table_names: HashMap::new(),
            column_names: HashMap::new(),
        };
        for column_name in RESERVED.iter() {
            res.column_names
                .insert(("storage".to_string(), column_name.clone()), 0);
        }
        res
    }

    fn start_table(&mut self, ctx: &Context, ele: &Ele) -> Context {
        let name = match &ele.name {
            Some(s) => s.clone(),
            None => "noname".to_string(),
        };

        let full_name = ctx.start_table(&name).table_name;
        let mut c = 0;
        if self
            .table_names
            .contains_key(&full_name)
        {
            c = self.table_names[&full_name] + 1;
            while self
                .table_names
                .contains_key(&format!("{}_{}", full_name, c))
            {
                c += 1;
            }
        }
        self.table_names
            .insert(full_name.clone(), c);

        let name = if c == 0 {
            name
        } else {
            self.table_names
                .insert(format!("{}_{}", full_name, c), c);
            format!("{}_{}", name, c)
        };

        let parent_table = &ctx.table_name;
        let ctx = ctx.start_table(&name);

        self.column_names.insert(
            (ctx.table_name.clone(), format!("{}_id", parent_table)),
            0,
        );
        for column_name in RESERVED.iter() {
            self.column_names
                .insert((ctx.table_name.clone(), column_name.clone()), 0);
        }
        ctx
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
            .insert((table.clone(), name.clone()), c);
        if c == 0 {
            name
        } else {
            let postfixed = format!("{}_{}", name, c);
            self.column_names
                .insert((table, postfixed.clone()), 0);
            postfixed
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

                    for column_name in RESERVED_BIGMAP.iter() {
                        self.column_names.insert(
                            (ctx.table_name.clone(), column_name.clone()),
                            0,
                        );
                    }

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
                        .build_enumeration_or(ctx, ele, &name, false)?
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
        is_index: bool,
    ) -> Result<(RelationalAST, String)> {
        let rel_entry = RelationalEntry {
            table_name: ctx.table_name.clone(),
            column_name: self.column_name(
                ctx,
                &ele_set_annot(ele, Some(column_name.to_string())),
                false,
            ),
            column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
            is_index,
            value: None,
        };

        self.build_enumeration_or_internal(
            ctx,
            ele,
            column_name,
            is_index,
            Some(rel_entry),
        )
    }

    fn build_enumeration_or_internal(
        &mut self,
        ctx: &Context,
        ele: &Ele,
        column_name: &str,
        is_index: bool,
        or_unfold: Option<RelationalEntry>,
    ) -> Result<(RelationalAST, String)> {
        match &ele.expr_type {
            ExprTy::ComplexExprTy(ComplexExprTy::OrEnumeration(
                left_type,
                right_type,
            )) => {
                let (left_ast, left_table) = self
                    .build_enumeration_or_internal(
                        ctx,
                        left_type,
                        column_name,
                        false,
                        None,
                    )?;
                let (right_ast, right_table) = self
                    .build_enumeration_or_internal(
                        ctx,
                        right_type,
                        column_name,
                        false,
                        None,
                    )?;
                Ok((
                    RelationalAST::OrEnumeration {
                        or_unfold,
                        left_table: if left_table != ctx.table_name {
                            Some(left_table)
                        } else {
                            None
                        },
                        left_ast: Box::new(left_ast),
                        right_table: if right_table != ctx.table_name {
                            Some(right_table)
                        } else {
                            None
                        },
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
                        is_index,
                    },
                },
                ctx.table_name.clone(),
            )),
            _ => {
                let name = match &ele.name {
                    Some(x) => x.clone(),
                    None => {
                        let name_from_type =
                            get_column_name(&ele.expr_type).to_string();
                        if !name_from_type.is_empty() {
                            name_from_type
                        } else {
                            "noname".to_string()
                        }
                    }
                };

                let ctx =
                    self.start_table(ctx, &ele_set_annot(ele, Some(name)));
                let ele = &ele_set_annot(ele, Some(ctx.table_leaf_name()));
                Ok((
                    if is_index {
                        self.build_index(&ctx, ele)?
                    } else {
                        self.build_relational_ast(&ctx, ele)?
                    },
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
                ComplexExprTy::Option(elem_type) => {
                    let ctx = ctx.next_with_prefix(ele.name.clone());
                    let elem_ast = self.build_index(&ctx, elem_type)?;
                    Ok(RelationalAST::Option {
                        elem_ast: Box::new(elem_ast),
                    })
                }
                ComplexExprTy::Pair(left_type, right_type) => {
                    let ctx = ctx.next_with_prefix(ele.name.clone());
                    let left = self.build_index(&ctx.next(), left_type)?;
                    let right = self.build_index(&ctx, right_type)?;
                    Ok(RelationalAST::Pair {
                        left_ast: Box::new(left),
                        right_ast: Box::new(right),
                    })
                }
                ComplexExprTy::OrEnumeration { .. } => {
                    let name = match &ele.name {
                        Some(s) => s.clone(),
                        None => "noname".to_string(),
                    };
                    Ok(self
                        .build_enumeration_or(ctx, ele, &name, true)?
                        .0)
                }
                _ => Err(anyhow!(
                    "unexpected input type to index: ele={:#?}",
                    ele
                )),
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

#[test]
fn test_relational_ast_builder() {
    fn simple(n: Option<String>, t: SimpleExprTy) -> Ele {
        Ele {
            expr_type: ExprTy::SimpleExprTy(t),
            name: n,
        }
    }
    fn or(n: Option<String>, l: Ele, r: Ele) -> Ele {
        Ele {
            expr_type: ExprTy::ComplexExprTy(ComplexExprTy::OrEnumeration(
                Box::new(l),
                Box::new(r),
            )),
            name: n,
        }
    }
    fn pair(n: Option<String>, l: Ele, r: Ele) -> Ele {
        Ele {
            expr_type: ExprTy::ComplexExprTy(ComplexExprTy::Pair(
                Box::new(l),
                Box::new(r),
            )),
            name: n,
        }
    }
    fn set(n: Option<String>, elems: Ele) -> Ele {
        Ele {
            expr_type: ExprTy::ComplexExprTy(ComplexExprTy::List(
                true,
                Box::new(elems),
            )),
            name: n,
        }
    }
    fn list(n: Option<String>, elems: Ele) -> Ele {
        Ele {
            expr_type: ExprTy::ComplexExprTy(ComplexExprTy::List(
                false,
                Box::new(elems),
            )),
            name: n,
        }
    }
    fn map(n: Option<String>, key: Ele, value: Ele) -> Ele {
        Ele {
            expr_type: ExprTy::ComplexExprTy(ComplexExprTy::Map(
                Box::new(key),
                Box::new(value),
            )),
            name: n,
        }
    }
    fn bigmap(n: Option<String>, key: Ele, value: Ele) -> Ele {
        Ele {
            expr_type: ExprTy::ComplexExprTy(ComplexExprTy::BigMap(
                Box::new(key),
                Box::new(value),
            )),
            name: n,
        }
    }
    fn option(n: Option<String>, elem: Ele) -> Ele {
        Ele {
            expr_type: ExprTy::ComplexExprTy(ComplexExprTy::Option(Box::new(
                elem,
            ))),
            name: n,
        }
    }

    struct TestCase {
        name: String,
        ele: Ele,
        exp: Option<RelationalAST>,
    }
    let tests: Vec<TestCase> = vec![
        TestCase {
            name: "simple type with name".to_string(),
            ele: simple(Some("contract_owner".to_string()), SimpleExprTy::String),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "contract_owner".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "simple type without name (resulting column name is generated based on type: string)".to_string(),
            ele: simple(None, SimpleExprTy::String),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "string".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "simple type without name (resulting column name is generated based on type: mutez)".to_string(),
            ele: simple(None, SimpleExprTy::Mutez),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "mutez".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "OrEnumeration containing Units stays in parent table".to_string(),
            ele: or(None, simple(Some("disabled".to_string()), SimpleExprTy::Unit), simple(None, SimpleExprTy::Unit)),
            exp: Some(RelationalAST::OrEnumeration {
                or_unfold: Some(RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "noname".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                }),
                left_table: None,
                left_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "noname_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Unit),
                    value: Some("disabled".to_string()),
                    is_index: false,
                }}),
                right_table: None,
                right_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "noname_2".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Unit),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "OrEnumeration containing no units creates child table (tc with only 1 variant non-unit)".to_string(),
            ele: or(None, simple(None, SimpleExprTy::Unit), simple(None, SimpleExprTy::Nat)),
            exp: Some(RelationalAST::OrEnumeration {
                or_unfold: Some(RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "noname".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                }),
                left_table: None,
                left_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "noname_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Unit),
                    value: None,
                    is_index: false,
                }}),
                right_table: Some("storage.nat".to_string()),
                right_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.nat".to_string(),
                    column_name: "nat".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "OrEnumeration containing no units creates child tables (tc with both variants non-unit)".to_string(),
            ele: or(Some("or_annot".to_string()), simple(Some("left_side".to_string()), SimpleExprTy::String), simple(Some("annot_defined".to_string()), SimpleExprTy::Mutez)),
            exp: Some(RelationalAST::OrEnumeration {
                or_unfold: Some(RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "or_annot".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                }),
                left_table: Some("storage.left_side".to_string()),
                left_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.left_side".to_string(),
                    column_name: "left_side".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                }}),
                right_table: Some("storage.annot_defined".to_string()),
                right_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.annot_defined".to_string(),
                    column_name: "annot_defined".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "OrEnumeration containing a pair creates child table with multi columns".to_string(),
            ele: or(Some("or_annot".to_string()), pair(Some("left_side".to_string()), simple(Some("var_a".to_string()), SimpleExprTy::String), simple(Some("var_b".to_string()), SimpleExprTy::Nat)), simple(Some("annot_defined".to_string()), SimpleExprTy::Unit)),
            exp: Some(RelationalAST::OrEnumeration {
                or_unfold: Some(RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "or_annot".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                }),
                left_table: Some("storage.left_side".to_string()),
                left_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.left_side".to_string(),
                    column_name: "left_side_var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                }}), right_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.left_side".to_string(),
                    column_name: "left_side_var_b".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }})}),
                right_table: None,
                right_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "or_annot_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Unit),
                    value: Some("annot_defined".to_string()),
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "set (no annot)".to_string(),
            ele: set(None, pair(Some("left_side".to_string()), simple(Some("var_a".to_string()), SimpleExprTy::String), simple(Some("var_b".to_string()), SimpleExprTy::Nat))),
            exp: Some(RelationalAST::List {
                table: "storage.noname".to_string(),
                elems_unique: true,
                elems_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.noname".to_string(),
                    column_name: "idx_left_side_var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: true,
                }}), right_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.noname".to_string(),
                    column_name: "idx_left_side_var_b".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: true,
                }})}),
            }),
        },
        TestCase {
            name: "set (with annot)".to_string(),
            ele: set(Some("denylist".to_string()), pair(Some("deny".to_string()), simple(Some("var_a".to_string()), SimpleExprTy::String), simple(Some("var_b".to_string()), SimpleExprTy::Nat))),
            exp: Some(RelationalAST::List {
                table: "storage.denylist".to_string(),
                elems_unique: true,
                elems_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.denylist".to_string(),
                    column_name: "idx_deny_var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: true,
                }}), right_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.denylist".to_string(),
                    column_name: "idx_deny_var_b".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: true,
                }})}),
            }),
        },
        TestCase {
            name: "list (no annot, only difference with set is its elems are not unique)".to_string(),
            ele: list(None, pair(Some("deny".to_string()), simple(Some("var_a".to_string()), SimpleExprTy::String), simple(Some("var_b".to_string()), SimpleExprTy::Nat))),
            exp: Some(RelationalAST::List {
                table: "storage.noname".to_string(),
                elems_unique: false,
                elems_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.noname".to_string(),
                    column_name: "deny_var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                }}), right_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.noname".to_string(),
                    column_name: "deny_var_b".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }})}),
            }),
        },
        TestCase {
            name: "map (no annot)".to_string(),
            ele: map(None, simple(Some("var_a".to_string()), SimpleExprTy::String), simple(Some("var_b".to_string()), SimpleExprTy::Nat)),
            exp: Some(RelationalAST::Map {
                table: "storage.noname".to_string(),
                key_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.noname".to_string(),
                    column_name: "idx_var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: true,
                }}), value_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.noname".to_string(),
                    column_name: "var_b".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "bigmap (with annot)".to_string(),
            ele: bigmap(Some("ledger".to_string()), simple(Some("var_a".to_string()), SimpleExprTy::String), simple(Some("var_b".to_string()), SimpleExprTy::Nat)),
            exp: Some(RelationalAST::BigMap {
                table: "storage.ledger".to_string(),
                key_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.ledger".to_string(),
                    column_name: "idx_var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: true,
                }}), value_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.ledger".to_string(),
                    column_name: "var_b".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "id is a reserved column name and is immediately postfixed".to_string(),
            ele: simple(Some("id".to_string()), SimpleExprTy::String),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "id_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "tx_context_id is a reserved column name and is immediately postfixed".to_string(),
            ele: simple(Some("tx_context_id".to_string()), SimpleExprTy::String),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "tx_context_id_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "level is a reserved column name and is immediately postfixed".to_string(),
            ele: simple(Some("level".to_string()), SimpleExprTy::String),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "level_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "level_timestamp is a reserved column name and is immediately postfixed".to_string(),
            ele: simple(Some("level_timestamp".to_string()), SimpleExprTy::String),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "level_timestamp_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "id is a reserved column name, also in child tables of storage".to_string(),
            ele: list(Some("addresses".to_string()), simple(Some("id".to_string()), SimpleExprTy::String)),
            exp: Some(RelationalAST::List{
                table: "storage.addresses".to_string(),
                elems_unique: false,
                elems_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.addresses".to_string(),
                        column_name: "id_1".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                    },
                }),
            }),
        },
        TestCase {
            name: "parent_id is a reserved column name in child tables".to_string(),
            ele: list(Some("addresses".to_string()), simple(Some("storage_id".to_string()), SimpleExprTy::String)),
            exp: Some(RelationalAST::List{
                table: "storage.addresses".to_string(),
                elems_unique: false,
                elems_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.addresses".to_string(),
                        column_name: "storage_id_1".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                    },
                }),
            }),
        },
        TestCase {
            name: "parent_id is a reserved column name in nested child tables".to_string(),
            ele: list(Some("addresses".to_string()), list(None, simple(Some("storage.addresses_id".to_string()), SimpleExprTy::String))),
            exp: Some(RelationalAST::List{
                table: "storage.addresses".to_string(),
                elems_unique: false,
                elems_ast: Box::new(RelationalAST::List{
                    table: "storage.addresses.noname".to_string(),
                    elems_unique: false,
                    elems_ast: Box::new(RelationalAST::Leaf {
                        rel_entry: RelationalEntry {
                            table_name: "storage.addresses.noname".to_string(),
                            column_name: "storage.addresses_id_1".to_string(),
                            column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                            value: None,
                            is_index: false,
                        },
                    }),
                }),
            }),
        },
        TestCase {
            name: "bigmap with a contract related field 'bigmap_id' gets postfixed, because it's a fieldname we need reserved".to_string(),
            ele: bigmap(Some("ledger".to_string()), simple(Some("var_a".to_string()), SimpleExprTy::String), simple(Some("bigmap_id".to_string()), SimpleExprTy::Nat)),
            exp: Some(RelationalAST::BigMap {
                table: "storage.ledger".to_string(),
                key_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.ledger".to_string(),
                    column_name: "idx_var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: true,
                }}), value_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.ledger".to_string(),
                    column_name: "bigmap_id_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "bigmap with a contract related field 'bigmap_id' gets postfixed, because it's a fieldname we need reserved (and the idx_ variant is not postfixed due to the idx_ part making it nonclashing with reserved bigmap_id)".to_string(),
            ele: bigmap(Some("ledger".to_string()), simple(Some("bigmap_id".to_string()), SimpleExprTy::String), simple(Some("bigmap_id".to_string()), SimpleExprTy::Nat)),
            exp: Some(RelationalAST::BigMap {
                table: "storage.ledger".to_string(),
                key_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.ledger".to_string(),
                    column_name: "idx_bigmap_id".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: true,
                }}), value_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.ledger".to_string(),
                    column_name: "bigmap_id_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "bigmap with a contract related field 'deleted' gets postfixed, because it's a fieldname we need reserved".to_string(),
            ele: bigmap(Some("ledger".to_string()), simple(Some("var_a".to_string()), SimpleExprTy::String), simple(Some("deleted".to_string()), SimpleExprTy::Nat)),
            exp: Some(RelationalAST::BigMap {
                table: "storage.ledger".to_string(),
                key_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.ledger".to_string(),
                    column_name: "idx_var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: true,
                }}), value_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage.ledger".to_string(),
                    column_name: "deleted_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "NON-bigmap with a contract related field 'deleted' does NOT get postfixed (because it's only a reserved keyword in the bigmaps' tables)".to_string(),
            ele: simple(Some("deleted".to_string()), SimpleExprTy::String),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "deleted".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "NON-bigmap with a contract related field 'bigmap_id' does NOT get postfixed (because it's only a reserved keyword in the bigmaps' tables)".to_string(),
            ele: simple(Some("bigmap_id".to_string()), SimpleExprTy::String),
            exp: Some(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "bigmap_id".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            }),
        },
        TestCase {
            name: "option (no annot)".to_string(),
            ele: option(None, simple(Some("var_a".to_string()), SimpleExprTy::String)),
            exp: Some(RelationalAST::Option {
                elem_ast: Box::new(RelationalAST::Leaf {rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "var_a".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        // More complex test cases involving multiple complex types, to test
        // how they interact with each other through global state such as
        // deduplication of table names and column names
        TestCase {
            name: "multiple without annot of same type in same table => uniq column name postfix generated".to_string(),
            ele: pair(None, simple(None, SimpleExprTy::Mutez), simple(None, SimpleExprTy::Mutez)),
            exp: Some(RelationalAST::Pair {
                left_ast: Box::new(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "mutez".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                    value: None,
                    is_index: false,
                }}),
                right_ast: Box::new(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "mutez_1".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "multiple without annot of different type in same table => no postfix generated, because it's not necessary due to different types".to_string(),
            ele: pair(None, simple(None, SimpleExprTy::Nat), simple(None, SimpleExprTy::Mutez)),
            exp: Some(RelationalAST::Pair {
                left_ast: Box::new(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "nat".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                    value: None,
                    is_index: false,
                }}),
                right_ast: Box::new(RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "mutez".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                    value: None,
                    is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "multiple without annot of same type in same table + 1 with annot that clashes with first generated postfix (variation 1) => uniq column name postfix generated".to_string(),
            ele: pair(None, simple(Some("mutez_1".to_string()), SimpleExprTy::String), pair(None, simple(None, SimpleExprTy::Mutez), simple(None, SimpleExprTy::Mutez))),
            exp: Some(RelationalAST::Pair {
                left_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez_1".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                }}),
                right_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: false,
                    }}),
                    right_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez_2".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: false,
                    }}),
                }),
            }),
        },
        TestCase {
            name: "multiple without annot of same type in same table + 1 with annot that clashes with first generated postfix (variation 2) => uniq column name postfix generated".to_string(),
            ele: pair(None, simple(Some("mutez".to_string()), SimpleExprTy::String), pair(None, simple(None, SimpleExprTy::Mutez), simple(None, SimpleExprTy::Mutez))),
            exp: Some(RelationalAST::Pair {
                left_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                }}),
                right_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez_1".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: false,
                    }}),
                    right_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez_2".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: false,
                    }}),
                }),
            }),
        },
        TestCase {
            name: "multiple without annot of same type in same table + 1 with annot that clashes with first generated postfix (variation 3) => uniq column name postfix generated".to_string(),
            ele: pair(None, pair(None, simple(None, SimpleExprTy::Mutez), simple(None, SimpleExprTy::Mutez)), simple(Some("mutez".to_string()), SimpleExprTy::String)),
            exp: Some(RelationalAST::Pair {
                left_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: false,
                    }}),
                    right_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez_1".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: false,
                    }}),
                }),
                right_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez_2".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "multiple without annot of same type in same table + 1 with annot that clashes with first generated postfix (variation 4) => uniq column name postfix generated".to_string(),
            ele: pair(None, pair(None, simple(None, SimpleExprTy::Mutez), simple(None, SimpleExprTy::Mutez)), simple(Some("mutez_1".to_string()), SimpleExprTy::String)),
            exp: Some(RelationalAST::Pair {
                left_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: false,
                    }}),
                    right_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez_1".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: false,
                    }}),
                }),
                right_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "mutez_1_1".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                }}),
            }),
        },
        TestCase {
            name: "multiple nameless tables => uniq table name postfix generated".to_string(),
            ele: pair(None, map(None, simple(Some("map_key".to_string()), SimpleExprTy::Mutez), simple(Some("map_value".to_string()), SimpleExprTy::Nat)), bigmap(None, simple(Some("bigmap_key".to_string()), SimpleExprTy::Mutez), simple(Some("bigmap_value".to_string()), SimpleExprTy::Nat))),
            exp: Some(RelationalAST::Pair {
                left_ast: Box::new(RelationalAST::Map {
                    table: "storage.noname".to_string(),
                    key_ast: Box::new(RelationalAST::Leaf {
                        rel_entry: RelationalEntry {
                            table_name: "storage.noname".to_string(),
                            column_name: "idx_map_key".to_string(),
                            column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                            value: None,
                            is_index: true,
                    }}),
                    value_ast: Box::new(RelationalAST::Leaf {
                        rel_entry: RelationalEntry {
                            table_name: "storage.noname".to_string(),
                            column_name: "map_value".to_string(),
                            column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                            value: None,
                            is_index: false,
                    }}),
                }),
                right_ast: Box::new(RelationalAST::BigMap {
                    table: "storage.noname_1".to_string(),
                    key_ast: Box::new(RelationalAST::Leaf {
                        rel_entry: RelationalEntry {
                            table_name: "storage.noname_1".to_string(),
                            column_name: "idx_bigmap_key".to_string(),
                            column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                            value: None,
                            is_index: true,
                    }}),
                    value_ast: Box::new(RelationalAST::Leaf {
                        rel_entry: RelationalEntry {
                            table_name: "storage.noname_1".to_string(),
                            column_name: "bigmap_value".to_string(),
                            column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                            value: None,
                            is_index: false,
                    }}),
                }),
            }),
        },
        TestCase {
            name: "multiple nameless tables, but they don't clash because they're in different child tables => no postfix added".to_string(),
            ele: map(Some("ledger".to_string()), simple(Some("map_key".to_string()), SimpleExprTy::Mutez), pair(None, bigmap(Some("ledger".to_string()), simple(Some("bigmap_key".to_string()), SimpleExprTy::Mutez), simple(Some("bigmap_value".to_string()), SimpleExprTy::Nat)), simple(Some("map_value".to_string()), SimpleExprTy::Nat))),
            exp: Some(RelationalAST::Map {
                table: "storage.ledger".to_string(),
                key_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.ledger".to_string(),
                        column_name: "idx_map_key".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                        value: None,
                        is_index: true,
                }}),
                value_ast: Box::new(RelationalAST::Pair {
                    left_ast: Box::new(RelationalAST::BigMap {
                        table: "storage.ledger.ledger".to_string(),
                        key_ast: Box::new(RelationalAST::Leaf {
                            rel_entry: RelationalEntry {
                                table_name: "storage.ledger.ledger".to_string(),
                                column_name: "idx_bigmap_key".to_string(),
                                column_type: ExprTy::SimpleExprTy(SimpleExprTy::Mutez),
                                value: None,
                                is_index: true,
                        }}),
                        value_ast: Box::new(RelationalAST::Leaf {
                            rel_entry: RelationalEntry {
                                table_name: "storage.ledger.ledger".to_string(),
                                column_name: "bigmap_value".to_string(),
                                column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                                value: None,
                                is_index: false,
                        }}),
                    }),
                    right_ast: Box::new(RelationalAST::Leaf {
                        rel_entry: RelationalEntry {
                            table_name: "storage.ledger".to_string(),
                            column_name: "map_value".to_string(),
                            column_type: ExprTy::SimpleExprTy(SimpleExprTy::Nat),
                            value: None,
                            is_index: false,
                    }}),
                }),
            }),
        },
    ];

    for tc in tests {
        println!("test case: {}", tc.name);

        let got =
            ASTBuilder::new().build_relational_ast(&Context::init(), &tc.ele);
        if tc.exp.is_none() {
            assert!(got.is_err());
            continue;
        }
        assert_eq!(tc.exp.unwrap(), got.unwrap());
    }
}
