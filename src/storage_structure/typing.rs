use crate::storage_value::parser;
use anyhow::{anyhow, Result};
use json::JsonValue;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SimpleExprTy {
    Address,
    Bool,
    Bytes,
    Int,
    KeyHash,
    Mutez,
    Nat,
    Stop,
    String,
    Timestamp,
    Unit,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ComplexExprTy {
    BigMap(Box<Ele>, Box<Ele>),
    List(bool, Box<Ele>),
    Map(Box<Ele>, Box<Ele>),
    Pair(Box<Ele>, Box<Ele>),
    OrEnumeration(Box<Ele>, Box<Ele>),
    Option(Box<Ele>), // TODO: move this out into SimpleExprTy??
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExprTy {
    SimpleExprTy(SimpleExprTy),
    ComplexExprTy(ComplexExprTy),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Ele {
    pub expr_type: ExprTy,
    pub name: Option<String>,
}

fn annotation(json: &json::JsonValue) -> Option<String> {
    if let JsonValue::Short(s) = &json["annots"][0] {
        Some(String::from(s.as_str())[1..].to_string())
    } else {
        None
    }
}

fn args(json: &JsonValue) -> Option<Vec<JsonValue>> {
    match &json["args"] {
        JsonValue::Array(a) => Some(a.clone()),
        _ => None,
    }
}

macro_rules! simple_expr {
    ($typ:expr, $name:expr) => {
        Ele {
            name: $name,
            expr_type: ExprTy::SimpleExprTy($typ),
        }
    };
}

macro_rules! complex_expr {
    ($typ:expr, $name:expr, $args:expr) => {{
        let args = $args.unwrap();
        Ele {
            name: $name,
            expr_type: ExprTy::ComplexExprTy($typ(
                Box::new(storage_ast_from_json(&args[0].clone())?),
                Box::new(storage_ast_from_json(&args[1].clone())?),
            )),
        }
    }};
}

/// An or can be a variant record, or a simple enumeration.
pub(crate) fn is_enumeration_or(json: &JsonValue) -> bool {
    let prim = match &json["prim"] {
        JsonValue::Short(s) => s.as_str(),
        JsonValue::String(s) => s.as_str(),
        _ => return false,
    };
    match prim {
        "or" => true,
        "unit" => false,
        _ => true,
    }
}

pub(crate) fn storage_ast_from_json(json: &JsonValue) -> Result<Ele> {
    let annot = annotation(json);
    let args = args(json);
    if let JsonValue::Short(prim) = json["prim"] {
        match prim.to_ascii_lowercase().as_str() {
            "address" => Ok(simple_expr!(SimpleExprTy::Address, annot)),
            "big_map" => Ok(complex_expr!(ComplexExprTy::BigMap, annot, args)),
            "bool" => Ok(simple_expr!(SimpleExprTy::Bool, annot)),
            "bytes" => Ok(simple_expr!(SimpleExprTy::Bytes, annot)),
            "int" => Ok(simple_expr!(SimpleExprTy::Int, annot)),
            "key" => Ok(simple_expr!(SimpleExprTy::KeyHash, annot)), // TODO: check this is correct
            "key_hash" => Ok(simple_expr!(SimpleExprTy::KeyHash, annot)),
            "map" => Ok(complex_expr!(ComplexExprTy::Map, annot, args)),
            "mutez" => Ok(simple_expr!(SimpleExprTy::Mutez, annot)),
            "nat" => Ok(simple_expr!(SimpleExprTy::Nat, annot)),
            "option" => {
                let args = args.ok_or_else(|| anyhow!("Args was none!"))?;
                Ok(Ele {
                    name: annot,
                    expr_type: ExprTy::ComplexExprTy(ComplexExprTy::Option(
                        Box::new(storage_ast_from_json(&args[0].clone())?),
                    )),
                })
            }
            "or" => {
                if is_enumeration_or(json) {
                    Ok(complex_expr!(ComplexExprTy::OrEnumeration, annot, args))
                } else {
                    unimplemented!(
                        "Or used as variant record found, don't know how to deal with it {}",
                        json.to_string()
                    );
                }
            }
            "pair" => {
                let args_count = args
                    .clone()
                    .ok_or_else(|| anyhow!("NoneError"))?
                    .len();
                match args_count {
                    0 | 1 => {
                        return Err(anyhow!("Pair with {} args", args_count))
                    }
                    2 => Ok(complex_expr!(ComplexExprTy::Pair, annot, args)),
                    _ => {
                        let mut args_cloned = args
                            .ok_or_else(|| anyhow!("Args was none!"))
                            .unwrap();
                        args_cloned.reverse();
                        let unfolded =
                            parser::lexer_unfold_many_pair(&mut args_cloned);
                        storage_ast_from_json(&unfolded)
                    }
                }
            }
            "set" => {
                let inner_ast =
                    storage_ast_from_json(&args.unwrap()[0]).unwrap();
                Ok(Ele {
                    name: annot,
                    expr_type: ExprTy::ComplexExprTy(ComplexExprTy::List(
                        true,
                        Box::new(inner_ast),
                    )),
                })
            }
            "list" => {
                warn!("!!! LIST DETECTED !!!");
                let inner_ast =
                    storage_ast_from_json(&args.unwrap()[0]).unwrap();
                Ok(Ele {
                    name: annot,
                    expr_type: ExprTy::ComplexExprTy(ComplexExprTy::List(
                        false,
                        Box::new(inner_ast),
                    )),
                })
            }
            "string" => Ok(simple_expr!(SimpleExprTy::String, annot)),
            "timestamp" => Ok(simple_expr!(SimpleExprTy::Timestamp, annot)),
            "unit" => Ok(simple_expr!(SimpleExprTy::Unit, annot)),
            "lambda" => Ok(simple_expr!(SimpleExprTy::Stop, annot)),
            _ => Err(anyhow!(
                "Unexpected storage json: {} {:#?}",
                prim.as_str(),
                json
            )),
        }
    } else {
        Err(anyhow!("Wrong JS {}", json.to_string()))
    }
}
