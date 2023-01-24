use crate::storage_value::parser;
use anyhow::{anyhow, Result};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExprTy {
    Address,
    Bool,
    Bytes,
    Int,
    Nat,
    Mutez,
    KeyHash,
    Signature,
    Contract,
    Stop,
    String,
    Timestamp,
    Unit,
    BigMap(Box<Ele>, Box<Ele>),
    List(bool, Box<Ele>),
    Map(Box<Ele>, Box<Ele>),
    Pair(Box<Ele>, Box<Ele>),
    OrEnumeration(Box<Ele>, Box<Ele>),
    Option(Box<Ele>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Ele {
    pub expr_type: ExprTy,
    pub name: Option<String>,
}

fn annotation(json: &serde_json::Value) -> Option<String> {
    match &json["annots"][0] {
        serde_json::Value::String(s) => Some(s[1..].to_string()),
        serde_json::Value::Null => None,
        _ => panic!("unexpected annot type!: {:?}", json["annots"]),
    }
}

fn args(json: &serde_json::Value) -> Option<Vec<serde_json::Value>> {
    match &json["args"] {
        serde_json::Value::Array(a) => Some(a.clone()),
        _ => None,
    }
}

macro_rules! simple_expr {
    ($typ:expr, $name:expr) => {
        Ele {
            name: $name,
            expr_type: $typ,
        }
    };
}

macro_rules! complex_expr {
    ($typ:expr, $name:expr, $args:expr) => {{
        let args = $args.unwrap();
        Ele {
            name: $name,
            expr_type: $typ(
                Box::new(type_ast_from_json(&args[0].clone())?),
                Box::new(type_ast_from_json(&args[1].clone())?),
            ),
        }
    }};
}

/// An or can be a variant record, or a simple enumeration.
pub(crate) fn is_enumeration_or(json: &serde_json::Value) -> bool {
    let prim = match &json["prim"] {
        serde_json::Value::String(s) => s.as_str(),
        _ => return false,
    };
    match prim {
        "or" => true,
        "unit" => false,
        _ => true,
    }
}

pub(crate) fn type_ast_from_json(json: &serde_json::Value) -> Result<Ele> {
    let annot = annotation(json);
    let args = args(json);
    if let serde_json::Value::String(prim) = &json["prim"] {
        match prim.to_ascii_lowercase().as_str() {
            "address" => Ok(simple_expr!(ExprTy::Address, annot)),
            "big_map" => Ok(complex_expr!(ExprTy::BigMap, annot, args)),
            "bool" => Ok(simple_expr!(ExprTy::Bool, annot)),
            "bytes" | "chest" | "chest_key" => Ok(simple_expr!(
                ExprTy::Bytes,
                annot.or_else(|| Some(
                    prim.to_ascii_lowercase()
                        .as_str()
                        .to_string()
                ))
            )),
            "int" => Ok(simple_expr!(ExprTy::Int, annot)),
            "key" => Ok(simple_expr!(ExprTy::KeyHash, annot)), // TODO: check this is correct
            "key_hash" => Ok(simple_expr!(ExprTy::KeyHash, annot)),
            "map" => Ok(complex_expr!(ExprTy::Map, annot, args)),
            "mutez" => Ok(simple_expr!(ExprTy::Mutez, annot)),
            "nat" => Ok(simple_expr!(ExprTy::Nat, annot)),
            "option" => {
                let args = args.ok_or_else(|| anyhow!("Args was none!"))?;
                Ok(Ele {
                    name: annot,
                    expr_type: ExprTy::Option(Box::new(type_ast_from_json(
                        &args[0].clone(),
                    )?)),
                })
            }
            "or" => {
                if is_enumeration_or(json) {
                    Ok(complex_expr!(ExprTy::OrEnumeration, annot, args))
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
                    2 => Ok(complex_expr!(ExprTy::Pair, annot, args)),
                    _ => {
                        let mut args_cloned = args
                            .ok_or_else(|| anyhow!("Args was none!"))
                            .unwrap();
                        args_cloned.reverse();
                        let unfolded =
                            parser::lexer_unfold_many_pair(&mut args_cloned);
                        type_ast_from_json(&unfolded)
                    }
                }
            }
            "set" => {
                let inner_ast = type_ast_from_json(&args.unwrap()[0])?;
                Ok(Ele {
                    name: annot,
                    expr_type: ExprTy::List(true, Box::new(inner_ast)),
                })
            }
            "list" => {
                let inner_ast = type_ast_from_json(&args.unwrap()[0])?;
                Ok(Ele {
                    name: annot,
                    expr_type: ExprTy::List(false, Box::new(inner_ast)),
                })
            }
            "string" => Ok(simple_expr!(ExprTy::String, annot)),
            "chain_id" | "bls12_381_g1" | "bls12_381_g2" | "bls12_381_fr" => {
                Ok(simple_expr!(
                    ExprTy::String,
                    annot.or_else(|| Some(
                        prim.to_ascii_lowercase()
                            .as_str()
                            .to_string()
                    ))
                ))
            }
            "timestamp" => Ok(simple_expr!(ExprTy::Timestamp, annot)),
            "unit" => Ok(simple_expr!(ExprTy::Unit, annot)),
            // - ignoring constants, as far as we can see now there's no reason
            // to index these
            // - ignoring tickets and sapling_state because it's not clear to
            // us right now how this info would be used exactly
            // - ignoring lambdas because they're a pandoras box. probably are
            // impossible to index in a meaningful way
            "constant" | "never" | "ticket" | "sapling_state" | "lambda" => {
                Ok(simple_expr!(ExprTy::Stop, annot))
            }
            "contract" | "signature" => {
                Ok(simple_expr!(ExprTy::KeyHash, annot))
            }
            _ => Err(anyhow!(
                "unexpected storage json: {} {:#?}",
                prim.as_str(),
                json
            )),
        }
    } else {
        Err(anyhow!("Wrong JS {}", json.to_string()))
    }
}
