use crate::err;
use crate::error::Res;
use json::JsonValue;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SimpleExpr {
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
pub enum ComplexExpr {
    BigMap(Box<Ele>, Box<Ele>),
    Map(Box<Ele>, Box<Ele>),
    Pair(Box<Ele>, Box<Ele>),
    OrEnumeration(Box<Ele>, Box<Ele>),
    Option(Box<Ele>), // TODO: move this out into SimpleExpr??
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    SimpleExpr(SimpleExpr),
    ComplexExpr(ComplexExpr),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Ele {
    pub expr: Expr,
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
            expr: Expr::SimpleExpr($typ),
        }
    };
}

macro_rules! complex_expr {
    ($typ:expr, $name:expr, $args:expr) => {{
        let args = $args.unwrap();
        Ele {
            name: $name,
            expr: Expr::ComplexExpr($typ(
                Box::new(storage_from_json(args[0].clone())?),
                Box::new(storage_from_json(args[1].clone())?),
            )),
        }
    }};
}

/// An or can be a variant record, or a simple enumeration.
pub fn is_enumeration_or(json: &JsonValue) -> bool {
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

pub fn storage_from_json(json: JsonValue) -> Res<Ele> {
    let annot = annotation(&json);
    let args = args(&json);
    debug!("prim is {:?}", json["prim"]);
    if let JsonValue::Short(prim) = &json["prim"] {
        match prim.as_str() {
            "address" => Ok(simple_expr!(SimpleExpr::Address, annot)),
            "big_map" => Ok(complex_expr!(ComplexExpr::BigMap, annot, args)),
            "bool" => Ok(simple_expr!(SimpleExpr::Bool, annot)),
            "bytes" => Ok(simple_expr!(SimpleExpr::Bytes, annot)),
            "int" => Ok(simple_expr!(SimpleExpr::Int, annot)),
            "key" => Ok(simple_expr!(SimpleExpr::KeyHash, annot)), // TODO: check this is correct
            "key_hash" => Ok(simple_expr!(SimpleExpr::KeyHash, annot)),
            "map" => Ok(complex_expr!(ComplexExpr::Map, annot, args)),
            "mutez" => Ok(simple_expr!(SimpleExpr::Mutez, annot)),
            "nat" => Ok(simple_expr!(SimpleExpr::Nat, annot)),
            "option" => {
                let args = args.ok_or_else(|| err!("Args was none!"))?;
                Ok(Ele {
                    name: annot,
                    expr: Expr::ComplexExpr(ComplexExpr::Option(Box::new(storage_from_json(
                        args[0].clone(),
                    )?))),
                })
            }
            "or" => {
                if is_enumeration_or(&json) {
                    Ok(complex_expr!(ComplexExpr::OrEnumeration, annot, args))
                } else {
                    unimplemented!(
                        "Or used as variant record found, don't know how to deal with it {}",
                        json.to_string()
                    );
                }
            }
            "pair" => {
                if args.clone().ok_or_else(|| err!("NoneError"))?.len() != 2 {
                    return Err(err!(
                        "Pair with {} args",
                        args.ok_or_else(|| err!("NoneError"))?.len()
                    ));
                }
                Ok(complex_expr!(ComplexExpr::Pair, annot, args))
            }
            "string" => Ok(simple_expr!(SimpleExpr::String, annot)),
            "timestamp" => Ok(simple_expr!(SimpleExpr::Timestamp, annot)),
            "unit" => Ok(simple_expr!(SimpleExpr::Unit, annot)),
            "lambda" => Ok(simple_expr!(SimpleExpr::Stop, annot)),
            _ => Err(err!(
                "Unexpected storage json: {} {:#?}",
                prim.as_str(),
                json
            )),
        }
    } else {
        panic!("Wrong JS {}", json.to_string());
    }
}
