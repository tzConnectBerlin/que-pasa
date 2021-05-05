use json::JsonValue;

#[derive(Clone, Copy, Debug)]
pub enum SimpleExpr {
    Address,
    Bool,
    Bytes,
    Int,
    KeyHash,
    Mutez,
    Nat,
    String,
    Timestamp,
    Unit,
}

#[derive(Clone, Debug)]
pub enum ComplexExpr {
    BigMap(Box<Ele>, Box<Ele>),
    Map(Box<Ele>, Box<Ele>),
    Pair(Box<Ele>, Box<Ele>),
    OrEnumeration(Box<Ele>, Box<Ele>),
    Option(Box<Ele>), // TODO: move this out into SimpleExpr??
}

#[derive(Clone, Debug)]
pub enum Expr {
    SimpleExpr(SimpleExpr),
    ComplexExpr(ComplexExpr),
}

#[derive(Clone, Debug)]
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
                Box::new(storage_from_json(args[0].clone())),
                Box::new(storage_from_json(args[1].clone())),
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
        "unit" => true,
        _ => false,
    }
}

pub fn storage_from_json(json: JsonValue) -> Ele {
    let annot = annotation(&json);
    let args = args(&json);
    debug!("prim is {:?}", json["prim"]);
    if let JsonValue::Short(prim) = &json["prim"] {
        match prim.as_str() {
            "address" => simple_expr!(SimpleExpr::Address, annot),
            "big_map" => complex_expr!(ComplexExpr::BigMap, annot, args),
            "bool" => simple_expr!(SimpleExpr::Bool, annot),
            "bytes" => simple_expr!(SimpleExpr::Bytes, annot),
            "int" => simple_expr!(SimpleExpr::Int, annot),
            "key_hash" => simple_expr!(SimpleExpr::KeyHash, annot),
            "map" => complex_expr!(ComplexExpr::Map, annot, args),
            "mutez" => simple_expr!(SimpleExpr::Mutez, annot),
            "nat" => simple_expr!(SimpleExpr::Nat, annot),
            "option" => {
                let args = args.unwrap();
                Ele {
                    name: annot,
                    expr: Expr::ComplexExpr(ComplexExpr::Option(Box::new(storage_from_json(
                        args[0].clone(),
                    )))),
                }
            }
            "or" => {
                if is_enumeration_or(&json) {
                    complex_expr!(ComplexExpr::OrEnumeration, annot, args)
                } else {
                    unimplemented!(
                        "Or used as variant record found, don't know how to deal with it {}",
                        json.to_string()
                    );
                }
            }
            "pair" => {
                if args.clone().unwrap().len() != 2 {
                    panic!("Pair with {} args", args.clone().unwrap().len());
                }
                complex_expr!(ComplexExpr::Pair, annot, args)
            }
            "string" => simple_expr!(SimpleExpr::String, annot),
            "timestamp" => simple_expr!(SimpleExpr::Timestamp, annot),
            "unit" => simple_expr!(SimpleExpr::Unit, annot),
            _ => panic!("Unexpected storage json: {} {:#?}", prim.as_str(), json),
        }
    } else {
        panic!("Wrong JS {}", json.to_string());
    }
}
