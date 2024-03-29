use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, LocalResult, TimeZone, Utc};
use num::{BigInt, ToPrimitive};
use serde_json::json;
use std::str::from_utf8;
use std::str::FromStr;

use crate::sql::insert;

#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
pub enum Value {
    Address(String),
    Bool(bool),
    Bytes(String),
    Int(BigInt),
    KeyHash(String),
    Mutez(BigInt),
    Nat(BigInt),
    String(String),
    Timestamp(DateTime<Utc>),
    Unit,
    None,
    Elt(Box<Value>, Box<Value>),
    List(Vec<Value>),
    Pair(Box<Value>, Box<Value>),
    Left(Box<Value>),
    Right(Box<Value>),
}

impl Value {
    pub fn unfold_list(&self) -> Value {
        match self {
            Value::List(xs) => match xs.len() {
                0 => Value::None,
                1 => xs[0].clone(),
                _ => {
                    let left = Box::new(xs[0].clone());
                    let rest: Vec<Value> = xs.iter().skip(1).cloned().collect();
                    let right = Box::new(Value::List(rest).unfold_list());
                    Value::Pair(left, right)
                }
            },
            _ => self.clone(),
        }
    }

    pub fn unpair_list(&self) -> Result<Value> {
        match self {
            Value::Pair(l, rest) => {
                let rest = (**rest).clone();
                let rest_unpaired = if let Value::Pair { .. } = rest {
                    if let Value::List(rest_unpaired) = rest.unpair_list()? {
                        Ok(rest_unpaired)
                    } else {
                        Err(anyhow!("bad paired list value (partially nested pairs, partially something else)"))
                    }
                } else {
                    Ok(vec![rest])
                }?;
                let mut xs = vec![(**l).clone()];
                xs.extend(rest_unpaired);
                Ok(Value::List(xs))
            }
            Value::List(_xs) => Ok(self.clone()),
            _ => Err(anyhow!("bad paired List value (neither pairs nor list)")),
        }
    }

    pub fn unpair_elts(&self) -> Result<Value> {
        match self {
            Value::Pair(l, rest) => {
                if let Value::Elt { .. } = **l {
                    let rest = (**rest).clone();
                    let rest_unpaired = match rest {
                        Value::Elt { .. } => Ok(vec![rest]),
                        Value::Pair { .. } => {
                            if let Value::List(rest_unpaired) =
                                rest.unpair_elts()?
                            {
                                Ok(rest_unpaired)
                            } else {
                                Err(anyhow!("bad paired Elt value (partially nested pairs of Elt, partially something else)"))
                            }
                        },
                        _ => Err(anyhow!("bad paired Elt value (partially nested pairs of Elt, partially something else)")),
                    }?;
                    let mut xs = vec![(**l).clone()];
                    xs.extend(rest_unpaired);
                    Ok(Value::List(xs))
                } else {
                    Ok(self.clone())
                }
            }
            _ => Ok(self.clone()),
        }
    }
}

pub(crate) fn parse_json(storage_json: &serde_json::Value) -> Result<Value> {
    let lexed = lex(storage_json);
    parse_lexed(&lexed)
        .with_context(|| "failed to parse storage json into Value")
}

pub(crate) fn decode_address(hex: &str) -> Result<String> {
    let addr_hex = &hex[0..44];
    let callback_hex = &hex[44..];
    let mut res = decode_bs58_address(addr_hex)?;
    if !callback_hex.is_empty() {
        res += format!("%{}", from_utf8(&hex::decode(callback_hex)?)?).as_str();
    }
    Ok(res)
}

fn decode_bs58_address(hex: &str) -> Result<String> {
    if hex.len() != 44 {
        return Err(anyhow!(
            "44 length byte arrays only supported right now, got {} (which has len={})",
            hex, hex.len()
        ));
    }
    let implicit = &hex[0..2] == "00";
    let kt = &hex[0..2] == "01";
    let _type = &hex[2..4];
    let rest = &hex[4..];
    let new_hex = if kt {
        format!("025a79{}", &hex[2..42])
    } else if implicit {
        match _type {
            "00" => format!("06a19f{}", rest),
            "01" => format!("06a1a1{}", rest),
            "02" => format!("06a1a4{}", rest),
            _ => return Err(anyhow!("Did not recognise byte array {}", hex)),
        }
    } else {
        return Err(anyhow!("Unknown format {}", hex));
    };
    let encoded = bs58::encode(hex::decode(new_hex.as_str())?)
        .with_check()
        .into_string();
    Ok(encoded)
}

fn lex(json: &serde_json::Value) -> serde_json::Value {
    if let serde_json::Value::Array(mut a) = json.clone() {
        if a.is_empty() {
            return json.clone();
        }
        a.reverse();
        lexer_unfold_many_pair(&mut a)
    } else {
        json.clone()
    }
}

/// Goes through the actual stored data and builds up a structure which can be used in combination with the node
/// data to stash it in the database.
pub(crate) fn parse_lexed(json: &serde_json::Value) -> Result<Value> {
    if let serde_json::Value::Array(a) = json {
        return Ok(Value::List(
            a.iter()
                .map(|x| parse_lexed(x).unwrap())
                .collect(),
        ));
    }
    let args: Vec<serde_json::Value> = match &json["args"] {
        serde_json::Value::Array(a) => a.clone(),
        _ => vec![],
    };
    if let Some(s) = &json["prim"].as_str() {
        let mut prim = s.to_string();
        prim.make_ascii_uppercase();
        match prim.as_str() {
            "ELT" => {
                if args.len() != 2 {
                    panic!("Elt with array length of {}", args.len());
                }
                return Ok(Value::Elt(
                    Box::new(parse_lexed(&args[0])?),
                    Box::new(parse_lexed(&args[1])?),
                ));
            }
            "FALSE" => return Ok(Value::Bool(false)),
            "LEFT" => return Ok(Value::Left(Box::new(parse_lexed(&args[0])?))),
            "NONE" => return Ok(Value::None),
            "RIGHT" => {
                return Ok(Value::Right(Box::new(parse_lexed(&args[0])?)))
            }
            "PAIR" => {
                match args.len() {
                    0 | 1 => return Ok(Value::None),
                    2 => {
                        return Ok(Value::Pair(
                            Box::new(parse_lexed(&args[0])?),
                            Box::new(parse_lexed(&args[1])?),
                        ));
                    }
                    _ => {
                        let mut args = args;
                        args.reverse(); // so we can pop() afterward. But TODO: fix
                        let lexed = lexer_unfold_many_pair(&mut args);
                        return parse_lexed(&lexed);
                    }
                }
            }
            "PUSH" => return Ok(Value::None),
            "SOME" => {
                if !args.is_empty() {
                    return parse_lexed(&args[0]);
                } else {
                    debug!("Got SOME with no content");
                    return Ok(Value::None);
                }
            }
            "TRUE" => return Ok(Value::Bool(true)),
            "UNIT" => return Ok(Value::Unit),

            _ => {
                debug!("Ignoring unknown prim {}", json["prim"]);
                return Ok(Value::None);
            }
        }
    }

    let keys: Vec<String> = json
        .as_object()
        .unwrap()
        .iter()
        .map(|(a, _)| String::from(a))
        .collect();
    if keys.len() == 1 {
        // it's a leaf node, hence a value.
        let key = &keys[0];
        let s = String::from(
            json[key]
                .as_str()
                .ok_or_else(|| anyhow!("Key {} not found", key))?,
        );
        return match key.as_str() {
            "address" => Ok(Value::Address(s)),
            "bytes" => Ok(Value::Bytes(s)),
            "int" => Ok(Value::Int(bigint(&s)?)),
            "mutez" => Ok(Value::Mutez(bigint(&s)?)),
            "nat" => Ok(Value::Nat(bigint(&s)?)),
            "string" => Ok(Value::String(s)),
            //"timestamp" => Ok(Value::Timestamp(s)),
            "unit" => Ok(Value::Unit),
            "prim" => Ok(prim(&s)),
            _ => {
                panic!("Couldn't match {} in {}", key, json);
            }
        };
    }

    if let serde_json::Value::Array(a) = json {
        let mut array = a.clone();
        array.reverse();
        return parse_lexed(&lexer_unfold_many_pair(&mut array));
    }

    warn!("Couldn't get a value from {:#?} with keys {:?}", json, keys);
    Ok(Value::None)
}

pub(crate) fn lexer_unfold_many_pair(
    v: &mut Vec<serde_json::Value>,
) -> serde_json::Value {
    match v.len() {
        0 => panic!("Called empty"),
        1 => v[0].clone(),
        _ => {
            let ele = v.pop().unwrap();
            let rest = lexer_unfold_many_pair(v);
            json!({
                "prim": "Pair",
                "args": json!([ele, rest]),
            })
        }
    }
}

fn bigint(source: &str) -> Result<BigInt> {
    Ok(BigInt::from_str(source)?)
}

pub(crate) fn parse_date(value: &Value) -> Result<insert::Value> {
    match value {
        Value::Int(s) => {
            let ts: i64 = s
                .to_i64()
                .ok_or_else(|| anyhow!("Num conversion failed"))?;
            match Utc.timestamp_opt(ts, 0) {
                LocalResult::Single(t) => Ok(insert::Value::Timestamp(Some(t))),
                LocalResult::None => Ok(insert::Value::Timestamp(None)),
                LocalResult::Ambiguous(_, _) => {
                    Err(anyhow!("Can't parse {:?}", value))
                }
            }
        }
        Value::String(s) => {
            let fixedoffset = if s.chars().all(|c| c.is_numeric()) {
                chrono::DateTime::parse_from_str(
                    format!("{}+0000", s).as_str(),
                    "%s%z",
                )?
            } else {
                chrono::DateTime::parse_from_rfc3339(s.as_str())?
            };
            Ok(insert::Value::Timestamp(Some(
                fixedoffset.with_timezone(&Utc),
            )))
        }
        _ => Err(anyhow!("Can't parse {:?}", value)),
    }
}

fn prim(s: &str) -> Value {
    match s {
        "False" => Value::Bool(true),
        "None" => Value::None,
        _ => panic!("Don't know what to do with prim {}", s),
    }
}

#[test]
fn test_decode() {
    let test_data = vec![
        (
            "00006b82198cb179e8306c1bedd08f12dc863f328886",
            "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
        ),
        (
            "01d62a20fd2574884476f3da2f1a41bb8cc289f8cc00",
            "KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq",
        ),
        (
            // there may be a callback address specified after the address itself
            // (tz1..%someFunction), we want to grab the tz1 address
            "016e4943f7a23ab9cbe56f48ff72f6c27e8956762400626f72726f775f63616c6c6261636b",
            "KT1JdufSdfg3WyxWJcCRNsBFV9V3x9TQBkJ2%borrow_callback",
        ),
    ];
    for (from, to) in test_data {
        assert_eq!(to, decode_address(from).unwrap().as_str());
    }
}
