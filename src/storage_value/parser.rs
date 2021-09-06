use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use json;
use json::JsonValue;
use num::{BigInt, ToPrimitive};
use std::str::from_utf8;
use std::str::FromStr;

#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
pub enum Value {
    Address(String),
    Bool(bool),
    Bytes(String),
    Elt(Box<Value>, Box<Value>),
    Int(BigInt),
    KeyHash(String),
    Left(Box<Value>),
    List(Vec<Value>),
    Mutez(BigInt),
    Nat(BigInt),
    None,
    Pair(Box<Value>, Box<Value>),
    Right(Box<Value>),
    String(String),
    Timestamp(DateTime<Utc>),
    Unit(Option<String>),
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
}

pub(crate) fn parse(storage_json: String) -> Result<Value> {
    let json_parsed = &json::parse(&storage_json)
        .with_context(|| "failed to parse storage data into json")?;
    let lexed = lex(json_parsed);
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

fn lex(json: &JsonValue) -> JsonValue {
    if let JsonValue::Array(mut a) = json.clone() {
        a.reverse();
        lexer_unfold_many_pair(&mut a)
    } else {
        json.clone()
    }
}

/// Goes through the actual stored data and builds up a structure which can be used in combination with the node
/// data to stash it in the database.
pub(crate) fn parse_lexed(json: &JsonValue) -> Result<Value> {
    if let JsonValue::Array(a) = json {
        return Ok(Value::List(
            a.iter()
                .map(|x| parse_lexed(x).unwrap())
                .collect(),
        ));
    }
    let args: Vec<JsonValue> = match &json["args"] {
        JsonValue::Array(a) => a.clone(),
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
            "UNIT" => return Ok(Value::Unit(None)),

            _ => {
                debug!("Ignoring unknown prim {}", json["prim"]);
                return Ok(Value::None);
            }
        }
    }

    let keys: Vec<String> = json
        .entries()
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
            "unit" => Ok(Value::Unit(None)),
            "prim" => Ok(prim(&s)),
            _ => {
                panic!(
                    "Couldn't match {} in {}",
                    key.to_string(),
                    json.to_string()
                );
            }
        };
    }

    if let JsonValue::Array(a) = json {
        let mut array = a.clone();
        array.reverse();
        return parse_lexed(&lexer_unfold_many_pair(&mut array));
    }

    warn!("Couldn't get a value from {:#?} with keys {:?}", json, keys);
    Ok(Value::None)
}

pub(crate) fn lexer_unfold_many_pair(v: &mut Vec<JsonValue>) -> JsonValue {
    match v.len() {
        0 => panic!("Called empty"),
        1 => v[0].clone(),
        _ => {
            let ele = v.pop();
            let rest = lexer_unfold_many_pair(v);
            return object! {
                "prim": "Pair",
                "args": [
                    ele,
                    rest,
                ]
            };
        }
    }
}

fn bigint(source: &str) -> Result<BigInt> {
    Ok(BigInt::from_str(source)?)
}

pub(crate) fn parse_date(value: &Value) -> Result<DateTime<Utc>> {
    match value {
        Value::Int(s) => {
            let ts: i64 = s
                .to_i64()
                .ok_or_else(|| anyhow!("Num conversion failed"))?;
            Ok(Utc.timestamp(ts, 0))
        }
        Value::String(s) => {
            let fixedoffset = chrono::DateTime::parse_from_rfc3339(s.as_str())?;
            Ok(fixedoffset.with_timezone(&Utc))
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
