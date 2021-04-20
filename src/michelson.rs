use crate::node::Node;
use curl::easy::Easy;
use json::JsonValue;
use num::BigInt;
use std::error::Error;
use std::str::FromStr;
use std::time::SystemTime;

pub fn load(uri: &String) -> Result<JsonValue, Box<dyn Error>> {
    debug!("Loading: {}", uri);
    let mut response = Vec::new();
    let mut handle = Easy::new();
    handle.timeout(std::time::Duration::from_secs(20))?;
    handle.url(uri)?;
    {
        let mut transfer = handle.transfer();
        transfer.write_function(|new_data| {
            response.extend_from_slice(new_data);
            Ok(new_data.len())
        })?;
        transfer.perform()?;
    }
    let json = json::parse(&std::str::from_utf8(&response)?).unwrap();
    Ok(json)
}

pub fn get_storage(contract_id: &String, level: u32) -> Result<JsonValue, Box<dyn Error>> {
    load(&format!(
        "https://testnet-tezos.giganode.io/chains/main/blocks/{}/context/contracts/{}/storage",
        level, contract_id
    ))
}

pub fn get_everything(contract_id: &str, level: Option<u32>) -> Result<JsonValue, Box<dyn Error>> {
    let level = match level {
        Some(x) => format!("{}", x),
        None => "head".to_string(),
    };
    let url = format!(
        "https://testnet-tezos.giganode.io/chains/main/blocks/{}/context/contracts/{}/script",
        level, contract_id
    );
    debug!("Loading contract data for {} url is {}", contract_id, url);
    load(&url)
}

#[derive(Debug)]
pub enum Value {
    Address(String),
    Bool(bool),
    Bytes(String),
    Elt(Box<Value>, Box<Value>),
    Int(BigInt),
    List(Vec<Box<Value>>),
    Mutez(BigInt),
    Nat(BigInt),
    None,
    Pair(Box<Value>, Box<Value>),
    String(String),
    Timestamp(SystemTime),
    Unit,
}

fn bigint(source: &String) -> Result<BigInt, Box<dyn Error>> {
    Ok(BigInt::from_str(&source).unwrap())
}

pub fn preparse_storage(json: &JsonValue) -> JsonValue {
    if let JsonValue::Array(mut a) = json.clone() {
        a.reverse();
        preparse_storage2(&mut a)
    } else {
        json.clone()
    }
}

pub fn preparse_storage2(v: &mut Vec<JsonValue>) -> JsonValue {
    if v.len() <= 1 {
        return v[0].clone();
    } else {
        let ele = v.pop();
        let rest = preparse_storage2(v);
        return object! {
            "prim": "Pair",
            "args": [
                ele,
                rest,
            ]
        };
    }
}

pub fn parse_storage(json: &JsonValue) -> Value {
    if let JsonValue::Array(a) = json {
        let mut inner: Vec<Box<Value>> = vec![];
        for i in a {
            inner.push(Box::new(parse_storage(&i)));
        }
        return Value::List(inner);
    }

    if let Some(s) = &json["prim"].as_str() {
        match s {
            &"Pair" => {
                let args = json["args"].clone();
                if let JsonValue::Array(mut array) = args {
                    if array.len() != 2 {
                        let mut array = array.clone();
                        let parsed = preparse_storage2(&mut array);
                        return parse_storage(&parsed);
                    }
                    return Value::Pair(
                        Box::new(parse_storage(&array[0])),
                        Box::new(parse_storage(&array[1])),
                    );
                }
            }
            &"Elt" => {
                if let JsonValue::Array(array) = &json["args"] {
                    if array.len() != 2 {
                        panic!("Pair with array length of {}", array.len());
                    }
                    return Value::Elt(
                        Box::new(parse_storage(&array[0])),
                        Box::new(parse_storage(&array[1])),
                    );
                }
            }
            _ => panic!("Unknown prim {}", json["prim"]),
        }
    }

    let keys: Vec<String> = json.entries().map(|(a, _)| String::from(a)).collect();
    if keys.len() == 1 {
        let key = &keys[0];
        let s = String::from(json[key].as_str().unwrap());
        return match key.as_str() {
            "address" => Value::Address(s),
            "bytes" => Value::Bytes(s),
            "int" => Value::Int(bigint(&s.to_string()).unwrap()),
            "mutez" => Value::Mutez(bigint(&s).unwrap()),
            "nat" => Value::Nat(bigint(&s).unwrap()),
            "string" => Value::String(s),
            "timestamp" => Value::Timestamp(SystemTime::now()), // TODO: parse
            "unit" => Value::Unit,
            "prim" => prim(&s),
            _ => panic!("Couldn't match {} in {}", key.to_string(), json.to_string()),
        };
    }
    error!("Couldn't get a value from {:#?} with keys {:?}", json, keys);
    Value::None
}

pub fn prim(s: &String) -> Value {
    match s.as_str() {
        "False" => Value::Bool(true),
        "None" => Value::None,
        _ => panic!("Don't know what to do with prim {}", s),
    }
}

pub fn update(value: &Value, node: &Node) {
    match value {
        Value::Pair(left, right) => {
            let l = node.left.as_ref().unwrap();
            let r = node.right.as_ref().unwrap();
            update(left, l);
            update(right, r);
        }
        Value::List(l) => {
            for element in l {
                debug!("Elt: {:?}", element);
                update(*&element, node);
            }
        }
        Value::Elt(keys, values) => {
            let l = node.left.as_ref().unwrap();
            let r = node.right.as_ref().unwrap();
            update(keys, l);
            update(values, r);
        }
        _ => {
            let table_name = match node.table_name.as_ref() {
                Some(x) => x.clone(),
                None => "".to_string(),
            };
            let column_name = match node.column_name.as_ref() {
                Some(x) => x.clone(),
                None => "".to_string(),
            };
            debug!(
                "{} {} = {:?} {:?}",
                table_name, column_name, value, node.expr
            )
        }
    }
}
