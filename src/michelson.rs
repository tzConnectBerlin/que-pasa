use crate::node::Node;
use curl::easy::Easy;
use json::JsonValue;
use num::BigInt;
use std::error::Error;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::SystemTime;

lazy_static! {
    static ref IDS: Mutex<u32> = Mutex::new(1u32);
}

fn get_id() -> u32 {
    let id = &mut *IDS.lock().unwrap();
    let val = *id;
    *id = *id + 1u32;
    val
}

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

/// Load from the ../test directory, only for testing
fn load_test(name: &str) -> String {
    std::fs::read_to_string(std::path::Path::new(name)).unwrap()
}

pub fn get_storage(contract_id: &String, level: u32) -> Result<JsonValue, Box<dyn Error>> {
    load(&format!(
        "https://testnet-tezos.giganode.io/chains/main/blocks/{}/context/contracts/{}/storage",
        level, contract_id
    ))
}

#[test]
fn test_generate() {
    let json = json::parse(&load_test(
        "test/KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq.script",
    ))
    .unwrap();
    let storage_definition = &json["code"][1]["args"][0];
    let ast = crate::storage::storage_from_json(storage_definition.clone());
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

pub fn get_operations(
    contract_id: &str,
    level: Option<u32>,
) -> Result<Vec<JsonValue>, Box<dyn Error>> {
    let level = match level {
        Some(x) => format!("{}", x),
        None => "head".to_string(),
    };
    let url = format!(
        "https://testnet-tezos.giganode.io/chains/main/blocks/{}",
        level
    );
    let json = load(&url)?;
    if let JsonValue::Array(operations) = &json["operations"][3] {
        let mut result = vec![];
        for operation in operations {
            result.push(operation.clone());
        }
        Ok(result)
    } else {
        let err: Box<dyn Error> = String::from("No operations found in block").into();
        Err(err)
    }
}

#[derive(Clone, Debug)]
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
    if v.len() == 1 {
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

/// Goes through the actual stored data and builds up a structure which can be used in combination with the node
/// data to stash it in the database.
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
                if let JsonValue::Array(array) = args {
                    if array.len() != 2 {
                        let mut array = array.clone();
                        array.reverse();
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
        // it's a leaf node, hence a value.
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

/// Walks simultaneously through the table definition and the actual values it finds, and attempts
/// to match them. Panics if it cannot do this (i.e. they do not match).
pub fn update(value: &Value, node: &Node) {
    update2(value, node, get_id(), None);
}

pub fn update2(value: &Value, node: &Node, mut id: u32, mut fk_id: Option<u32>) {
    match node._type {
        // When a new table is initialised, we increment id and make the old id the fk constraint
        crate::node::Type::Table => {
            debug!("Creating table from node {:#?}", node);
            fk_id = Some(id);
            id = get_id();
        }
        _ => (),
    }

    match value {
        Value::Pair(left, right) => {
            let l = node.left.as_ref().unwrap();
            let r = node.right.as_ref().unwrap();
            update2(right, r, id, fk_id);
            update2(left, l, id, fk_id);
        }
        Value::List(l) => {
            for element in l {
                debug!("Elt: {:?}", element);
                update2(*&element, node, id, fk_id);
            }
        }
        Value::Elt(keys, values) => {
            let l = node.left.as_ref().unwrap();
            let r = node.right.as_ref().unwrap();
            update2(keys, l, id, fk_id);
            update2(values, r, id, fk_id);
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
            );
            crate::table::insert::add_column(table_name, id, fk_id, column_name, value.clone());
        }
    }
}
