use crate::node::Node;
use curl::easy::Easy;
use json::JsonValue;
use num::{BigInt, ToPrimitive};
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

#[derive(Clone, Debug)]
pub enum Value {
    Address(String),
    Bool(bool),
    Bytes(String),
    Elt(Box<Value>, Box<Value>),
    Int(BigInt),
    Left(Box<Value>),
    List(Vec<Box<Value>>),
    Mutez(BigInt),
    Nat(BigInt),
    None,
    Pair(Box<Value>, Box<Value>),
    Right(Box<Value>),
    String(String),
    Timestamp(SystemTime),
    Unit(Option<String>),
}

type BigMapMap = std::collections::HashMap<u32, Node>;

pub struct StorageParser {
    big_map_map: BigMapMap,
}

impl StorageParser {
    pub fn new() -> Self {
        Self {
            big_map_map: BigMapMap::new(),
        }
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

    pub fn get_storage(
        &self,
        contract_id: &String,
        level: u32,
    ) -> Result<JsonValue, Box<dyn Error>> {
        Self::load(&format!(
            "https://testnet-tezos.giganode.io/chains/main/blocks/{}/context/contracts/{}/storage",
            level, contract_id
        ))
    }

    pub fn get_everything(
        contract_id: &str,
        level: Option<u32>,
    ) -> Result<JsonValue, Box<dyn Error>> {
        let level = match level {
            Some(x) => format!("{}", x),
            None => "head".to_string(),
        };
        let url = format!(
            "https://testnet-tezos.giganode.io/chains/main/blocks/{}/context/contracts/{}/script",
            level, contract_id
        );
        debug!("Loading contract data for {} url is {}", contract_id, url);
        Self::load(&url)
    }

    fn bigint(source: &String) -> Result<BigInt, Box<dyn Error>> {
        Ok(BigInt::from_str(&source).unwrap())
    }

    pub fn preparse_storage(&self, json: &JsonValue) -> JsonValue {
        if let JsonValue::Array(mut a) = json.clone() {
            a.reverse();
            self.preparse_storage2(&mut a)
        } else {
            json.clone()
        }
    }

    pub fn preparse_storage2(&self, v: &mut Vec<JsonValue>) -> JsonValue {
        if v.len() == 1 {
            return v[0].clone();
        } else {
            let ele = v.pop();
            let rest = self.preparse_storage2(v);
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
    pub fn parse_storage(&self, json: &JsonValue) -> Value {
        if let JsonValue::Array(a) = json {
            let mut inner: Vec<Box<Value>> = vec![];
            for i in a {
                inner.push(Box::new(self.parse_storage(&i)));
            }
            return Value::List(inner);
        }
        let args: Vec<JsonValue> = match &json["args"] {
            JsonValue::Array(a) => a.clone(),
            _ => vec![],
        };
        if let Some(s) = &json["prim"].as_str() {
            match s {
                &"Elt" => {
                    if args.len() != 2 {
                        panic!("Pair with array length of {}", args.len());
                    }
                    return Value::Elt(
                        Box::new(self.parse_storage(&args[0])),
                        Box::new(self.parse_storage(&args[1])),
                    );
                }
                &"False" => return Value::Bool(false),
                &"Left" => return Value::Left(Box::new(self.parse_storage(&args[0]))),
                &"None" => return Value::None,
                &"Right" => return Value::Right(Box::new(self.parse_storage(&args[0]))),
                &"Pair" => {
                    if args.len() != 2 {
                        let mut args = args.clone();
                        args.reverse(); // TODO: figure out the whole reverse thing
                        let parsed = self.preparse_storage2(&mut args);
                        return self.parse_storage(&parsed);
                    }
                    return Value::Pair(
                        Box::new(self.parse_storage(&args[0])),
                        Box::new(self.parse_storage(&args[1])),
                    );
                }
                &"Some" => return self.parse_storage(&args[0]),
                &"Unit" => return Value::Unit(None),
                _ => {
                    panic!("Unknown prim {}", json["prim"]);
                }
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
                "int" => Value::Int(Self::bigint(&s.to_string()).unwrap()),
                "mutez" => Value::Mutez(Self::bigint(&s).unwrap()),
                "nat" => Value::Nat(Self::bigint(&s).unwrap()),
                "string" => Value::String(s),
                "timestamp" => Value::Timestamp(SystemTime::now()), // TODO: parse
                "unit" => Value::Unit(None),
                "prim" => Self::prim(&s),
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

    pub fn process_big_map(&mut self, json: &JsonValue) -> Result<(), Box<dyn Error>> {
        println!("{}", json.to_string());
        let big_map_id: u32 = json["big_map"].to_string().parse().unwrap();
        let key: Value = self.parse_storage(&self.preparse_storage(&json["key"]));
        let value: Value = self.parse_storage(&self.preparse_storage(&json["value"]));
        let node: Node = self.big_map_map.get(&big_map_id).unwrap().clone();
        match json["action"].as_str().unwrap() {
            "update" => {
                let id = get_id();
                self.update2(&key, &node.left.unwrap(), id, None);
                self.update2(&value, &node.right.unwrap(), id, None);
            }
            _ => panic!("{}", json.to_string()),
        };
        Ok(())
    }

    /// Walks simultaneously through the table definition and the actual values it finds, and attempts
    /// to match them. Panics if it cannot do this (i.e. they do not match).
    pub fn update(&mut self, value: &Value, node: &Node) {
        self.update2(value, node, get_id(), None);
    }

    pub fn update2(&mut self, value: &Value, node: &Node, mut id: u32, mut fk_id: Option<u32>) {
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
            Value::Elt(keys, values) => {
                let l = node.left.as_ref().unwrap();
                let r = node.right.as_ref().unwrap();
                self.update2(keys, l, id, fk_id);
                self.update2(values, r, id, fk_id);
            }
            Value::Left(left) => {
                self.update2(left, node.left.as_ref().unwrap(), id, fk_id);
            }
            Value::Right(right) => {
                self.update2(right, node.right.as_ref().unwrap(), id, fk_id);
            }
            Value::List(l) => {
                for element in l {
                    debug!("Elt: {:?}", element);
                    self.update2(*&element, node, id, fk_id);
                }
            }
            Value::Pair(left, right) => {
                let l = node.left.as_ref().unwrap();
                let r = node.right.as_ref().unwrap();
                self.update2(right, r, id, fk_id);
                self.update2(left, l, id, fk_id);
            }
            Value::Unit(None) => {
                println!("Unit: value is {:#?}, node is {:#?}", value, node);
                let name = match node.name.as_ref() {
                    Some(x) => x.clone(),
                    None => "Unknown Unit name".to_string(),
                };
                self.update2(
                    &Value::Unit(Some(node.value.as_ref().unwrap().clone())),
                    node,
                    id,
                    fk_id,
                );
            }
            _ => {
                let table_name = node.table_name.as_ref().unwrap().to_string();
                println!("node: {:?} value: {:?}", node, value);
                let column_name = node.column_name.as_ref().unwrap().to_string();
                debug!(
                    "{} {} = {:?} {:?}",
                    table_name, column_name, value, node.expr
                );

                // If this is a big map, save the id
                match &node.expr {
                    crate::storage::Expr::ComplexExpr(ce) => match ce {
                        crate::storage::ComplexExpr::BigMap(_, _) => {
                            println!("{:?}", value);
                            if let Value::Int(i) = value {
                                println!("{}", i);
                                self.save_bigmap_location(i.to_u32().unwrap(), node.clone());
                            } else {
                                panic!("Found big map with non-int id: {:?}", node);
                            }
                        }
                        _ => (),
                    },
                    _ => crate::table::insert::add_column(
                        table_name,
                        id,
                        fk_id,
                        column_name,
                        value.clone(),
                    ),
                }
            }
        }
    }

    fn save_bigmap_location(&mut self, id: u32, node: Node) {
        println!("Saving {} -> {:?}", id, node);
        self.big_map_map.insert(id, node);
    }
}
