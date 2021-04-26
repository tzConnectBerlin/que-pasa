use crate::error::Res;
use crate::node::Node;
use chrono::{DateTime, TimeZone, Utc};
use curl::easy::Easy;
use json::JsonValue;
use num::{BigInt, ToPrimitive};
use std::error::Error;
use std::str::FromStr;
use std::sync::Mutex;

const NODE_URL: &str = "http://edo2full.newby.org:8732";
//const NODE_URL: &str = "https://testnet-tezos.giganode.io";

lazy_static! {
    static ref IDS: Mutex<u32> = Mutex::new(1u32);
}

pub fn get_id() -> u32 {
    let id = &mut *IDS.lock().unwrap();
    let val = *id;
    *id = *id + 1u32;
    debug!("michelson::get_id {}", id);
    val
}

pub fn curr_id() -> u32 {
    *IDS.lock().unwrap()
}

pub fn set_id(new_id: u32) {
    let id = &mut *IDS.lock().unwrap();
    *id = new_id;
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
    Timestamp(DateTime<Utc>),
    Unit(Option<String>),
}

#[derive(Clone, Debug)]
pub struct Level {
    pub _level: u32,
    pub hash: String,
}

type BigMapMap = std::collections::HashMap<u32, (u32, Node)>;

pub struct StorageParser {
    big_map_map: BigMapMap,
}

impl StorageParser {
    pub fn new() -> Self {
        Self {
            big_map_map: BigMapMap::new(),
        }
    }

    /// Load a uri (of course)
    fn load(uri: &String) -> Result<JsonValue, Box<dyn Error>> {
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

    /// Return the highest level on the chain
    pub fn head() -> Res<Level> {
        let json = Self::load(&format!("{}/chains/main/blocks/head", NODE_URL))?;
        Ok(Level {
            _level: json["header"]["level"]
                .as_u32()
                .ok_or(crate::error::Error::boxed("Couldn't get level from node"))?,
            hash: json["hash"].to_string(),
        })
    }

    pub fn level(level: u32) -> Res<Level> {
        let json = Self::load(&format!("{}/chains/main/blocks/{}", NODE_URL, level))?;
        Ok(Level {
            _level: json["header"]["level"]
                .as_u32()
                .ok_or(crate::error::Error::boxed("Couldn't get level from node"))?,
            hash: json["hash"].to_string(),
        })
    }

    /// Get the storage at a level
    pub fn get_storage(
        &self,
        contract_id: &String,
        level: u32,
    ) -> Result<JsonValue, Box<dyn Error>> {
        Self::load(&format!(
            "{}/chains/main/blocks/{}/context/contracts/{}/storage",
            NODE_URL, level, contract_id
        ))
    }

    /// Get all of the data for the contract.
    pub fn get_everything(
        contract_id: &str,
        level: Option<u32>,
    ) -> Result<JsonValue, Box<dyn Error>> {
        let level = match level {
            Some(x) => format!("{}", x),
            None => "head".to_string(),
        };
        let url = format!(
            "{}/chains/main/blocks/{}/context/contracts/{}/script",
            NODE_URL, level, contract_id
        );
        debug!("Loading contract data for {} url is {}", contract_id, url);
        Self::load(&url)
    }

    /// Pass in a set of operations from the node, get back the parts which update big maps
    pub fn get_big_map_operations_from_operations(
        json: &JsonValue,
    ) -> Result<Vec<JsonValue>, Box<dyn Error>> {
        if let JsonValue::Array(a) =
            &json["contents"][0]["metadata"]["operation_result"]["big_map_diff"]
        {
            Ok(a.clone())
        } else {
            Err(crate::error::Error::boxed(&format!(
                "Not array: {}",
                json.to_string()
            )))
        }
    }

    pub fn get_storage_from_operation(json: &JsonValue) -> Result<JsonValue, Box<dyn Error>> {
        Ok(json["contents"][0]["metadata"]["operation_result"]["storage"].clone())
    }

    pub fn get_operations_from_node(
        contract_id: &str,
        level: Option<u32>,
    ) -> Result<Vec<JsonValue>, Box<dyn Error>> {
        let level = match level {
            Some(x) => format!("{}", x),
            None => "head".to_string(),
        };
        let url = format!("{}/chains/main/blocks/{}", NODE_URL, level);
        let json = StorageParser::load(&url)?;
        Self::get_operations_from_block_json(contract_id, &json)
    }

    pub fn get_operations_from_block_json(
        contract_id: &str,
        json: &JsonValue,
    ) -> Result<Vec<JsonValue>, Box<dyn Error>> {
        if let JsonValue::Array(operations) = &json["operations"][3] {
            let mut result = vec![];
            for operation in operations {
                if let JsonValue::String(id) = &operation["contents"][0]["destination"] {
                    if id == contract_id {
                        result.push(operation.clone());
                    } else {
                        debug!("{} Didn't match!", id);
                    }
                }
            }
            Ok(result)
        } else {
            let err: Box<dyn Error> = String::from("No operations section found in block").into();
            Err(err)
        }
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

    fn parse_date(value: &Value) -> Result<DateTime<Utc>, Box<dyn Error>> {
        match value {
            Value::Int(s) => {
                let ts: i64 = s.to_i64().ok_or("Num conversion failed")?;
                Ok(Utc.timestamp(ts, 0))
            }
            _ => Err(crate::error::Error::boxed(&format!(
                "Can't parse {:?}",
                value
            ))),
        }
    }

    pub fn decode_address(hex: &str) -> Res<String> {
        if hex.len() != 44 {
            return Err(crate::error::Error::boxed(&format!(
                "44 length byte arrays only supported right now, got {}",
                hex
            )));
        }
        let implicit = &hex[0..2] == "00";
        let kt = &hex[0..2] == "01";
        let _type = &hex[2..4];
        let rest = &hex[4..];
        let new_hex = if kt {
            format!("025a79{}", &hex[2..42]).to_string()
        } else if implicit {
            match _type {
                "00" => format!("06a19f{}", rest).to_string(),
                "01" => format!("06a1a1{}", rest).to_string(),
                "02" => format!("06a1a4{}", rest).to_string(),
                _ => {
                    return Err(crate::error::Error::boxed(&format!(
                        "Did not recognise byte array {}",
                        hex
                    )))
                }
            }
        } else {
            return Err(crate::error::Error::boxed(&format!(
                "Unknown format {}",
                hex
            )));
        };
        println!("new_hex: {}", new_hex);
        let encoded = bs58::encode(hex::decode(new_hex.as_str())?)
            .with_check()
            .into_string();
        Ok(encoded)
    }

    /// Goes through the actual stored data and builds up a structure which can be used in combination with the node
    /// data to stash it in the database.
    pub fn parse_storage(&self, json: &JsonValue) -> Res<Value> {
        if let JsonValue::Array(a) = json {
            let mut inner: Vec<Box<Value>> = vec![];
            for i in a {
                inner.push(Box::new(self.parse_storage(&i)?));
            }
            return Ok(Value::List(inner));
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
                    return Ok(Value::Elt(
                        Box::new(self.parse_storage(&args[0])?),
                        Box::new(self.parse_storage(&args[1])?),
                    ));
                }
                &"False" => return Ok(Value::Bool(false)),
                &"Left" => return Ok(Value::Left(Box::new(self.parse_storage(&args[0])?))),
                &"None" => return Ok(Value::None),
                &"Right" => return Ok(Value::Right(Box::new(self.parse_storage(&args[0])?))),
                &"Pair" => {
                    if args.len() != 2 {
                        let mut args = args.clone();
                        args.reverse(); // so we can pop() afterward. But TODO: fix
                        let parsed = self.preparse_storage2(&mut args);
                        return self.parse_storage(&parsed);
                    }
                    return Ok(Value::Pair(
                        Box::new(self.parse_storage(&args[0])?),
                        Box::new(self.parse_storage(&args[1])?),
                    ));
                }
                &"Some" => return self.parse_storage(&args[0]),
                &"Unit" => return Ok(Value::Unit(None)),
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
                "address" => Ok(Value::Address(s)),
                "bytes" => Ok(Value::Bytes(s)),
                "int" => Ok(Value::Int(Self::bigint(&s.to_string())?)),
                "mutez" => Ok(Value::Mutez(Self::bigint(&s)?)),
                "nat" => Ok(Value::Nat(Self::bigint(&s).unwrap())),
                "string" => Ok(Value::String(s)),
                //"timestamp" => Ok(Value::Timestamp(s)),
                "unit" => Ok(Value::Unit(None)),
                "prim" => Ok(Self::prim(&s)),
                _ => panic!("Couldn't match {} in {}", key.to_string(), json.to_string()),
            };
        }
        error!("Couldn't get a value from {:#?} with keys {:?}", json, keys);
        Ok(Value::None)
    }

    pub fn prim(s: &String) -> Value {
        match s.as_str() {
            "False" => Value::Bool(true),
            "None" => Value::None,
            _ => panic!("Don't know what to do with prim {}", s),
        }
    }

    pub fn process_big_map(&mut self, json: &JsonValue) -> Result<(), Box<dyn Error>> {
        debug!("process_big_map {}", json.to_string());
        let big_map_id: u32 = json["big_map"].to_string().parse().unwrap();
        let key: Value = self.parse_storage(&self.preparse_storage(&json["key"]))?;
        let value: Value = self.parse_storage(&self.preparse_storage(&json["value"]))?;
        let (fk, node): (u32, Node) = self.big_map_map.get(&big_map_id).unwrap().clone();
        match json["action"].as_str().unwrap() {
            "update" => {
                let id = get_id();
                self.read_storage_internal(&key, &node.left.unwrap(), id, Some(fk));
                self.read_storage_internal(&value, &node.right.unwrap(), id, Some(fk));
            }
            _ => panic!("{}", json.to_string()),
        };
        Ok(())
    }

    /// Walks simultaneously through the table definition and the actual values it finds, and attempts
    /// to match them. Panics if it cannot do this (i.e. they do not match).
    pub fn read_storage(&mut self, value: &Value, node: &Node) -> Result<(), Box<dyn Error>> {
        self.read_storage_internal(value, node, get_id(), None);
        Ok(())
    }

    // we detect the start of a new table by the annotations in the Node struct. But we only want to
    // do this once per table, so we must ensure we don't get confused by multiple Elts for multiple
    // rows
    fn is_new_table(node: &Node, value: &Value) -> bool {
        match node._type {
            // When a new table is initialised, we increment id and make the old id the fk constraint
            crate::node::Type::Table => match node.expr {
                crate::storage::Expr::ComplexExpr(crate::storage::ComplexExpr::Map(_, _)) => {
                    match value {
                        Value::Elt(_, _) => false,
                        Value::List(_) => true,
                        _ => {
                            panic!("Unexpected value {:?}", value);
                        }
                    }
                }
                _ => false,
            },
            _ => false,
        }
    }

    pub fn read_storage_internal(
        &mut self,
        value: &Value,
        node: &Node,
        mut id: u32,
        mut fk_id: Option<u32>,
    ) {
        debug!("read_storage_internal id: {} Node: {:?}", id, node);
        if Self::is_new_table(node, value) {
            // get a new id and make the old one the current foreign key
            fk_id = Some(id);
            id = get_id();
            debug!(
                "Creating table from node {:?} with id {} and fk_id {:?}",
                node, id, fk_id
            );
        }

        match value {
            Value::Elt(keys, values) => {
                // entry in a map or a big map.
                let l = node.left.as_ref().unwrap();
                let r = node.right.as_ref().unwrap();
                self.read_storage_internal(keys, l, id, fk_id);
                self.read_storage_internal(values, r, id, fk_id);
            }
            Value::Left(left) => {
                self.read_storage_internal(left, node.left.as_ref().unwrap(), id, fk_id);
            }
            Value::Right(right) => {
                self.read_storage_internal(right, node.right.as_ref().unwrap(), id, fk_id);
            }
            Value::List(l) => {
                for element in l {
                    debug!("Elt: {:?}", element);
                    self.read_storage_internal(*&element, node, id, fk_id);
                }
            }
            Value::Pair(left, right) => {
                let l = node.left.as_ref().unwrap();
                let r = node.right.as_ref().unwrap();
                self.read_storage_internal(right, r, id, fk_id);
                self.read_storage_internal(left, l, id, fk_id);
            }
            Value::Unit(None) => {
                debug!("Unit: value is {:#?}, node is {:#?}", value, node);
                self.read_storage_internal(
                    &Value::Unit(Some(node.value.as_ref().unwrap().clone())),
                    node,
                    id,
                    fk_id,
                );
            }
            _ => {
                // this is a value, and should be saved.
                let table_name = node.table_name.as_ref().unwrap().to_string();
                debug!("node: {:?} value: {:?}", node, value);
                let column_name = node.column_name.as_ref().unwrap().to_string();
                debug!(
                    "{} {} = {:?} {:?}",
                    table_name, column_name, value, node.expr
                );

                // If this is a big map, save the id and the fk_id currently
                // being used, for later processing
                match &node.expr {
                    crate::storage::Expr::ComplexExpr(ce) => match ce {
                        crate::storage::ComplexExpr::BigMap(_, _) => {
                            debug!("{:?}", value);
                            if let Value::Int(i) = value {
                                debug!("{}", i);
                                debug!("Saving bigmap {:} with parent {}", i, id);
                                self.save_bigmap_location(i.to_u32().unwrap(), id, node.clone());
                            } else {
                                panic!("Found big map with non-int id: {:?}", node);
                            }
                        }
                        _ => (),
                    },
                    crate::storage::Expr::SimpleExpr(crate::storage::SimpleExpr::Timestamp) => {
                        crate::table::insert::add_column(
                            table_name,
                            id,
                            fk_id,
                            column_name,
                            Value::Timestamp(Self::parse_date(&value.clone()).unwrap()),
                        );
                    }
                    crate::storage::Expr::SimpleExpr(crate::storage::SimpleExpr::Address) => {
                        crate::table::insert::add_column(
                            table_name,
                            id,
                            fk_id,
                            column_name,
                            if let Value::Bytes(a) = value {
                                // sometimes we get bytes where we expected an address.
                                Value::Address(Self::decode_address(&a).unwrap())
                            } else {
                                value.clone()
                            },
                        );
                    }
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

    fn save_bigmap_location(&mut self, bigmap_id: u32, fk: u32, node: Node) {
        debug!("Saving {} -> ({:?}, {:?})", bigmap_id, fk, node);
        self.big_map_map.insert(bigmap_id, (fk, node));
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
    ];
    for (from, to) in test_data {
        assert_eq!(to, StorageParser::decode_address(from).unwrap().as_str());
    }
}
