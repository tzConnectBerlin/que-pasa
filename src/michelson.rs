use crate::err;
use crate::error::Res;
use crate::node::Node;
use chrono::{DateTime, TimeZone, Utc};
use curl::easy::Easy;
use json::JsonValue;
use num::{BigInt, ToPrimitive};
use std::error::Error;
use std::str::FromStr;
use std::sync::atomic::AtomicU32;

lazy_static! {
    static ref NODE_URL: String = match std::env::var("NODE_URL") {
        Ok(s) => s,
        Err(_) => "http://edo2full.newby.org:8732".to_string(),
    };
}

pub struct IdGenerator {
    id: AtomicU32,
}

impl IdGenerator {
    pub fn new(initial_value: u32) -> Self {
        Self {
            id: AtomicU32::new(initial_value),
        }
    }

    pub fn get_id(&mut self) -> u32 {
        let id = self.id.get_mut();
        let old_id: u32 = *id;
        *id += 1;
        old_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug)]
pub struct Level {
    pub _level: u32,
    pub hash: Option<String>,
}

type BigMapMap = std::collections::HashMap<u32, (u32, Node)>;

pub struct StorageParser {
    big_map_map: BigMapMap,
    pub id_generator: IdGenerator,
}

impl StorageParser {
    pub fn new(initial_id: u32) -> Self {
        Self {
            big_map_map: BigMapMap::new(),
            id_generator: IdGenerator::new(initial_id),
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
        let json = json::parse(&std::str::from_utf8(&response)?)?;
        Ok(json)
    }

    /// Return the highest level on the chain
    pub fn head() -> Res<Level> {
        let current_line = line!();
        println!("{:?} {}", *NODE_URL, current_line);
        let json = Self::load(&format!("{}/chains/main/blocks/head", *NODE_URL))?;
        Ok(Level {
            _level: json["header"]["level"]
                .as_u32()
                .ok_or_else(|| err!("Couldn't get level from node"))?,
            hash: Some(json["hash"].to_string()),
        })
    }

    pub fn level(level: u32) -> Res<Level> {
        let json = Self::level_json(level)?;
        Ok(Level {
            _level: json["header"]["level"]
                .as_u32()
                .ok_or(err!("Couldn't get level from node"))?,
            hash: Some(json["hash"].to_string()),
        })
    }

    pub fn level_json(level: u32) -> Res<JsonValue> {
        Self::load(&format!("{}/chains/main/blocks/{}", *NODE_URL, level))
    }

    pub fn level_has_tx_for_us(json: &JsonValue, contract_id: &str) -> Res<bool> {
        if let JsonValue::Array(array) = &json["operations"][3] {
            for op in array {
                if let JsonValue::Array(sub_ops) = &op["contents"] {
                    for sub_op in sub_ops {
                        debug!("destination: {}", sub_op["destination"].to_string());
                        if sub_op["destination"].to_string().as_str() == contract_id {
                            return Ok(true);
                        }
                    }
                }
            }
        } else {
            return Err(err!("Didn't find operations in JSON {:#?}", json));
        }
        Ok(false)
    }

    /// Get the storage at a level
    pub fn get_storage(
        &self,
        contract_id: &String,
        level: u32,
    ) -> Result<JsonValue, Box<dyn Error>> {
        Self::load(&format!(
            "{}/chains/main/blocks/{}/context/contracts/{}/storage",
            *NODE_URL, level, contract_id
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
            *NODE_URL, level, contract_id
        );
        debug!("Loading contract data for {} url is {}", contract_id, url);
        Self::load(&url)
    }

    pub fn get_originations_from_block(
        block: &JsonValue,
    ) -> Result<Vec<JsonValue>, Box<dyn Error>> {
        let mut result = vec![];
        if let JsonValue::Array(operations) = &block["operations"] {
            for ops in operations {
                if let JsonValue::Array(array) = ops {
                    for op in array {
                        result.extend(Self::get_matching_from_operations(
                            &op,
                            &"originated_contracts",
                        )?);
                    }
                }
            }
        } else {
            return Err(err!("operations not found in json {}", block.to_string()));
        }
        Ok(result)
    }

    pub fn block_has_contract_origination(block: &JsonValue, contract_id: &str) -> Res<bool> {
        Ok(Self::get_originations_from_block(block)?
            .iter()
            .any(|x| x == contract_id.to_string()))
    }

    pub fn get_big_map_operations_from_operations(
        ops: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Box<dyn Error>> {
        let mut result = vec![];
        for op in ops {
            result.extend(Self::get_matching_from_operations(op, &"big_map_diff")?);
        }
        Ok(result)
    }

    /// Pass in some json, get back matching fields
    pub fn get_matching_from_operations(json: &JsonValue, what: &str) -> Res<Vec<JsonValue>> {
        // TODO: make more specific.
        let mut result: Vec<JsonValue> = vec![];
        match json {
            JsonValue::Object(attributes) => {
                for (key, value) in attributes.iter() {
                    if key.eq(&what.to_string()) {
                        if let JsonValue::Array(a) = value {
                            return Ok(a.clone());
                        }
                    };
                    if let JsonValue::Object(_) = value {
                        result.extend(Self::get_matching_from_operations(&value, what)?);
                    }
                    if let JsonValue::Array(a) = value {
                        for i in a {
                            result.extend(Self::get_matching_from_operations(&i, what)?);
                        }
                    }
                }
            }
            _ => (),
        }
        Ok(result)
    }

    pub fn get_storage_from_operation(json: &JsonValue) -> Result<JsonValue, Box<dyn Error>> {
        Ok(json["metadata"]["operation_result"]["storage"].clone())
    }

    pub fn get_operations_from_node(
        contract_id: &str,
        level: Option<u32>,
    ) -> Result<Vec<JsonValue>, Box<dyn Error>> {
        let level = match level {
            Some(x) => format!("{}", x),
            None => "head".to_string(),
        };
        let url = format!("{}/chains/main/blocks/{}", *NODE_URL, level);
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
                if let JsonValue::Array(ops) = &operation["contents"] {
                    for op in ops {
                        if let Some(dest) = &op["destination"].as_str() {
                            if dest == &contract_id {
                                debug!("Match!");
                                debug!("{:?}", operation);
                                result.push(operation.clone());
                            }
                        } else {
                            debug!("{:?} Didn't match!", &op["destination"]);
                        }
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
        Ok(BigInt::from_str(&source)?)
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
        match v.len() {
            0 => panic!("Called empty"),
            1 => v[0].clone(),
            _ => {
                let ele = v.pop();
                debug!("{:?}", v);
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
    }

    fn parse_date(value: &Value) -> Result<DateTime<Utc>, Box<dyn Error>> {
        match value {
            Value::Int(s) => {
                let ts: i64 = s.to_i64().ok_or("Num conversion failed")?;
                Ok(Utc.timestamp(ts, 0))
            }
            Value::String(s) => {
                println!("{}", s);
                let fixedoffset = chrono::DateTime::parse_from_rfc3339(s.as_str())?;
                Ok(fixedoffset.with_timezone(&Utc))
            }
            _ => Err(err!("Can't parse {:?}", value)),
        }
    }

    pub fn decode_address(hex: &str) -> Res<String> {
        if hex.len() != 44 {
            return Err(err!(
                "44 length byte arrays only supported right now, got {}",
                hex
            ));
        }
        let implicit = &hex[0..2] == "00";
        let kt = &hex[0..2] == "01";
        let _type = &hex[2..4];
        let rest = &hex[4..];
        let new_hex = if kt {
            format!("025a79{}", &hex[2..42]).to_string()
        } else if implicit {
            match _type {
                "00" => format!("06a19f{}", rest),
                "01" => format!("06a1a1{}", rest),
                "02" => format!("06a1a4{}", rest),
                _ => return Err(err!("Did not recognise byte array {}", hex)),
            }
        } else {
            return Err(err!("Unknown format {}", hex));
        };
        let encoded = bs58::encode(hex::decode(new_hex.as_str())?)
            .with_check()
            .into_string();
        Ok(encoded)
    }

    /// Goes through the actual stored data and builds up a structure which can be used in combination with the node
    /// data to stash it in the database.
    pub fn parse_storage(&self, json: &JsonValue) -> Res<Value> {
        debug!("parse_storage: {:?}", json);
        if let JsonValue::Array(a) = json {
            match a.len() {
                0 => {
                    // TODO: must understand why this happens
                    return Ok(Value::None);
                }
                1 => {
                    return self.parse_storage(&a[0]);
                }
                _ => {
                    // let left = Box::new(self.parse_storage(&a[0].clone())?);
                    // let right = Box::new(self.parse_storage(&JsonValue::Array(a[1..].to_vec()))?);
                    // return Ok(Value::Pair(left, right));
                }
            }
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
                        Box::new(self.parse_storage(&args[0])?),
                        Box::new(self.parse_storage(&args[1])?),
                    ));
                }
                "FALSE" => return Ok(Value::Bool(false)),
                "LEFT" => return Ok(Value::Left(Box::new(self.parse_storage(&args[0])?))),
                "NONE" => return Ok(Value::None),
                "RIGHT" => return Ok(Value::Right(Box::new(self.parse_storage(&args[0])?))),
                "PAIR" => {
                    match args.len() {
                        0 | 1 => return Ok(Value::None),
                        2 => {
                            return Ok(Value::Pair(
                                Box::new(self.parse_storage(&args[0])?),
                                Box::new(self.parse_storage(&args[1])?),
                            ));
                        }
                        _ => {
                            let mut args = args;
                            args.reverse(); // so we can pop() afterward. But TODO: fix
                            let parsed = self.preparse_storage2(&mut args);
                            return self.parse_storage(&parsed);
                        }
                    }
                }
                "PUSH" => return Ok(Value::None),
                "SOME" => {
                    if !args.is_empty() {
                        return self.parse_storage(&args[0]);
                    } else {
                        warn!("Got SOME with no content");
                        return Ok(Value::None);
                    }
                }
                "TRUE" => return Ok(Value::Bool(true)),
                "UNIT" => return Ok(Value::Unit(None)),

                _ => {
                    warn!("Unknown prim {}", json["prim"]);
                    return Ok(Value::None);
                }
            }
        }

        let keys: Vec<String> = json.entries().map(|(a, _)| String::from(a)).collect();
        if keys.len() == 1 {
            // it's a leaf node, hence a value.
            let key = &keys[0];
            let s = String::from(json[key].as_str().ok_or(format!("Key {} not found", key))?);
            return match key.as_str() {
                "address" => Ok(Value::Address(s)),
                "bytes" => Ok(Value::Bytes(s)),
                "int" => Ok(Value::Int(Self::bigint(&s)?)),
                "mutez" => Ok(Value::Mutez(Self::bigint(&s)?)),
                "nat" => Ok(Value::Nat(Self::bigint(&s)?)),
                "string" => Ok(Value::String(s)),
                //"timestamp" => Ok(Value::Timestamp(s)),
                "unit" => Ok(Value::Unit(None)),
                "prim" => Ok(Self::prim(&s)),
                _ => {
                    panic!("Couldn't match {} in {}", key.to_string(), json.to_string());
                }
            };
        }

        if let JsonValue::Array(a) = json {
            let mut array = a.clone();
            array.reverse();
            return self.parse_storage(&self.preparse_storage2(&mut array));
        }

        warn!("Couldn't get a value from {:#?} with keys {:?}", json, keys);
        Ok(Value::None)
    }

    pub fn prim(s: &String) -> Value {
        match s.as_str() {
            "False" => Value::Bool(true),
            "None" => Value::None,
            _ => panic!("Don't know what to do with prim {}", s),
        }
    }

            /*
    pub fn store_big_map_list(mut context: Context) -> () {

        for id in store_big_map_list.iter {
            //create table
            //create column
            //populate with value
            let context = context.start_table(get_table_name(Some(name.clone())));
            let id = self.id_generator.get_id();
            crate::table::insert::add_column(
                "storage".to_string(),
                id,
                None,
                "deleted".to_string(),
                Value::Int(id)
            );
        }    
    }
            */

    pub fn process_big_map(&mut self, json: &JsonValue) -> Result<(), Box<dyn Error>> {
        let big_map_id: u32 = json["big_map"].to_string().parse()?;
        let key: Value = self.parse_storage(&self.preparse_storage(&json["key"]))?;
        let value: Value = self.parse_storage(&self.preparse_storage(&json["value"]))?;
        if let Some((_fk, node)) = self.big_map_map.get(&big_map_id) {
            let node = node.clone();
            match json["action"]
                .as_str()
                .ok_or("Couldn't find 'action' in JSON")?
            {
                "update" => {
                    let id = self.id_generator.get_id();
                    self.read_storage_internal(
                        &key,
                        &*node.left.ok_or("Missing key to big map")?,
                        id,
                        None,
                        node.table_name.clone(),
                    );
                    match value {
                        Value::None => {
                            crate::table::insert::add_column(
                                node.table_name.ok_or("Missing name for table")?,
                                id,
                                None,
                                "deleted".to_string(),
                                Value::Bool(true),
                            );
                        }
                        _ => self.read_storage_internal(
                            &value,
                            &*node.right.ok_or("Missing value to big map")?,
                            id,
                            None,
                            node.table_name,
                        ),
                    }
                }
                "alloc" => {
                    debug!("Alloc called like this: {}", json.to_string());
                }
                _ => panic!("{}", json.to_string()),
            }
        } else {
            debug!("Someone else's big map! {}", big_map_id);
        };
        Ok(())
    }

    /// Walks simultaneously through the table definition and the actual values it finds, and attempts
    /// to match them. Panics if it cannot do this (i.e. they do not match).
    pub fn read_storage(&mut self, value: &Value, node: &Node) -> Result<(), Box<dyn Error>> {
        let id = self.id_generator.get_id();
        crate::table::insert::add_column(
            "storage".to_string(),
            id,
            None,
            "deleted".to_string(),
            Value::Bool(false),
        );
        self.read_storage_internal(value, node, id, None, Some("storage".to_string()));
        Ok(())
    }

    pub fn read_storage_internal(
        &mut self,
        value: &Value,
        node: &Node,
        mut id: u32,
        mut fk_id: Option<u32>,
        mut last_table: Option<String>,
    ) {
        match node.expr {
            // we don't even try to store lambdas.
            crate::storage::Expr::SimpleExpr(crate::storage::SimpleExpr::Stop) => return,
            // or enumerations need to be evaluated once to populate the enum field,
            // and once to fill in auxiliary tables.
            crate::storage::Expr::ComplexExpr(crate::storage::ComplexExpr::OrEnumeration(_, _)) => {
                fn resolve_or(value: &Value, node: &Node) -> Option<String> {
                    debug!(
                        "resolve_or: value: {:?}
node: {:?}",
                        value, node
                    );
                    match value {
                        Value::Left(left) => resolve_or(left, &node.left.as_ref().unwrap()),
                        Value::Right(right) => resolve_or(right, &node.right.as_ref().unwrap()),
                        Value::Pair(_, _) => node.table_name.clone(),
                        Value::Unit(val) => val.clone(),
                        _ => node.name.clone(),
                    }
                }
                if let Some(val) = resolve_or(value, node) {
                    crate::table::insert::add_column(
                        node.table_name.as_ref().unwrap().to_string(),
                        id,
                        fk_id,
                        node.column_name.clone().unwrap(),
                        Value::Unit(Some(val)),
                    );
                };
            }
            _ => (),
        }

        if last_table != node.table_name {
            debug!("{:?} <> {:?} new table", last_table, node.table_name);

            last_table = node.table_name.clone();
            fk_id = Some(id);
            id = self.id_generator.get_id();
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
                self.read_storage_internal(keys, l, id, fk_id, last_table.clone());
                self.read_storage_internal(values, r, id, fk_id, last_table);
            }
            Value::Left(left) => {
                self.read_storage_internal(
                    left,
                    node.left.as_ref().unwrap(),
                    id,
                    fk_id,
                    last_table,
                );
            }
            Value::Right(right) => {
                self.read_storage_internal(
                    right,
                    node.right.as_ref().unwrap(),
                    id,
                    fk_id,
                    last_table,
                );
            }
            Value::List(l) => {
                for element in l {
                    debug!("Elt: {:?}", element);
                    let id = self.id_generator.get_id();
                    self.read_storage_internal(element, node, id, fk_id, last_table.clone());
                }
            }
            Value::Pair(left, right) => {
                let l = node.left.as_ref().unwrap();
                let r = node.right.as_ref().unwrap();
                self.read_storage_internal(right, r, id, fk_id, last_table.clone());
                self.read_storage_internal(left, l, id, fk_id, last_table);
            }
            Value::Unit(None) => {
                debug!("Unit: value is {:#?}, node is {:#?}", value, node);
                crate::table::insert::add_column(
                    node.table_name.as_ref().unwrap().to_string(),
                    id,
                    fk_id,
                    node.column_name.as_ref().unwrap().to_string(),
                    Value::String(node.value.clone().unwrap()),
                );
            }
            _ => {
                // this is a value, and should be saved.
                let table_name = node.table_name.as_ref().unwrap().to_string();
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
