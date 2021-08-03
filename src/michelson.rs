use crate::block;
use crate::err;
use crate::error::Res;
use crate::relational::RelationalAST;
use crate::table::insert::*;
use chrono::{DateTime, TimeZone, Utc};
use curl::easy::Easy;
use json::JsonValue;
use num::{BigInt, ToPrimitive};
use std::collections::HashMap;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::atomic::AtomicU32;

const MAX_ARAY_LENGTH: usize = 20; // Max length of array that we'll convert to PAIRs.

lazy_static! {
    static ref NODE_URL: String = match std::env::var("NODE_URL") {
        Ok(s) => s,
        Err(_) => "http://edo2full.newby.org:8732".to_string(),
    };
}

macro_rules! serde2json {
    ($serde:expr) => {
        json::parse(&serde_json::to_string(&$serde)?)?
    };
}

#[derive(Clone, Debug)]
pub struct TxContext {
    pub id: Option<u32>,
    pub level: u32,
    pub operation_group_number: u32,
    pub operation_number: u32,
    pub operation_hash: String,
    pub source: Option<String>,
    pub destination: Option<String>,
}

impl Hash for TxContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.level.hash(state);
        self.operation_group_number.hash(state);
        self.operation_number.hash(state);
        self.operation_hash.hash(state);
        self.source.hash(state);
        self.destination.hash(state);
    }
}

impl PartialEq for TxContext {
    fn eq(&self, other: &Self) -> bool {
        self.level == other.level
            && self.operation_group_number == other.operation_group_number
            && self.operation_number == other.operation_number
            && self.operation_hash == other.operation_hash
            && self.source == other.source
            && self.destination == other.destination
    }
}

impl Eq for TxContext {}

pub type TxContextMap = HashMap<TxContext, TxContext>;

pub struct IdGenerator {
    id: AtomicU32,
}

impl IdGenerator {
    pub(crate) fn new(initial_value: u32) -> Self {
        Self {
            id: AtomicU32::new(initial_value),
        }
    }

    pub(crate) fn get_id(&mut self) -> u32 {
        let id = self.id.get_mut();
        let old_id: u32 = *id;
        *id += 1;
        debug!("get_id(): {}", old_id);
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
    pub baked_at: Option<DateTime<Utc>>,
}

type BigMapMap = std::collections::HashMap<u32, (u32, RelationalAST)>;

pub struct StorageParser {
    big_map_map: BigMapMap,
    pub id_generator: IdGenerator,
    inserts: crate::table::insert::Inserts,
    pub tx_contexts: TxContextMap,
}

impl StorageParser {
    pub(crate) fn new(initial_id: u32) -> Self {
        Self {
            big_map_map: BigMapMap::new(),
            id_generator: IdGenerator::new(initial_id),
            inserts: crate::table::insert::Inserts::new(),
            tx_contexts: HashMap::new(),
        }
    }

    /// Load a uri (of course)
    fn load(uri: &str) -> Result<JsonValue, Box<dyn Error>> {
        debug!("Loading: {}", uri,);
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
        let json = json::parse(std::str::from_utf8(&response)?)?;
        Ok(json)
    }

    fn tx_context(&mut self, mut tx_context: TxContext) -> TxContext {
        if let Some(result) = self.tx_contexts.get(&tx_context) {
            result.clone()
        } else {
            tx_context.id = Some(self.id_generator.get_id());
            self.tx_contexts
                .insert(tx_context.clone(), tx_context.clone());
            tx_context
        }
    }

    fn parse_rfc3339(rfc3339: &str) -> Res<DateTime<Utc>> {
        let fixedoffset = chrono::DateTime::parse_from_rfc3339(rfc3339)?;
        Ok(fixedoffset.with_timezone(&Utc))
    }

    fn timestamp_from_block(json: &JsonValue) -> Res<DateTime<Utc>> {
        Self::parse_rfc3339(
            json["header"]["timestamp"]
                .as_str()
                .ok_or_else(|| err!("Couldn't parse string {:?}", json["header"]["timestamp"]))?,
        )
    }
    /// Return the highest level on the chain
    pub(crate) fn head() -> Res<Level> {
        let json = Self::load(&format!("{}/chains/main/blocks/head", *NODE_URL))?;
        Ok(Level {
            _level: json["header"]["level"]
                .as_u32()
                .ok_or_else(|| err!("Couldn't get level from node"))?,
            hash: Some(json["hash"].to_string()),
            baked_at: Some(Self::timestamp_from_block(&json)?),
        })
    }

    pub(crate) fn level(level: u32) -> Res<Level> {
        let (json, block) = Self::level_json(level)?;
        Ok(Level {
            _level: block.header.level as u32,
            hash: Some(block.hash),
            baked_at: Some(Self::timestamp_from_block(&json)?),
        })
    }

    pub(crate) fn level_json(level: u32) -> Res<(JsonValue, block::Block)> {
        let res = Self::load(&format!("{}/chains/main/blocks/{}", *NODE_URL, level))?;
        let block: crate::block::Block = serde_json::from_str(&res.to_string())?;
        Ok((res, block))
    }

    pub(crate) fn block_has_tx_for_us(block: &block::Block, contract_id: &str) -> Res<bool> {
        let destination = Some(contract_id.to_string());
        for operations in &block.operations {
            for operation in operations {
                for content in &operation.contents {
                    if content.destination == destination {
                        return Ok(true);
                    }
                    for result in &content.metadata.internal_operation_results {
                        if result.destination == destination {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    /// Get all of the data for the contract.
    pub(crate) fn get_everything(
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

    pub(crate) fn block_has_contract_origination(
        block: &block::Block,
        contract_id: &str,
    ) -> Res<bool> {
        for operations in &block.operations {
            for operation in operations {
                for content in &operation.contents {
                    for operation_result in &content.metadata.operation_result {
                        for originated_contract in &operation_result.originated_contracts {
                            if originated_contract == contract_id {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    pub(crate) fn get_big_map_diffs_from_operation(
        &mut self,
        level: u32,
        operation_group_number: u32,
        operation_number: u32,
        operation: &block::Operation,
    ) -> Res<Vec<(TxContext, block::BigMapDiff)>> {
        let mut result: Vec<(TxContext, block::BigMapDiff)> = vec![];
        debug!("operation: {}", serde_json::to_string(&operation).unwrap());
        for content in &operation.contents {
            debug!("content: {:#?}", content);
            if let Some(operation_result) = &content.metadata.operation_result {
                if let Some(big_map_diffs) = &operation_result.big_map_diff {
                    result.extend(big_map_diffs.iter().map(|big_map_diff| {
                        (
                            self.tx_context(TxContext {
                                id: None,
                                level,
                                operation_number,
                                operation_group_number,
                                operation_hash: operation.hash.clone(),
                                source: content.source.clone(),
                                destination: content.destination.clone(),
                            }),
                            big_map_diff.clone(),
                        )
                    }));
                }
            }
            for internal_operation_result in &content.metadata.internal_operation_results {
                if let Some(big_map_diffs) = &internal_operation_result.result.big_map_diff {
                    debug!("Internal big_map_diffs {:?}", big_map_diffs);
                    result.extend(big_map_diffs.iter().map(|big_map_diff| {
                        (
                            self.tx_context(TxContext {
                                id: None,
                                level,
                                operation_group_number,
                                operation_number,
                                operation_hash: operation.hash.clone(),
                                source: content.source.clone(),
                                destination: content.destination.clone(),
                            }),
                            big_map_diff.clone(),
                        )
                    }));
                }
            }
        }
        Ok(result)
    }

    pub(crate) fn get_storage_from_operation(
        &mut self,
        level: u32,
        operation_group_number: u32,
        operation_number: u32,
        operation: &crate::block::Operation,
        contract_id: &str,
    ) -> Res<Vec<(TxContext, ::serde_json::Value)>> {
        let mut results: Vec<(TxContext, serde_json::Value)> = vec![];

        for content in &operation.contents {
            if let Some(destination) = &content.destination {
                if destination == contract_id {
                    if let Some(operation_result) = &content.metadata.operation_result {
                        if let Some(storage) = &operation_result.storage {
                            results.push((
                                self.tx_context(TxContext {
                                    id: None,
                                    level,
                                    operation_group_number,
                                    operation_number,
                                    operation_hash: operation.hash.clone(),
                                    source: content.source.clone(),
                                    destination: content.destination.clone(),
                                }),
                                storage.clone(),
                            ));
                        } else {
                            err!("No storage found!");
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    fn bigint(source: &str) -> Result<BigInt, Box<dyn Error>> {
        Ok(BigInt::from_str(source)?)
    }

    fn lex(&self, json: &JsonValue) -> JsonValue {
        if let JsonValue::Array(mut a) = json.clone() {
            a.reverse();
            self.lexer_unfold_sequences(&mut a)
        } else {
            json.clone()
        }
    }

    fn lexer_unfold_sequences(&self, v: &mut Vec<JsonValue>) -> JsonValue {
        match v.len() {
            0 => panic!("Called empty"),
            1 => v[0].clone(),
            _ => {
                let ele = v.pop();
                debug!("{:?}", v);
                let rest = self.lexer_unfold_sequences(v);
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

    pub(crate) fn decode_address(hex: &str) -> Res<String> {
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
            format!("025a79{}", &hex[2..42])
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

    pub(crate) fn parse(&self, storage_json: String) -> Res<Value> {
        let json_parsed = &json::parse(&storage_json)?;
        let lexed = self.lex(json_parsed);
        self.parse_lexed(&lexed)
    }

    /// Goes through the actual stored data and builds up a structure which can be used in combination with the node
    /// data to stash it in the database.
    fn parse_lexed(&self, json: &JsonValue) -> Res<Value> {
        debug!("parse_lexed: {:?}", json);
        if let JsonValue::Array(a) = json {
            match a.len() {
                0 => {
                    // TODO: must understand why this happens
                    return Ok(Value::None);
                }
                1 => {
                    return self.parse_lexed(&a[0]);
                }
                _ => {
                    if a.len() < MAX_ARAY_LENGTH {
                        let left = Box::new(self.parse_lexed(&a[0].clone())?);
                        let right = Box::new(self.parse_lexed(&JsonValue::Array(a[1..].to_vec()))?);
                        return Ok(Value::Pair(left, right));
                    } else {
                        return Ok(Value::None);
                    }
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
                        Box::new(self.parse_lexed(&args[0])?),
                        Box::new(self.parse_lexed(&args[1])?),
                    ));
                }
                "FALSE" => return Ok(Value::Bool(false)),
                "LEFT" => return Ok(Value::Left(Box::new(self.parse_lexed(&args[0])?))),
                "NONE" => return Ok(Value::None),
                "RIGHT" => return Ok(Value::Right(Box::new(self.parse_lexed(&args[0])?))),
                "PAIR" => {
                    match args.len() {
                        0 | 1 => return Ok(Value::None),
                        2 => {
                            return Ok(Value::Pair(
                                Box::new(self.parse_lexed(&args[0])?),
                                Box::new(self.parse_lexed(&args[1])?),
                            ));
                        }
                        _ => {
                            let mut args = args;
                            args.reverse(); // so we can pop() afterward. But TODO: fix
                            let lexed = self.lexer_unfold_sequences(&mut args);
                            return self.parse_lexed(&lexed);
                        }
                    }
                }
                "PUSH" => return Ok(Value::None),
                "SOME" => {
                    if !args.is_empty() {
                        return self.parse_lexed(&args[0]);
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
            if a.len() < 400 {
                let mut array = a.clone();
                array.reverse();
                return self.parse_lexed(&self.lexer_unfold_sequences(&mut array));
            }
        }

        warn!("Couldn't get a value from {:#?} with keys {:?}", json, keys);
        Ok(Value::None)
    }

    pub(crate) fn prim(s: &str) -> Value {
        match s {
            "False" => Value::Bool(true),
            "None" => Value::None,
            _ => panic!("Don't know what to do with prim {}", s),
        }
    }

    pub(crate) fn process_big_map_diff(
        &mut self,
        diff: &block::BigMapDiff,
        tx_context: &TxContext,
    ) -> Res<()> {
        debug!("big_map_diff: {:?}", diff);
        match diff.action.as_str() {
            "update" => {
                let big_map_id: u32 = match &diff.big_map {
                    Some(id) => id.parse()?,
                    None => return Err(err!("No big map id found in diff {:?}", diff)),
                };
                debug!("Processing big map with id {}", big_map_id);
                //debug!("Big maps are {:?}", self.big_map_map);
                let (_fk, rel_ast) = match self.big_map_map.get(&big_map_id) {
                    Some((fk, n)) => (fk, n),
                    None => return Ok(()),
                };
                let rel_ast = rel_ast.clone();
                let id = self.id_generator.get_id();
                self.read_storage_internal(
                    &self.parse_lexed(&serde2json!(&diff
                        .key
                        .clone()
                        .ok_or("Missing key to big map in diff")?))?,
                    &*rel_ast.left.ok_or("Missing key to big map")?,
                    id,
                    None,
                    rel_ast.table_name.clone(),
                    tx_context,
                );
                match &diff.value {
                    None => {
                        self.add_column(
                            rel_ast.table_name.ok_or("Missing name for table")?,
                            id,
                            None,
                            "deleted".to_string(),
                            Value::Bool(true),
                            tx_context,
                        );
                        Ok(())
                    }
                    Some(val) => {
                        self.read_storage_internal(
                            &self.parse_lexed(&serde2json!(&val))?,
                            &*rel_ast.right.ok_or("Missing value to big map")?,
                            id,
                            None,
                            rel_ast.table_name,
                            tx_context,
                        );
                        Ok(())
                    }
                }
            }
            "alloc" => {
                debug!("Alloc called like this: {}", serde_json::to_string(&diff)?);
                Ok(())
            }
            "copy" => {
                debug!("Copy called like this: {}", serde_json::to_string(&diff)?);
                Ok(())
            }
            _ => {
                panic!("{}", serde_json::to_string(&diff)?);
            }
        }
    }

    /// Walks simultaneously through the table definition and the actual values it finds, and attempts
    /// to match them. Panics if it cannot do this (i.e. they do not match).
    pub(crate) fn read_storage(
        &mut self,
        value: &Value,
        rel_ast: &RelationalAST,
        tx_context: &TxContext,
    ) -> Result<(), Box<dyn Error>> {
        let id = self.id_generator.get_id();
        /*
        self.add_column(
            "storage".to_string(),
            id,
            None,
            "deleted".to_string(),
            Value::Bool(false),
        );
        */
        self.read_storage_internal(value, rel_ast, id, None, None, tx_context);
        Ok(())
    }

    pub(crate) fn read_storage_internal(
        &mut self,
        value: &Value,
        rel_ast: &RelationalAST,
        mut id: u32,
        mut fk_id: Option<u32>,
        mut last_table: Option<String>,
        tx_context: &TxContext,
    ) {
        match rel_ast.expr {
            // we don't even try to store lambdas.
            crate::storage::Expr::SimpleExpr(crate::storage::SimpleExpr::Stop) => return,
            // or enumerations need to be evaluated once to populate the enum field,
            // and once to fill in auxiliary tables.
            crate::storage::Expr::ComplexExpr(crate::storage::ComplexExpr::OrEnumeration(_, _)) => {
                fn resolve_or(value: &Value, rel_ast: &RelationalAST) -> Option<String> {
                    debug!(
                        "resolve_or: value: {:?}
rel_ast: {:?}",
                        value, rel_ast
                    );
                    match value {
                        Value::Left(left) => resolve_or(left, rel_ast.left.as_ref().unwrap()),
                        Value::Right(right) => resolve_or(right, rel_ast.right.as_ref().unwrap()),
                        Value::Pair(_, _) => rel_ast.table_name.clone(),
                        Value::Unit(val) => val.clone(),
                        _ => rel_ast.name.clone(),
                    }
                }
                if let Some(val) = resolve_or(value, rel_ast) {
                    self.add_column(
                        rel_ast.table_name.as_ref().unwrap().to_string(),
                        id,
                        fk_id,
                        rel_ast.column_name.clone().unwrap(),
                        Value::Unit(Some(val)),
                        tx_context,
                    );
                };
            }
            _ => (),
        }

        if last_table != rel_ast.table_name {
            debug!("{:?} <> {:?} new table", last_table, rel_ast.table_name);
            last_table = rel_ast.table_name.clone();
            if rel_ast.table_name != Some("storage".to_string()) {
                fk_id = Some(id);
                id = self.id_generator.get_id();
                debug!(
                    "Creating table from rel_ast {:?} with id {} and fk_id {:?}",
                    rel_ast, id, fk_id
                );
            }
        }

        match value {
            Value::Elt(keys, values) => {
                // entry in a map or a big map.
                let l = rel_ast.left.as_ref().unwrap();
                let r = rel_ast.right.as_ref().unwrap();
                self.read_storage_internal(keys, l, id, fk_id, last_table.clone(), tx_context);
                self.read_storage_internal(values, r, id, fk_id, last_table, tx_context);
            }
            Value::Left(left) => {
                self.read_storage_internal(
                    left,
                    rel_ast.left.as_ref().unwrap(),
                    id,
                    fk_id,
                    last_table,
                    tx_context,
                );
            }
            Value::Right(right) => {
                self.read_storage_internal(
                    right,
                    rel_ast.right.as_ref().unwrap(),
                    id,
                    fk_id,
                    last_table,
                    tx_context,
                );
            }
            Value::List(l) => {
                for element in l {
                    debug!("Elt: {:?}", element);
                    let id = self.id_generator.get_id();
                    self.read_storage_internal(
                        element,
                        rel_ast,
                        id,
                        fk_id,
                        last_table.clone(),
                        tx_context,
                    );
                }
            }
            Value::Pair(left, right) => {
                //Debug!(
                //    "rel_ast: {:?}
                //value: {:?}",
                //    rel_ast, value
                //);
                let l = rel_ast.left.as_ref().unwrap();
                let r = rel_ast.right.as_ref().unwrap();
                self.read_storage_internal(right, r, id, fk_id, last_table.clone(), tx_context);
                self.read_storage_internal(left, l, id, fk_id, last_table, tx_context);
            }
            Value::Unit(None) => {
                debug!("Unit: value is {:#?}, rel_ast is {:#?}", value, rel_ast);
                self.add_column(
                    rel_ast.table_name.as_ref().unwrap().to_string(),
                    id,
                    fk_id,
                    rel_ast.column_name.as_ref().unwrap().to_string(),
                    match &rel_ast.value {
                        Some(s) => Value::String(s.clone()),
                        None => Value::None,
                    },
                    tx_context,
                );
            }
            _ => {
                // this is a value, and should be saved.
                let table_name = rel_ast.table_name.as_ref().unwrap().to_string();
                let column_name = rel_ast.column_name.as_ref().unwrap().to_string();
                debug!(
                    "{} {} = {:?} {:?}",
                    table_name, column_name, value, rel_ast.expr
                );

                // If this is a big map, save the id and the fk_id currently
                // being used, for later processing
                match &rel_ast.expr {
                    crate::storage::Expr::ComplexExpr(ce) => {
                        if let crate::storage::ComplexExpr::BigMap(_, _) = ce {
                            debug!("value: {:?}", value);
                            if let Value::Int(i) = value {
                                debug!("{}", i);
                                debug!("Saving bigmap {:} with parent {}", i, id);
                                self.save_bigmap_location(i.to_u32().unwrap(), id, rel_ast.clone());
                            } else {
                                panic!("Found big map with non-int id: {:?}", rel_ast);
                            }
                        }
                    }
                    crate::storage::Expr::SimpleExpr(crate::storage::SimpleExpr::Timestamp) => {
                        self.add_column(
                            table_name,
                            id,
                            fk_id,
                            column_name,
                            Value::Timestamp(Self::parse_date(&value.clone()).unwrap()),
                            tx_context,
                        );
                    }
                    crate::storage::Expr::SimpleExpr(crate::storage::SimpleExpr::Address) => {
                        self.add_column(
                            table_name,
                            id,
                            fk_id,
                            column_name,
                            if let Value::Bytes(a) = value {
                                // sometimes we get bytes where we expected an address.
                                Value::Address(Self::decode_address(a).unwrap())
                            } else {
                                value.clone()
                            },
                            tx_context,
                        );
                    }
                    _ => self.add_column(
                        table_name,
                        id,
                        fk_id,
                        column_name,
                        value.clone(),
                        tx_context,
                    ),
                }
            }
        }
    }

    fn save_bigmap_location(&mut self, bigmap_id: u32, fk: u32, rel_ast: RelationalAST) {
        debug!("Saving {} -> ({:?}, {:?})", bigmap_id, fk, rel_ast);
        self.big_map_map.insert(bigmap_id, (fk, rel_ast));
    }

    fn add_insert(
        &mut self,
        table_name: String,
        id: u32,
        fk_id: Option<u32>,
        columns: Vec<Column>,
    ) {
        debug!(
            "table::add_insert {}, {}, {:?}, {:?}",
            table_name, id, fk_id, columns
        );
        self.inserts.insert(
            InsertKey {
                table_name: table_name.clone(),
                id,
            },
            Insert {
                table_name,
                id,
                fk_id,
                columns,
            },
        );
    }

    fn add_column(
        &mut self,
        table_name: String,
        id: u32,
        fk_id: Option<u32>,
        column_name: String,
        value: Value,
        tx_context: &TxContext,
    ) {
        debug!(
            "add_column {}, {}, {:?}, {}, {:?}",
            table_name, id, fk_id, column_name, value
        );

        let mut insert = match self.get_insert(table_name.clone(), id, fk_id) {
            Some(x) => x,
            None => Insert {
                table_name: table_name.clone(),
                id,
                fk_id,
                columns: vec![Column {
                    name: "tx_context_id".to_string(),
                    value: Value::Int(tx_context.id.unwrap().into()),
                }],
            },
        };
        insert.columns.push(Column {
            name: column_name,
            value,
        });
        self.add_insert(table_name, id, fk_id, insert.columns);
    }

    pub(crate) fn get_insert(
        &mut self,
        table_name: String,
        id: u32,
        fk_id: Option<u32>,
    ) -> Option<Insert> {
        self.inserts.get(&InsertKey { table_name, id }).map(|e| {
            assert!(e.fk_id == fk_id);
            (*e).clone()
        })
    }

    pub(crate) fn get_inserts(&self) -> Inserts {
        return self.inserts.clone();
    }

    pub(crate) fn clear_inserts(&mut self) {
        self.inserts.clear();
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
