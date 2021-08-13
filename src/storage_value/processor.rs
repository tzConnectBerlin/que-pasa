use crate::err;
use crate::error::Res;
use crate::octez::block;
use crate::sql::table::insert::*;
use crate::storage_structure::relational::{RelationalAST, RelationalEntry};
use crate::storage_structure::typing::{ExprTy, SimpleExprTy};
use crate::storage_value::parser;
use num::ToPrimitive;
use std::collections::HashMap;
use std::error::Error;
use std::hash::{Hash, Hasher};

macro_rules! serde2json {
    ($serde:expr) => {
        json::parse(&serde_json::to_string(&$serde)?)?
    };
}

macro_rules! must_match_rel {
    ($rel_ast:expr, $typ:path { $($fields:tt),+ }, $impl:block) => {
        match $rel_ast {
            $typ { $($fields),+ } => $impl
            _ => {
                let err: Box<dyn Error> = format!("failed to match storage value with storage type").into();
                Err(err)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProcessStorageContext {
    pub last_table: Option<String>,
    pub id: u32,
    pub fk_id: Option<u32>,
}
impl ProcessStorageContext {
    pub fn new(id: u32) -> ProcessStorageContext {
        ProcessStorageContext {
            id,
            last_table: None,
            fk_id: None,
        }
    }
    pub fn with_id(&self, id: u32) -> Self {
        let mut c = self.clone();
        c.id = id;
        c
    }
    pub fn with_fk_id(&self, fk_id: u32) -> Self {
        let mut c = self.clone();
        c.fk_id = Some(fk_id);
        c
    }
    pub fn with_last_table(&self, last_table: String) -> Self {
        let mut c = self.clone();
        c.last_table = Some(last_table);
        c
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TxContext {
    pub id: Option<u32>,
    pub level: u32,
    pub operation_group_number: u32,
    pub operation_number: u32,
    pub operation_hash: String,
    pub source: Option<String>,
    pub destination: Option<String>,
    pub entrypoint: Option<String>,
}

impl Hash for TxContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.level.hash(state);
        self.operation_group_number.hash(state);
        self.operation_number.hash(state);
        self.operation_hash.hash(state);
        self.source.hash(state);
        self.destination.hash(state);
        self.entrypoint.hash(state);
    }
}

// Manual impl PartialEq in order to exclude the <id> field
impl PartialEq for TxContext {
    fn eq(&self, other: &Self) -> bool {
        self.level == other.level
            && self.operation_group_number == other.operation_group_number
            && self.operation_number == other.operation_number
            && self.operation_hash == other.operation_hash
            && self.source == other.source
            && self.destination == other.destination
            && self.entrypoint == other.entrypoint
    }
}

impl Eq for TxContext {}

pub(crate) type TxContextMap = HashMap<TxContext, TxContext>;

pub struct IdGenerator {
    id: u32,
}

impl IdGenerator {
    pub(crate) fn new(initial_value: u32) -> Self {
        Self { id: initial_value }
    }

    pub(crate) fn get_id(&mut self) -> u32 {
        let old_id = self.id;
        self.id += 1;
        old_id
    }
}

type BigMapMap = std::collections::HashMap<u32, (u32, RelationalAST)>;

pub(crate) struct StorageProcessor {
    big_map_map: BigMapMap,
    id_generator: IdGenerator,
    inserts: Inserts,
    tx_contexts: TxContextMap,
}

impl StorageProcessor {
    pub(crate) fn new(initial_id: u32) -> Self {
        Self {
            big_map_map: BigMapMap::new(),
            inserts: crate::table::insert::Inserts::new(),
            tx_contexts: HashMap::new(),
            id_generator: IdGenerator::new(initial_id),
        }
    }

    pub(crate) fn process_block(
        &mut self,
        block: block::Block,
        rel_ast: &RelationalAST,
        contract_id: &str,
    ) -> Res<(Inserts, Vec<TxContext>)> {
        self.inserts.clear();
        self.tx_contexts.clear();

        let mut storages: Vec<(TxContext, serde_json::Value)> = vec![];
        let mut big_map_diffs: Vec<(TxContext, block::BigMapDiff)> = vec![];
        let operations = block.operations();

        let mut operation_group_number = 0u32;
        for operation_group in operations {
            operation_group_number += 1;
            let mut operation_number = 0u32;
            for operation in operation_group {
                operation_number += 1;
                storages.extend(self.get_storage_from_operation(
                    block.header.level,
                    operation_group_number,
                    operation_number,
                    &operation,
                    contract_id,
                )?);
                big_map_diffs.extend(self.get_big_map_diffs_from_operation(
                    block.header.level,
                    operation_group_number,
                    operation_number,
                    &operation,
                )?);
            }
        }

        for (tx_context, store) in &storages {
            let storage_json = serde_json::to_string(store)?;
            let parsed_storage = parser::parse(storage_json)?;

            self.process_storage_value(&parsed_storage, rel_ast, tx_context)?;
        }

        for (tx_content, diff) in &big_map_diffs {
            self.process_big_map_diff(diff, tx_content)?;
        }

        Ok((
            self.inserts.clone(),
            self.tx_contexts.keys().cloned().collect(),
        ))
    }

    pub(crate) fn get_id_value(&self) -> u32 {
        self.id_generator.id
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

    fn get_big_map_diffs_from_operation(
        &mut self,
        level: u32,
        operation_group_number: u32,
        operation_number: u32,
        operation: &block::Operation,
    ) -> Res<Vec<(TxContext, block::BigMapDiff)>> {
        let mut result: Vec<(TxContext, block::BigMapDiff)> = vec![];
        for content in &operation.contents {
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
                                entrypoint: content.parameters.clone().map(|p| p.entrypoint),
                            }),
                            big_map_diff.clone(),
                        )
                    }));
                }
            }
            for internal_operation_result in &content.metadata.internal_operation_results {
                if let Some(big_map_diffs) = &internal_operation_result.result.big_map_diff {
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
                                entrypoint: content.parameters.clone().map(|p| p.entrypoint),
                            }),
                            big_map_diff.clone(),
                        )
                    }));
                }
            }
        }
        Ok(result)
    }

    fn get_storage_from_operation(
        &mut self,
        level: u32,
        operation_group_number: u32,
        operation_number: u32,
        operation: &block::Operation,
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
                                    entrypoint: content.parameters.clone().map(|p| p.entrypoint),
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

    fn unfold_value(&self, v: &parser::Value, rel_ast: &RelationalAST) -> parser::Value {
        match rel_ast {
            RelationalAST::List { .. } => {
                // do not unfold list
                v.clone()
            }
            _ => v.unfold_list(),
        }
    }

    fn resolve_or(
        &self,
        ctx: &ProcessStorageContext,
        parent_entry: &RelationalEntry,
        v: &parser::Value,
        rel_ast: &RelationalAST,
    ) -> Result<RelationalEntry, Box<dyn Error>> {
        match &self.unfold_value(v, rel_ast) {
            parser::Value::Left(left) => must_match_rel!(
                rel_ast,
                RelationalAST::OrEnumeration {
                    or_unfold,
                    left_table,
                    left_ast,
                    ..
                },
                {
                    self.resolve_or(
                        &ctx.with_last_table(left_table.clone()),
                        or_unfold,
                        left,
                        left_ast,
                    )
                }
            ),
            parser::Value::Right(right) => must_match_rel!(
                rel_ast,
                RelationalAST::OrEnumeration {
                    or_unfold,
                    right_table,
                    right_ast,
                    ..
                },
                {
                    self.resolve_or(
                        &ctx.with_last_table(right_table.clone()),
                        or_unfold,
                        right,
                        right_ast,
                    )
                }
            ),
            parser::Value::Pair { .. } => {
                let mut res = parent_entry.clone();
                res.value = ctx.last_table.clone();
                Ok(res)
            }
            parser::Value::Unit(val) => {
                must_match_rel!(rel_ast, RelationalAST::Leaf { rel_entry }, {
                    let mut res = rel_entry.clone();
                    res.value = val.clone();
                    Ok(res)
                })
            }
            _ => must_match_rel!(rel_ast, RelationalAST::Leaf { rel_entry }, {
                let mut res = parent_entry.clone();
                res.value = Some(rel_entry.column_name.clone());
                Ok(res)
            }),
        }
    }

    fn process_big_map_diff(
        &mut self,
        diff: &block::BigMapDiff,
        tx_context: &TxContext,
    ) -> Res<()> {
        match diff.action.as_str() {
            "update" => {
                let big_map_id: u32 = match &diff.big_map {
                    Some(id) => id.parse()?,
                    None => return Err(err!("No big map id found in diff {:?}", diff)),
                };

                let (_fk, rel_ast) = match self.big_map_map.get(&big_map_id) {
                    Some((fk, n)) => (fk, n.clone()),
                    None => {
                        return Ok(());
                    }
                };
                must_match_rel!(
                    rel_ast,
                    RelationalAST::BigMap {
                        table,
                        key_ast,
                        value_ast
                    },
                    {
                        let ctx = &ProcessStorageContext::new(self.id_generator.get_id())
                            .with_last_table(table.clone());
                        self.process_storage_value_internal(
                            ctx,
                            &parser::parse_lexed(&serde2json!(&diff
                                .key
                                .clone()
                                .ok_or("Missing key to big map in diff")?))?,
                            &key_ast,
                            tx_context,
                        )?;
                        match &diff.value {
                            None => self.sql_add_cell(
                                ctx,
                                &table,
                                &"deleted".to_string(),
                                parser::Value::Bool(true),
                                tx_context,
                            ),
                            Some(val) => self.process_storage_value_internal(
                                ctx,
                                &parser::parse_lexed(&serde2json!(&val))?,
                                &value_ast,
                                tx_context,
                            )?,
                        };
                        Ok(())
                    }
                )
            }
            "alloc" => Ok(()),
            "copy" => Ok(()),
            action => Err(format!("big_map action unknown: action={}", action).into()),
        }
    }

    /// Walks simultaneously through the table definition and the actual values it finds, and attempts
    /// to match them. raises an error if it cannot do this (i.e. they do not match).
    fn process_storage_value(
        &mut self,
        value: &parser::Value,
        rel_ast: &RelationalAST,
        tx_context: &TxContext,
    ) -> Result<(), Box<dyn Error>> {
        let ctx = &ProcessStorageContext::new(self.id_generator.get_id());
        self.sql_add_cell(
            ctx,
            &"storage".to_string(),
            &"deleted".to_string(),
            parser::Value::Bool(false),
            tx_context,
        );
        self.process_storage_value_internal(
            &ctx.with_last_table("storage".to_string()),
            &value.unfold_list(),
            rel_ast,
            tx_context,
        )?;
        Ok(())
    }

    fn update_context(
        &mut self,
        ctx: &ProcessStorageContext,
        current_table: Option<String>,
    ) -> ProcessStorageContext {
        if let Some(table_name) = current_table {
            if ctx.last_table != Some(table_name.clone()) {
                return ctx
                    .with_last_table(table_name)
                    .with_fk_id(ctx.id)
                    .with_id(self.id_generator.get_id());
            }
        }
        ctx.clone()
    }

    fn process_storage_value_internal(
        &mut self,
        ctx: &ProcessStorageContext,
        value: &parser::Value,
        rel_ast: &RelationalAST,
        tx_context: &TxContext,
    ) -> Result<(), Box<dyn Error>> {
        let v = &self.unfold_value(value, rel_ast);
        match rel_ast {
            RelationalAST::Leaf { rel_entry } => {
                if let ExprTy::SimpleExprTy(SimpleExprTy::Stop) = rel_entry.column_type {
                    // we don't even try to store lambdas.
                    return Ok(());
                }
            }
            RelationalAST::OrEnumeration { or_unfold, .. } => {
                let rel_entry = self.resolve_or(ctx, or_unfold, v, rel_ast)?;
                if rel_entry.value != None {
                    self.sql_add_cell(
                        ctx,
                        &rel_entry.table_name,
                        &rel_entry.column_name,
                        parser::Value::Unit(rel_entry.value),
                        tx_context,
                    );
                }
            }
            _ => {}
        };

        let ctx = &self.update_context(ctx, rel_ast.table_entry());

        match v {
            parser::Value::Elt(keys, values) => must_match_rel!(
                rel_ast,
                RelationalAST::Map {
                    key_ast,
                    value_ast,
                    ..
                },
                {
                    self.process_storage_value_internal(ctx, keys, key_ast, tx_context)?;
                    self.process_storage_value_internal(ctx, values, value_ast, tx_context)?;
                    Ok(())
                }
            )
            .or(must_match_rel!(
                rel_ast,
                RelationalAST::BigMap {
                    key_ast,
                    value_ast,
                    ..
                },
                {
                    self.process_storage_value_internal(ctx, keys, key_ast, tx_context)?;
                    self.process_storage_value_internal(ctx, values, value_ast, tx_context)?;
                    Ok(())
                }
            )),
            parser::Value::Left(left) => {
                must_match_rel!(
                    rel_ast,
                    RelationalAST::OrEnumeration {
                        left_table,
                        left_ast,
                        ..
                    },
                    {
                        let ctx = &self.update_context(ctx, Some(left_table.clone()));
                        self.process_storage_value_internal(ctx, left, left_ast, tx_context)?;
                        Ok(())
                    }
                )
            }
            parser::Value::Right(right) => {
                must_match_rel!(
                    rel_ast,
                    RelationalAST::OrEnumeration {
                        right_table,
                        right_ast,
                        ..
                    },
                    {
                        let ctx = &self.update_context(ctx, Some(right_table.clone()));
                        self.process_storage_value_internal(ctx, right, right_ast, tx_context)?;
                        Ok(())
                    }
                )
            }
            parser::Value::List(l) => {
                must_match_rel!(rel_ast, RelationalAST::List { elems_ast, .. }, {
                    for element in l {
                        let id = self.id_generator.get_id();
                        self.process_storage_value_internal(
                            &ctx.with_id(id),
                            element,
                            elems_ast,
                            tx_context,
                        )?;
                    }
                    Ok(())
                })
            }
            parser::Value::Pair(left, right) => must_match_rel!(
                rel_ast,
                RelationalAST::Pair {
                    left_ast,
                    right_ast
                },
                {
                    self.process_storage_value_internal(ctx, right, right_ast, tx_context)?;
                    self.process_storage_value_internal(ctx, left, left_ast, tx_context)?;
                    Ok(())
                }
            )
            .or(must_match_rel!(
                rel_ast,
                RelationalAST::BigMap {
                    key_ast,
                    value_ast,
                    ..
                },
                {
                    self.process_storage_value_internal(ctx, right, key_ast, tx_context)?;
                    self.process_storage_value_internal(ctx, left, value_ast, tx_context)?;
                    Ok(())
                }
            )),
            parser::Value::Unit(None) => {
                must_match_rel!(rel_ast, RelationalAST::Leaf { rel_entry }, {
                    self.sql_add_cell(
                        ctx,
                        &rel_entry.table_name,
                        &rel_entry.column_name,
                        match &rel_entry.value {
                            Some(s) => parser::Value::String(s.clone()),
                            None => parser::Value::None,
                        },
                        tx_context,
                    );
                    Ok(())
                })
            }
            _ => {
                // If this is a big map, save the id and the fk_id currently
                // being used, for later processing
                match rel_ast {
                    RelationalAST::BigMap { .. } => {
                        if let parser::Value::Int(i) = value {
                            self.save_bigmap_location(i.to_u32().unwrap(), ctx.id, rel_ast.clone());
                            Ok(())
                        } else {
                            Err(format!("Found big map with non-int id: {:?}", rel_ast).into())
                        }
                    }
                    RelationalAST::Leaf { rel_entry } => {
                        if let ExprTy::SimpleExprTy(simple_type) = rel_entry.column_type {
                            let v = match simple_type {
                                SimpleExprTy::Timestamp => parser::Value::Timestamp(
                                    parser::parse_date(&value.clone()).unwrap(),
                                ),
                                SimpleExprTy::Address => {
                                    if let parser::Value::Bytes(a) = v {
                                        // sometimes we get bytes where we expected an address.
                                        parser::Value::Address(parser::decode_address(a).unwrap())
                                    } else {
                                        v.clone()
                                    }
                                }
                                _ => v.clone(),
                            };
                            self.sql_add_cell(
                                ctx,
                                &rel_entry.table_name,
                                &rel_entry.column_name,
                                v,
                                tx_context,
                            );
                            Ok(())
                        } else {
                            Err("RelationalAST::Leaf has complex expr type".into())
                        }
                    }
                    _ => Ok(()),
                }
            }
        }
    }

    fn save_bigmap_location(&mut self, bigmap_id: u32, fk: u32, rel_ast: RelationalAST) {
        self.big_map_map.insert(bigmap_id, (fk, rel_ast));
    }

    fn sql_add_cell(
        &mut self,
        ctx: &ProcessStorageContext,
        table_name: &str,
        column_name: &str,
        value: parser::Value,
        tx_context: &TxContext,
    ) {
        let mut insert = match self.get_insert(table_name, ctx.id, ctx.fk_id) {
            Some(x) => x,
            None => Insert {
                table_name: table_name.to_string(),
                id: ctx.id,
                fk_id: ctx.fk_id,
                columns: vec![Column {
                    name: "tx_context_id".to_string(),
                    value: parser::Value::Int(tx_context.id.unwrap().into()),
                }],
            },
        };
        insert.columns.push(Column {
            name: column_name.to_string(),
            value,
        });

        self.inserts.insert(
            InsertKey {
                table_name: table_name.to_string(),
                id: ctx.id,
            },
            Insert {
                table_name: table_name.to_string(),
                id: ctx.id,
                fk_id: ctx.fk_id,
                columns: insert.columns,
            },
        );
    }

    fn get_insert(&self, table_name: &str, id: u32, fk_id: Option<u32>) -> Option<Insert> {
        self.inserts
            .get(&InsertKey {
                table_name: table_name.to_string(),
                id,
            })
            .map(|e| {
                assert!(e.fk_id == fk_id);
                (*e).clone()
            })
    }
}
