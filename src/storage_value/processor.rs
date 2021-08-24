use crate::octez::block;
use crate::sql::insert;
use crate::sql::insert::{Column, Insert, InsertKey, Inserts};
use crate::storage_structure::relational::{RelationalAST, RelationalEntry};
use crate::storage_structure::typing::{ExprTy, SimpleExprTy};
use crate::storage_value::parser;
use anyhow::{anyhow, Context, Result};
use num::ToPrimitive;
use rust_decimal::prelude::*;
use std::collections::HashMap;
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
                Err(anyhow!("failed to match storage value with storage type"))
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
    pub operation_hash: String,
    pub operation_group_number: usize,
    pub operation_number: usize,
    pub content_number: usize,
    pub internal_number: Option<usize>,
    pub source: Option<String>,
    pub destination: Option<String>,
    pub entrypoint: Option<String>,
}

impl Hash for TxContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.level.hash(state);
        self.operation_hash.hash(state);
        self.operation_group_number.hash(state);
        self.operation_number.hash(state);
        self.content_number.hash(state);
        self.internal_number.hash(state);
        self.source.hash(state);
        self.destination.hash(state);
        self.entrypoint.hash(state);
    }
}

// Manual impl PartialEq in order to exclude the <id> field
impl PartialEq for TxContext {
    fn eq(&self, other: &Self) -> bool {
        self.level == other.level
            && self.operation_hash == other.operation_hash
            && self.operation_group_number == other.operation_group_number
            && self.operation_number == other.operation_number
            && self.content_number == other.content_number
            && self.internal_number == other.internal_number
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
            inserts: Inserts::new(),
            tx_contexts: HashMap::new(),
            id_generator: IdGenerator::new(initial_id),
        }
    }

    pub(crate) fn process_block(
        &mut self,
        block: &block::Block,
        rel_ast: &RelationalAST,
        contract_id: &str,
    ) -> Result<(Inserts, Vec<TxContext>)> {
        self.inserts.clear();
        self.tx_contexts.clear();

        let mut storages: Vec<(TxContext, serde_json::Value)> = vec![];
        let mut big_map_diffs: Vec<(TxContext, block::BigMapDiff)> = vec![];
        let operations = block.operations();

        for (operation_group_number, operation_group) in
            operations.iter().enumerate()
        {
            for (operation_number, operation) in
                operation_group.iter().enumerate()
            {
                storages.extend(self.get_storage_from_operation(
                    block.header.level,
                    operation_group_number,
                    operation_number,
                    operation,
                    contract_id,
                )?);
                big_map_diffs.extend(self.get_big_map_diffs_from_operation(
                    block.header.level,
                    operation_group_number,
                    operation_number,
                    operation,
                    contract_id,
                )?);
            }
        }

        for (tx_context, store) in &storages {
            let storage_json = serde_json::to_string(store)?;
            let parsed_storage = parser::parse(storage_json)?;

            self.process_storage_value(&parsed_storage, rel_ast, tx_context)
                .with_context(|| {
                    format!(
                        "process_block: process storage value failed (tx_context={:?})",
                        tx_context
                    )
                })?;
        }

        for (tx_content, diff) in &big_map_diffs {
            self.process_big_map_diff(diff, tx_content)
                .with_context(|| {
                    format!("process_block: process big_map diff failed (tx_context={:?})", tx_content)
                })?;
        }

        Ok((
            self.inserts.clone(),
            self.tx_contexts
                .keys()
                .cloned()
                .collect(),
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
        operation_group_number: usize,
        operation_number: usize,
        operation: &block::Operation,
        contract_id: &str,
    ) -> Result<Vec<(TxContext, block::BigMapDiff)>> {
        let c_dest = Some(contract_id.to_string());
        let mut result: Vec<(TxContext, block::BigMapDiff)> = vec![];

        for (content_number, content) in operation.contents.iter().enumerate() {
            if let Some(operation_result) = &content.metadata.operation_result {
                if content.destination == c_dest {
                    if let Some(big_map_diffs) = &operation_result.big_map_diff
                    {
                        result.extend(big_map_diffs.iter().map(
                            |big_map_diff| {
                                (
                                    self.tx_context(TxContext {
                                        id: None,
                                        level,
                                        operation_hash: operation.hash.clone(),
                                        operation_number,
                                        operation_group_number,
                                        content_number,
                                        internal_number: None,
                                        source: content.source.clone(),
                                        destination: content
                                            .destination
                                            .clone(),
                                        entrypoint: content
                                            .parameters
                                            .clone()
                                            .map(|p| p.entrypoint),
                                    }),
                                    big_map_diff.clone(),
                                )
                            },
                        ));
                    }
                }

                for (internal_number, internal_op) in content
                    .metadata
                    .internal_operation_results
                    .iter()
                    .enumerate()
                {
                    if internal_op.destination == c_dest {
                        if let Some(big_map_diffs) =
                            &internal_op.result.big_map_diff
                        {
                            result.extend(big_map_diffs.iter().map(
                                |big_map_diff| {
                                    (
                                        self.tx_context(TxContext {
                                            id: None,
                                            level,
                                            operation_hash: operation
                                                .hash
                                                .clone(),
                                            operation_group_number,
                                            operation_number,
                                            content_number,
                                            internal_number: Some(
                                                internal_number,
                                            ),
                                            source: Some(
                                                internal_op.source.clone(),
                                            ),
                                            destination: internal_op
                                                .destination
                                                .clone(),
                                            entrypoint: internal_op
                                                .parameters
                                                .clone()
                                                .map(|p| p.entrypoint),
                                        }),
                                        big_map_diff.clone(),
                                    )
                                },
                            ));
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    fn get_storage_from_operation(
        &mut self,
        level: u32,
        operation_group_number: usize,
        operation_number: usize,
        operation: &block::Operation,
        contract_id: &str,
    ) -> Result<Vec<(TxContext, ::serde_json::Value)>> {
        let mut results: Vec<(TxContext, serde_json::Value)> = vec![];

        let c_dest = Some(contract_id.to_string());
        for (content_number, content) in operation.contents.iter().enumerate() {
            if let Some(operation_result) = &content.metadata.operation_result {
                if operation_result.status == "applied" {
                    if content.destination == c_dest {
                        let tx_context = TxContext {
                            id: None,
                            level,
                            operation_hash: operation.hash.clone(),
                            operation_group_number,
                            operation_number,
                            content_number,
                            internal_number: None,
                            source: content.source.clone(),
                            destination: content.destination.clone(),
                            entrypoint: content
                                .parameters
                                .clone()
                                .map(|p| p.entrypoint),
                        };
                        if let Some(storage) = &operation_result.storage {
                            results.push((
                                self.tx_context(tx_context),
                                storage.clone(),
                            ));
                        } else {
                            return Err(anyhow!(
                                "no storage found! tx_context={:#?}",
                                tx_context
                            ));
                        }
                    }
                    for (internal_number, internal_op) in content
                        .metadata
                        .internal_operation_results
                        .iter()
                        .enumerate()
                    {
                        if internal_op.destination == c_dest {
                            let tx_context = TxContext {
                                id: None,
                                level,
                                operation_hash: operation.hash.clone(),
                                operation_group_number,
                                operation_number,
                                content_number,
                                internal_number: Some(internal_number),
                                source: Some(internal_op.source.clone()),
                                destination: internal_op.destination.clone(),
                                entrypoint: internal_op
                                    .parameters
                                    .clone()
                                    .map(|p| p.entrypoint),
                            };
                            if let Some(storage) = &internal_op.result.storage {
                                results.push((
                                    self.tx_context(tx_context),
                                    storage.clone(),
                                ));
                            } else {
                                return Err(anyhow!(
                                    "no storage found! tx_context={:#?}",
                                    tx_context
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    fn unfold_value(
        &self,
        v: &parser::Value,
        rel_ast: &RelationalAST,
    ) -> parser::Value {
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
    ) -> Result<RelationalEntry> {
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
    ) -> Result<()> {
        match diff.action.as_str() {
            "update" => {
                let big_map_id: u32 = match &diff.big_map {
                    Some(id) => id.parse()?,
                    None => {
                        return Err(anyhow!(
                            "no big map id found in diff {:?}",
                            diff
                        ))
                    }
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
                        let ctx = &ProcessStorageContext::new(
                            self.id_generator.get_id(),
                        )
                        .with_last_table(table.clone());
                        self.process_storage_value_internal(
                            ctx,
                            &parser::parse_lexed(&serde2json!(&diff
                                .key
                                .clone()
                                .ok_or_else(|| anyhow!(
                                    "missing key to big map in diff"
                                ))?))?,
                            &key_ast,
                            tx_context,
                        )?;
                        match &diff.value {
                            None => self.sql_add_cell(
                                ctx,
                                &table,
                                &"deleted".to_string(),
                                insert::Value::Bool(true),
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
            action => Err(anyhow!("big_map action unknown: action={}", action)),
        }
    }

    /// Walks simultaneously through the table definition and the actual values it finds, and attempts
    /// to match them. raises an error if it cannot do this (i.e. they do not match).
    fn process_storage_value(
        &mut self,
        value: &parser::Value,
        rel_ast: &RelationalAST,
        tx_context: &TxContext,
    ) -> Result<()> {
        let ctx = &ProcessStorageContext::new(self.id_generator.get_id());
        self.sql_add_cell(
            ctx,
            &"storage".to_string(),
            &"deleted".to_string(),
            insert::Value::Bool(false),
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
    ) -> Result<()> {
        let v = &self.unfold_value(value, rel_ast);
        match rel_ast {
            RelationalAST::Leaf { rel_entry } => {
                if let ExprTy::SimpleExprTy(SimpleExprTy::Stop) =
                    rel_entry.column_type
                {
                    // we don't even try to store lambdas.
                    return Ok(());
                }
            }
            RelationalAST::OrEnumeration { or_unfold, .. } => {
                let rel_entry = self.resolve_or(ctx, or_unfold, v, rel_ast)?;
                if let Some(value) = rel_entry.value {
                    self.sql_add_cell(
                        ctx,
                        &rel_entry.table_name,
                        &rel_entry.column_name,
                        insert::Value::String(value),
                        tx_context,
                    );
                }
            }
            RelationalAST::Option { elem_ast } => {
                if *v != parser::Value::None {
                    self.process_storage_value_internal(
                        ctx, v, elem_ast, tx_context,
                    )?;
                }
                return Ok(());
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
                    self.process_storage_value_internal(
                        ctx, keys, key_ast, tx_context,
                    )?;
                    self.process_storage_value_internal(
                        ctx, values, value_ast, tx_context,
                    )?;
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
                    self.process_storage_value_internal(
                        ctx, keys, key_ast, tx_context,
                    )?;
                    self.process_storage_value_internal(
                        ctx, values, value_ast, tx_context,
                    )?;
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
                        let ctx =
                            &self.update_context(ctx, Some(left_table.clone()));
                        self.process_storage_value_internal(
                            ctx, left, left_ast, tx_context,
                        )?;
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
                        let ctx = &self
                            .update_context(ctx, Some(right_table.clone()));
                        self.process_storage_value_internal(
                            ctx, right, right_ast, tx_context,
                        )?;
                        Ok(())
                    }
                )
            }
            parser::Value::List(l) => {
                must_match_rel!(
                    rel_ast,
                    RelationalAST::List { elems_ast, .. },
                    {
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
                    }
                )
            }
            parser::Value::Pair(left, right) => must_match_rel!(
                rel_ast,
                RelationalAST::Pair {
                    left_ast,
                    right_ast
                },
                {
                    self.process_storage_value_internal(
                        ctx, right, right_ast, tx_context,
                    )?;
                    self.process_storage_value_internal(
                        ctx, left, left_ast, tx_context,
                    )?;
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
                    self.process_storage_value_internal(
                        ctx, right, key_ast, tx_context,
                    )?;
                    self.process_storage_value_internal(
                        ctx, left, value_ast, tx_context,
                    )?;
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
                            Some(s) => insert::Value::String(s.clone()),
                            None => insert::Value::Null,
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
                            self.save_bigmap_location(
                                i.to_u32().unwrap(),
                                ctx.id,
                                rel_ast.clone(),
                            );
                            Ok(())
                        } else {
                            Err(anyhow!(
                                "found big map with non-int id: {:?}",
                                rel_ast
                            ))
                        }
                    }
                    RelationalAST::Leaf { rel_entry } => {
                        if let ExprTy::SimpleExprTy(simple_type) =
                            rel_entry.column_type
                        {
                            let v =
                                Self::storage2sql_value(&simple_type, value)?;
                            self.sql_add_cell(
                                ctx,
                                &rel_entry.table_name,
                                &rel_entry.column_name,
                                v,
                                tx_context,
                            );
                            Ok(())
                        } else {
                            Err(anyhow!(
                                "relationalAST::Leaf has complex expr type"
                            ))
                        }
                    }
                    _ => Ok(()),
                }
            }
        }
    }

    fn storage2sql_value(
        t: &SimpleExprTy,
        v: &parser::Value,
    ) -> Result<insert::Value> {
        debug!("t: {:#?}, v: {:#?}", t, v);
        match t {
            SimpleExprTy::String => {
                if let parser::Value::String(s) = v {
                    Ok(insert::Value::String(s.clone()))
                } else {
                    Err(anyhow!(
                        "storage2sql_value: failed to match type with value"
                    ))
                }
            }
            SimpleExprTy::KeyHash => {
                if let parser::Value::Bytes(bs) = v {
                    Ok(insert::Value::String(bs.clone()))
                } else {
                    Err(anyhow!(
                        "storage2sql_value: failed to match type with value"
                    ))
                }
            }
            SimpleExprTy::Timestamp => {
                Ok(insert::Value::Timestamp(parser::parse_date(v)?))
            }
            SimpleExprTy::Address => {
                match v {
                    parser::Value::Bytes(bs) =>
                    // sometimes we get bytes where we expected an address.
                    {
                        Ok(insert::Value::String(
                            parser::decode_address(bs).unwrap(),
                        ))
                    }
                    parser::Value::Address(addr) => {
                        Ok(insert::Value::String(addr.clone()))
                    }
                    _ => Err(anyhow!(
                        "storage2sql_value: failed to match type with value"
                    )),
                }
            }
            SimpleExprTy::Bool => {
                if let parser::Value::Bool(b) = v {
                    Ok(insert::Value::Bool(*b))
                } else {
                    Err(anyhow!(
                        "storage2sql_value: failed to match type with value"
                    ))
                }
            }
            SimpleExprTy::Bytes => {
                if let parser::Value::Bytes(bs) = v {
                    Ok(insert::Value::String(bs.clone()))
                } else {
                    Err(anyhow!(
                        "storage2sql_value: failed to match type with value"
                    ))
                }
            }
            SimpleExprTy::Unit => match v {
                parser::Value::Unit(None) => Ok(insert::Value::Null),
                parser::Value::Unit(Some(u)) => {
                    Ok(insert::Value::String(u.clone()))
                }
                _ => Err(anyhow!(
                    "storage2sql_value: failed to match type with value"
                )),
            },
            SimpleExprTy::Int | SimpleExprTy::Mutez | SimpleExprTy::Nat => {
                match v {
                    parser::Value::Int(i)
                    | parser::Value::Mutez(i)
                    | parser::Value::Nat(i) => Ok(insert::Value::Numeric(
                        Decimal::from_str(i.to_str_radix(10).as_str()).unwrap(),
                    )),
                    _ => Err(anyhow!(
                        "storage2sql_value: failed to match type with value"
                    )),
                }
            }
            _ => Err(anyhow!(
                "storage2sql_value: failed to match type with value"
            )),
        }
    }

    fn save_bigmap_location(
        &mut self,
        bigmap_id: u32,
        fk: u32,
        rel_ast: RelationalAST,
    ) {
        self.big_map_map
            .insert(bigmap_id, (fk, rel_ast));
    }

    fn sql_add_cell(
        &mut self,
        ctx: &ProcessStorageContext,
        table_name: &str,
        column_name: &str,
        value: insert::Value,
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
                    value: insert::Value::Int(tx_context.id.unwrap() as i32),
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

    fn get_insert(
        &self,
        table_name: &str,
        id: u32,
        fk_id: Option<u32>,
    ) -> Option<Insert> {
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
