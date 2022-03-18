use crate::debug;
use crate::octez::block;
use crate::octez::block::{Tx, TxContext};
use crate::octez::node::StorageGetter;
use crate::sql::db;
use crate::sql::insert;
use crate::sql::insert::{Column, Insert, InsertKey, Inserts};
use crate::sql::types::BigmapMetaAction;
use crate::stats::StatsLogger;
use crate::storage_structure::relational::{
    Contract, RelationalAST, RelationalEntry,
};
use crate::storage_structure::typing::{ExprTy, SimpleExprTy};
use crate::storage_update::bigmap;
use crate::storage_update::bigmap::IntraBlockBigmapDiffsProcessor;
use crate::storage_value::parser;
use anyhow::{anyhow, Context, Result};
use num::ToPrimitive;
use pg_bigdecimal::{BigDecimal, PgNumeric};
use serde_json::json;
use std::collections::HashMap;

#[cfg(test)]
use pretty_assertions::assert_eq;

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
    pub last_table: String,
    pub id: i64,
    pub fk_id: Option<i64>,
}
impl ProcessStorageContext {
    pub fn new(id: i64, root_table: String) -> ProcessStorageContext {
        ProcessStorageContext {
            id,
            last_table: root_table,
            fk_id: None,
        }
    }
    pub fn with_id(&self, id: i64) -> Self {
        let mut c = self.clone();
        c.id = id;
        c
    }
    pub fn with_fk_id(&self, fk_id: i64) -> Self {
        let mut c = self.clone();
        c.fk_id = Some(fk_id);
        c
    }
    pub fn with_last_table(&self, last_table: String) -> Self {
        let mut c = self.clone();
        c.last_table = last_table;
        c
    }
}

pub(crate) type TxContextMap = HashMap<TxContext, Tx>;

pub struct IdGenerator {
    id: i64,
}

impl IdGenerator {
    pub(crate) fn new(initial_value: i64) -> Self {
        Self { id: initial_value }
    }

    pub(crate) fn get_id(&mut self) -> i64 {
        let old_id = self.id;
        self.id += 1;
        old_id
    }
}

type BigMapMap = std::collections::HashMap<i32, (i64, RelationalAST)>;

pub(crate) struct StorageProcessor<NodeCli, BigmapKeys>
where
    NodeCli: StorageGetter,
    BigmapKeys: db::BigmapKeysGetter,
{
    bigmap_map: BigMapMap,
    bigmap_keyhashes: db::BigmapEntries,
    bigmap_meta_actions: Vec<BigmapMetaAction>,
    bigmap_contract_deps: HashMap<(String, i32, bool), ()>,
    id_generator: IdGenerator,
    inserts: Inserts,
    tx_contexts: TxContextMap,
    node_cli: NodeCli,
    bigmap_keys: BigmapKeys,

    stats: Option<StatsLogger>,
}

impl<NodeCli, BigmapKeys> StorageProcessor<NodeCli, BigmapKeys>
where
    NodeCli: StorageGetter,
    BigmapKeys: db::BigmapKeysGetter,
{
    pub(crate) fn new(
        initial_id: i64,
        node_cli: NodeCli,
        bigmap_keys: BigmapKeys,
    ) -> Self {
        Self {
            bigmap_map: BigMapMap::new(),
            inserts: Inserts::new(),
            tx_contexts: HashMap::new(),
            bigmap_keyhashes: HashMap::new(),
            bigmap_meta_actions: vec![],
            bigmap_contract_deps: HashMap::new(),
            id_generator: IdGenerator::new(initial_id),
            node_cli,
            bigmap_keys,

            stats: None,
        }
    }

    pub(crate) fn set_stats_logger(&mut self, l: StatsLogger) {
        self.stats = Some(l);
    }

    fn add_bigmap_keyhash(
        &mut self,
        tx_context: TxContext,
        bigmap: i32,
        keyhash: String,
        key: serde_json::Value,
        value: Option<serde_json::Value>,
    ) {
        self.bigmap_keyhashes
            .insert((bigmap, tx_context, keyhash), (key, value));
    }

    pub(crate) fn get_bigmap_keyhashes(&self) -> db::BigmapEntries {
        self.bigmap_keyhashes.clone()
    }

    pub(crate) fn process_block(
        &mut self,
        block: &block::Block,
        diffs: &IntraBlockBigmapDiffsProcessor,
        contract: &Contract,
    ) -> Result<()> {
        self.bigmap_map.clear();
        self.bigmap_keyhashes.clear();
        self.bigmap_meta_actions.clear();

        let storages: Vec<(TxContext, Option<(String, parser::Value)>, parser::Value)> =
            block.map_tx_contexts(|tx_context, tx, is_origination, op_res| {
                if tx_context.contract != contract.cid.address {
                    return Ok(None);
                }

                let param_parsed: Option<(String, parser::Value)> = if let Some(entrypoint) = &tx.entrypoint {
                    if let Some(v) = &tx.entrypoint_args {
                        Some((entrypoint.clone(), parser::parse_lexed(v)?))
                    } else {
                        warn!("should not have None args to non None entrypoint?");
                        None
                    }
                } else {
                    None
                };

                if is_origination {
                    let storage = parser::parse_json(
                        &self.node_cli.get_contract_storage(
                            &contract.cid.address,
                            tx_context.level,
                        )?,
                    )?;
                    Ok(Some((self.tx_context(tx_context, tx), param_parsed, storage)))
                } else if let Some(storage) = &op_res.storage {
                    Ok(Some((
                        self.tx_context(tx_context, tx),
                        param_parsed,
                        parser::parse_lexed(storage)?,
                    )))
                } else {
                    Err(anyhow!(
                        "bad contract call: no storage update. tx_context={:#?}",
                        tx_context
                    ))
                }
            })?;

        for (tx_context, param_parsed, parsed_storage) in &storages {
            if let Some((entrypoint, param_v)) = param_parsed {
                #[cfg(not(test))]
                let allow_missing_entrpoint_asts: bool = false;

                // Our unit tests were set-up before we were parsing parameters
                // Therefore allowing tests to gracefully ignore missing an entrypoint's AST
                #[cfg(test)]
                let allow_missing_entrpoint_asts: bool = true;

                if !allow_missing_entrpoint_asts
                    || contract
                        .entrypoint_asts
                        .contains_key(entrypoint)
                {
                    if !contract
                        .entrypoint_asts
                        .contains_key(entrypoint)
                    {
                        return Err(anyhow!(
                            "entrypoint '{}' missing. tx_context={:?}",
                            entrypoint,
                            tx_context
                        ))?;
                    }
                    self.process_michelson_value(param_v, &contract.entrypoint_asts[entrypoint], tx_context, format!("entry.{}", entrypoint).as_str())
                    .with_context(|| {
                        format!(
                            "process_block: process storage value failed (tx_context={:?})",
                            tx_context
                        )
                    })?;
                }
            }

            self.process_michelson_value(parsed_storage, &contract.storage_ast, tx_context, "storage")
                .with_context(|| {
                    format!(
                        "process_block: process storage value failed (tx_context={:?})",
                        tx_context
                    )
                })?;

            let mut bigmaps = diffs.get_tx_context_owned_bigmaps(tx_context);
            bigmaps.append(
                &mut self
                    .bigmap_map
                    .keys()
                    .cloned()
                    .collect(),
            );

            bigmaps.sort_unstable();
            bigmaps.dedup();

            for bigmap in bigmaps {
                let (deps, ops) =
                    diffs.normalized_diffs(bigmap, tx_context, bigmap >= 0);
                for op in ops.iter().rev() {
                    self.process_bigmap_op(op, tx_context)?;
                }
                if self.bigmap_map.contains_key(&bigmap) {
                    for (src_bigmap, src_context) in deps {
                        let is_deep_copy = bigmap >= 0;
                        self.bigmap_contract_deps.insert(
                            (
                                src_context.contract.clone(),
                                src_bigmap,
                                is_deep_copy,
                            ),
                            (),
                        );
                        if is_deep_copy {
                            self.process_bigmap_copy(
                                tx_context, src_bigmap, bigmap,
                            )?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn drain_bigmap_contract_dependencies(
        &mut self,
    ) -> Vec<(String, i32, bool)> {
        self.bigmap_contract_deps
            .drain()
            .map(|(k, _)| k)
            .collect()
    }

    pub(crate) fn drain_bigmap_meta_actions(
        &mut self,
    ) -> Vec<BigmapMetaAction> {
        self.bigmap_meta_actions
            .drain(..)
            .collect()
    }

    pub(crate) fn drain_txs(&mut self) -> (Vec<TxContext>, Vec<Tx>) {
        self.tx_contexts.drain().unzip()
    }

    pub(crate) fn drain_inserts(&mut self) -> Inserts {
        self.inserts.drain().collect()
    }

    fn process_bigmap_copy(
        &mut self,
        ctx: &TxContext,
        src_bigmap: i32,
        dest_bigmap: i32,
    ) -> Result<()> {
        let at_level = ctx.level - 1;
        let entries = self
            .bigmap_keys
            .get(at_level, src_bigmap)?;

        let num_entries = entries.len();

        for (i, (keyhash, key, value)) in entries.into_iter().enumerate() {
            //let value = self
            //    .node_cli
            //    .get_bigmap_value(at_level, src_bigmap, &keyhash)?;
            if value.is_none() {
                continue;
            }

            let op = bigmap::Op::Update {
                bigmap: dest_bigmap,
                keyhash,
                key,
                value,
            };
            self.process_bigmap_op(&op, ctx)?;

            if let Some(stats) = &self.stats {
                stats.set(
                    format!(
                        "bigmap copy ({} -> {} at {:?})",
                        src_bigmap, dest_bigmap, ctx
                    )
                    .as_str(),
                    "keys processed",
                    format!("{}/{}", i, num_entries),
                )?;
            }
        }

        if let Some(stats) = &self.stats {
            stats.unset(
                format!(
                    "bigmap copy ({} -> {} at {:?})",
                    src_bigmap, dest_bigmap, ctx
                )
                .as_str(),
                "keys processed",
            )?;
        }
        Ok(())
    }

    fn tx_context(
        &mut self,
        mut tx_context: TxContext,
        mut tx: Tx,
    ) -> TxContext {
        if let Some((result, _)) = self
            .tx_contexts
            .get_key_value(&tx_context)
        {
            result.clone()
        } else {
            let id = self.id_generator.get_id();
            tx_context.id = Some(id);
            tx.tx_context_id = id;
            self.tx_contexts
                .insert(tx_context.clone(), tx);
            tx_context
        }
    }

    fn unfold_value(
        &self,
        v: &parser::Value,
        rel_ast: &RelationalAST,
    ) -> Result<parser::Value> {
        match rel_ast {
            RelationalAST::Map { .. } | RelationalAST::BigMap { .. } => {
                v.unpair_elts()
            }
            RelationalAST::List { .. } => {
                // do not unfold list
                v.unpair_list()
            }
            _ => Ok(v.unfold_list()),
        }
    }

    fn resolve_or(
        &self,
        parent_table: &str,
        parent_entry: &RelationalEntry,
        v: &parser::Value,
        rel_ast: &RelationalAST,
    ) -> Result<RelationalEntry> {
        debug!(
            "resolve_or: v={}, rel_ast={}",
            debug::pp_depth(2, v),
            debug::pp_depth(2, rel_ast)
        );
        match &self.unfold_value(v, rel_ast)? {
            parser::Value::Left(left) => must_match_rel!(
                rel_ast,
                RelationalAST::OrEnumeration {
                    left_table,
                    left_ast,
                    ..
                },
                {
                    self.resolve_or(
                        left_table
                            .as_ref()
                            .map(|t| t.as_str())
                            .unwrap_or(parent_table),
                        parent_entry,
                        left,
                        left_ast,
                    )
                }
            ),
            parser::Value::Right(right) => must_match_rel!(
                rel_ast,
                RelationalAST::OrEnumeration {
                    right_table,
                    right_ast,
                    ..
                },
                {
                    self.resolve_or(
                        right_table
                            .as_ref()
                            .map(|t| t.as_str())
                            .unwrap_or(parent_table),
                        parent_entry,
                        right,
                        right_ast,
                    )
                }
            ),
            parser::Value::Pair { .. } | parser::Value::List { .. } => {
                let mut res = parent_entry.clone();
                res.value = Some(parent_table.to_string());
                Ok(res)
            }
            parser::Value::Unit => {
                must_match_rel!(rel_ast, RelationalAST::Leaf { rel_entry }, {
                    let mut res = parent_entry.clone();
                    res.value = rel_entry.value.clone();
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

    fn process_bigmap_op(
        &mut self,
        op: &bigmap::Op,
        tx_context: &TxContext,
    ) -> Result<()> {
        match op {
            bigmap::Op::Update {
                bigmap,
                keyhash,
                key,
                value,
            } => {
                if self.bigmap_keyhashes.contains_key(&(
                    *bigmap,
                    tx_context.clone(),
                    keyhash.clone(),
                )) {
                    return Ok(());
                }
                let (_fk, rel_ast) = match self.bigmap_map.get(bigmap) {
                    Some((fk, n)) => (fk, n.clone()),
                    None => {
                        return Ok(());
                        // return Err(anyhow!(
                        //     "no big map content found {:?}",
                        //     diff
                        // ))
                    }
                };
                must_match_rel!(
                    rel_ast,
                    RelationalAST::BigMap {
                        table,
                        key_ast,
                        value_ast,
                        ..
                    },
                    {
                        self.add_bigmap_keyhash(
                            tx_context.clone(),
                            *bigmap,
                            keyhash.clone(),
                            key.clone(),
                            value.clone(),
                        );

                        let ctx = &ProcessStorageContext::new(
                            self.id_generator.get_id(),
                            table.clone(),
                        );
                        self.process_michelson_value_internal(
                            ctx,
                            &parser::parse_lexed(key)?,
                            &key_ast,
                            tx_context,
                        )?;
                        match &value {
                            None => self.sql_add_cell(
                                ctx,
                                &table,
                                &"deleted".to_string(),
                                insert::Value::Bool(true),
                                tx_context,
                            ),
                            Some(val) => {
                                self.process_michelson_value_internal(
                                    ctx,
                                    &parser::parse_lexed(val)?,
                                    &value_ast,
                                    tx_context,
                                )?;
                            }
                        };
                        self.sql_add_cell(
                            ctx,
                            &table,
                            &"bigmap_id".to_string(),
                            insert::Value::Int(*bigmap),
                            tx_context,
                        );
                        Ok(())
                    }
                )
            }
            bigmap::Op::Alloc { bigmap } => {
                let (_fk, rel_ast) = match self.bigmap_map.get(bigmap) {
                    Some((fk, n)) => (fk, n.clone()),
                    None => {
                        return Err(anyhow!(
                            "no big map content found {:?}",
                            op
                        ))
                    }
                };
                must_match_rel!(rel_ast, RelationalAST::BigMap { table, .. }, {
                    self.bigmap_meta_actions
                        .push(BigmapMetaAction {
                            tx_context_id: tx_context.id.unwrap(),
                            bigmap_id: *bigmap,

                            action: "alloc".to_string(),
                            value: Some(json!({
                                "contract_address": tx_context.contract,
                                "table": table
                            })),
                        });
                    Ok(())
                })
            }
            bigmap::Op::Copy { bigmap, source } => {
                let (_fk, rel_ast) = match self.bigmap_map.get(bigmap) {
                    Some((fk, n)) => (fk, n.clone()),
                    None => {
                        return Err(anyhow!(
                            "no big map content found {:?}",
                            op
                        ))
                    }
                };
                must_match_rel!(
                    rel_ast,
                    RelationalAST::BigMap { table, .. },
                    {
                        let ctx = &ProcessStorageContext::new(
                            self.id_generator.get_id(),
                            table.clone(),
                        );
                        self.sql_add_cell(
                            ctx,
                            &table,
                            &"bigmap_id".to_string(),
                            insert::Value::Int(*bigmap),
                            tx_context,
                        );
                        Ok(())
                    }
                )?;
                self.bigmap_meta_actions
                    .push(BigmapMetaAction {
                        tx_context_id: tx_context.id.unwrap(),
                        bigmap_id: *bigmap,

                        action: "copy".to_string(),
                        value: Some(json!({ "source": source })),
                    });
                Ok(())
            }
            bigmap::Op::Clear { bigmap } => {
                self.bigmap_meta_actions
                    .push(BigmapMetaAction {
                        tx_context_id: tx_context.id.unwrap(),
                        bigmap_id: *bigmap,

                        action: "clear".to_string(),
                        value: None,
                    });
                Ok(())
            }
        }
    }

    /// Walks simultaneously through the table definition and the actual values it finds, and attempts
    /// to match them. raises an error if it cannot do this (i.e. they do not match).
    fn process_michelson_value(
        &mut self,
        value: &parser::Value,
        rel_ast: &RelationalAST,
        tx_context: &TxContext,
        root_table_name: &str,
    ) -> Result<()> {
        let ctx = &ProcessStorageContext::new(
            self.id_generator.get_id(),
            root_table_name.to_string(),
        );
        self.process_michelson_value_internal(ctx, value, rel_ast, tx_context)?;
        Ok(())
    }

    fn update_context(
        &mut self,
        ctx: &ProcessStorageContext,
        current_table: Option<String>,
        tx_context: &TxContext,
    ) -> ProcessStorageContext {
        if let Some(table_name) = current_table {
            if ctx.last_table != table_name {
                self.sql_touch_insert(ctx, &ctx.last_table, tx_context);

                return ctx
                    .with_last_table(table_name)
                    .with_fk_id(ctx.id)
                    .with_id(self.id_generator.get_id());
            }
        }
        ctx.clone()
    }

    fn process_michelson_value_internal(
        &mut self,
        ctx: &ProcessStorageContext,
        value: &parser::Value,
        rel_ast: &RelationalAST,
        tx_context: &TxContext,
    ) -> Result<()> {
        let v = &self.unfold_value(value, rel_ast)?;
        debug!(
            "value: {}, v: {}, rel_ast: {}",
            debug::pp_depth(3, value),
            debug::pp_depth(3, v),
            debug::pp_depth(4, rel_ast)
        );
        match rel_ast {
            RelationalAST::Leaf { rel_entry } => {
                if let ExprTy::SimpleExprTy(SimpleExprTy::Stop) =
                    rel_entry.column_type
                {
                    // we don't even try to store lambdas.
                    return Ok(());
                }
            }
            RelationalAST::OrEnumeration {
                or_unfold: Some(or_unfold),
                ..
            } => {
                let rel_entry =
                    self.resolve_or(&ctx.last_table, or_unfold, v, rel_ast)?;
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
                    self.process_michelson_value_internal(
                        ctx, v, elem_ast, tx_context,
                    )?;
                } else {
                    self.sql_touch_insert(ctx, &ctx.last_table, tx_context);
                }
                return Ok(());
            }
            _ => {}
        };

        let ctx = &self.update_context(ctx, rel_ast.table_entry(), tx_context);

        match v {
            parser::Value::Elt(key, value) => must_match_rel!(
                rel_ast,
                RelationalAST::Map {
                    key_ast,
                    value_ast,
                    ..
                },
                {
                    self.process_michelson_value_internal(
                        ctx, key, key_ast, tx_context,
                    )?;
                    self.process_michelson_value_internal(
                        ctx, value, value_ast, tx_context,
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
                    self.process_michelson_value_internal(
                        ctx, key, key_ast, tx_context,
                    )?;
                    self.process_michelson_value_internal(
                        ctx, value, value_ast, tx_context,
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
                        if left_table.is_none() {
                            return Ok(());
                        }
                        let ctx = &self.update_context(
                            ctx,
                            left_table.clone(),
                            tx_context,
                        );
                        self.process_michelson_value_internal(
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
                        if right_table.is_none() {
                            return Ok(());
                        }
                        let ctx = &self.update_context(
                            ctx,
                            right_table.clone(),
                            tx_context,
                        );
                        self.process_michelson_value_internal(
                            ctx, right, right_ast, tx_context,
                        )?;
                        Ok(())
                    }
                )
            }
            parser::Value::List(l) => must_match_rel!(
                rel_ast,
                RelationalAST::List { elems_ast, .. },
                {
                    let mut ctx: ProcessStorageContext = ctx.clone();
                    for element in l {
                        self.process_michelson_value_internal(
                            &ctx, element, elems_ast, tx_context,
                        )?;
                        ctx = ctx.with_id(self.id_generator.get_id());
                    }
                    Ok(())
                }
            )
            .or(must_match_rel!(rel_ast, RelationalAST::Map { .. }, {
                let mut ctx: ProcessStorageContext = ctx.clone();
                for element in l {
                    self.process_michelson_value_internal(
                        &ctx, element, rel_ast, tx_context,
                    )?;
                    ctx = ctx.with_id(self.id_generator.get_id());
                }
                Ok(())
            }))
            .or(must_match_rel!(
                rel_ast,
                RelationalAST::BigMap { .. },
                {
                    let mut ctx: ProcessStorageContext = ctx.clone();
                    for element in l {
                        self.process_michelson_value_internal(
                            &ctx, element, rel_ast, tx_context,
                        )?;
                        ctx = ctx.with_id(self.id_generator.get_id());
                    }
                    Ok(())
                }
            )),
            parser::Value::Pair(left, right) => must_match_rel!(
                rel_ast,
                RelationalAST::Pair {
                    left_ast,
                    right_ast
                },
                {
                    self.process_michelson_value_internal(
                        ctx, right, right_ast, tx_context,
                    )?;
                    self.process_michelson_value_internal(
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
                    self.process_michelson_value_internal(
                        ctx, right, key_ast, tx_context,
                    )?;
                    self.process_michelson_value_internal(
                        ctx, left, value_ast, tx_context,
                    )?;
                    Ok(())
                }
            )),
            parser::Value::Unit => {
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
                                i.to_i32().ok_or_else(|| {
                                    anyhow!("failed to translate bigmap id ({}) into i32", i)
                                })?,
                                ctx.id,
                                rel_ast.clone(),
                            );
                            Ok(())
                        } else {
                            Err(anyhow!(
                                "found big map with non-int id ({:?}): {:?}",
                                value,
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
                    //_ => Ok(())
                    _ => Err(anyhow!(
                        "failed to match {:#?} with {:#?}",
                        v,
                        rel_ast
                    )),
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
            SimpleExprTy::Bytes
            | SimpleExprTy::KeyHash
            | SimpleExprTy::String => match v {
                parser::Value::Bytes(s) | parser::Value::String(s) => {
                    Ok(insert::Value::String(s.clone()))
                }
                _ => Err(anyhow!(
                    "storage2sql_value: failed to match type with value"
                )),
            },
            SimpleExprTy::Timestamp => Ok(parser::parse_date(v)?),
            SimpleExprTy::Address => {
                match v {
                    parser::Value::Bytes(bs) =>
                    // sometimes we get bytes where we expected an address.
                    {
                        Ok(insert::Value::String(parser::decode_address(bs)?))
                    }
                    parser::Value::Address(addr)
                    | parser::Value::String(addr) => {
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
            SimpleExprTy::Unit => match v {
                parser::Value::Unit => Ok(insert::Value::Null),
                _ => Err(anyhow!(
                    "storage2sql_value: failed to match type with value"
                )),
            },
            SimpleExprTy::Int | SimpleExprTy::Nat | SimpleExprTy::Mutez => {
                match v {
                    parser::Value::Int(i)
                    | parser::Value::Mutez(i)
                    | parser::Value::Nat(i) => Ok(insert::Value::Numeric(
                        PgNumeric::new(Some(BigDecimal::new(i.clone(), 0))),
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
        bigmap_id: i32,
        fk: i64,
        rel_ast: RelationalAST,
    ) {
        self.bigmap_map
            .insert(bigmap_id, (fk, rel_ast));
    }

    fn sql_touch_insert(
        &mut self,
        ctx: &ProcessStorageContext,
        table_name: &str,
        tx_context: &TxContext,
    ) -> Insert {
        match self.get_insert(table_name, ctx.id, ctx.fk_id) {
            Some(x) => x,
            None => {
                let value = Insert {
                    table_name: table_name.to_string(),
                    id: ctx.id,
                    fk_id: ctx.fk_id,
                    columns: vec![Column {
                        name: "tx_context_id".to_string(),
                        value: insert::Value::BigInt(tx_context.id.unwrap()),
                    }],
                };
                self.inserts.insert(
                    InsertKey {
                        table_name: table_name.to_string(),
                        id: ctx.id,
                    },
                    value.clone(),
                );
                value
            }
        }
    }

    fn sql_add_cell(
        &mut self,
        ctx: &ProcessStorageContext,
        table_name: &str,
        column_name: &str,
        value: insert::Value,
        tx_context: &TxContext,
    ) {
        let mut insert = self.sql_touch_insert(ctx, table_name, tx_context);
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
        id: i64,
        fk_id: Option<i64>,
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

    #[cfg(test)]
    pub fn process_michelson_value_test(
        &mut self,
        value: &parser::Value,
        rel_ast: &RelationalAST,
        tx_context: &TxContext,
    ) -> Result<()> {
        self.process_michelson_value(value, rel_ast, tx_context, "storage")
    }
}

#[test]
fn test_process_michelson_value() {
    use num::BigInt;

    fn numeric(i: i32) -> insert::Value {
        insert::Value::Numeric(PgNumeric::new(Some(BigDecimal::from(i))))
    }

    struct TestCase {
        name: String,
        rel_ast: RelationalAST,
        value: parser::Value,
        tx_context: TxContext,
        exp_inserts: Vec<Insert>,
    }
    let tests: Vec<TestCase> = vec![
        TestCase {
            name: "basic string".to_string(),
            rel_ast: RelationalAST::Leaf {
                rel_entry: RelationalEntry {
                    table_name: "storage".to_string(),
                    column_name: "contract_owner".to_string(),
                    column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                    value: None,
                    is_index: false,
                },
            },
            value: parser::Value::String("test value".to_string()),
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![Insert {
                table_name: "storage".to_string(),
                id: 1,
                fk_id: None,
                columns: vec![
                    Column {
                        name: "tx_context_id".to_string(),
                        value: insert::Value::BigInt(32),
                    },
                    Column {
                        name: "contract_owner".to_string(),
                        value: insert::Value::String("test value".to_string()),
                    },
                ],
            }],
        },
        TestCase {
            name: "option accepts value (storage simply has underlying value present, it's not a 'Some(x)' but rather an x that we get)"
                .to_string(),
            rel_ast: RelationalAST::Option {
                elem_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "contract_owner".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                    },
                }),
            },
            value: parser::Value::String("the value".to_string()),
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![Insert {
                table_name: "storage".to_string(),
                id: 1,
                fk_id: None,
                columns: vec![Column {
                    name: "tx_context_id".to_string(),
                    value: insert::Value::BigInt(32),
                }, Column {
                    name: "contract_owner".to_string(),
                    value: insert::Value::String("the value".to_string()),
                }],
            }],
        },
        TestCase {
            name: "option accepts none (simply no column value then)"
                .to_string(),
            rel_ast: RelationalAST::Option {
                elem_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "contract_owner".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                    },
                }),
            },
            value: parser::Value::None,
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![Insert {
                table_name: "storage".to_string(),
                id: 1,
                fk_id: None,
                columns: vec![Column {
                    name: "tx_context_id".to_string(),
                    value: insert::Value::BigInt(32),
                }],
            }],
        },
        TestCase {
            name: "set of integers".to_string(),
            rel_ast: RelationalAST::List {
                table: "storage.the_set".to_string(),
                elems_unique: true,
                elems_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.the_set".to_string(),
                        column_name: "idx_foo".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Int),
                        value: None,
                        is_index: true,
                    },
                }),
            },
            value: parser::Value::List(vec![
                parser::Value::Int(BigInt::from(0 as i32)),
                parser::Value::Int(BigInt::from(-5 as i32)),
            ]),
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![
                Insert {
                    table_name: "storage".to_string(),
                    id: 1,
                    fk_id: None,
                    columns: vec![Column {
                        name: "tx_context_id".to_string(),
                        value: insert::Value::BigInt(32),
                    }],
                },
                Insert {
                    table_name: "storage.the_set".to_string(),
                    id: 2,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(0),
                        },
                    ],
                },
                Insert {
                    table_name: "storage.the_set".to_string(),
                    id: 3,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(-5),
                        },
                    ],
                },
            ],
        },
        TestCase {
            name: "set of integers (nested pairs is accepted too)".to_string(),
            rel_ast: RelationalAST::List {
                table: "storage.the_set".to_string(),
                elems_unique: true,
                elems_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.the_set".to_string(),
                        column_name: "idx_foo".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Int),
                        value: None,
                        is_index: true,
                    },
                }),
            },
            value: parser::Value::Pair(
                Box::new(parser::Value::Int(BigInt::from(0 as i32))),
                Box::new(parser::Value::Pair(
                    Box::new(parser::Value::Int(BigInt::from(-5 as i32))),
                    Box::new(parser::Value::Int(BigInt::from(-2 as i32))),
                )),
            ),
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![
                Insert {
                    table_name: "storage".to_string(),
                    id: 1,
                    fk_id: None,
                    columns: vec![Column {
                        name: "tx_context_id".to_string(),
                        value: insert::Value::BigInt(32),
                    }],
                },
                Insert {
                    table_name: "storage.the_set".to_string(),
                    id: 2,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(0),
                        },
                    ],
                },
                Insert {
                    table_name: "storage.the_set".to_string(),
                    id: 3,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(-5),
                        },
                    ],
                },
                Insert {
                    table_name: "storage.the_set".to_string(),
                    id: 4,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(-2),
                        },
                    ],
                },
            ],
        },
        TestCase {
            name: "list w/ storage variable".to_string(),
            rel_ast: RelationalAST::Pair {
                left_ast: Box::new(RelationalAST::List {
                    table: "storage.the_set".to_string(),
                    elems_unique: true,
                    elems_ast: Box::new(RelationalAST::Leaf {
                        rel_entry: RelationalEntry {
                            table_name: "storage.the_set".to_string(),
                            column_name: "idx_foo".to_string(),
                            column_type: ExprTy::SimpleExprTy(
                                SimpleExprTy::Int,
                            ),
                            value: None,
                            is_index: true,
                        },
                    }),
                }),
                right_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage".to_string(),
                        column_name: "bar".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                    },
                }),
            },
            value: parser::Value::Pair(
                Box::new(parser::Value::List(vec![
                    parser::Value::Int(BigInt::from(0 as i32)),
                    parser::Value::Int(BigInt::from(-5 as i32)),
                ])),
                Box::new(parser::Value::String("value".to_string())),
            ),
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![
                Insert {
                    table_name: "storage".to_string(),
                    id: 1,
                    fk_id: None,
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "bar".to_string(),
                            value: insert::Value::String("value".to_string()),
                        },
                    ],
                },
                Insert {
                    table_name: "storage.the_set".to_string(),
                    id: 2,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(0),
                        },
                    ],
                },
                Insert {
                    table_name: "storage.the_set".to_string(),
                    id: 3,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(-5),
                        },
                    ],
                },
            ],
        },
        TestCase {
            name: "empty set of integers".to_string(),
            rel_ast: RelationalAST::List {
                table: "storage.the_set".to_string(),
                elems_unique: true,
                elems_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.the_set".to_string(),
                        column_name: "idx_foo".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Int),
                        value: None,
                        is_index: true,
                    },
                }),
            },
            value: parser::Value::List(vec![]),
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![Insert {
                // note: still generates an entry for the storage table
                table_name: "storage".to_string(),
                id: 1,
                fk_id: None,
                columns: vec![Column {
                    name: "tx_context_id".to_string(),
                    value: insert::Value::BigInt(32),
                }],
            }],
        },
        TestCase {
            name: "bigmap: empty set of values".to_string(),
            rel_ast: RelationalAST::BigMap {
                table: "storage.the_bigmap".to_string(),
                key_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.the_bigmap".to_string(),
                        column_name: "idx_foo".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Int),
                        value: None,
                        is_index: true,
                    },
                }),
                value_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.the_bigmap".to_string(),
                        column_name: "bar".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                    },
                }),
            },
            value: parser::Value::List(vec![]),
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![Insert {
                // note: still generates an entry for the storage table
                table_name: "storage".to_string(),
                id: 1,
                fk_id: None,
                columns: vec![Column {
                    name: "tx_context_id".to_string(),
                    value: insert::Value::BigInt(32),
                }],
            }],
        },
        TestCase {
            name: "bigmap: with set of values".to_string(),
            rel_ast: RelationalAST::BigMap {
                table: "storage.the_bigmap".to_string(),
                key_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.the_bigmap".to_string(),
                        column_name: "idx_foo".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::Int),
                        value: None,
                        is_index: true,
                    },
                }),
                value_ast: Box::new(RelationalAST::Leaf {
                    rel_entry: RelationalEntry {
                        table_name: "storage.the_bigmap".to_string(),
                        column_name: "bar".to_string(),
                        column_type: ExprTy::SimpleExprTy(SimpleExprTy::String),
                        value: None,
                        is_index: false,
                    },
                }),
            },
            value: parser::Value::List(vec![
                parser::Value::Elt(
                    Box::new(parser::Value::Int(BigInt::from(3 as i32))),
                    Box::new(parser::Value::String("some_value".to_string())),
                ),
                parser::Value::Elt(
                    Box::new(parser::Value::Int(BigInt::from(1 as i32))),
                    Box::new(parser::Value::String(
                        "another_value".to_string(),
                    )),
                ),
            ]),
            tx_context: TxContext {
                id: Some(32),
                level: 10,
                contract: "test".to_string(),
                operation_group_number: 1,
                operation_number: 2,
                content_number: 3,
                internal_number: None,
            },
            exp_inserts: vec![
                Insert {
                    table_name: "storage".to_string(),
                    id: 1,
                    fk_id: None,
                    columns: vec![Column {
                        name: "tx_context_id".to_string(),
                        value: insert::Value::BigInt(32),
                    }],
                },
                Insert {
                    table_name: "storage.the_bigmap".to_string(),
                    id: 2,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(3),
                        },
                        Column {
                            name: "bar".to_string(),
                            value: insert::Value::String(
                                "some_value".to_string(),
                            ),
                        },
                    ],
                },
                Insert {
                    table_name: "storage.the_bigmap".to_string(),
                    id: 3,
                    fk_id: Some(1),
                    columns: vec![
                        Column {
                            name: "tx_context_id".to_string(),
                            value: insert::Value::BigInt(32),
                        },
                        Column {
                            name: "idx_foo".to_string(),
                            value: numeric(1),
                        },
                        Column {
                            name: "bar".to_string(),
                            value: insert::Value::String(
                                "another_value".to_string(),
                            ),
                        },
                    ],
                },
            ],
        },
    ];

    for tc in tests {
        println!("test case: {}", tc.name);

        let mut exp = Inserts::new();
        let mut ordering: Vec<InsertKey> = vec![];
        for insert in &tc.exp_inserts {
            let k = InsertKey {
                table_name: insert.table_name.clone(),
                id: insert.id,
            };
            exp.insert(k.clone(), insert.clone());
            ordering.push(k);
        }

        let mut processor = StorageProcessor::new(
            1,
            DummyStorageGetter {},
            DummyBigmapKeysGetter {},
        );

        let res = processor.process_michelson_value_test(
            &tc.value,
            &tc.rel_ast,
            &tc.tx_context,
        );
        assert!(res.is_ok());

        let got = processor.drain_inserts();
        let mut got_ordered: Vec<Insert> = vec![];
        for exp_key in &ordering {
            if !got.contains_key(exp_key) {
                continue;
            }
            got_ordered.push(got[exp_key].clone())
        }
        for (got_key, got_value) in &got {
            if exp.contains_key(got_key) {
                continue;
            }
            got_ordered.push(got_value.clone());
        }
        assert_eq!(tc.exp_inserts, got_ordered);
    }
}

#[test]
fn test_process_block() {
    // this tests the generated table structures against known good ones.
    // if it fails for a good reason, the output can be used to repopulate the
    // test files. To do this, execute script/generate_test_output.bash
    use crate::octez::block::Block;
    use crate::sql::insert;
    use crate::sql::insert::Insert;
    use crate::sql::table_builder::{TableBuilder, TableMap};
    use crate::storage_structure::relational::ASTBuilder;
    use crate::storage_structure::typing;
    use ron::ser::{to_string_pretty, PrettyConfig};
    use std::str::FromStr;

    env_logger::init();

    fn get_rel_ast_from_script_json(
        json: &serde_json::Value,
    ) -> Result<RelationalAST> {
        let storage_definition = json["code"]
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["prim"] == "storage")
            .unwrap()["args"][0]
            .clone();
        debug!("{}", storage_definition.to_string());
        let type_ast = typing::type_ast_from_json(&storage_definition)?;
        let rel_ast = ASTBuilder::new()
            .build_relational_ast(
                &crate::relational::Context::init("storage"),
                &type_ast,
            )
            .unwrap();
        Ok(rel_ast)
    }

    #[derive(Debug)]
    struct Contract<'a> {
        id: &'a str,
        levels: Vec<u32>,
    }

    let mut contracts: Vec<Contract> = vec![];

    let paths = fs::read_dir("test/").unwrap();
    for path in paths {
        println!("Name: {}", path.unwrap().path().display());
        // TODO
    }

    fn sort_inserts(tables: &TableMap, inserts: &mut Vec<Insert>) {
        inserts.sort_by_key(|insert| {
            let mut sort_on: Vec<String> = vec![];
            if tables.contains_key(&insert.table_name) {
                sort_on = tables[&insert.table_name]
                    .indices
                    .iter()
                    .filter(|idx| idx != &"id")
                    .cloned()
                    .collect();
                sort_on.extend(
                    tables[&insert.table_name]
                        .columns
                        .keys()
                        .filter(|col| {
                            col != &"id"
                                && !tables[&insert.table_name]
                                    .indices
                                    .iter()
                                    .any(|idx| idx == *col)
                        })
                        .cloned()
                        .collect::<Vec<String>>(),
                );
                // sort on id last, only relevant when dealing with non-unique
                // containers (which is only the michelson List type).
                sort_on.push("id".to_string());
            }
            let mut res: Vec<insert::Value> = sort_on
                .iter()
                .map(|idx| {
                    insert
                        .get_column(idx)
                        .unwrap()
                        .map_or(insert::Value::Null, |col| col.value.clone())
                })
                .collect();
            res.insert(0, insert::Value::String(insert.table_name.clone()));
            println!("sorting by: {:?}", sort_on);
            res
        });
    }

    let mut results: Vec<(&str, u32, Vec<Insert>)> = vec![];
    let mut expected: Vec<(&str, u32, Vec<Insert>)> = vec![];
    for contract in &contracts {
        let mut storage_processor = StorageProcessor::new(
            1,
            DummyStorageGetter {},
            DummyBigmapKeysGetter {},
        );

        // verify that the test case is sane
        let mut unique_levels = contract.levels.clone();
        unique_levels.sort();
        unique_levels.dedup();
        assert_eq!(contract.levels.len(), unique_levels.len());

        let script_json = serde_json::Value::from_str(&debug::load_test(
            &format!("test/{}.script", contract.id),
        ))
        .unwrap();
        let rel_ast = get_rel_ast_from_script_json(&script_json).unwrap();
        debug!("rel ast: {:#?}", rel_ast);

        // having the table layout is useful for sorting the test results and
        // expected results in deterministic order (we'll use the table's index)
        let mut builder = TableBuilder::new("storage");
        builder.populate(&rel_ast);
        let tables = &builder.tables;

        for level in &contract.levels {
            println!("contract={}, level={}", contract.id, level);

            let block: Block = serde_json::from_str(&debug::load_test(
                &format!("test/{}.level-{}.json", contract.id, level),
            ))
            .unwrap();

            let diffs =
                IntraBlockBigmapDiffsProcessor::from_block(&block).unwrap();
            storage_processor
                .process_block(
                    &block,
                    &diffs,
                    &crate::storage_structure::relational::Contract {
                        cid: crate::config::ContractID {
                            name: contract.id.to_string(),
                            address: contract.id.to_string(),
                        },
                        storage_ast: rel_ast.clone(),
                        level_floor: None,
                        entrypoint_asts: HashMap::new(),
                    },
                )
                .unwrap();
            let inserts = storage_processor.drain_inserts();
            storage_processor.drain_txs();
            storage_processor.drain_bigmap_contract_dependencies();

            let filename =
                format!("test/{}-{}-inserts.json", contract.id, level);
            println!("cat > {} <<ENDOFJSON", filename);
            println!(
                "{}",
                to_string_pretty(&inserts, PrettyConfig::new()).unwrap()
            );
            println!(
                "ENDOFJSON
    "
            );

            let mut result: Vec<Insert> = inserts.values().cloned().collect();
            sort_inserts(tables, &mut result);
            results.push((contract.id, *level, result));

            use std::path::Path;
            let p = Path::new(&filename);

            use std::fs::File;
            if let Ok(file) = File::open(p) {
                use std::io::BufReader;
                let reader = BufReader::new(file);
                println!("filename: {}", filename);
                let v: Inserts = ron::de::from_reader(reader).unwrap();

                let mut expected_result: Vec<Insert> =
                    v.values().cloned().collect();
                sort_inserts(tables, &mut expected_result);

                expected.push((contract.id, *level, expected_result));
            }
        }
    }
    assert_eq!(expected, results);
}

#[cfg(test)]
struct DummyStorageGetter {}
#[cfg(test)]
impl crate::octez::node::StorageGetter for DummyStorageGetter {
    fn get_contract_storage(
        &self,
        _contract_id: &str,
        _level: u32,
    ) -> Result<serde_json::Value> {
        Err(anyhow!("dummy storage getter was not expected to be called in test_block tests"))
    }

    fn get_bigmap_value(
        &self,
        _level: u32,
        _bigmap_id: i32,
        _keyhash: &str,
    ) -> Result<Option<serde_json::Value>> {
        Err(anyhow!("dummy storage getter was not expected to be called in test_block tests"))
    }
}

#[cfg(test)]
struct DummyBigmapKeysGetter {}
#[cfg(test)]
impl crate::sql::db::BigmapKeysGetter for DummyBigmapKeysGetter {
    fn get(
        &mut self,
        _level: u32,
        _bigmap_id: i32,
    ) -> Result<Vec<(String, String)>> {
        Err(anyhow!("dummy bigmap keys getter was not expected to be called in test_block tests"))
    }
}
