use crate::debug;
use crate::octez::block;
use crate::octez::block::TxContext;
use crate::octez::node::StorageGetter;
use crate::sql::db::BigmapKeysGetter;
use crate::sql::insert;
use crate::sql::insert::{Column, Insert, InsertKey, Inserts};
use crate::storage_structure::relational::{RelationalAST, RelationalEntry};
use crate::storage_structure::typing::{ExprTy, SimpleExprTy};
use crate::storage_update::bigmap;
use crate::storage_update::bigmap::IntraBlockBigmapDiffsProcessor;
use crate::storage_value::parser;
use anyhow::{anyhow, Context, Result};
use num::ToPrimitive;
use pg_bigdecimal::{BigDecimal, PgNumeric};
use std::collections::HashMap;

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
    pub id: i64,
    pub fk_id: Option<i64>,
}
impl ProcessStorageContext {
    pub fn new(id: i64) -> ProcessStorageContext {
        ProcessStorageContext {
            id,
            last_table: None,
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
        c.last_table = Some(last_table);
        c
    }
}

pub(crate) type TxContextMap = HashMap<TxContext, TxContext>;

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
    BigmapKeys: BigmapKeysGetter,
{
    bigmap_map: BigMapMap,
    bigmap_keyhashes: Vec<(TxContext, i32, String, String)>,
    id_generator: IdGenerator,
    inserts: Inserts,
    tx_contexts: TxContextMap,
    node_cli: NodeCli,
    bigmap_keys: BigmapKeys,
}

impl<NodeCli, BigmapKeys> StorageProcessor<NodeCli, BigmapKeys>
where
    NodeCli: StorageGetter,
    BigmapKeys: BigmapKeysGetter,
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
            bigmap_keyhashes: Vec::new(),
            id_generator: IdGenerator::new(initial_id),
            node_cli,
            bigmap_keys,
        }
    }

    fn add_bigmap_keyhash(
        &mut self,
        tx_context: TxContext,
        bigmap: i32,
        keyhash: String,
        key: String,
    ) {
        self.bigmap_keyhashes
            .push((tx_context, bigmap, keyhash, key));
    }

    pub(crate) fn get_bigmap_keyhashes(
        &self,
    ) -> Vec<(TxContext, i32, String, String)> {
        self.bigmap_keyhashes.clone()
    }

    pub(crate) fn process_block(
        &mut self,
        block: &block::Block,
        diffs: &IntraBlockBigmapDiffsProcessor,
        contract_id: &str,
        rel_ast: &RelationalAST,
    ) -> Result<(Inserts, Vec<TxContext>, Vec<String>)> {
        self.inserts.clear();
        self.tx_contexts.clear();
        self.bigmap_map.clear();
        self.bigmap_keyhashes.clear();

        let storages: Vec<(TxContext, parser::Value)> =
            block.map_tx_contexts(|tx_context, is_origination, op_res| {
                if tx_context.contract != contract_id {
                    return Ok(None);
                }

                if is_origination {
                    let storage = parser::parse_json(
                        &self.node_cli.get_contract_storage(
                            contract_id,
                            tx_context.level,
                        )?,
                    )?;
                    Ok(Some((self.tx_context(tx_context), storage)))
                } else if let Some(storage) = &op_res.storage {
                    Ok(Some((
                        self.tx_context(tx_context),
                        parser::parse_lexed(&serde2json!(storage))?,
                    )))
                } else {
                    Err(anyhow!(
                "bad contract call: no storage update. tx_context={:#?}",
                tx_context
            ))
                }
            })?;

        let mut bigmap_contract_deps: HashMap<String, ()> = HashMap::new();
        for (tx_context, parsed_storage) in &storages {
            let tx_context = &self.tx_context(tx_context.clone());
            self.process_storage_value(parsed_storage, rel_ast, tx_context)
                .with_context(|| {
                    format!(
                        "process_block: process storage value failed (tx_context={:?})",
                        tx_context
                    )
                })?;

            let mut bigmaps = diffs.get_tx_context_owned_bigmaps(tx_context);

            bigmaps.sort_unstable();
            for bigmap in bigmaps {
                let (deps, ops) = diffs.normalized_diffs(bigmap, tx_context);
                if self.bigmap_map.contains_key(&bigmap) {
                    for (src_bigmap, src_context) in deps {
                        bigmap_contract_deps
                            .insert(src_context.contract.clone(), ());
                        self.process_bigmap_copy(
                            tx_context.clone(),
                            src_bigmap,
                            bigmap,
                        )?;
                    }
                }
                for op in &ops {
                    self.process_bigmap_op(op, tx_context)?;
                }
            }
        }

        Ok((
            self.inserts.clone(),
            self.tx_contexts
                .keys()
                .cloned()
                .collect(),
            bigmap_contract_deps
                .into_keys()
                .collect::<Vec<String>>(),
        ))
    }

    fn process_bigmap_copy(
        &mut self,
        mut ctx: TxContext,
        src_bigmap: i32,
        dest_bigmap: i32,
    ) -> Result<()> {
        ctx.internal_number = match ctx.internal_number {
            None => Some(-1),
            Some(x) => Some(x - 1),
        };
        let ctx = &self.tx_context(ctx);

        let at_level = ctx.level - 1;
        for (keyhash, key) in self
            .bigmap_keys
            .get(at_level, src_bigmap)?
        {
            let value = self
                .node_cli
                .get_bigmap_value(at_level, src_bigmap, &keyhash)?;
            if value.is_none() {
                continue;
            }

            let op = bigmap::Op::Update {
                bigmap: dest_bigmap,
                keyhash,
                key: serde_json::from_str(&key)?,
                value: serde_json::from_str(&value.unwrap().to_string())?,
            };
            self.process_bigmap_op(&op, ctx)?;
        }
        Ok(())
    }

    pub(crate) fn get_id_value(&self) -> i64 {
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
        ctx: &ProcessStorageContext,
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
            parser::Value::Pair { .. } | parser::Value::List { .. } => {
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
                        value_ast
                    },
                    {
                        self.add_bigmap_keyhash(
                            tx_context.clone(),
                            *bigmap,
                            keyhash.clone(),
                            key.to_string(),
                        );

                        let ctx = &ProcessStorageContext::new(
                            self.id_generator.get_id(),
                        )
                        .with_last_table(table.clone());
                        self.process_storage_value_internal(
                            ctx,
                            &parser::parse_lexed(&serde2json!(&key))?,
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
                            Some(val) => self.process_storage_value_internal(
                                ctx,
                                &parser::parse_lexed(&serde2json!(&val))?,
                                &value_ast,
                                tx_context,
                            )?,
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
            bigmap::Op::Clear { bigmap } => {
                let ctx =
                    &ProcessStorageContext::new(self.id_generator.get_id());
                self.sql_add_cell(
                    ctx,
                    &"bigmap_clears".to_string(),
                    &"bigmap_id".to_string(),
                    insert::Value::Int(*bigmap),
                    tx_context,
                );
                Ok(())
            }
            _ => Ok(()),
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
        self.process_storage_value_internal(
            &ctx.with_last_table("storage".to_string()),
            &value,
            rel_ast,
            tx_context,
        )?;
        Ok(())
    }

    fn update_context(
        &mut self,
        ctx: &ProcessStorageContext,
        current_table: Option<String>,
        tx_context: &TxContext,
    ) -> ProcessStorageContext {
        if let Some(table_name) = current_table {
            if ctx.last_table != Some(table_name.clone()) {
                if let Some(last_table) = &ctx.last_table {
                    self.sql_touch_insert(&ctx.clone(), last_table, tx_context);
                }

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
                    self.process_storage_value_internal(
                        ctx, key, key_ast, tx_context,
                    )?;
                    self.process_storage_value_internal(
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
                    self.process_storage_value_internal(
                        ctx, key, key_ast, tx_context,
                    )?;
                    self.process_storage_value_internal(
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
                        let ctx = &self.update_context(
                            ctx,
                            Some(left_table.clone()),
                            tx_context,
                        );
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
                        let ctx = &self.update_context(
                            ctx,
                            Some(right_table.clone()),
                            tx_context,
                        );
                        self.process_storage_value_internal(
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
            .or(must_match_rel!(rel_ast, RelationalAST::Map { .. }, {
                for element in l {
                    let id = self.id_generator.get_id();
                    self.process_storage_value_internal(
                        &ctx.with_id(id),
                        element,
                        rel_ast,
                        tx_context,
                    )?;
                }
                Ok(())
            }))
            .or(must_match_rel!(
                rel_ast,
                RelationalAST::BigMap { .. },
                {
                    for element in l {
                        let id = self.id_generator.get_id();
                        self.process_storage_value_internal(
                            &ctx.with_id(id),
                            element,
                            rel_ast,
                            tx_context,
                        )?;
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
            SimpleExprTy::Timestamp => {
                Ok(insert::Value::Timestamp(parser::parse_date(v)?))
            }
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
                parser::Value::Unit(None) => Ok(insert::Value::Null),
                parser::Value::Unit(Some(u)) => {
                    Ok(insert::Value::String(u.clone()))
                }
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
        let name = match column_name {
            "id" => ".id".to_string(),
            "tx_context_id" => ".tx_context_id".to_string(),
            s => s.to_string(),
        };
        insert
            .columns
            .push(Column { name, value });

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
}
