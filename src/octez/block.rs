use crate::itertools::Itertools;
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::contract_denylist::is_contract_denylisted;

const LIQUIDITY_BAKING_LEVEL: u32 = 1589247;
const LIQUIDITY_BAKING: &str = "KT1TxqZ8QtKvLu3V3JH7Gx58n7Co8pgtpQU5";
const LIQUIDITY_BAKING_TOKEN: &str = "KT1AafHA1C1vk959wvHWBispY9Y2f3fxBUUo";

pub(crate) fn get_implicit_origination_level(contract: &str) -> Option<u32> {
    if contract == LIQUIDITY_BAKING || contract == LIQUIDITY_BAKING_TOKEN {
        return Some(LIQUIDITY_BAKING_LEVEL);
    }
    None
}

#[derive(Clone, Debug)]

pub struct LevelMeta {
    pub level: u32,
    pub hash: Option<String>,
    pub prev_hash: Option<String>,
    pub baked_at: Option<DateTime<Utc>>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Block {
    pub hash: String,
    pub header: Header,
    pub operations: Vec<Vec<Operation>>,

    #[serde(skip)]
    protocol: String,
    #[serde(skip)]
    chain_id: String,
    #[serde(skip)]
    metadata: Metadata,
}

#[derive(Clone, Debug)]
pub(crate) struct TxContext {
    pub id: Option<i64>,
    pub level: u32,
    pub contract: String,
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
        self.contract.hash(state);
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
            && self.contract == other.contract
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
impl PartialOrd for TxContext {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let res = self.level.cmp(&other.level);
        if res != Ordering::Equal {
            return Some(res);
        }
        let res = self
            .operation_group_number
            .cmp(&other.operation_group_number);
        if res != Ordering::Equal {
            return Some(res);
        }
        let res = self
            .operation_number
            .cmp(&other.operation_number);
        if res != Ordering::Equal {
            return Some(res);
        }
        let res = self
            .content_number
            .cmp(&other.content_number);
        if res != Ordering::Equal {
            return Some(res);
        }
        let res = self
            .internal_number
            .cmp(&other.internal_number);
        if res != Ordering::Equal {
            return Some(res);
        }
        Some(Ordering::Equal)
    }
}

impl Eq for TxContext {}
impl Ord for TxContext {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Block {
    pub(crate) fn operations(&self) -> Vec<Vec<Operation>> {
        self.operations.clone()
    }

    pub(crate) fn map_tx_contexts<F, O>(
        &self,
        mut f: F,
    ) -> anyhow::Result<Vec<O>>
    where
        F: FnMut(
            TxContext,
            bool,
            &OperationResult,
        ) -> anyhow::Result<Option<O>>,
    {
        let mut res: Vec<O> = vec![];
        for (operation_group_number, operation_group) in
            self.operations().iter().enumerate()
        {
            for (operation_number, operation) in
                operation_group.iter().enumerate()
            {
                for (content_number, content) in
                    operation.contents.iter().enumerate()
                {
                    if let Some(operation_result) =
                        &content.metadata.operation_result
                    {
                        if operation_result.status != "applied" {
                            continue;
                        }
                        if let Some(dest_addr) = &content.destination {
                            if is_contract(dest_addr) {
                                let fres = f(
                                    TxContext {
                                        id: None,
                                        level: self.header.level,
                                        contract: dest_addr.clone(),
                                        operation_hash: operation.hash.clone(),
                                        operation_group_number,
                                        operation_number,
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
                                    },
                                    false,
                                    operation_result,
                                )?;
                                if let Some(elem) = fres {
                                    res.push(elem);
                                }

                                for (internal_number, internal_op) in content
                                    .metadata
                                    .internal_operation_results
                                    .iter()
                                    .enumerate()
                                {
                                    if internal_op.result.status != "applied" {
                                        continue;
                                    }
                                    if let Some(internal_dest_addr) =
                                        &internal_op.destination
                                    {
                                        if is_contract(internal_dest_addr) {
                                            let fres = f(
                                                TxContext {
                                                    id: None,
                                                    level: self.header.level,
                                                    contract:
                                                        internal_dest_addr
                                                            .to_string(),
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
                                                        internal_op
                                                            .source
                                                            .clone(),
                                                    ),
                                                    destination: internal_op
                                                        .destination
                                                        .clone(),
                                                    entrypoint: internal_op
                                                        .parameters
                                                        .clone()
                                                        .map(|p| p.entrypoint),
                                                },
                                                false,
                                                &internal_op.result,
                                            )?;
                                            if let Some(elem) = fres {
                                                res.push(elem);
                                            }
                                        }
                                    }

                                    for contract in
                                        &internal_op.result.originated_contracts
                                    {
                                        let fres = f(
                                            TxContext {
                                                id: None,
                                                level: self.header.level,
                                                contract: contract.clone(),
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
                                            },
                                            true,
                                            &internal_op.result,
                                        )?;
                                        if let Some(elem) = fres {
                                            res.push(elem);
                                        }
                                    }
                                }
                            }
                        }

                        for contract in &operation_result.originated_contracts {
                            let fres = f(
                                TxContext {
                                    id: None,
                                    level: self.header.level,
                                    contract: contract.clone(),
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
                                },
                                true,
                                operation_result,
                            )?;
                            if let Some(elem) = fres {
                                res.push(elem);
                            }
                        }
                    }
                }
            }
        }
        Ok(res)
    }

    pub(crate) fn is_contract_active(&self, contract_address: &str) -> bool {
        if is_contract_denylisted(contract_address) {
            return false;
        }

        let destination = Some(contract_address.to_string());
        for operations in &self.operations {
            for operation in operations {
                for content in &operation.contents {
                    if let Some(operation_result) =
                        &content.metadata.operation_result
                    {
                        if operation_result.status != "applied" {
                            continue;
                        }
                        if content.destination == destination {
                            return true;
                        }
                        for result in &content
                            .metadata
                            .internal_operation_results
                        {
                            if result.destination == destination {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        is_implicit_active(self.header.level, contract_address)
    }

    pub(crate) fn has_contract_origination(
        &self,
        contract_address: &str,
    ) -> bool {
        if self.header.level == 1589247
            && (contract_address == LIQUIDITY_BAKING
                || contract_address == LIQUIDITY_BAKING_TOKEN)
        {
            return true;
        }

        self.contract_originations()
            .iter()
            .any(|c| c == contract_address)
    }

    fn contract_originations(&self) -> Vec<String> {
        self.map_tx_contexts(|tx_context, is_origination, _op_res| {
            if !is_origination {
                return Ok(None);
            }
            Ok(Some(tx_context.contract))
        })
        .unwrap()
    }

    pub(crate) fn active_contracts(&self) -> Vec<String> {
        let mut res: Vec<String> = self
            .map_tx_contexts(|tx_context, _is_origination, _op_res| {
                Ok(Some(tx_context.contract))
            })
            .unwrap();
        if self.header.level == LIQUIDITY_BAKING_LEVEL {
            res.push(LIQUIDITY_BAKING.to_string());
            res.push(LIQUIDITY_BAKING_TOKEN.to_string());
        }
        res.iter()
            .filter(|address| is_contract(address))
            .unique()
            .cloned()
            .collect()
    }
}

fn is_implicit_active(level: u32, contract_address: &str) -> bool {
    // liquidity baking has 2 implicit contract creation events in the block prior to Granada's activation block
    level == LIQUIDITY_BAKING_LEVEL
        && (contract_address == LIQUIDITY_BAKING
            || contract_address == LIQUIDITY_BAKING_TOKEN)
}

fn is_contract(address: &str) -> bool {
    address.starts_with("KT1") && !is_contract_denylisted(address)
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Header {
    pub level: u32,
    pub predecessor: String,

    #[serde(skip)]
    timestamp: String,
    #[serde(skip)]
    validation_pass: i64,
    #[serde(skip)]
    operations_hash: String,
    #[serde(skip)]
    fitness: Vec<String>,
    #[serde(skip)]
    context: String,
    #[serde(skip)]
    priority: i64,
    #[serde(skip)]
    proof_of_work_nonce: String,
    #[serde(skip)]
    signature: String,
    #[serde(skip)]
    proto: i64,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Metadata {
    pub protocol: String,
    pub next_protocol: String,
    pub test_chain_status: TestChainStatus,
    pub max_operations_ttl: i64,
    pub max_operation_data_length: i64,
    pub max_block_header_length: i64,
    pub max_operation_list_length: Vec<MaxOperationListLength>,
    pub baker: String,
    pub level_info: LevelInfo,
    pub voting_period_info: VotingPeriodInfo,
    pub nonce_hash: ::serde_json::Value,
    pub consumed_gas: Option<String>,
    pub deactivated: Vec<::serde_json::Value>,
    pub balance_updates: Option<Vec<BalanceUpdate>>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct TestChainStatus {
    pub status: String,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct MaxOperationListLength {
    #[serde(rename = "max_size")]
    pub max_size: i64,
    #[serde(rename = "max_op")]
    pub max_op: Option<i64>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct Level {
    pub level: i64,
    #[serde(rename = "level_position")]
    pub level_position: i64,
    pub cycle: i64,
    #[serde(rename = "cycle_position")]
    pub cycle_position: i64,
    #[serde(rename = "voting_period")]
    pub voting_period: i64,
    #[serde(rename = "voting_period_position")]
    pub voting_period_position: i64,
    #[serde(rename = "expected_commitment")]
    pub expected_commitment: bool,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct LevelInfo {
    pub level: i64,
    #[serde(rename = "level_position")]
    pub level_position: i64,
    pub cycle: i64,
    #[serde(rename = "cycle_position")]
    pub cycle_position: i64,
    #[serde(rename = "expected_commitment")]
    pub expected_commitment: bool,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct VotingPeriodInfo {
    #[serde(rename = "voting_period")]
    pub voting_period: VotingPeriod,
    pub position: i64,
    pub remaining: i64,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct VotingPeriod {
    pub index: i64,
    pub kind: String,
    #[serde(rename = "start_position")]
    pub start_position: i64,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct BalanceUpdate {
    pub kind: String,
    pub contract: Option<String>,
    pub change: String,
    pub origin: Option<String>,
    pub category: Option<String>,
    pub delegate: Option<String>,
    pub cycle: Option<i64>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Operation {
    pub hash: String,
    pub contents: Vec<Content>,

    #[serde(skip)]
    protocol: String,
    #[serde(skip)]
    signature: Option<String>,
    #[serde(skip)]
    chain_id: String,
    #[serde(skip)]
    branch: String,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Content {
    pub slot: Option<i64>,
    pub metadata: OperationMetadata,
    pub destination: Option<String>,
    pub source: Option<String>,
    pub parameters: Option<Parameters>,

    #[serde(skip)]
    kind: String,
    #[serde(skip)]
    endorsement: Option<Endorsement>,
    #[serde(skip)]
    fee: Option<String>,
    #[serde(skip)]
    counter: Option<String>,
    #[serde(skip)]
    gas_limit: Option<String>,
    #[serde(skip)]
    storage_limit: Option<String>,
    #[serde(skip)]
    amount: Option<String>,
    #[serde(skip)]
    balance: Option<String>,
    #[serde(skip)]
    script: Option<Script>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct Endorsement {
    pub branch: String,
    pub operations: Operations,
    pub signature: String,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Operations {
    pub kind: String,
    pub level: i64,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct OperationMetadata {
    pub operation_result: Option<OperationResult>,
    #[serde(default)]
    pub internal_operation_results: Vec<InternalOperationResult>,

    #[serde(skip)]
    delegate: Option<String>,
    #[serde(skip)]
    balance_updates: Vec<BalanceUpdate>,
    #[serde(skip)]
    slots: Vec<i64>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct OperationResult {
    #[serde(default)]
    pub originated_contracts: Vec<String>,
    pub status: String,
    pub storage: Option<::serde_json::Value>,
    pub big_map_diff: Option<Vec<BigMapDiff>>,

    #[serde(skip)]
    balance_updates: Option<Vec<BalanceUpdate>>,
    #[serde(skip)]
    consumed_gas: Option<String>,
    #[serde(skip)]
    consumed_milligas: Option<String>,
    #[serde(skip)]
    storage_size: Option<String>,
    #[serde(skip)]
    paid_storage_size_diff: Option<String>,
    #[serde(skip)]
    lazy_storage_diff: Option<Vec<serde_json::Value>>,
    //    pub lazy_storage_diff: Option<Vec<LazyStorageDiff>>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct BigMapDiff {
    pub action: String,
    pub big_map: Option<String>,
    pub source_big_map: Option<String>,
    pub destination_big_map: Option<String>,
    pub key: Option<serde_json::Value>,
    pub value: Option<serde_json::Value>,

    #[serde(skip)]
    key_hash: Option<String>,
    #[serde(skip)]
    key_type: Option<KeyType>,
    #[serde(skip)]
    value_type: Option<ValueType>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Key {
    pub string: Option<String>,
    pub prim: Option<String>,
    pub args: Option<Vec<Arg>>,
    pub int: Option<String>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Arg {
    pub prim: Option<String>,
    pub bytes: Option<String>,
    pub int: Option<String>,
    #[serde(default)]
    pub args: Option<Vec<Arg>>,
    pub annots: Option<Vec<String>>,
    pub string: Option<String>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Value {
    pub string: Option<String>,
    pub prim: Option<String>,
    #[serde(default)]
    pub bytes: Option<String>,
    pub args: Vec<::serde_json::Value>,
    pub int: Option<String>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct KeyType {
    pub prim: Option<String>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct ValueType {
    pub prim: Option<String>,
    pub args: Option<Vec<Arg>>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct LazyStorageDiff {
    pub kind: String,
    pub id: String,
    pub diff: Diff,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Diff {
    pub action: String,
    pub updates: Vec<Update>,
    pub key_type: Option<KeyType>,
    pub value_type: Option<ValueType2>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Update {
    pub key_hash: String,
    pub key: serde_json::Value,
    pub value: Option<serde_json::Value>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct ValueType2 {
    pub prim: Option<String>,
    pub args: Option<Vec<Arg>>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct InternalOperationResult {
    pub kind: String,
    pub source: String,
    pub nonce: i64,
    pub amount: Option<String>,
    pub destination: Option<String>,
    pub parameters: Option<Parameters>,
    pub result: OperationResult,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Parameters {
    #[serde(default)]
    pub entrypoint: String,

    #[serde(skip)]
    value: Option<serde_json::Value>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Result {
    pub status: String,
    pub storage: Option<::serde_json::Value>,
    pub big_map_diff: Option<Vec<BigMapDiff>>,

    #[serde(skip)]
    balance_updates: Option<Vec<BalanceUpdate>>,
    #[serde(skip)]
    consumed_gas: Option<String>,
    #[serde(skip)]
    consumed_milligas: Option<String>,
    #[serde(skip)]
    storage_size: Option<String>,
    #[serde(skip)]
    paid_storage_size_diff: Option<String>,
    #[serde(skip)]
    lazy_storage_diff: Option<Vec<serde_json::Value>>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Storage {
    pub prim: Option<String>,
    pub args: Vec<Vec<::serde_json::Value>>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Script {
    pub code: Vec<Code>,
    pub storage: serde_json::Value,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct Code {
    pub prim: Option<String>,
    pub args: Option<Vec<::serde_json::Value>>,
}
