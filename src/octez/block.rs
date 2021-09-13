use crate::itertools::Itertools;
use chrono::{DateTime, Utc};

use crate::contract_denylist::is_contract_denylisted;

const LIQUIDITY_BAKING_LEVEL: u32 = 1589247;
const LIQUIDITY_BAKING: &str = "KT1TxqZ8QtKvLu3V3JH7Gx58n7Co8pgtpQU5";
const LIQUIDITY_BAKING_TOKEN: &str = "KT1AafHA1C1vk959wvHWBispY9Y2f3fxBUUo";

#[derive(Clone, Debug)]
pub struct LevelMeta {
    pub level: u32,
    pub hash: Option<String>,
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
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub protocol: String,
    #[serde(rename = "chain_id")]
    pub chain_id: String,
    pub hash: String,
    pub header: Header,
    pub metadata: Metadata,
    pub operations: Vec<Vec<Operation>>,
}

impl Block {
    pub(crate) fn operations(&self) -> Vec<Vec<Operation>> {
        self.operations.clone()
    }

    pub(crate) fn is_contract_active(&self, contract_address: &str) -> bool {
        if is_contract_denylisted(contract_address) {
            return false;
        }

        let destination = Some(contract_address.to_string());
        for operations in &self.operations {
            for operation in operations {
                for content in &operation.contents {
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
        is_implicit_active(self.header.level, contract_address)
    }

    pub(crate) fn has_contract_origination(
        &self,
        contract_address: &str,
    ) -> bool {
        // liquidity baking has an implicit contract creation event in this block
        let liquidity_baking =
            "KT1TxqZ8QtKvLu3V3JH7Gx58n7Co8pgtpQU5".to_string();
        let liquidity_baking_token =
            "KT1AafHA1C1vk959wvHWBispY9Y2f3fxBUUo".to_string();
        if self.header.level == 1589247
            && (contract_address == liquidity_baking
                || contract_address == liquidity_baking_token)
        {
            return true;
        }

        self.operations.iter().any(|ops| {
            ops.iter().any(|op| {
                op.contents.iter().any(|content| {
                    content
                        .metadata
                        .operation_result
                        .iter()
                        .any(|op_res| {
                            op_res
                                .originated_contracts
                                .iter()
                                .any(|c| c == contract_address)
                        })
                })
            })
        })
    }

    pub(crate) fn active_contracts(&self) -> Vec<String> {
        let mut res: Vec<String> = vec![];
        for operations in &self.operations {
            for operation in operations {
                for content in &operation.contents {
                    if let Some(address) = &content.destination {
                        res.push(address.clone());
                    }
                    for result in &content
                        .metadata
                        .internal_operation_results
                    {
                        if let Some(address) = &result.destination {
                            res.push(address.clone())
                        }
                    }
                }
            }
        }
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
#[serde(rename_all = "camelCase")]
pub struct Header {
    pub level: u32,
    pub proto: i64,
    pub predecessor: String,
    pub timestamp: String,
    #[serde(rename = "validation_pass")]
    pub validation_pass: i64,
    #[serde(rename = "operations_hash")]
    pub operations_hash: String,
    pub fitness: Vec<String>,
    pub context: String,
    pub priority: i64,
    #[serde(rename = "proof_of_work_nonce")]
    pub proof_of_work_nonce: String,
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
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub protocol: String,
    #[serde(rename = "next_protocol")]
    pub next_protocol: String,
    #[serde(rename = "test_chain_status")]
    pub test_chain_status: TestChainStatus,
    #[serde(rename = "max_operations_ttl")]
    pub max_operations_ttl: i64,
    #[serde(rename = "max_operation_data_length")]
    pub max_operation_data_length: i64,
    #[serde(rename = "max_block_header_length")]
    pub max_block_header_length: i64,
    #[serde(rename = "max_operation_list_length")]
    pub max_operation_list_length: Vec<MaxOperationListLength>,
    pub baker: String,
    #[serde(rename = "level_info", default)]
    pub level_info: LevelInfo,
    #[serde(rename = "voting_period_info", default)]
    pub voting_period_info: VotingPeriodInfo,
    #[serde(rename = "nonce_hash")]
    pub nonce_hash: ::serde_json::Value,
    #[serde(rename = "consumed_gas")]
    pub consumed_gas: Option<String>,
    pub deactivated: Vec<::serde_json::Value>,
    #[serde(rename = "balance_updates")]
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
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub protocol: String,
    #[serde(rename = "chain_id")]
    pub chain_id: String,
    pub hash: String,
    pub branch: String,
    pub contents: Vec<Content>,
    pub signature: Option<String>,
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
pub struct Content {
    pub kind: String,
    pub endorsement: Option<Endorsement>,
    pub slot: Option<i64>,
    pub metadata: OperationMetadata,
    pub source: Option<String>,
    pub fee: Option<String>,
    pub counter: Option<String>,
    #[serde(rename = "gas_limit")]
    pub gas_limit: Option<String>,
    #[serde(rename = "storage_limit")]
    pub storage_limit: Option<String>,
    pub amount: Option<String>,
    pub destination: Option<String>,
    pub parameters: Option<Parameters>,
    pub balance: Option<String>,
    pub script: Option<Script>,
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct OperationMetadata {
    #[serde(rename = "balance_updates", default)]
    pub balance_updates: Vec<BalanceUpdate>,
    pub delegate: Option<String>,
    #[serde(default)]
    pub slots: Vec<i64>,
    #[serde(rename = "operation_result")]
    pub operation_result: Option<OperationResult>,
    #[serde(rename = "internal_operation_results")]
    #[serde(default)]
    pub internal_operation_results: Vec<InternalOperationResult>,
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
pub struct OperationResult {
    pub status: String,
    pub storage: Option<::serde_json::Value>,
    #[serde(rename = "big_map_diff")]
    pub big_map_diff: Option<Vec<BigMapDiff>>,
    #[serde(rename = "balance_updates")]
    #[serde(default)]
    pub balance_updates: Option<Vec<BalanceUpdate>>,
    #[serde(rename = "consumed_gas")]
    pub consumed_gas: Option<String>,
    #[serde(rename = "consumed_milligas")]
    pub consumed_milligas: Option<String>,
    #[serde(rename = "storage_size")]
    pub storage_size: Option<String>,
    #[serde(rename = "paid_storage_size_diff")]
    pub paid_storage_size_diff: Option<String>,
    #[serde(rename = "lazy_storage_diff")]
    pub lazy_storage_diff: Option<Vec<serde_json::Value>>,
    //    pub lazy_storage_diff: Option<Vec<LazyStorageDiff>>,
    #[serde(rename = "originated_contracts")]
    #[serde(default)]
    pub originated_contracts: Vec<String>,
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
pub struct BigMapDiff {
    pub action: String,
    #[serde(rename = "big_map")]
    pub big_map: Option<String>,
    #[serde(rename = "source_big_map")]
    pub source_big_map: Option<String>,
    #[serde(rename = "destination_big_map")]
    pub destination_big_map: Option<String>,
    #[serde(rename = "key_hash")]
    pub key_hash: Option<String>,
    pub key: Option<serde_json::Value>,
    pub value: Option<serde_json::Value>,
    #[serde(rename = "key_type")]
    pub key_type: Option<KeyType>,
    #[serde(rename = "value_type")]
    pub value_type: Option<ValueType>,
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct Diff {
    pub action: String,
    pub updates: Vec<Update>,
    #[serde(rename = "key_type")]
    pub key_type: Option<KeyType>,
    #[serde(rename = "value_type")]
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
#[serde(rename_all = "camelCase")]
pub struct Update {
    #[serde(rename = "key_hash")]
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct Parameters {
    #[serde(default)]
    pub entrypoint: String,
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
#[serde(rename_all = "camelCase")]
pub struct Result {
    pub status: String,
    pub storage: Option<::serde_json::Value>,
    #[serde(rename = "big_map_diff")]
    pub big_map_diff: Option<Vec<BigMapDiff>>,
    #[serde(rename = "balance_updates")]
    pub balance_updates: Option<Vec<BalanceUpdate>>,
    #[serde(rename = "consumed_gas")]
    pub consumed_gas: Option<String>,
    #[serde(rename = "consumed_milligas")]
    pub consumed_milligas: Option<String>,
    #[serde(rename = "storage_size")]
    pub storage_size: Option<String>,
    #[serde(rename = "paid_storage_size_diff")]
    pub paid_storage_size_diff: Option<String>,
    #[serde(rename = "lazy_storage_diff")]
    pub lazy_storage_diff: Option<Vec<serde_json::Value>>,
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct Code {
    pub prim: Option<String>,
    pub args: Option<Vec<::serde_json::Value>>,
}
