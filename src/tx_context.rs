use std::collections::HashMap;
use std::hash::{Hash, Hasher};

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
