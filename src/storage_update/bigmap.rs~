use std::collections::HashMap;

use crate::octez::block::{Block, KeyType, ValueType};
use crate::storage_update::processor::TxContext;

#[derive(Clone, Debug, PartialEq)]
enum Op {
    Update {
        bigmap: i32,
        key: KeyType,
        value: ValueType,
    },
    Delete {
        bigmap: i32,
        key: KeyType,
    },
    Clear {
        bigmap: i32,
    },
    Copy {
        bigmap: i32,
        source: i32,
    },
}

impl Op {
    pub fn get_bigmap(&self) -> i32 {
        match self {
            Op::Update { bigmap, .. } => *bigmap,
            Op::Delete { bigmap, .. } => *bigmap,
            Op::Clear { bigmap, .. } => *bigmap,
            Op::Copy { bigmap, .. } => *bigmap,
        }
    }

    pub fn set_bigmap(&mut self, id: i32) {
        match self {
            Op::Update { bigmap, .. } => *bigmap = id,
            Op::Delete { bigmap, .. } => *bigmap = id,
            Op::Clear { bigmap } => *bigmap = id,
            Op::Copy { bigmap, .. } => *bigmap = id,
        }
    }
}

struct BigmapDiffsProcessor {
    tx_bigmap_ops: HashMap<TxContext, Vec<Op>>,
}

impl BigmapDiffsProcessor {
    /*
    pub(crate) fn from_block(b: &Block) -> Self {
        for (operation_group_number, operation_group) in
            operations.iter().enumerate()
        {
            for (operation_number, operation) in
                operation_group.iter().enumerate()
            {}
        }
    }
    */

    #[cfg(test)]
    pub fn from_testlist(l: &[(TxContext, Vec<Op>)]) -> Self {
        let mut res = Self {
            tx_bigmap_ops: HashMap::new(),
        };
        for (tx_context, ops) in l {
            res.tx_bigmap_ops
                .insert(tx_context.clone(), ops.clone());
        }
        res
    }

    pub(crate) fn normalize_diffs(
        &self,
        bigmap_target: i32,
        at: &TxContext,
    ) -> (Vec<i32>, Vec<Op>) {
        let (deps, mut res) = self.normalized_diffs_at(bigmap_target, at);

        res.reverse();
        (deps, res)
    }

    fn normalized_diffs_at(
        &self,
        bigmap_target: i32,
        at: &TxContext,
    ) -> (Vec<i32>, Vec<Op>) {
        let mut deps: Vec<i32> = vec![];
        let mut normalized: Vec<Op> = vec![];

        let mut targets: HashMap<i32, ()> = HashMap::new();
        targets.insert(bigmap_target, ());

        for op in self.tx_bigmap_ops[at].iter().rev() {
            if !targets.contains_key(&op.get_bigmap()) {
                continue;
            }
            match op {
                Op::Update { .. } | Op::Delete { .. } => {
                    let mut op_: Op = op.clone();
                    op_.set_bigmap(bigmap_target);
                    normalized.push(op_);
                }
                Op::Copy { source, .. } => {
                    let (deps_, normalized_) =
                        self.normalized_diffs_until(*source, at);

                    if deps_.is_empty() {
                        deps.push(*source);
                    } else {
                        deps.extend(deps_);
                    }
                    normalized.extend(normalized_);

                    targets.insert(*source, ());
                }
                Op::Clear { bigmap } => {
                    if *bigmap != bigmap_target {
                        // Probably does not happen, but just to be sure this branch is included.
                        // If it does happen, we don't want to pick up updates from before the Clear, so
                        // stop recursing here for this dependent bigmap
                        targets.remove(bigmap);
                    } else {
                        normalized.push(op.clone());
                    }
                }
            };
        }
        (deps, normalized)
    }

    fn normalized_diffs_until(
        &self,
        bigmap_target: i32,
        until: &TxContext,
    ) -> (Vec<i32>, Vec<Op>) {
        let mut normalized: Vec<Op> = vec![];
        let mut deps: Vec<i32> = vec![];

        let mut keys: Vec<&TxContext> = self
            .tx_bigmap_ops
            .keys()
            .filter(|k| *k < until)
            .collect();
        keys.sort();
        keys.reverse();

        for tx_context in keys {
            let (deps_, normalized_) =
                self.normalized_diffs_at(bigmap_target, tx_context);
            deps.extend(deps_);
            normalized.extend(normalized_);
        }
        (deps, normalized)
    }
}

#[test]
fn test_normalizer() {
    fn tx_context() -> TxContext {
        TxContext {
            level: 0,
            operation_group_number: 0,
            operation_number: 0,
            content_number: 0,
            internal_number: None,
            contract: "".to_string(),
            operation_hash: "".to_string(),
            source: None,
            destination: None,
            entrypoint: None,
            id: None,
        }
    }
    struct TestCase {
        name: String,

        tx_bigmap_ops: Vec<(TxContext, Vec<Op>)>,
        normalize_tx_idx: usize,
        normalize_bigmap: i32,

        exp_deps: Vec<i32>,
        exp_ops: Vec<Op>,
    }
    let testcases: Vec<TestCase> = vec![
        TestCase {
            name: "simple".to_string(),

            tx_bigmap_ops: vec![(tx_context(), vec![Op::Clear { bigmap: 0 }])],
            normalize_tx_idx: 0,
            normalize_bigmap: 0,

            exp_deps: vec![],
            exp_ops: vec![Op::Clear { bigmap: 0 }],
        },
        TestCase {
            name: "empty".to_string(),

            tx_bigmap_ops: vec![(tx_context(), vec![])],
            normalize_tx_idx: 0,
            normalize_bigmap: 10,

            exp_deps: vec![],
            exp_ops: vec![],
        },
    ];
    for tc in testcases {
        let (got_deps, got_ops) =
            BigmapDiffsProcessor::from_testlist(&tc.tx_bigmap_ops)
                .normalize_diffs(
                    tc.normalize_bigmap,
                    &tc.tx_bigmap_ops[tc.normalize_tx_idx].0,
                );
        assert_eq!(tc.exp_deps, got_deps);
        assert_eq!(tc.exp_ops, got_ops);
    }
}
