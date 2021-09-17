use std::collections::HashMap;

#[cfg(test)]
use pretty_assertions::assert_eq;

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

    pub(crate) fn normalized_diffs(
        &self,
        bigmap_target: i32,
        at: &TxContext,
    ) -> (Vec<i32>, Vec<Op>) {
        let mut deps: Vec<i32> = vec![];
        let mut res: Vec<Op> = vec![];

        let mut keys: Vec<&TxContext> = self
            .tx_bigmap_ops
            .keys()
            .filter(|k| *k <= at)
            .collect();
        keys.sort();
        keys.reverse();
        println!("keys: {:?}", keys);

        let mut targets: Vec<i32> = vec![bigmap_target];
        for tx_context in keys {
            if targets.is_empty() {
                break;
            }
            for op in self.tx_bigmap_ops[tx_context]
                .iter()
                .rev()
            {
                let mut clear_targets = vec![];
                for target in targets.clone() {
                    if op.get_bigmap() != target {
                        continue;
                    }
                    match op {
                        Op::Update { .. } | Op::Delete { .. } => {
                            let mut op_: Op = op.clone();
                            op_.set_bigmap(bigmap_target);
                            res.push(op_);
                        }
                        Op::Copy { source, .. } => {
                            deps.push(*source);

                            targets.push(*source);
                        }
                        Op::Clear { bigmap } => {
                            // Probably does not happen, but just to be sure this branch is included.
                            // If it does happen, we don't want to pick up updates from before the Clear, so
                            // stop recursing here for this dependent bigmap
                            clear_targets.push(*bigmap);
                        }
                    };
                }
                targets = targets
                    .iter()
                    .filter(|k| {
                        !clear_targets
                            .iter()
                            .any(|clear| *k == clear)
                    })
                    .copied()
                    .collect();
            }
            targets = targets
                .iter()
                .filter(|bigmap| **bigmap != bigmap_target)
                .copied()
                .collect();
        }

        res.reverse();
        (deps, res)
    }

    fn normalized_diffs_recursive(
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
                    deps.push(*source);
                    let (deps_, mut normalized_) =
                        self.normalized_diffs_until(*source, at);
                    for op in normalized_.iter_mut() {
                        op.set_bigmap(bigmap_target);
                    }

                    deps.extend(deps_);
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
                    deps.push(*source);
                    let (deps_, mut normalized_) =
                        self.normalized_diffs_until(*source, at);
                    for op in normalized_.iter_mut() {
                        op.set_bigmap(bigmap_target);
                    }

                    deps.extend(deps_);
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
    fn tx_context(level: u32) -> TxContext {
        TxContext {
            level,
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
    fn op_update(bigmap: i32, ident: i32) -> Op {
        Op::Update {
            bigmap,
            key: KeyType {
                prim: Some(format!("{}", ident)),
            },
            value: ValueType {
                prim: None,
                args: None,
            },
        }
    }
    struct TestCase {
        name: String,

        tx_bigmap_ops: Vec<(TxContext, Vec<Op>)>,
        normalize_tx_context: TxContext,
        normalize_bigmap: i32,

        exp_deps: Vec<i32>,
        exp_ops: Vec<Op>,
    }
    let testcases: Vec<TestCase> = vec![
        TestCase {
            name: "basic".to_string(),

            tx_bigmap_ops: vec![(
                tx_context(1),
                vec![op_update(0, 1), op_update(1, 1)],
            )],
            normalize_tx_context: tx_context(1),
            normalize_bigmap: 0,

            exp_deps: vec![],
            exp_ops: vec![op_update(0, 1)],
        },
        TestCase {
            name: "empty".to_string(),

            tx_bigmap_ops: vec![(tx_context(1), vec![])],
            normalize_tx_context: tx_context(1),
            normalize_bigmap: 10,

            exp_deps: vec![],
            exp_ops: vec![],
        },
        TestCase {
            name: "basic copy (and updates after the copy are omitted)"
                .to_string(),

            tx_bigmap_ops: vec![(
                tx_context(1),
                vec![
                    op_update(10, 1),
                    op_update(10, 2),
                    Op::Copy {
                        bigmap: 0,
                        source: 10,
                    },
                    op_update(10, 3),
                ],
            )],
            normalize_tx_context: tx_context(1),
            normalize_bigmap: 0,

            exp_deps: vec![10],
            exp_ops: vec![op_update(0, 1), op_update(0, 2)],
        },
        TestCase {
            name: "nested copy".to_string(),

            tx_bigmap_ops: vec![(
                tx_context(1),
                vec![
                    op_update(10, 1),
                    Op::Copy {
                        bigmap: 5,
                        source: 10,
                    },
                    op_update(5, 2),
                    Op::Copy {
                        bigmap: 0,
                        source: 5,
                    },
                ],
            )],
            normalize_tx_context: tx_context(1),
            normalize_bigmap: 0,

            exp_deps: vec![5, 10],
            exp_ops: vec![op_update(0, 1), op_update(0, 2)],
        },
        TestCase {
            name: "copy takes from prior tx_contexts too, target does not"
                .to_string(),

            tx_bigmap_ops: vec![
                (
                    tx_context(1),
                    vec![
                        op_update(5, 1),  // should be included
                        op_update(5, 2),  // should be included
                        op_update(0, 10), // should be omitted
                    ],
                ),
                (
                    tx_context(2),
                    vec![
                        op_update(5, 3),
                        Op::Copy {
                            bigmap: 0,
                            source: 5,
                        },
                        op_update(0, 4),
                    ],
                ),
                (
                    tx_context(3),
                    vec![
                        op_update(0, 5), // should be omitted (later tx)
                        op_update(5, 4), // should be omitted (later tx)
                    ],
                ),
            ],
            normalize_tx_context: tx_context(2),
            normalize_bigmap: 0,

            exp_deps: vec![5],
            exp_ops: vec![
                op_update(0, 1),
                op_update(0, 2),
                op_update(0, 3),
                op_update(0, 4),
            ],
        },
        // what follows are test cases about scenarios that probably are impossible in the Tezos blockchain context, but just in case testing that we deal with them in a sensible way:
        TestCase {
            name: "copy: updates before a clear are omitted".to_string(),

            tx_bigmap_ops: vec![
                (tx_context(1), vec![op_update(10, 0)]),
                (
                    tx_context(2),
                    vec![
                        op_update(10, 1),
                        op_update(10, 2),
                        Op::Clear { bigmap: 10 },
                        op_update(10, 3),
                        op_update(10, 4),
                        Op::Copy {
                            bigmap: 0,
                            source: 10,
                        },
                    ],
                ),
            ],
            normalize_tx_context: tx_context(2),
            normalize_bigmap: 0,

            exp_deps: vec![10],
            exp_ops: vec![op_update(0, 3), op_update(0, 4)],
        },
        TestCase {
            name: "update before a clear of target bitmap are omitted"
                .to_string(),

            tx_bigmap_ops: vec![(
                tx_context(1),
                vec![
                    op_update(0, 1),
                    op_update(0, 2),
                    Op::Clear { bigmap: 0 },
                    op_update(0, 3),
                    op_update(0, 4),
                ],
            )],
            normalize_tx_context: tx_context(2),
            normalize_bigmap: 0,

            exp_deps: vec![],
            exp_ops: vec![op_update(0, 3), op_update(0, 4)],
        },
    ];
    for tc in testcases {
        println!("test case: {}", tc.name);
        let (got_deps, got_ops) =
            BigmapDiffsProcessor::from_testlist(&tc.tx_bigmap_ops)
                .normalized_diffs(
                    tc.normalize_bigmap,
                    &tc.normalize_tx_context,
                );
        assert_eq!(tc.exp_deps, got_deps);
        assert_eq!(tc.exp_ops, got_ops);
    }
}
