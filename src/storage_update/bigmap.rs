use anyhow::{anyhow, Result};
use std::collections::HashMap;

#[cfg(test)]
use pretty_assertions::assert_eq;

use crate::octez::block::{BigMapDiff, Block, TxContext};

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Update {
        bigmap: i32,
        key: serde_json::Value,
        value: Option<serde_json::Value>, // if None: it means remove key in bigmap
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
            Op::Clear { bigmap, .. } => *bigmap,
            Op::Copy { bigmap, .. } => *bigmap,
        }
    }

    pub fn set_bigmap(&mut self, id: i32) {
        match self {
            Op::Update { bigmap, .. } => *bigmap = id,
            Op::Clear { bigmap } => *bigmap = id,
            Op::Copy { bigmap, .. } => *bigmap = id,
        }
    }

    pub fn from_raw(raw: &BigMapDiff) -> Result<Option<Self>> {
        match raw.action.as_str() {
            "update" => Ok(Some(Op::Update {
                bigmap: raw
                    .big_map
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("no big map id found in diff {:?}", raw)
                    })?
                    .parse()?,
                key: raw
                    .key
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!("no key in big map update {:?}", raw)
                    })?,
                value: raw.value.clone(),
            })),
            "copy" => Ok(Some(Op::Copy {
                bigmap: raw
                    .destination_big_map
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("no big map id found in diff {:?}", raw)
                    })?
                    .parse()?,
                source: raw
                    .source_big_map
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("no big map id found in diff {:?}", raw)
                    })?
                    .parse()?,
            })),
            "remove" => Ok(Some(Op::Clear {
                bigmap: raw
                    .big_map
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("no big map id found in diff {:?}", raw)
                    })?
                    .parse()?,
            })),
            "alloc" => Ok(None),
            _ => Err(anyhow!("unsupported bigmap diff action {}", raw.action)),
        }
    }
}

pub struct IntraBlockBigmapDiffsProcessor {
    tx_bigmap_ops: HashMap<TxContext, Vec<Op>>,
}

impl IntraBlockBigmapDiffsProcessor {
    pub(crate) fn from_block(block: &Block) -> Self {
        let mut res = Self {
            tx_bigmap_ops: HashMap::new(),
        };

        let tx_bigmap_ops = block
            .map_tx_contexts(|tx_context, _is_origination, op_res| {
                if op_res.big_map_diff.is_none() {
                    Ok(Some((tx_context, vec![])))
                } else {
                    let mut ops: Vec<Op> = vec![];
                    for op in op_res.big_map_diff.as_ref().unwrap() {
                        if let Some(op_parsed) = Op::from_raw(op)? {
                            ops.push(op_parsed);
                        }
                    }
                    println!("{:#?}: {:#?}", tx_context, ops);
                    Ok(Some((tx_context, ops)))
                }
            })
            .unwrap();
        for (tx_context, ops) in tx_bigmap_ops {
            res.tx_bigmap_ops
                .insert(tx_context, ops);
        }
        res
    }

    #[cfg(test)]
    fn from_testlist(l: &[(TxContext, Vec<Op>)]) -> Self {
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
    ) -> (Vec<(i32, TxContext)>, Vec<Op>) {
        let mut deps: Vec<(i32, TxContext)> = vec![];
        let mut res: Vec<Op> = vec![];

        let mut keys: Vec<&TxContext> = self
            .tx_bigmap_ops
            .keys()
            .filter(|k| *k <= at)
            .collect();
        keys.sort();
        keys.reverse();

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
                        Op::Update { .. } => {
                            let mut op_: Op = op.clone();
                            op_.set_bigmap(bigmap_target);
                            res.push(op_);
                        }
                        Op::Copy { source, bigmap } => {
                            deps.push((*source, tx_context.clone()));
                            deps = deps
                                .into_iter()
                                .filter(|(d, _)| d != bigmap)
                                .collect();

                            targets.push(*source);
                        }
                        Op::Clear { bigmap } => {
                            // Probably does not happen, but just to be sure this branch is included.
                            // If it does happen, we don't want to pick up updates from before the Clear, so
                            // stop recursing here for this dependent bigmap
                            if *bigmap != bigmap_target {
                                clear_targets.push(*bigmap);
                            } else {
                                res.push(op.clone());
                            }
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

    pub(crate) fn get_tx_context_owned_bigmaps(
        &self,
        tx_context: &TxContext,
    ) -> Vec<i32> {
        let mut res: HashMap<i32, ()> = HashMap::new();

        // owned bigmaps always have a positive integer identifier
        for op in &self.tx_bigmap_ops[tx_context] {
            let bigmap = op.get_bigmap();
            if bigmap >= 0 {
                res.insert(bigmap, ());
            }
        }
        res.keys()
            .copied()
            .collect::<Vec<i32>>()
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
            key: serde_json::Value::String(format!("{}", ident)),
            value: None,
        }
    }
    struct TestCase {
        name: String,

        tx_bigmap_ops: Vec<(TxContext, Vec<Op>)>,
        normalize_tx_context: TxContext,
        normalize_bigmap: i32,

        exp_deps: Vec<(i32, TxContext)>,
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

            exp_deps: vec![(10, tx_context(1))],
            exp_ops: vec![op_update(0, 1), op_update(0, 2)],
        },
        TestCase {
            name: "nested copy, only nested source is in exp_deps".to_string(),

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

            exp_deps: vec![(10, tx_context(1))],
            exp_ops: vec![op_update(0, 1), op_update(0, 2)],
        },
        TestCase {
            name: "nested copy (complex)".to_string(),

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
                    Op::Copy {
                        bigmap: 0,
                        source: 5,
                    },
                ],
            )],
            normalize_tx_context: tx_context(1),
            normalize_bigmap: 0,

            exp_deps: vec![(10, tx_context(1)), (10, tx_context(1))],
            exp_ops: vec![
                op_update(0, 1),
                op_update(0, 1),
                op_update(0, 2),
                op_update(0, 2),
            ],
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

            exp_deps: vec![(5, tx_context(2))],
            exp_ops: vec![
                op_update(0, 1),
                op_update(0, 2),
                op_update(0, 3),
                op_update(0, 4),
            ],
        },
        // what follows are test cases about scenarios that probably
        // are impossible in the Tezos blockchain context, but just in
        // case.. testing that we deal with them in a sensible way:
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

            exp_deps: vec![(10, tx_context(2))],
            exp_ops: vec![op_update(0, 3), op_update(0, 4)],
        },
        TestCase {
            name: "updates before a clear of target bitmap are not omitted"
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
            exp_ops: vec![
                op_update(0, 1),
                op_update(0, 2),
                Op::Clear { bigmap: 0 },
                op_update(0, 3),
                op_update(0, 4),
            ],
        },
    ];
    for tc in testcases {
        println!("test case: {}", tc.name);
        let (got_deps, got_ops) =
            IntraBlockBigmapDiffsProcessor::from_testlist(&tc.tx_bigmap_ops)
                .normalized_diffs(
                    tc.normalize_bigmap,
                    &tc.normalize_tx_context,
                );
        assert_eq!(tc.exp_deps, got_deps);
        assert_eq!(tc.exp_ops, got_ops);
    }
}
