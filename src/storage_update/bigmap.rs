use anyhow::{anyhow, Result};
use std::collections::HashMap;

#[cfg(test)]
use pretty_assertions::assert_eq;

use crate::octez::block::{
    BigMapDiff, Block, LazyStorageDiff, TxContext, Update, Updates::*,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Op {
    Alloc {
        bigmap: i32,
    },
    Update {
        bigmap: i32,
        keyhash: String,
        key: serde_json::Value,
        value: Option<serde_json::Value>, // if None: it means remove key in bigmap
    },
    Copy {
        bigmap: i32,
        source: i32,
    },
    Clear {
        bigmap: i32,
    },
}

impl Op {
    pub fn get_bigmap(&self) -> i32 {
        match self {
            Op::Update { bigmap, .. } => *bigmap,
            Op::Clear { bigmap, .. } => *bigmap,
            Op::Copy { bigmap, .. } => *bigmap,
            Op::Alloc { bigmap } => *bigmap,
        }
    }

    pub fn set_bigmap(&mut self, id: i32) {
        match self {
            Op::Update { bigmap, .. } => *bigmap = id,
            Op::Clear { bigmap } => *bigmap = id,
            Op::Copy { bigmap, .. } => *bigmap = id,
            Op::Alloc { bigmap } => *bigmap = id,
        }
    }

    pub fn from_raw_lazy(raw: &LazyStorageDiff) -> Result<Vec<Self>> {
        // The structure that replaced the deprecated "big_map_diff"
        if raw.kind != "big_map" {
            return Ok(vec![]);
        }
        let bigmap = raw.id.parse::<i32>()?;
        let mut ops = match raw.diff.action.as_str() {
            "update" | "alloc" => {
                let updates: Vec<&Update> = match &raw.diff.updates {
                    Some(Update(u)) => vec![u],
                    Some(Updates(us)) => us.iter().collect(),
                    _ => {
                        return Err(anyhow!(
                            "unknown updates shape: {:#?}",
                            raw.diff.updates
                        ))
                    }
                };

                updates
                    .iter()
                    .map(|update| {
                        Ok(Op::Update {
                            bigmap,
                            keyhash: update.key_hash.clone().ok_or_else(
                                || {
                                    anyhow!(
                                        "no key_hash in big map update {:?}",
                                        raw
                                    )
                                },
                            )?,
                            key: update
                                .key
                                .as_ref()
                                .cloned()
                                .ok_or_else(|| {
                                    anyhow!(
                                        "no key in big map update {:?}",
                                        raw
                                    )
                                })?,
                            value: update.value.clone(),
                        })
                    })
                    .collect::<Result<Vec<Self>>>()
            }
            "copy" => Ok(vec![Op::Copy {
                bigmap,
                source: raw
                    .diff
                    .source
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("'source' missing in big_map copy {:?}", raw)
                    })?
                    .parse()?,
            }]),
            "remove" => Ok(vec![Op::Clear { bigmap }]),
            _ => Err(anyhow!("unknown big_map action: {}", raw.diff.action)),
        }?;
        if raw.diff.action == "alloc" {
            ops.insert(
                0,
                Op::Alloc {
                    bigmap: raw.id.parse()?,
                },
            );
        }
        Ok(ops)
    }

    pub fn from_raw(raw: &BigMapDiff) -> Result<Option<Self>> {
        // From the depricated "big_map_diff" entries
        match raw.action.as_str() {
            "update" => Ok(Some(Op::Update {
                bigmap: raw
                    .big_map
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("no big map id found in diff {:?}", raw)
                    })?
                    .parse()?,
                keyhash: raw.key_hash.clone().ok_or_else(|| {
                    anyhow!("no key_hash in big map update {:?}", raw)
                })?,
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

#[derive(Debug)]
pub struct IntraBlockBigmapDiffsProcessor {
    tx_bigmap_ops: HashMap<TxContext, Vec<Op>>,
}

impl IntraBlockBigmapDiffsProcessor {
    pub(crate) fn from_block(block: &Block) -> Result<Self> {
        let mut res = Self {
            tx_bigmap_ops: HashMap::new(),
        };

        let tx_bigmap_ops = block.map_tx_contexts(
            |tx_context, _tx, _is_origination, op_res| {
                let mut ops: Vec<Op> = vec![];
                for lazy_diff in op_res
                    .lazy_storage_diff
                    .as_ref()
                    .unwrap()
                {
                    ops.extend(Op::from_raw_lazy(lazy_diff)?);
                }
                Ok(Some((tx_context, ops)))
            },
        )?;
        for (tx_context, ops) in tx_bigmap_ops {
            res.tx_bigmap_ops
                .insert(tx_context, ops);
        }

        if false {
            let mut keys: Vec<&TxContext> = res.tx_bigmap_ops.keys().collect();
            keys.sort();
            for k in keys {
                println!("tx[{:#?}]: {:#?}", k, res.tx_bigmap_ops[k]);
            }
        }

        Ok(res)
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
        deep_copy: bool,
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
        let mut prev_scope = keys[0].clone();
        prev_scope.internal_number = None;
        prev_scope.contract = "".to_string();

        for tx_context in keys {
            let mut current_scope = tx_context.clone();
            current_scope.internal_number = None;
            current_scope.contract = "".to_string();
            if prev_scope != current_scope {
                // temporary bigmaps (ie those with id < 0) only live in the
                // scope of tx contents (the content operation itself +
                // the internal operations)
                targets = targets
                    .into_iter()
                    .filter(|d| d >= &0)
                    .collect();
                prev_scope = current_scope;
            }
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
                        Op::Alloc { bigmap } => {
                            if *bigmap == bigmap_target {
                                res.push(op.clone());
                            }
                        }
                        Op::Update { .. } => {
                            if !deep_copy && op.get_bigmap() != bigmap_target {
                                continue;
                            }
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

                            if *bigmap < 0 {
                                targets = targets
                                    .into_iter()
                                    .filter(|d| d != bigmap)
                                    .collect();
                            }

                            if !deep_copy && *source >= 0 {
                                res.push(Op::Copy {
                                    source: *source,
                                    bigmap: bigmap_target,
                                });
                            }
                        }
                        Op::Clear { bigmap } => {
                            if *bigmap != bigmap_target {
                                // Probably does not happen, but just to be sure this branch is included.
                                // If it does happen, we don't want to pick up updates from before the Clear, so
                                // stop recursing here for this dependent bigmap
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
                .filter(|bigmap| **bigmap < 0)
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
    fn tx_context(level: u32, internal: Option<i32>) -> TxContext {
        TxContext {
            id: None,
            level,
            operation_group_number: 0,
            operation_number: 0,
            content_number: 0,
            internal_number: internal,
            contract: "".to_string(),
        }
    }
    fn op_update(bigmap: i32, ident: i32) -> Op {
        Op::Update {
            bigmap,
            keyhash: "".to_string(),
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
                tx_context(1, None),
                vec![op_update(0, 1), op_update(1, 1)],
            )],
            normalize_tx_context: tx_context(1, None),
            normalize_bigmap: 0,

            exp_deps: vec![],
            exp_ops: vec![op_update(0, 1)],
        },
        TestCase {
            name: "empty".to_string(),

            tx_bigmap_ops: vec![(tx_context(1, None), vec![])],
            normalize_tx_context: tx_context(1, None),
            normalize_bigmap: 10,

            exp_deps: vec![],
            exp_ops: vec![],
        },
        TestCase {
            name: "basic copy (and updates after the copy are omitted)"
                .to_string(),

            tx_bigmap_ops: vec![(
                tx_context(1, None),
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
            normalize_tx_context: tx_context(1, None),
            normalize_bigmap: 0,

            exp_deps: vec![(10, tx_context(1, None))],
            exp_ops: vec![op_update(0, 1), op_update(0, 2)],
        },
        TestCase {
            name: "nested copy, only nested source is in exp_deps".to_string(),

            tx_bigmap_ops: vec![(
                tx_context(1, None),
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
            normalize_tx_context: tx_context(1, None),
            normalize_bigmap: 0,

            exp_deps: vec![(10, tx_context(1, None))],
            exp_ops: vec![op_update(0, 1), op_update(0, 2)],
        },
        TestCase {
            name: "nested copy (complex)".to_string(),

            tx_bigmap_ops: vec![(
                tx_context(1, None),
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
            normalize_tx_context: tx_context(1, None),
            normalize_bigmap: 0,

            exp_deps: vec![(10, tx_context(1, None)), (10, tx_context(1, None))],
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
                    tx_context(2, None),
                    vec![
                        op_update(-5, 1),  // should be included
                        op_update(-5, 2),  // should be included
                        op_update(0, 10), // should be omitted
                    ],
                ),
                (
                    tx_context(2, Some(0)),
                    vec![
                        op_update(-5, 3),
                        Op::Copy {
                            bigmap: 0,
                            source: -5,
                        },
                        op_update(0, 4),
                    ],
                ),
                (
                    tx_context(3, None),
                    vec![
                        op_update(0, 5), // should be omitted (later tx)
                        op_update(5, 4), // should be omitted (later tx)
                    ],
                ),
            ],
            normalize_tx_context: tx_context(2, Some(0)),
            normalize_bigmap: 0,

            exp_deps: vec![(-5, tx_context(2, Some(0)))],
            exp_ops: vec![
                op_update(0, 1),
                op_update(0, 2),
                op_update(0, 3),
                op_update(0, 4),
            ],
        },
        TestCase {
            name: "-bigmap ids are temporary, and only live in the scope of origin copy".to_string(),

            tx_bigmap_ops: vec![(
            tx_context(1, None),
            vec![
                op_update(3, 1),
                Op::Copy{
                bigmap: -2, // should not be picked up when getting diffs for bigmap_id=0
                source: 3
                },
            ]), (
                tx_context(2, None),
                vec![
                    op_update(10, 2),
                    Op::Copy {
                        bigmap: -2,
                        source: 10,
                    },
                    op_update(-2, 3),
                    Op::Copy {
                        bigmap: 0,
                        source: -2,
                    },
                ],
            )],
            normalize_tx_context: tx_context(2, None),
            normalize_bigmap: 0,

            exp_deps: vec![(10, tx_context(2, None))],
            exp_ops: vec![
                op_update(0, 2),
                op_update(0, 3),
            ],
        },
        // what follows are test cases about scenarios that probably
        // are impossible in the Tezos blockchain context, but just in
        // case.. testing that we deal with them in a sensible way:
        TestCase {
            name: "copy: updates before a clear are omitted".to_string(),

            tx_bigmap_ops: vec![
                (tx_context(1, None), vec![op_update(10, 0)]),
                (
                    tx_context(2, None),
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
            normalize_tx_context: tx_context(2, None),
            normalize_bigmap: 0,

            exp_deps: vec![(10, tx_context(2, None))],
            exp_ops: vec![op_update(0, 3), op_update(0, 4)],
        },
        TestCase {
            name: "updates before a clear of target bitmap are not omitted"
                .to_string(),

            tx_bigmap_ops: vec![(
                tx_context(1, None),
                vec![
                    op_update(0, 1),
                    op_update(0, 2),
                    Op::Clear { bigmap: 0 },
                    op_update(0, 3),
                    op_update(0, 4),
                ],
            )],
            normalize_tx_context: tx_context(2, None),
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
                    true,
                );
        assert_eq!(tc.exp_deps, got_deps);
        assert_eq!(tc.exp_ops, got_ops);
    }
}
