#!/bin/bash
cd $(git rev-parse --show-toplevel)

[ -z $DIFFTOOL ] && DIFFTOOL=kdiff3

MODE=assert
if [ $# -gt 0 ]; then
    MODE=$1
fi
if [[ "$MODE" != "generate" && "$MODE" != "assert" && "$MODE" != "inspect" ]]; then
    echo "unsupported command '$MODE', following commands are allowed: ['generate', 'assert', 'inspect']"
    exit 1
fi
echo "mode: $MODE"

export PGHOST=localhost
export PGPORT=35432
export PGUSER=test
export PGPASSWORD=test
export PGDATABASE=test

export DOCKER_ARGS='-d'
db_docker=`./script/local-db.bash`
trap "echo stopping docker db..; docker kill $db_docker" EXIT

SETUP_WAIT=3s
echo "waiting for $SETUP_WAIT for testdb initialization.."
sleep $SETUP_WAIT

export NODE_URL=https://mainnet-tezos.giganode.io
export DATABASE_URL=postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE

function query {
    query_id=$(( query_id + 1 ))
    echo "query $query_id: $1"
    res=`psql -c "$1"`

    exp=`printf "%s;\n%s" "$1" "$res"`
    exp_file=test/regression/"$query_id".query
    if [[ "$MODE" == "assert" ]]; then
        # diff --suppress-common-lines -y $exp_file - <<< "$exp" || exit 1
        tmp=`mktemp`
        echo "$exp" > $tmp
        if ! cmp $tmp $exp_file ; then
            $DIFFTOOL $tmp $exp_file
            exit 1
        fi
    else
        printf "***\nexpectation generated:\n%s\n***\n\n" "$exp"
        echo "$exp" > $exp_file
    fi
}

function assert {
    query_id=0
    query 'select count(1) from que_pasa.tx_contexts' || exit 1
    query 'select count(1) from que_pasa.contracts' || exit 1
    query 'select count(1) from que_pasa.contract_levels' || exit 1
    query 'select count(1) from que_pasa.contract_deps' || exit 1

    query 'select administrator, all_tokens, paused, level, level_timestamp from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage_live"' || exit 1
    query 'select level, level_timestamp, idx_address, idx_nat, nat from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage.ledger_live" order by idx_address, idx_nat' || exit 1
    query 'select ordering, level, level_timestamp, idx_address, idx_nat, nat from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage.ledger_ordered" order by ordering, idx_address, idx_nat' || exit 1

    # This query finds all foreign key references that have no index on the
    # columns w/ foreign key reference. We have to make sure there are _none_
    # of these because we delete data with cascade. If there are any missing
    # indexes, it kills the performance of delete.
    #
    # source of query: https://www.cybertec-postgresql.com/en/index-your-foreign-key/
    sql=`cat <<- EOF
SELECT c.conrelid::regclass AS "table",
       /* list of key column names in order */
       string_agg(a.attname, ',' ORDER BY x.n) AS columns,
       pg_catalog.pg_size_pretty(
          pg_catalog.pg_relation_size(c.conrelid)
       ) AS size,
       c.conname AS constraint,
       c.confrelid::regclass AS referenced_table
FROM pg_catalog.pg_constraint c
   /* enumerated key column numbers per foreign key */
   CROSS JOIN LATERAL
      unnest(c.conkey) WITH ORDINALITY AS x(attnum, n)
   /* name for each key column */
   JOIN pg_catalog.pg_attribute a
      ON a.attnum = x.attnum
         AND a.attrelid = c.conrelid
WHERE NOT EXISTS
        /* is there a matching index for the constraint? */
        (SELECT 1 FROM pg_catalog.pg_index i
         WHERE i.indrelid = c.conrelid
           /* it must not be a partial index */
           AND i.indpred IS NULL
           /* the first index columns must be the same as the
              key columns, but order doesn't matter */
           AND (i.indkey::smallint[])[0:cardinality(c.conkey)-1]
               OPERATOR(pg_catalog.@>) c.conkey)
  AND c.contype = 'f'
GROUP BY c.conrelid, c.conname, c.confrelid
ORDER BY pg_catalog.pg_relation_size(c.conrelid) DESC;
EOF
`
    query "$sql" || exit 1
}

cargo +nightly run -- --index-all-contracts -l 1500000-1500001 || exit 1
cargo +nightly run --features regression_force_update_derived -- --index-all-contracts -l 1500002-1500005 || exit 1
cargo +nightly run --features regression_force_update_derived -- --index-all-contracts -l 1700002-1700005 || exit 1

# the +nightly latter has a delete bigmap, the first 3 have rows indexed of the deleted bigmap
cargo +nightly run --features regression_force_update_derived -- --index-all-contracts -l 1768431 || exit 1
cargo +nightly run --features regression_force_update_derived -- --index-all-contracts -l 1768503 || exit 1
cargo +nightly run --features regression_force_update_derived -- --index-all-contracts -l 1768506 || exit 1
cargo +nightly run --features regression_force_update_derived -- --index-all-contracts -l 1768606 || exit 1

if [[ "$MODE" == "inspect" ]]; then
    psql
    exit 0
fi
if [[ "$MODE" == "generate" ]]; then
    rm test/regression/*.query
fi

assert

if [[ "$MODE" == "generate" ]]; then
    exit
fi

# verifying here that the repopulate also works with deleted bigmap rows
cargo run -- --index-all-contracts -l 1768606 || exit 1

assert

if [[ "$MODE" == "assert" ]]; then
    echo 'all good'
fi
