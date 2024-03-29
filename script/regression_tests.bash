#!/usr/bin/env bash
cd $(git rev-parse --show-toplevel)

set -e -u

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
db_docker=`./script/local-db.bash -c max_locks_per_transaction=100000`
TRAP="echo stopping docker db..; docker kill $db_docker"
trap "$TRAP" EXIT

SETUP_WAIT=5s
echo "waiting for $SETUP_WAIT for testdb initialization.."
sleep $SETUP_WAIT

export NODE_URL=http://mainnet-archive.tzconnect.berlin:18732
export DATABASE_URL=postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE
function query {
    query_id=$(( query_id + 1 ))
    echo "query $query_id: $1"
    res=`psql -c "$1"`

    exp=`printf "%s;\n%s" "$1" "$res"`
    exp_file=test/regression/"$query_id".query
    if [[ "$MODE" == "assert" ]]; then
        tmp=`mktemp`
        echo "$exp" > $tmp
        if ! cmp $tmp $exp_file ; then
            ${DIFFTOOL:-kdiff3} $tmp $exp_file
            exit 1
        fi
    else
        printf "***\nexpectation generated:\n%s\n***\n\n" "$exp"
        echo "$exp" > $exp_file
    fi
}

function assert {
    query_id=0
    query 'select count(1) from "custom_Main_Schema".tx_contexts'
    query 'select count(1) from "custom_Main_Schema".contracts'
    query 'select count(1) from "custom_Main_Schema".contract_levels'
    query 'select count(1) from "custom_Main_Schema".contract_deps'

    query 'select administrator, all_tokens, paused, level, level_timestamp from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage_live"'
    query 'select level, level_timestamp, idx_address, idx_nat, nat from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage.ledger_live" order by idx_address, idx_nat'
    query 'select ordering, level, level_timestamp, idx_address, idx_nat, nat from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage.ledger_ordered" order by ordering, idx_address, idx_nat'

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
    query "$sql"

    # parameter indexing tests
    query 'select amount, artist, fa2, objkt_id, price, royalties from "KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq"."entry.ask" order by objkt_id'
    query 'select artist, fa2, objkt_id, royalties from "KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq"."entry.bid" order by objkt_id'
    query 'select nat from "KT1M8asPmVQhFG6yujzttGonznkghocEkbFk"."entry.deposit"'
    query 'select address, amount, token_id from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."entry.mint" order by token_id'

    # _at function tests
    query 'select address, amount, token_id from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."entry.mint_at"(1768506)'
    query 'select address, amount, token_id from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."entry.mint_at"(1768606, 3, 17)'
    query 'select address, amount, token_id from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."entry.mint_at"(1768606, 3, 16)'
    query 'select address, amount, token_id from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."entry.mint_at"(1768506, 3, 12)'
    query 'select address, amount, token_id from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."entry.mint_at"(1768506, 3, 13)'
}

function with_http_proxy_inject {
    mkfifo proxy_input
    trap "rm proxy_input; $TRAP" EXIT
    proxy_inject_output=`mktemp`

    cat proxy_input | ./script/http-inject.rb > "$proxy_inject_output" &
    proxy_pid=$!
    trap "kill -s 9 $proxy_pid; rm proxy_input; $TRAP" EXIT

    http_proxy=http://localhost:8080 $@

    echo '' > proxy_input
    rm proxy_input
    trap "$TRAP" EXIT

    exp_file=test/http_proxy_"$@".exp
    if [[ "$MODE" == "assert" ]]; then
      if ! cmp "$proxy_inject_output" "$exp_file" ; then
          ${DIFFTOOL:-kdiff3} "$proxy_inject_output" "$exp_file"
          exit 1
      fi
    else
        printf "***\nexpectation generated:\n%s\n***\n\n"
        cat "$proxy_inject_output"
        mv "$proxy_inject_output" "$exp_file"
    fi
}

export RUST_BACKTRACE=1

cargo run -- --main-schema custom_Main_Schema --index-all-contracts -l 1500000-1500001
cargo run --features regression_force_update_derived -- --main-schema custom_Main_Schema --index-all-contracts -l 1500002-1500005
# running one time with 'new' fake response fields injected, to verify que pasa doesn't break on unexpected new json fields
with_http_proxy_inject cargo run --features regression_force_update_derived -- --main-schema custom_Main_Schema --index-all-contracts -l 1700002-1700005

# the latter has a delete bigmap, the first 3 have rows indexed of the deleted bigmap
cargo run --features regression_force_update_derived -- --main-schema custom_Main_Schema --index-all-contracts -l 1768431
cargo run --features regression_force_update_derived --  --main-schema custom_Main_Schema --index-all-contracts -l 1768503
cargo run --features regression_force_update_derived -- --main-schema custom_Main_Schema --index-all-contracts -l 1768506
cargo run --features regression_force_update_derived -- --main-schema custom_Main_Schema --index-all-contracts -l 1768606

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
cargo run -- --main-schema custom_Main_Schema --index-all-contracts -l 1768606

assert

if [[ "$MODE" == "assert" ]]; then
    echo 'all good'
fi
