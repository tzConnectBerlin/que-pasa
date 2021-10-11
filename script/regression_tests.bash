#!/bin/bash
cd $(git rev-parse --show-toplevel)

MODE=assert
if [ $# -gt 0 ]; then
    MODE=$1
fi
if [[ "$MODE" != "generate" && "$MODE" != "assert" && "$MODE" != "inspect" ]]; then
    echo "unsupported command '$MODE', following commands are allowed: ['generate', 'assert', 'inspect']"
    exit 1
fi
echo "mode: $MODE"

if [[ "$MODE" == "generate" ]]; then
    rm test/regression/*.query
fi

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
    res=`psql -c "$1"`

    exp=`printf "%s;\n%s" "$1" "$res"`
    exp_file=test/regression/`echo "$1" | md5sum | awk '{print $1}'`.query
    if [[ "$MODE" == "assert" ]]; then
        diff --suppress-common-lines -y $exp_file - <<< "$exp" || exit 1
    else
        printf "***\nexpectation generated:\n%s\n***\n\n" "$exp"
        echo "$exp" > $exp_file
    fi
}

cargo run -- --index-all-contracts -l 1500000-1500001
cargo run --features regression -- --index-all-contracts -l 1500002-1500005 --always-update-derived
if [[ "$MODE" == "inspect" ]]; then
    psql
    exit 0
fi
query 'select count(1) from tx_contexts' || exit 1
query 'select count(1) from contracts' || exit 1
query 'select count(1) from contract_levels' || exit 1
query 'select count(1) from contract_deps' || exit 1
query 'select * from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage_live"' || exit 1
query 'select * from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage.ledger_live" order by idx_address, idx_nat' || exit 1
query 'select * from "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"."storage.ledger_ordered" order by ordering' || exit 1

if [[ "$MODE" == "assert" ]]; then
    echo 'all good'
fi
