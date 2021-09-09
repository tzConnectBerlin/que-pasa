# Que Pasa, the baby indexer for Tezos

This repo contains the baby indexer, an indexer for one (1) smart contract. It reads the contract's storage definition and generates SQL DDL for a SQL representation of the tables, which it then populates.

Currently the indexer works with PostgreSQL 12.

## Detailed overview

The indexer stores data for only one contract. In future this will be extended, first to multiple instances of the same contract, and perhaps then to multiple contracts. For now, cross-schema joins can be used to include results from several contracts.

There are two kinds of data stored--big maps, and everything else. Big map changes are stored on their own; everything else is written into the database in its entirety on each update.

The first thing to do when using the indexer is to generate a schema, using the `generate-sql` command. Run like this:

```
storage-sql -c <contract_id> generate-sql
```

it will load and parse the contract's storage, and generate a SQL representation of it. This can be ingested directly by `psql`.

The database URL is set in the environment variable `DATABASE_URL`, like this:

```
DATABASE_URL=postgres://newby:foobar@localhost:5433/tezos
```

Running the indexer can be done in several ways. The least efficient is simply to run it with no arguments. Invoked in this way it will start from the head of the chain and work back, storing transactions directed at the contract it's been told about. Of course this will take a while, and you do not wish to wait. In the `scripts/` directory you will find a script called `get-levels.py`, which asks Better Call Dev for the levels relevant to this contract. You execute the script like this:

```
newby@stink:~/projects/tezos/storage2sql$ ./script/get-levels.py edo2net KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq
149127,149127,138208,138208,138201,138201,135501,135501,132390,132390,132388,132384,132383,132367,132367,132343,132343,132339,132327,132318,132318,132300,132300,132298,132285,132282,132278,132278,132262,132262,132259,132259,132242,132240,132222,132219,132219,132211,132201,132201,132091
```

The comma-separated list of levels can be imported into the indexer as so,

```
cargo run -- -c KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq -l 149127,149127,138208,138208,138201,138201,135501,135501,132390,132390,132388,132384,132383,132367,132367,132343,132343,132339,132327,132318,132318,132300,132300,132298,132285,132282,132278,132278,132262,132262,132259,132259,132242,132240,132222,132219,132219,132211,132201,132201,132091 --init
    Finished dev [unoptimized + debuginfo] target(s) in 0.05s
     Running `target/debug/storage2sql -c KT1U7Adyu5A7JWvEVSKjJEkG2He2SU1nATfq -l 149127,149127,138208,138208,138201,138201,135501,135501,132390,132390,132388,132384,132383,132367,132367,132343,132343,132339,132327,132318,132318,132300,132300,132298,132285,132282,132278,132278,132262,132262,132259,132259,132242,132240,132222,132219,132219,132211,132201,132201,132091 --init`
Initialising--all data in DB will be destroyed. Interrupt within 5 seconds to abort
```

Note the `--init` argument, which will delete all data from the database. The `-l` argument reads in the levels passed, and then all levels between these numbers are marked as imported.
npm install -g postgraphile

## Database structure

### Tables
The main table in the DB is `storage`; all other tables have a prefix which indicates where they are in the contract storage. For instance a map called `foo` in the main storage will live in a table called `storage.foo`, with a foreign key constraint, `storage_id` pointing back to the storage row which relates to it. Deeper levels of nesting will go on, and on.

All tables have a `_level` field, which enables searching the database for its state at any time, while also making simple queries much more complicated. See below for some SQL queries which return the current state of the database, and are suitable for creating views.

Variant records come in two varieties. The simplest are those which are simply one or another `unit` types, with different annotations. These become text fields in the database. The other type are true variant records, and are currently not implemented. In the future they will become subsidiary tables, as maps and big maps are, with a text field in the parent table indicating which form of the record is present.

Big map updates are stored independently of the rest of the storage, as one would expect. Since we need to be able to look back at the history of the chain, there is a `deleted` flag which tells one whether the row has been removed. If the most recent version of the map for the keys you specify has this flag set, there is no row.

## Cook book

The big map tables contain a row for each insertion, update and deletion.

Queries like this one will get the most recent row:

```
select * from "storage.questions" sq inner join (select idx_string_0, max(_level) as max_level from "storage.questions" group by idx_string_0) sq2 on sq.idx_string_0 = sq2.idx_string_0 and sq._level = sq2.max_level;

```

## Installation
Make sure all dependencies are present on your machine. Then clone our repository, and run `cargo install` anywhere inside it.

### Dependencies
Rust's build system `cargo` is required.

## Usage
1). First, setup the database by running with `--init`. This will create a set of global tables (tables that are shared between each indexed contracts).
2). Specify for which contracts to run (see section "Contracts setup").
3). An initial sync is now required (processing of all relevant blocks up til now). This can be done by processing every block from head until contracts origination, though it will require fetching all blocks in this range, including blocks that are irrelevant to the setup. For the alternative fast sync see section "Fast sync".
4). Now we're synced. Any subsequent runs will run in a continuous mode, where we wait for new blocks to arrive and process them when they do.

Forks are automatically detected. When detected, indexed data belonging to the orphaned blocks is cleaned up. Make sure your backend does not expect the newest data to be immutable.

### Contracts setup
Specify for which contracts to run in a settings.yaml file:
```
contracts:
- name: nft
  address: KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton
- name: marketplace
  address: KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn
```
and set the `--contract-settings` CLI argument to point to the yaml file. Or alternatively/and additionally specify contracts through the `--contracts` CLI argument:
```
que-pasa \
  .. \
  --contracts \
    nft=KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton \
    marketplace=KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn \
  ..
```

### Fast sync
It is possible to only process the blocks relevant to the setup. For this to work it's necessary to ask from an external source in which blocks the setup contracts have been active. Currently the only external source supported is better-call.dev. If you wish to enable fast sync, set the `--bcd` and `--network` CLI args when running Que Pasa for the first time (or when running for an additional contract for the first time).
