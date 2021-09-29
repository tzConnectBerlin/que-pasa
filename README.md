# Que Pasa, the baby indexer for Tezos

This repo contains the baby indexer, an indexer for dApps. It indexes only the contracts you want it to index. It reads the contract's storage definition and generates SQL DDL for a SQL representation of the tables, which it then populates.

In short, Que Pasa translates the marketplace contract in:

![](https://i.imgur.com/VhnGtss.png)

, into a database schema with following tables:

![](https://i.imgur.com/Reb4NR2.png)

Where, for example, table "storage" has the following columns:

![](https://i.imgur.com/6Adw1Cp.png)

Currently the indexer works with PostgreSQL (we have been running with PostgreSQL 12 and 13).

## Detailed overview

The indexer stores data for only the contracts specified. Each contract's data is stored in its own schema. Cross-schema joins can be used to include results from several contracts.

Every updated storage is inserted in its entirety (as a snapshot), with exception to Big map updates; each change is stored. This allows the indexer to be stateless (in other words, it doesn't care about what levels are processed in what order).

For nearly all tables (including bigmap tables, excluding tables nested inside bigmaps) a `_live` view and a `_ordered` view is generated:
- `_live` contains the current state.
- `_ordered` for snapshots (non-bigmap) contains all snapshots in sequence of Tezos' execution order, and for changes (bigmaps) contains all updates in sequence of Tezos' execution order.

Forks are automatically detected. When detected, indexed data belonging to the orphaned blocks is cleaned up. Make sure your backend does not expect the newest data to be immutable.

## Installation

Make sure all dependencies are present on your machine:
- Rust's build system `cargo` is required.

Then clone our repository, and run `cargo install --path .` inside its root directory.

Following subsections give exact installation command sequences for specific operating systems.

### Linux and MacOS systems

```
curl https://sh.rustup.rs -sSf | sh;
git clone git@github.com:tzConnectBerlin/que-pasa.git;
cd que-pasa;
cargo install --path .
```

## Usage

Required settings:
- Node URL
- Database URL (for more info see "Database settings" section)
- Which contracts to index (for more info see "Contracts settings" section)

Once those have been set:
1. First, an initial sync is required (processing of all relevant blocks up til now). This can be done by processing every block from head until contracts origination, though it will require fetching all blocks in this range (including blocks that are irrelevant to the setup). For the alternative fast sync see section "Fast sync".
2. Now we're synced. Any subsequent runs will run in a continuous mode, where we wait for new blocks to arrive and process them when they do.

### Database settings

The database URL is set in the environment variable `DATABASE_URL` or passed under the `--database-url` CLI argument, like this:

```
PGUSER=..
PGPASS=..
PGDATABASE=..
PGPORT=..
PGHOST=..

DATABASE_URL=postgres://$PGUSER:$PGPASS@$PGHOST:$PGPORT/$PGDATABASE
```

### Contracts Settings

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

It is possible to only process the blocks relevant to the setup. For this to work it's necessary to ask from an external source in which blocks the setup contracts have been active. Currently the only external source supported is better-call.dev. If you wish to enable fast sync, set the `--bcd-url` and `--network` CLI args when running Que Pasa for the first time (or when running for an additional contract for the first time).

## Database structure

### Tables
The main table in each indexed contract's DB schema is `storage`; all other tables have a prefix which indicates where they are in the contract storage. For instance a map called `foo` in the main storage will live in a table called `storage.foo`, with a foreign key constraint, `storage_id` pointing back to the storage row which relates to it. Deeper levels of nesting will go on, and on.

All tables have a `tx_context_id` field, which enables searching the database for its state at any time, while also making simple queries much more complicated. See the definitions for the `_live` and `_ordered` views for insights on how to create custom queries on the tables directly.

Variant records come in two varieties. The simplest are those which are simply one or another `unit` types, with different annotations. These become text fields in the database. The other type are true variant records, they become subsidiary tables, as maps and big maps are, with a text field in the parent table indicating which form of the record is present.

Big map updates are stored independently of the rest of the storage, as one would expect. Since we need to be able to look back at the history of the chain, there is a `deleted` flag which tells one whether the row has been removed (note: we don't update rows' deleted flag, we create a new row with deleted=true and value columns set to null). This means that if the most recent version of the map for the keys you specify has this deleted flag set, those keys in this bigmap are no longer alive/present.

# Limitations

- We're (currently) not indexing: tickets, sapling states, lambda values. If they are present in an indexed contract, they're ignored. In other words, values of these types will not arrive in the db.
- Generated table names can become quite long. Some contracts may be impeded by name length limitations of the underlying database system. For example, PostgreSQL's default setup only allows table names of up to 63 characters.
