# The Baby Tezos Indexer

This repo contains the baby indexer, an indexer for one (1) smart contract. It reads the contract's storage definition and generates SQL DDL for a SQL representation of the tables, which it then populates.


## Cook book

The big map tables contain a row for each insertion, update and deletion.

Queries like this one will get the most recent row:

```
select * from "storage.questions" sq inner join (select idx_string_0, max(_level) as max_level from "storage.questions" group by idx_string_0) sq2 on sq.idx_string_0 = sq2.idx_string_0 and sq._level = sq2.max_level;

```
