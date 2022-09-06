CREATE TABLE levels (
    level INTEGER PRIMARY KEY,
    hash VARCHAR(60),
    prev_hash VARCHAR(60),
    baked_at TIMESTAMP WITH TIME ZONE);

CREATE UNIQUE INDEX levels_level ON levels(level);
CREATE UNIQUE INDEX levels_hash ON levels(hash);

CREATE TYPE indexer_mode AS ENUM (
    'Bootstrap',
    'Head'
);
CREATE TABLE contracts (
    name TEXT PRIMARY KEY,
    address VARCHAR(100) NOT NULL,
    mode indexer_mode NOT NULL DEFAULT 'Bootstrap',

    UNIQUE(address)
);

CREATE TABLE dynamic_loader_contracts (
    name TEXT PRIMARY KEY,
    address VARCHAR(100) NOT NULL,

    UNIQUE(address)
);

CREATE TABLE contract_levels (
    contract TEXT NOT NULL REFERENCES contracts(name) ON DELETE CASCADE,
    level INTEGER NOT NULL,
    is_origination BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY(contract, level)
);

CREATE INDEX ON contract_levels(level);
CREATE INDEX ON contract_levels(contract, is_origination);

CREATE TABLE indexer_state (
    quepasa_version TEXT NOT NULL,
    max_id BIGINT NOT NULL
);
INSERT INTO indexer_state (
    quepasa_version, max_id
) VALUES (
    '{quepasa_version}', 1
);

create table tx_contexts (
    id bigint not null primary key,
    level integer not null references levels(level) on delete cascade,
    contract text not null,
    operation_group_number integer not null,
    operation_number integer not null,
    content_number integer not null,
    internal_number integer
);

CREATE UNIQUE INDEX ON tx_contexts(
    level,
    contract,
    operation_group_number,
    operation_number,
    content_number,
    coalesce(internal_number, -1)
);

CREATE TABLE txs (
    id BIGSERIAL PRIMARY KEY,
    tx_context_id BIGINT NOT NULL REFERENCES tx_contexts(id) ON DELETE CASCADE,

    operation_hash varchar(100) not null,
    source VARCHAR(100) NOT NULL,
    destination VARCHAR(100),
    entrypoint VARCHAR(100),

    amount NUMERIC,
    fee BIGINT,
    gas_limit BIGINT,
    storage_limit BIGINT,

    consumed_milligas BIGINT,
    storage_size BIGINT,
    paid_storage_size_diff BIGINT
);

CREATE UNIQUE INDEX ON txs(tx_context_id);

CREATE VIEW txs_ordered AS (
    SELECT
        DENSE_RANK() OVER (
            ORDER BY
                ctx.level,
                ctx.operation_group_number,
                ctx.operation_number,
                ctx.content_number,
                coalesce(ctx.internal_number, -1)
        ) ordering,
        ctx.level,
        meta.baked_at as level_timestamp,
        tx.*
    FROM txs tx
    JOIN tx_contexts ctx
      ON ctx.id = tx.tx_context_id
    JOIN levels meta
      ON meta.level = ctx.level
    ORDER BY ordering
);

CREATE TABLE bigmap_meta_actions (
    id BIGSERIAL PRIMARY KEY,

    tx_context_id BIGINT NOT NULL REFERENCES tx_contexts(id) ON DELETE CASCADE,
    bigmap_id INT NOT NULL,

    action TEXT NOT NULL,
    value JSONB
);

CREATE INDEX ON bigmap_meta_actions(bigmap_id, action, tx_context_id);
CREATE INDEX ON bigmap_meta_actions(tx_context_id);

CREATE TABLE contract_deps (
    level INT NOT NULL,

    src_contract TEXT NOT NULL,
    dest_schema TEXT NOT NULL,
    is_deep_copy BOOLEAN NOT NULL DEFAULT true,

    PRIMARY KEY (level, src_contract, dest_schema, is_deep_copy)
);

CREATE TABLE bigmap_keys(
    id BIGSERIAL PRIMARY KEY,
    bigmap_id INTEGER NOT NULL,
    tx_context_id BIGINT NOT NULL,
    keyhash TEXT NOT NULL,
    key JSONB NOT NULL,
    value JSONB,

    UNIQUE(tx_context_id, bigmap_id, keyhash),
    FOREIGN KEY (tx_context_id) REFERENCES tx_contexts(id) ON DELETE CASCADE
);


CREATE OR REPLACE FUNCTION "{main_schema}".last_context_at(lvl INT) RETURNS TABLE (tx_context_id BIGINT, level INT, operation_group_number INT, operation_number INT, content_number INT, internal_number INT)
AS $$
    SELECT
      ctx.id as tx_context_id,
      level,
      operation_group_number,
      operation_number,
      content_number,
      internal_number
    FROM "{main_schema}".tx_contexts AS ctx
    WHERE id = (
      SELECT id
      FROM "{main_schema}".tx_contexts
      WHERE level <= lvl
      ORDER BY level DESC, operation_group_number DESC, operation_number DESC, content_number DESC, COALESCE(internal_number, -1) DESC
      LIMIT 1
    )
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "{main_schema}".last_context_at(lvl INT, op_grp INT) RETURNS TABLE (tx_context_id BIGINT, level INT, operation_group_number INT, operation_number INT, content_number INT, internal_number INT)
AS $$
    SELECT
      ctx.id AS tx_context_id,
      level,
      operation_group_number,
      operation_number,
      content_number,
      internal_number
    FROM "{main_schema}".tx_contexts AS ctx
    WHERE id = (
      SELECT id
      FROM "{main_schema}".tx_contexts
      WHERE ARRAY[
            level,
            operation_group_number]
         <=
            ARRAY[
              lvl,
              op_grp]
      ORDER BY level DESC, operation_group_number DESC, operation_number DESC, content_number DESC, COALESCE(internal_number, -1) DESC
      LIMIT 1
    )
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "{main_schema}".last_context_at(lvl INT, op_grp INT, op INT) RETURNS TABLE (tx_context_id BIGINT, level INT, operation_group_number INT, operation_number INT, content_number INT, internal_number INT)
AS $$
    SELECT
      ctx.id,
      level,
      operation_group_number,
      operation_number,
      content_number,
      internal_number
    FROM "{main_schema}".tx_contexts AS ctx
    WHERE id = (
      SELECT id
      FROM "{main_schema}".tx_contexts
      WHERE ARRAY[
            level,
            operation_group_number,
            operation_number]
         <=
            ARRAY[
              lvl,
              op_grp,
              op]
      ORDER BY level DESC, operation_group_number DESC, operation_number DESC, content_number DESC, COALESCE(internal_number, -1) DESC
      LIMIT 1
    )
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "{main_schema}".last_context_at(lvl INT, op_grp INT, op INT, content INT) RETURNS TABLE (tx_context_id BIGINT, level INT, operation_group_number INT, operation_number INT, content_number INT, internal_number INT)
AS $$
    SELECT
      ctx.id AS tx_context_id,
      level,
      operation_group_number,
      operation_number,
      content_number,
      internal_number
    FROM "{main_schema}".tx_contexts AS ctx
    WHERE id = (
      SELECT id
      FROM "{main_schema}".tx_contexts
      WHERE ARRAY[
            level,
            operation_group_number,
            operation_number,
            content_number]
         <=
            ARRAY[
              lvl,
              op_grp,
              op,
              content]
      ORDER BY level DESC, operation_group_number DESC, operation_number DESC, content_number DESC, COALESCE(internal_number, -1) DESC
      LIMIT 1
    )
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "{main_schema}".last_context_at(lvl INT, op_grp INT, op INT, content INT, internal INT) RETURNS TABLE (tx_context_id BIGINT, level INT, operation_group_number INT, operation_number INT, content_number INT, internal_number INT)
AS $$
    SELECT
      ctx.id AS tx_context_id,
      level,
      operation_group_number,
      operation_number,
      content_number,
      internal_number
    FROM "{main_schema}".tx_contexts AS ctx
    WHERE id = (
      SELECT id
      FROM "{main_schema}".tx_contexts
      WHERE ARRAY[
            level,
            operation_group_number,
            operation_number,
            content_number,
            internal_number]
         <=
            ARRAY[
              lvl,
              op_grp,
              op,
              content,
              internal]
      ORDER BY level DESC, operation_group_number DESC, operation_number DESC, content_number DESC, COALESCE(internal_number, -1) DESC
      LIMIT 1
    )
$$ LANGUAGE SQL;
