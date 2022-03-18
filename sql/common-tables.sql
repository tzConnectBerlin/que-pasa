CREATE TABLE levels (
    level INTEGER PRIMARY KEY,
    hash VARCHAR(60),
    prev_hash VARCHAR(60),
    baked_at TIMESTAMP WITH TIME ZONE);

CREATE UNIQUE INDEX levels_level ON levels(level);
CREATE UNIQUE INDEX levels_hash ON levels(hash);

CREATE TABLE contracts (
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

CREATE TYPE indexer_mode AS ENUM (
    'Bootstrap',
    'Head'
);
CREATE TABLE indexer_state (
    quepasa_version TEXT NOT NULL,
    max_id BIGINT NOT NULL,
    mode indexer_mode NOT NULL
);
INSERT INTO indexer_state (
    quepasa_version, max_id, mode
) VALUES (
    '{quepasa_version}', 1, 'Bootstrap'
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

CREATE TABLE contract_deps (
    level INT NOT NULL,

    src_contract TEXT NOT NULL,
    dest_schema TEXT NOT NULL,

    PRIMARY KEY (level, src_contract, dest_schema)
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

-- CREATE TABLE bigmap_alloc(
--     id BIGSERIAL PRIMARY KEY,
--     bigmap_id INTEGER NOT NULL UNIQUE,
--     tx_context_id BIGINT NOT NULL,  -- <- the context wherein alloc happened
--     contract STRING NOT NULL,
--     table_name STRING NOT NULL
-- );


-- CREATE OR REPLACE FUNCTION get_entry_values(at BIGINT) RETURNS integer AS
--   FOR elem IN
--     SELECT * FROM test."entry.update.noname" WHERE tx_context_id = at
--   LOOP
--     IF elem.is_ref THEN
--
--     END IF;
--   END LOOP;
-- $$ LANGUAGE plpgsql;
