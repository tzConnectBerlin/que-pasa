CREATE TABLE levels (
        level INTEGER PRIMARY KEY,
        hash VARCHAR(60),
        baked_at TIMESTAMP WITH TIME ZONE);

CREATE UNIQUE INDEX levels_level ON levels(level);
CREATE UNIQUE INDEX levels_hash ON levels(hash);

CREATE TABLE contracts (
    name TEXT PRIMARY KEY,
    address VARCHAR(100) NOT NULL
);

CREATE TABLE contract_levels (
    contract TEXT NOT NULL REFERENCES contracts(name) ON DELETE CASCADE,
    level INTEGER NOT NULL,
    is_origination BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY(contract, level)
);

CREATE TABLE max_id (
       max_id INT4
);

INSERT INTO max_id (max_id) VALUES (1);

CREATE TABLE tx_contexts(
       id INTEGER NOT NULL PRIMARY KEY,
       level INTEGER NOT NULL REFERENCES levels(level) ON DELETE CASCADE,
       contract TEXT NOT NULL,
       operation_hash VARCHAR(100) NOT NULL,
       operation_group_number INTEGER NOT NULL,
       operation_number INTEGER NOT NULL,
       content_number INTEGER NOT NULL,
       internal_number INTEGER,
       source VARCHAR(100) NOT NULL,
       destination VARCHAR(100),
       entrypoint VARCHAR(100));


CREATE UNIQUE INDEX ON tx_contexts(
    level,
    contract,
    operation_hash,
    operation_group_number,
    operation_number,
    content_number,
    coalesce(internal_number, -1));

CREATE TABLE bigmap_deps(
    tx_context_id INTEGER NOT NULL,

    source_contract TEXT NOT NULL,
    source_bigmap INTEGER NOT NULL,

    dest_schema TEXT NOT NULL,
    dest_table TEXT NOT NULL,
    dest_bigmap INTEGER NOT NULL,

    FOREIGN KEY (tx_context_id) REFERENCES tx_contexts(id) ON DELETE CASCADE
);


CREATE TABLE bigmap_copied_rows(
    src_contract TEXT NOT NULL,

    src_tx_context_id INTEGER NOT NULL,
    dest_tx_context_id INTEGER NOT NULL,

    dest_schema TEXT NOT NULL,
    dest_table TEXT NOT NULL,
    dest_row_id INTEGER NOT NULL,

    FOREIGN KEY (src_tx_context_id) REFERENCES tx_contexts(id),
    FOREIGN KEY (dest_tx_context_id) REFERENCES tx_contexts(id) ON DELETE CASCADE
);
