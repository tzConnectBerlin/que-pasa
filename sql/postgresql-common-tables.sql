CREATE TABLE levels (
        _level INTEGER PRIMARY KEY,
        hash VARCHAR(60),
        baked_at TIMESTAMP WITH TIME ZONE);

CREATE UNIQUE INDEX levels__level ON levels(_level);
CREATE UNIQUE INDEX levels_hash ON levels(hash);

CREATE TABLE contract_levels (
    contract VARCHAR(100) NOT NULL,
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
       level INTEGER NOT NULL REFERENCES levels(_level) ON DELETE CASCADE,
       contract VARCHAR(100) NOT NULL,
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

CREATE SCHEMA {contract_schema};
