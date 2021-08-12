CREATE TABLE levels (
        id SERIAL PRIMARY KEY,
        _level INTEGER NOT NULL,
        is_origination BOOLEAN DEFAULT FALSE,
        hash VARCHAR(60),
        baked_at TIMESTAMP WITH TIME ZONE);


CREATE UNIQUE INDEX levels__level ON levels(_level);
CREATE UNIQUE INDEX levels_hash ON levels(hash);

CREATE TABLE max_id (
       max_id INT4
);

INSERT INTO max_id (max_id) VALUES (1);

CREATE TABLE tx_contexts(
       id INTEGER NOT NULL PRIMARY KEY,
       level INTEGER NOT NULL REFERENCES levels(_level) ON DELETE CASCADE,
       operation_hash VARCHAR(100) NOT NULL,
       operation_group_number INTEGER NOT NULL,
       operation_number INTEGER NOT NULL,
       source VARCHAR(100) NOT NULL,
       destination VARCHAR(100) NOT NULL);
