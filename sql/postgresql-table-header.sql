CREATE TABLE "{contract_schema}.{table}" (
        id SERIAL PRIMARY KEY,
        deleted BOOLEAN DEFAULT false,
        tx_context_id INTEGER NOT NULL,
