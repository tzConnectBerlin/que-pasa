-- update based on newly processed block (note: _must_ be a *newer* block)

DELETE FROM "{contract_schema}"."{table}_live";
INSERT INTO "{contract_schema}"."{table}_live" live
SELECT
    *
FROM (
    SELECT
        ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.id
        {columns}
    FROM "{contract_schema}"."{table}" t
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
    WHERE t.tx_context_id IN ({tx_context_ids})
    ORDER BY
        ctx.operation_group_number DESC,
        ctx.operation_number DESC,
        ctx.content_number DESC,
        COALESCE(ctx.internal_number, -2) DESC
    LIMIT 1
) q;


INSERT INTO "{contract_schema}"."{table}_ordered"
SELECT
    ordering + (SELECT max(ordering) FROM "{contract_schema}"."{table}_ordered") as ordering,
    level,
    level_timestamp,
    id
    {columns}
FROM (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                ctx.level,
                ctx.operation_group_number,
                ctx.operation_number,
                ctx.content_number,
                COALESCE(ctx.internal_number, -2)
        ) AS ordering,
        ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.id
        {columns}
    FROM "{contract_schema}"."{table}" t
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
) q;
