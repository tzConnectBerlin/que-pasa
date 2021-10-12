-- update based on newly processed block (note: _must_ be a *newer* block)

DELETE FROM "{contract_schema}"."{table}_live"
WHERE bigmap_id IN (
    SELECT
        bigmap_id
    FROM "{contract_schema}".bigmap_clears
    WHERE tx_context_id in ({tx_context_ids})
);

DELETE FROM "{contract_schema}"."{table}_live"
WHERE id IN (
    SELECT
        live.id
    FROM (
        SELECT DISTINCT ON({indices})
            {indices}
        FROM "{contract_schema}"."{table}" t
        JOIN tx_contexts ctx
          ON ctx.id = t.tx_context_id
        JOIN levels level_meta
          ON level_meta.level = ctx.level
        WHERE t.tx_context_id IN ({tx_context_ids})
        ORDER BY
            {indices},
            ctx.operation_group_number DESC,
            ctx.operation_number DESC,
            ctx.content_number DESC,
            COALESCE(ctx.internal_number, -2) DESC
    ) as deleted_indices
    JOIN "{contract_schema}"."{table}_live" live
      ON {indices_equal_deleted_live}
);

INSERT INTO "{contract_schema}"."{table}_live" (
    level, level_timestamp, id, tx_context_id, bigmap_id {columns_anon}
)
SELECT
    level,
    level_timestamp,
    id,
    tx_context_id,
    bigmap_id
    {columns}
FROM (
    SELECT DISTINCT ON({indices})
        ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.*
    FROM "{contract_schema}"."{table}" t
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
    WHERE t.tx_context_id IN ({tx_context_ids})
    ORDER BY
        {indices},
        ctx.operation_group_number DESC,
        ctx.operation_number DESC,
        ctx.content_number DESC,
        COALESCE(ctx.internal_number, -2) DESC
) t
WHERE NOT t.deleted;


INSERT INTO "{contract_schema}"."{table}_ordered" (
    ordering, level, level_timestamp, id, tx_context_id, deleted {columns_anon}
)
SELECT
    ordering + (SELECT max(ordering) FROM "{contract_schema}"."{table}_ordered") as ordering,
    level,
    level_timestamp,
    id,
    tx_context_id,
    deleted
    {columns}
FROM (
    SELECT
        DENSE_RANK() OVER (
            ORDER BY
                ctx.operation_group_number,
                ctx.operation_number,
                ctx.content_number,
                COALESCE(ctx.internal_number, -2)
        ) AS ordering,
        ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.id,
        t.tx_context_id,
        t.deleted
        {columns}
    FROM (
        SELECT
            t.tx_context_id,
            t.id,
            t.deleted
            {columns}
        FROM "{contract_schema}"."{table}" t
        WHERE t.tx_context_id IN ({tx_context_ids})

        UNION ALL

        SELECT
            t.tx_context_id,
            -ROW_NUMBER() OVER () + (
                SELECT LEAST(0, MIN(id)) FROM "{contract_schema}"."{table}"
            ) AS id,
            'true' AS deleted
            {columns}
        FROM (
            SELECT DISTINCT ON({indices})
                clr.tx_context_id,
                LAST_VALUE(t.deleted) OVER w as latest_deleted
                {columns_latest}
            FROM "{contract_schema}".bigmap_clears clr
            JOIN "{contract_schema}"."{table}" t
              ON t.bigmap_id = clr.bigmap_id
            JOIN tx_contexts ctx
              ON ctx.id = t.tx_context_id
            WHERE clr.tx_context_id IN ({tx_context_ids})
            WINDOW w AS (
                PARTITION BY ({indices})
                ORDER BY
                    ctx.level,
                    ctx.operation_group_number,
                    ctx.operation_number,
                    ctx.content_number,
                    COALESCE(ctx.internal_number, -2)
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        ) t
        LEFT JOIN "{contract_schema}"."{table}" t2
          ON  {indices_equal_t_t2}
          AND t2.tx_context_id = t.tx_context_id
        WHERE NOT t.latest_deleted
          AND t2 IS NULL
    ) t  -- t with bigmap clears unfolded
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
) t;
