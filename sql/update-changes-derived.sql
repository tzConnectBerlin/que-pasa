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
        SELECT
            {indices}
        FROM (
            SELECT DISTINCT ON({indices})
                t.deleted,
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
        ) t
        where t.deleted
    ) as deleted_indices
    JOIN "{contract_schema}"."{table}_live" live
      ON {indices_equal}
);

INSERT INTO "{contract_schema}"."{table}_live" (
    level, level_timestamp, id, tx_context_id, bigmap_id {columns}
)
SELECT
    *
FROM (
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
    where not t.deleted
) t;


INSERT INTO "{contract_schema}"."{table}_ordered" (
    ordering, level, level_timestamp, id, tx_context_id, deleted {columns}
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
        ROW_NUMBER() OVER (
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
            tx_context_id,
            NULL AS id,
            'true' AS deleted
            {columns}
        FROM (
            SELECT DISTINCT
                clr.tx_context_id,
                last_value(deleted) over (
                    PARTITION BY ({indices})
                    ORDER BY
                        ctx.operation_group_number,
                        ctx.operation_number,
                        ctx.content_number,
                        COALESCE(ctx.internal_number, -2)
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS latest_deleted
                {columns}
            FROM "{contract_schema}".bigmap_clears clr
            JOIN "{contract_schema}"."{table}" t
              ON t.bigmap_id = clr.bigmap_id
            JOIN tx_contexts ctx
              ON ctx.id = t.tx_context_id
            WHERE clr.tx_context_id IN ({tx_context_ids})
        ) t
        WHERE NOT t.latest_deleted
    ) t  -- t with bigmap clears unfolded
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
) t;
