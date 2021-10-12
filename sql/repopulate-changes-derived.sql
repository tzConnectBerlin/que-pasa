--repopulate

DELETE FROM "{contract_schema}"."{table}_live";
INSERT INTO "{contract_schema}"."{table}_live" (
    level, level_timestamp, id, tx_context_id, bigmap_id {columns_anon}
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
        FROM (
            SELECT
                t.*
            FROM "{contract_schema}"."{table}" t
            WHERE t.bigmap_id NOT IN (SELECT bigmap_id FROM "{contract_schema}".bigmap_clears)
        ) t
        JOIN tx_contexts ctx
          ON ctx.id = t.tx_context_id
        JOIN levels level_meta
          ON level_meta.level = ctx.level
        ORDER BY
            {indices},
            ctx.level DESC,
            ctx.operation_group_number DESC,
            ctx.operation_number DESC,
            ctx.content_number DESC,
            COALESCE(ctx.internal_number, -2) DESC
    ) t
    where not t.deleted
) q;


DELETE FROM "{contract_schema}"."{table}_ordered";
INSERT INTO "{contract_schema}"."{table}_ordered" (
    ordering, level, level_timestamp, id, tx_context_id, deleted {columns_anon}
)
SELECT
    *
FROM (
    SELECT
        DENSE_RANK() OVER (
            ORDER BY
                ctx.level,
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

        UNION ALL

        SELECT
            tx_context_id,
            -ROW_NUMBER() OVER () + (
                SELECT LEAST(0, MIN(id)) FROM "{contract_schema}"."{table}"
            ) AS id,
            'true' AS deleted
            {columns}
        FROM (
            SELECT DISTINCT
                clr.tx_context_id,
                last_value(t.deleted) over (
                    PARTITION BY ({indices})
                    ORDER BY
                        ctx.level,
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
            LEFT JOIN "{contract_schema}"."{table}" t2
              ON  {indices_equal_t_t2}
              AND t2.tx_context_id = clr.tx_context_id
            WHERE t2 IS NULL
        ) t
        WHERE NOT t.latest_deleted
    ) t  -- t with bigmap clears unfolded
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
) q;
