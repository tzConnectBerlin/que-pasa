CREATE VIEW "{contract_schema}"."{table}_live" AS (
    SELECT
        level,
        level_timestamp
        {columns}
    FROM (
        SELECT DISTINCT ON({indices})
            ctx.level as level,
            level_meta.baked_at as level_timestamp,
            t.*
        FROM (
            SELECT
                t.*
            FROM "{contract_schema}"."{table}" t
            WHERE t.bigmap_id NOT IN (SELECT bigmap_id FROM "{contract_schema}".bigmap_clears)
              AND t.id NOT IN (SELECT dest_row_id FROM bigmap_copied_rows)

            UNION ALL

            SELECT
                *
            FROM (
                SELECT DISTINCT ON({indices})
                    t.*
                FROM "{contract_schema}"."{table}" t
                JOIN bigmap_copied_rows cpy
                  ON cpy.dest_row_id = t.id
                JOIN tx_contexts src_ctx
                  ON src_ctx.id = cpy.src_tx_context_id
                WHERE t.bigmap_id NOT IN (SELECT bigmap_id FROM "{contract_schema}".bigmap_clears)
                ORDER BY
                    {indices},
                    src_ctx.level DESC,
                    src_ctx.operation_group_number DESC,
                    src_ctx.operation_number DESC,
                    src_ctx.content_number DESC,
                    COALESCE(src_ctx.internal_number, -1) DESC
            ) q
            WHERE NOT q.deleted
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
            COALESCE(ctx.internal_number, -1) DESC
    ) t
    where not t.deleted
);

CREATE VIEW "{contract_schema}"."{table}_ordered" AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                ctx.level,
                ctx.operation_group_number,
                ctx.operation_number,
                ctx.content_number,
                COALESCE(ctx.internal_number, -1)
        ) AS ordering,
        ctx.level as level,
        level_meta.baked_at as level_timestamp,
        t.deleted
        {columns}
    FROM (
        SELECT
            t.tx_context_id,
            t.deleted
            {columns}
        FROM "{contract_schema}"."{table}" t
        WHERE t.id NOT IN (SELECT dest_row_id FROM bigmap_copied_rows)

        UNION ALL

        SELECT
            *
        FROM (
            SELECT DISTINCT ON({indices})
                t.tx_context_id,
                t.deleted
                {columns}
            FROM "{contract_schema}"."{table}" t
            JOIN bigmap_copied_rows cpy
              ON cpy.dest_row_id = t.id
            JOIN tx_contexts src_ctx
              ON src_ctx.id = cpy.src_tx_context_id
            ORDER BY
                {indices},
                src_ctx.level DESC,
                src_ctx.operation_group_number DESC,
                src_ctx.operation_number DESC,
                src_ctx.content_number DESC,
                COALESCE(src_ctx.internal_number, -1) DESC
        ) q
        WHERE NOT q.deleted

        UNION ALL

        SELECT
            clr.tx_context_id,
            'true' as deleted
            {columns}
        FROM "{contract_schema}"."{table}" t
        JOIN "{contract_schema}".bigmap_clears clr
          ON t.bigmap_id = clr.bigmap_id
        WHERE 'false' = (
            SELECT DISTINCT last_value(deleted) over (
	    	ORDER BY
                    ctx_.level,
                    ctx_.operation_group_number,
                    ctx_.operation_number,
                    ctx_.content_number,
                    COALESCE(ctx_.internal_number, -1)
		ROWS BETWEEN UNBOUNDED PRECEEDING AND UNBOUNDED FOLLOWING) as latest
            FROM "{contract_schema}"."{table}" t_
            JOIN tx_contexts ctx_ ON ctx_.id = t_.tx_context_id
            WHERE t_.bigmap_id = t.bigmap_id
              AND {indices_check}
        )
    ) t  -- t with bigmap clears unfolded
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
);
