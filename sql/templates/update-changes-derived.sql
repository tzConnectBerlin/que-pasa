-- update based on newly processed block (note: _must_ be a *newer* block)

{% macro unfold(column_names, from_table, sep_first) %}
    {%- for col in column_names -%}
        {%- if sep_first.clone() || !loop.first %}, {% endif -%}
        {% if !from_table.is_empty() %}{{ from_table }}.{% endif %}{{ col }}
    {%- endfor -%}
{% endmacro %}


DELETE FROM "{{ contract_schema }}"."{{ table }}_live"
WHERE bigmap_id IN (
    SELECT
        bigmap_id
    FROM "{{ main_schema }}".bigmap_meta_actions
    WHERE tx_context_id in ({% call unfold(tx_context_ids, "", false) %})
      AND action = 'clear'
);

DELETE FROM "{{ contract_schema }}"."{{ table }}_live"
WHERE id IN (
    SELECT
        live.id
    FROM (
        SELECT DISTINCT
            {% call unfold(indices, "t", false) %}
        FROM "{{ contract_schema }}"."{{ table }}" t
        JOIN "{{ main_schema }}".tx_contexts ctx
          ON ctx.id = t.tx_context_id
        JOIN "{{ main_schema }}".levels level_meta
          ON level_meta.level = ctx.level
        WHERE t.tx_context_id IN ({% call unfold(tx_context_ids, "", false) %})
    ) as overwritten_indices
    JOIN "{{ contract_schema }}"."{{ table }}_live" live
      ON
        {% for idx in indices %}
            {% if !loop.first %} AND {% endif %}
            overwritten_indices.{{ idx }} = live.{{ idx }}
        {%- endfor %}
);

INSERT INTO "{{ contract_schema }}"."{{ table }}_live" (
    level, level_timestamp, id, tx_context_id, bigmap_id {% call unfold(columns, "", true) %}
)
SELECT
    level,
    level_timestamp,
    id,
    tx_context_id,
    bigmap_id
    {% call unfold(columns, "", true) %}
FROM (
    SELECT DISTINCT ON({% call unfold(indices, "t", false) %})
        ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.*
    FROM "{{ contract_schema }}"."{{ table }}" t
    JOIN "{{ main_schema }}".tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN "{{ main_schema }}".levels level_meta
      ON level_meta.level = ctx.level
    WHERE t.tx_context_id IN ({% call unfold(tx_context_ids, "", false) %})
    ORDER BY
        {% call unfold(indices, "t", false) %},
        ctx.level DESC,
        ctx.operation_group_number DESC,
        ctx.operation_number DESC,
        ctx.content_number DESC,
        COALESCE(ctx.internal_number, -1) DESC
) t
WHERE NOT t.deleted;


INSERT INTO "{{ contract_schema }}"."{{ table }}_ordered" (
    ordering, level, level_timestamp, id, tx_context_id, deleted {% call unfold(columns, "", true) %}
)
SELECT
    ordering + COALESCE((SELECT max(ordering) FROM "{{ contract_schema }}"."{{ table }}_ordered"), 0) as ordering,
    level,
    level_timestamp,
    id,
    tx_context_id,
    deleted
    {% call unfold(columns, "", true) %}
FROM (
    SELECT
        DENSE_RANK() OVER (
            ORDER BY
                ctx.level,
                ctx.operation_group_number,
                ctx.operation_number,
                ctx.content_number,
                COALESCE(ctx.internal_number, -1)
        ) AS ordering,
        ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.id,
        t.tx_context_id,
        t.deleted
        {% call unfold(columns, "t", true) %}
    FROM (
        SELECT
            t.tx_context_id,
            t.id,
            t.deleted
            {% call unfold(columns, "t", true) %}
        FROM "{{ contract_schema }}"."{{ table }}" t
        WHERE t.tx_context_id IN ({% call unfold(tx_context_ids, "", false) %})

        UNION ALL

        SELECT
            t.tx_context_id,
            t.id,
            'true' AS deleted
            {% call unfold(columns, "t", true) %}
        FROM (
            SELECT DISTINCT ON({% call unfold(indices, "t", false) %})
                bigmap_meta.tx_context_id,
                LAST_VALUE(t.id) OVER w as id,
                LAST_VALUE(t.deleted) OVER w as latest_deleted
              {%- for col in columns %}
                , LAST_VALUE(t.{{ col }}) OVER w as {{ col }}
              {%- endfor %}
            FROM "{{ main_schema }}".bigmap_meta_actions bigmap_meta
            JOIN "{{ contract_schema }}"."{{ table }}" t
              ON t.bigmap_id = bigmap_meta.bigmap_id
            JOIN "{{ main_schema }}".tx_contexts ctx
              ON ctx.id = t.tx_context_id
            WHERE bigmap_meta.tx_context_id IN ({% call unfold(tx_context_ids, "", false) %})
              AND bigmap_meta.action = 'clear'
            WINDOW w AS (
                PARTITION BY ({% call unfold(indices, "t", false) %})
                ORDER BY
                    ctx.level,
                    ctx.operation_group_number,
                    ctx.operation_number,
                    ctx.content_number,
                    COALESCE(ctx.internal_number, -1)
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        ) t
        LEFT JOIN "{{ contract_schema }}"."{{ table }}" t2
          ON  t2.tx_context_id = t.tx_context_id
        {%- for idx in indices %}
          AND t.{{ idx }} = t2.{{ idx }}
        {% endfor %}
        WHERE NOT t.latest_deleted
          AND t2 IS NULL
    ) t  -- t with bigmap clears unfolded
    JOIN "{{ main_schema }}".tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN "{{ main_schema }}".levels level_meta
      ON level_meta.level = ctx.level
) t;
