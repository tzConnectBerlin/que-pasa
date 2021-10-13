-- update based on newly processed block (note: _must_ be a *newer* block)

{% macro unfold(column_names, from_table, sep_first) %}
    {%- for col in column_names -%}
        {%- if sep_first.clone() || !loop.first %}, {% endif -%}
        {% if !from_table.is_empty() %}{{ from_table }}.{% endif %}{{ col }}
    {%- endfor -%}
{% endmacro %}


DELETE FROM "{{ contract_schema }}"."{{ table }}_live";
INSERT INTO "{{ contract_schema }}"."{{ table }}_live" (
    level, level_timestamp, id, tx_context_id {% call unfold(columns, "", true) %}
)
SELECT
    *
FROM (
    SELECT
        ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.id,
        t.tx_context_id
        {% call unfold(columns, "t", true) %}
    FROM "{{ contract_schema }}"."{{ table }}" t
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
    WHERE t.tx_context_id IN ({% call unfold(tx_context_ids, "", false) %})
    ORDER BY
        ctx.operation_group_number DESC,
        ctx.operation_number DESC,
        ctx.content_number DESC,
        COALESCE(ctx.internal_number, -2) DESC
    LIMIT 1
) t;


INSERT INTO "{{ contract_schema }}"."{{ table }}_ordered" (
    ordering, level, level_timestamp, id, tx_context_id {% call unfold(columns, "", true) %}
)
SELECT
    ordering + (SELECT max(ordering) FROM "{{ contract_schema }}"."{{ table }}_ordered") as ordering,
    level,
    level_timestamp,
    id,
    tx_context_id
    {% call unfold(columns, "", true) %}
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
        t.tx_context_id
        {% call unfold(columns, "t", true) %}
    FROM "{{ contract_schema }}"."{{ table }}" t
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN levels level_meta
      ON level_meta.level = ctx.level
    WHERE t.tx_context_id IN ({% call unfold(tx_context_ids, "", false) %})
) t;
