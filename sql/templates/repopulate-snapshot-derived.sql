-- repopulate

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
        last_ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.id,
        t.tx_context_id
        {% call unfold(columns, "t", true) %}
    FROM "{{ contract_schema }}"."{{ table }}" t, (
      SELECT
        ctx.id,
        ctx.level
      FROM "{{ contract_schema }}"."{{ parent_table }}" t
      JOIN "{{ main_schema }}".tx_contexts ctx
        ON ctx.id = t.tx_context_id
      ORDER BY
          ctx.level DESC,
          ctx.operation_group_number DESC,
          ctx.operation_number DESC,
          ctx.content_number DESC,
          COALESCE(ctx.internal_number, -1) DESC
      LIMIT 1
    ) last_ctx
    JOIN "{{ main_schema }}".levels level_meta
      ON level_meta.level = last_ctx.level
    WHERE t.tx_context_id = last_ctx.id
) q;


DELETE FROM "{{ contract_schema }}"."{{ table }}_ordered";
INSERT INTO "{{ contract_schema }}"."{{ table }}_ordered" (
    ordering, level, level_timestamp, id, tx_context_id {% call unfold(columns, "", true) %}
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
                COALESCE(ctx.internal_number, -1)
        ) AS ordering,
        ctx.level AS level,
        level_meta.baked_at AS level_timestamp,
        t.id,
        t.tx_context_id
        {% call unfold(columns, "t", true) %}
    FROM "{{ contract_schema }}"."{{ table }}" t
    JOIN "{{ main_schema }}".tx_contexts ctx
      ON ctx.id = t.tx_context_id
    JOIN "{{ main_schema }}".levels level_meta
      ON level_meta.level = ctx.level
) q;
