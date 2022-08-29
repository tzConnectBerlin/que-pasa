{% macro unfold(column_names, from_table, sep_first) %}
    {%- for col in column_names -%}
        {%- if sep_first.clone() || !loop.first %}, {% endif -%}
        {% if !from_table.is_empty() %}{{ from_table }}.{% endif %}{{ col }}
    {%- endfor -%}
{% endmacro %}


CREATE OR REPLACE FUNCTION "{{ contract_schema }}"."{{ table }}_at"(lvl INT, op_grp INT, op INT, content INT, internal INT) RETURNS TABLE ({% call unfold(typed_columns, "", false) %})
AS $$
  SELECT
  {% call unfold(columns, "q", false) %}
  FROM (
    SELECT DISTINCT
      LAST_VALUE(t.deleted) OVER w AS deleted
    {%- for col in columns %}
      , LAST_VALUE(t.{{ col }}) OVER w AS {{ col }}
    {%- endfor %}
    FROM "{{ contract_schema }}"."{{ table }}_ordered" AS t
    CROSS JOIN "{{ main_schema }}".contracts AS contract
    JOIN "{{ main_schema }}".tx_contexts ctx
      ON  ctx.id = t.tx_context_id
      AND ctx.contract = contract.address
    WHERE contract.name = '{{ contract_schema }}'
      AND ARRAY[
            ctx.level,
            ctx.operation_group_number,
            ctx.operation_number,
            ctx.content_number,
            COALESCE(ctx.internal_number, -1)]
          <=
          ARRAY[
            lvl,
            op_grp,
            op,
            content,
            COALESCE(internal, -1)]
    WINDOW w AS (
        PARTITION BY ({% call unfold(indices, "t", false) %})
        ORDER BY t.ordering
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
  ) AS q
  WHERE NOT q.deleted
$$ LANGUAGE SQL;
