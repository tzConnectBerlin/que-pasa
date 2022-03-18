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
    JOIN que_pasa.tx_contexts ctx
      ON  ctx.id = t.tx_context_id
    WHERE ctx.contract = '{{ contract_schema }}'
      AND ARRAY[
            ctx.level,
            ctx.operation_group_number,
            ctx.operation_number,
            ctx.content_number,
            ctx.internal_number]
          <=
          ARRAY[
            lvl,
            op_grp,
            op,
            content,
            internal]
    WINDOW w AS (
        PARTITION BY ({% call unfold(indices, "t", false) %})
        ORDER BY t.ordering
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
  ) AS q
  WHERE NOT q.deleted
$$ LANGUAGE SQL;
