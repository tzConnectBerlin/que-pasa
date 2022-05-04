{% macro unfold(column_names, from_table, sep_first) %}
    {%- for col in column_names -%}
        {%- if sep_first.clone() || !loop.first %}, {% endif -%}
        {% if !from_table.is_empty() %}{{ from_table }}.{% endif %}{{ col }}
    {%- endfor -%}
{% endmacro %}


CREATE OR REPLACE FUNCTION "{{ contract_schema }}"."{{ table }}_at"(lvl INT, op_grp INT, op INT, content INT, internal INT) RETURNS TABLE ({% call unfold(typed_columns, "", false) %})
AS $$
  WITH latest_context_id AS (
    SELECT
      ctx.id AS tx_context_id
    FROM "{{ contract_schema }}"."{{ table }}" AS t
    JOIN que_pasa.tx_contexts ctx
      ON ctx.id = t.tx_context_id
    WHERE ARRAY[
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
    ORDER BY ctx.level DESC, ctx.operation_group_number DESC, ctx.operation_number DESC, ctx.content_number DESC, COALESCE(ctx.internal_number, -1) DESC
    LIMIT 1
  )
  SELECT
    {% call unfold(columns, "t", false) %}
  FROM "{{ contract_schema }}"."{{ table }}" AS t
  WHERE t.tx_context_id = (SELECT tx_context_id FROM latest_context_id)
$$ LANGUAGE SQL;
