{% macro unfold(column_names, from_table, sep_first) %}
    {%- for col in column_names -%}
        {%- if sep_first.clone() || !loop.first %}, {% endif -%}
        {% if !from_table.is_empty() %}{{ from_table }}.{% endif %}{{ col }}
    {%- endfor -%}
{% endmacro %}


CREATE OR REPLACE FUNCTION "{{ contract_schema }}"."{{ table }}_at"(lvl INT, op_grp INT, op INT, content INT, internal INT) RETURNS TABLE ({% call unfold(typed_columns, "", false) %})
AS $$
  WITH contract_addr AS (
    SELECT address
    FROM que_pasa.contracts
    WHERE name = '{{ contract_schema }}'
  ), latest_context_id AS (
    SELECT ctx.id tx_context_id
    FROM que_pasa.tx_contexts ctx
    JOIN contract_addr
      ON contract_addr.address = ctx.contract
    JOIN "{{ contract_schema }}"."{{ table }}" AS t
      ON t.tx_context_id = ctx.id
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
    ORDER BY level DESC, operation_group_number DESC, operation_number DESC, content_number DESC, COALESCE(internal_number, -1) DESC
    LIMIT 1
  )
  SELECT DISTINCT
  {% call unfold(columns, "t", false) %}
  FROM "{{ contract_schema }}"."{{ table }}" AS t
  WHERE t.tx_context_id = (SELECT tx_context_id FROM latest_context_id)
$$ LANGUAGE SQL;
