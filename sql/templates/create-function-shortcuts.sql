{% macro unfold(column_names, from_table, sep_first) %}
    {%- for col in column_names -%}
        {%- if sep_first.clone() || !loop.first %}, {% endif -%}
        {% if !from_table.is_empty() %}{{ from_table }}.{% endif %}{{ col }}
    {%- endfor -%}
{% endmacro %}


CREATE OR REPLACE FUNCTION "{{ contract_schema }}"."{{ table }}_{{ function_postfix }}"(lvl INT) RETURNS TABLE ({% call unfold(typed_columns, "", false) %})
AS $$
  SELECT
    *
  FROM (
    SELECT
      "{{ contract_schema }}"."{{ table }}_{{ function_postfix }}"(ctx.level, ctx.operation_group_number, ctx.operation_number, ctx.content_number, ctx.internal_number)
    FROM "{{ main_schema }}".last_context_at(lvl) AS ctx
  ) q
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "{{ contract_schema }}"."{{ table }}_{{ function_postfix }}"(lvl INT, op_grp INT) RETURNS TABLE ({% call unfold(typed_columns, "", false) %})
AS $$
  SELECT
    "{{ contract_schema }}"."{{ table }}_{{ function_postfix }}"(ctx.level, ctx.operation_group_number, ctx.operation_number, ctx.content_number, ctx.internal_number)
  FROM "{{ main_schema }}".last_context_at(lvl, op_grp) AS ctx
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "{{ contract_schema }}"."{{ table }}_{{ function_postfix }}"(lvl INT, op_grp INT, op INT) RETURNS TABLE ({% call unfold(typed_columns, "", false) %})
AS $$
  SELECT
    "{{ contract_schema }}"."{{ table }}_{{ function_postfix }}"(ctx.level, ctx.operation_group_number, ctx.operation_number, ctx.content_number, ctx.internal_number)
  FROM "{{ main_schema }}".last_context_at(lvl, op_grp, op) AS ctx
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "{{ contract_schema }}"."{{ table }}_{{ function_postfix }}"(lvl INT, op_grp INT, op INT, content INT) RETURNS TABLE ({% call unfold(typed_columns, "", false) %})
AS $$
  SELECT
    "{{ contract_schema }}"."{{ table }}_{{ function_postfix }}"(ctx.level, ctx.operation_group_number, ctx.operation_number, ctx.content_number, ctx.internal_number)
  FROM "{{ main_schema }}".last_context_at(lvl, op_grp, op, content) AS ctx
$$ LANGUAGE SQL;
