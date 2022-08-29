{% macro unfold(column_names, from_table, sep_first) %}
    {%- for col in column_names -%}
        {%- if sep_first.clone() || !loop.first %}, {% endif -%}
        {% if !from_table.is_empty() %}{{ from_table }}.{% endif %}{{ col }}
    {%- endfor -%}
{% endmacro %}


CREATE OR REPLACE FUNCTION "{{ contract_schema }}"."{{ table }}_at_deref"(lvl INT, op_grp INT, op INT, content INT, internal INT) RETURNS TABLE ({% call unfold(typed_columns, "", false) %})
AS $$
DECLARE
  bigmap_id INT;
  bigmap_target INT;
  source RECORD;
  source_schema TEXT;
BEGIN
  FOR bigmap_id, {% call unfold(columns, "", false) %} IN
    SELECT t.bigmap_id, {% call unfold(columns, "t", false) %} FROM "{{ contract_schema }}"."{{ table }}_at"(lvl, op_grp, op, content, internal) AS t ORDER BY t.bigmap_id
  LOOP
    IF bigmap_id IS NULL THEN
      in_schema := '{{ contract_schema }}';
      in_table := '{{ table }}';
      RETURN NEXT;
    ELSE
      bigmap_target := bigmap_id;

      SELECT
        value->>'contract_address' AS address,
        value->>'table' AS "table"
      INTO source
      FROM "{{ main_schema }}".bigmap_meta_actions AS meta
      WHERE action = 'alloc'
        AND meta.bigmap_id = (
          SELECT (value->'source')::INT
          FROM "{{ main_schema }}".bigmap_meta_actions AS meta
          WHERE meta.action = 'copy'
            AND meta.bigmap_id = bigmap_target
            AND tx_context_id = (
              SELECT ctx.id
              FROM "{{ main_schema }}".tx_contexts AS ctx
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
              ORDER BY ctx.level DESC, ctx.operation_group_number DESC, ctx.operation_number DESC, ctx.content_number DESC, ctx.internal_number DESC
              LIMIT 1
            )
      );

      SELECT name
      INTO source_schema
      FROM "{{ main_schema }}".contracts
      WHERE address = source.address;

      in_schema := source_schema;
      in_table := source."table";

      FOR id IN
        EXECUTE 'SELECT id FROM ' || quote_ident(source_schema) || '.' || quote_ident(source."table" || '_at') || '($1, $2, $3, $4, $5)'
        USING lvl, op_grp, op, content, internal
      LOOP
        RETURN NEXT;
      END LOOP;
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
