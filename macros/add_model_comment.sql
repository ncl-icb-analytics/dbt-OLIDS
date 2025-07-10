{% macro add_model_comment() %}
  {%- if execute -%}
    {%- set materialization = config.get('materialized') -%}
    {%- if materialization == 'view' -%}
      COMMENT ON VIEW {{ this }} IS '{{ generate_table_comment() }}'
    {%- elif materialization == 'materialized_view' -%}
      COMMENT ON MATERIALIZED VIEW {{ this }} IS '{{ generate_table_comment() }}'
    {%- elif materialization in ('table', 'incremental') -%}
      COMMENT ON TABLE {{ this }} IS '{{ generate_table_comment() }}'
    {%- elif materialization == 'ephemeral' -%}
      -- Ephemeral models don't create database objects, so no comment needed
      SELECT 1 WHERE FALSE
    {%- else -%}
      -- Default to table comment for unknown materializations
      COMMENT ON TABLE {{ this }} IS '{{ generate_table_comment() }}'
    {%- endif -%}
  {%- endif -%}
{% endmacro %}