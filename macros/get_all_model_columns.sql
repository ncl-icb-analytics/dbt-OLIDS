{% macro get_all_model_columns() %}
  {# Get all models from the project using graph object #}
  {% set results = {} %}
  
  {% if execute %}
    {# Use dbt's run_query to get all model names from information_schema #}
    {% set query %}
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = '{{ target.schema }}'
      AND table_name NOT LIKE '%_dbt_tmp%'
      AND table_name NOT LIKE '%_backup%'
      ORDER BY table_name
    {% endset %}
    
    {% set model_results = run_query(query) %}
    {% set model_names = model_results.columns[0].values() %}
    
    {% for model_name in model_names %}
      {% set model_name_clean = model_name|string|lower %}
      {% set relation = api.Relation.create(database=target.database, schema=target.schema, identifier=model_name_clean) %}
      
      {# Check if this relation exists and get its columns #}
      {% set columns = adapter.get_columns_in_relation(relation) %}
      {% if columns %}
        {% set column_names = [] %}
        {% for col in columns %}
          {% set column_names = column_names.append(col.name|lower) %}
        {% endfor %}
        {% set _ = results.update({model_name_clean: column_names}) %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {{ return(results) }}
{% endmacro %} 