{% macro check_source_columns() %}
  {% set query %}
    SELECT table_name
    FROM "Data_Store_OLIDS_Alpha".INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'OLIDS_MASKED'
    ORDER BY table_name
  {% endset %}
  
  {% set table_results = run_query(query) %}
  
  {% if execute %}
    {% for table_row in table_results %}
      {% set table_name = table_row[0] %}
      {% set column_query %}
        SELECT column_name
        FROM "Data_Store_OLIDS_Alpha".INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = 'OLIDS_MASKED'
          AND table_name = '{{ table_name }}'
        ORDER BY ordinal_position
      {% endset %}
      
      {% set column_results = run_query(column_query) %}
      
      {{ log('Table: ' ~ table_name, info=True) }}
      {% for column_row in column_results %}
        {{ log('  - ' ~ column_row[0], info=True) }}
      {% endfor %}
      {{ log('', info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}