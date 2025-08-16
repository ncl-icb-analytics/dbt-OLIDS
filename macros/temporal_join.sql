{% macro temporal_join(fact_table, fact_date_column, dimension_table, join_key='person_id') %}
  {{- fact_table }} f
  INNER JOIN {{ dimension_table }} d
    ON f.{{ join_key }} = d.{{ join_key }}
    AND f.{{ fact_date_column }} >= d.effective_start_date
    AND (d.effective_end_date IS NULL OR f.{{ fact_date_column }} < d.effective_end_date)
{% endmacro %}