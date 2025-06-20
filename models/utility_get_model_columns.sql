/*
Utility model to extract column information for all models in the project.
This generates JSON output that can be consumed by external scripts.
Uses information_schema to dynamically discover all models.
*/

{% set model_columns = get_all_model_columns() %}

SELECT 
  '{{ model_columns | tojson | replace("'", "''") }}' as model_columns_json 