{% macro archive_definitions() %}
  {#- Simple post-hook: Just append current state to history if not already there -#}
  
  {% set history_table = this.schema ~ '.def_indicator_history' %}
  
  -- Create history table if needed
  CREATE TABLE IF NOT EXISTS {{ history_table }} (
    indicator_id STRING,
    indicator_type STRING,
    category STRING,
    clinical_domain STRING,
    name_short STRING,
    description_short STRING,
    description_long STRING,
    source_model STRING,
    source_column STRING,
    is_qof BOOLEAN,
    qof_indicator STRING,
    sort_order STRING,
    metadata_extracted_at TIMESTAMP,
    version_number INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    archived_at TIMESTAMP,
    archived_by STRING,
    dbt_run_id STRING
  );

  -- Add table comment using existing macro pattern
  COMMENT ON TABLE {{ history_table }} IS '{{ generate_history_table_comment("def_indicator", "Historical versions of indicator definitions. Automatically maintained archive of all changes to indicator definitions. Each time an indicator definition changes, the previous version is preserved here with validity dates and version numbers.") }}';

  -- Insert current definitions if not already archived with same content
  INSERT INTO {{ history_table }}
  SELECT 
    indicator_id,
    indicator_type,
    category,
    clinical_domain,
    name_short,
    description_short,
    description_long,
    source_model,
    source_column,
    is_qof,
    qof_indicator,
    sort_order,
    metadata_extracted_at,
    1 AS version_number,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    CURRENT_TIMESTAMP() AS archived_at,
    '{{ target.user }}' AS archived_by,
    '{{ invocation_id }}' AS dbt_run_id
  FROM {{ this }} curr
  WHERE NOT EXISTS (
    SELECT 1 FROM {{ history_table }} h
    WHERE h.indicator_id = curr.indicator_id
    AND h.indicator_type = curr.indicator_type
    AND COALESCE(h.category, '') = COALESCE(curr.category, '')
    AND COALESCE(h.clinical_domain, '') = COALESCE(curr.clinical_domain, '')
    AND h.name_short = curr.name_short
    AND h.description_short = curr.description_short
    AND h.description_long = curr.description_long
    AND h.source_model = curr.source_model
    AND h.source_column = curr.source_column
    AND COALESCE(h.is_qof, FALSE) = COALESCE(curr.is_qof, FALSE)
    AND COALESCE(h.qof_indicator, '') = COALESCE(curr.qof_indicator, '')
  );

{% endmacro %}

{% macro archive_usage() %}
  {#- Simple post-hook for usage -#}
  
  {% set history_table = this.schema ~ '.def_indicator_usage_history' %}
  
  CREATE TABLE IF NOT EXISTS {{ history_table }} (
    indicator_id STRING,
    usage_context STRING,
    metadata_extracted_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    archived_at TIMESTAMP,
    dbt_run_id STRING
  );

  -- Add table comment using existing macro pattern
  COMMENT ON TABLE {{ history_table }} IS '{{ generate_history_table_comment("def_indicator_usage", "Historical tracking of indicator usage contexts. Archive of where indicators have been used over time. Tracks additions and removals of indicators from dashboards and reporting contexts.") }}';

  INSERT INTO {{ history_table }}
  SELECT 
    indicator_id,
    usage_context,
    metadata_extracted_at,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    CURRENT_TIMESTAMP() AS archived_at,
    '{{ invocation_id }}' AS dbt_run_id
  FROM {{ this }} curr
  WHERE NOT EXISTS (
    SELECT 1 FROM {{ history_table }} h
    WHERE h.indicator_id = curr.indicator_id
    AND h.usage_context = curr.usage_context
  );

{% endmacro %}

{% macro archive_codes() %}
  {#- Simple post-hook for codes -#}
  
  {% set history_table = this.schema ~ '.def_indicator_codes_history' %}
  
  CREATE TABLE IF NOT EXISTS {{ history_table }} (
    indicator_id STRING,
    cluster_id STRING,
    code_category STRING,
    code_system STRING,
    code STRING,
    code_description STRING,
    metadata_extracted_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    archived_at TIMESTAMP,
    dbt_run_id STRING
  );

  -- Add table comment using existing macro pattern
  COMMENT ON TABLE {{ history_table }} IS '{{ generate_history_table_comment("def_indicator_codes", "Historical versions of indicator code mappings. Archive of all SNOMED codes associated with indicators over time. Tracks changes to inclusion/exclusion/resolution criteria.") }}';

  INSERT INTO {{ history_table }}
  SELECT 
    indicator_id,
    cluster_id,
    code_category,
    code_system,
    code,
    code_description,
    metadata_extracted_at,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    CURRENT_TIMESTAMP() AS archived_at,
    '{{ invocation_id }}' AS dbt_run_id
  FROM {{ this }} curr
  WHERE NOT EXISTS (
    SELECT 1 FROM {{ history_table }} h
    WHERE h.indicator_id = curr.indicator_id
    AND h.code = curr.code
  );

{% endmacro %}

{% macro archive_thresholds() %}
  {#- Simple post-hook for thresholds table -#}
  
  {% set history_table = this.schema ~ '.def_indicator_thresholds_history' %}
  
  CREATE TABLE IF NOT EXISTS {{ history_table }} (
    indicator_id STRING,
    population_group STRING,
    threshold_type STRING,
    threshold_value STRING,
    threshold_operator STRING,
    threshold_unit STRING,
    description STRING,
    sort_order INT,
    metadata_extracted_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    archived_at TIMESTAMP,
    dbt_run_id STRING
  );

  -- Add table comment using existing macro pattern
  COMMENT ON TABLE {{ history_table }} IS '{{ generate_history_table_comment("def_indicator_thresholds", "Historical versions of indicator threshold definitions. Archive of population-specific targets, diagnostic criteria, and risk boundaries over time. Tracks changes to clinical guidelines and thresholds.") }}';

  INSERT INTO {{ history_table }}
  SELECT 
    indicator_id,
    population_group,
    threshold_type,
    threshold_value,
    threshold_operator,
    threshold_unit,
    description,
    sort_order,
    metadata_extracted_at,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    CURRENT_TIMESTAMP() AS archived_at,
    '{{ invocation_id }}' AS dbt_run_id
  FROM {{ this }} curr
  WHERE NOT EXISTS (
    SELECT 1 FROM {{ history_table }} h
    WHERE h.indicator_id = curr.indicator_id
    AND h.population_group = curr.population_group
    AND h.threshold_type = curr.threshold_type
    AND h.threshold_value = curr.threshold_value
    AND h.threshold_operator = curr.threshold_operator
  );

{% endmacro %}