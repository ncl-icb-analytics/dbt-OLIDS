{% macro archive_definitions() %}
  {#- SCD Type 2 with daily granularity - only track changes across different days -#}
  
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
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    archived_at TIMESTAMP,
    archived_by STRING,
    dbt_run_id STRING
  );

  -- Add table comment using existing macro pattern
  COMMENT ON TABLE {{ history_table }} IS '{{ generate_history_table_comment("def_indicator", "Historical versions of indicator definitions with daily granularity. Tracks changes to indicator definitions across different days only. Multiple runs on the same day update the existing record rather than creating new versions.") }}';

  -- Step 1: Close out any existing records for indicators that have changed today
  UPDATE {{ history_table }}
  SET 
    valid_to = DATEADD(DAY, -1, CURRENT_DATE()),
    is_current = FALSE
  WHERE is_current = TRUE
    AND valid_to IS NULL
    AND indicator_id IN (
      SELECT DISTINCT indicator_id 
      FROM {{ this }}
    )
    AND EXISTS (
      -- Check if the current definition differs from history
      SELECT 1 
      FROM {{ this }} curr
      WHERE curr.indicator_id = {{ history_table }}.indicator_id
        AND (
          curr.indicator_type != {{ history_table }}.indicator_type
          OR COALESCE(curr.category, '') != COALESCE({{ history_table }}.category, '')
          OR COALESCE(curr.clinical_domain, '') != COALESCE({{ history_table }}.clinical_domain, '')
          OR curr.name_short != {{ history_table }}.name_short
          OR curr.description_short != {{ history_table }}.description_short
          OR curr.description_long != {{ history_table }}.description_long
          OR curr.source_model != {{ history_table }}.source_model
          OR curr.source_column != {{ history_table }}.source_column
          OR COALESCE(curr.is_qof, FALSE) != COALESCE({{ history_table }}.is_qof, FALSE)
          OR COALESCE(curr.qof_indicator, '') != COALESCE({{ history_table }}.qof_indicator, '')
          OR curr.sort_order != {{ history_table }}.sort_order
        )
    );

  -- Step 2: Merge - Update existing records for today OR insert new ones
  MERGE INTO {{ history_table }} AS hist
  USING {{ this }} AS curr
  ON hist.indicator_id = curr.indicator_id
    AND hist.valid_from = CURRENT_DATE()
    AND hist.is_current = TRUE
  WHEN MATCHED THEN UPDATE SET
    indicator_type = curr.indicator_type,
    category = curr.category,
    clinical_domain = curr.clinical_domain,
    name_short = curr.name_short,
    description_short = curr.description_short,
    description_long = curr.description_long,
    source_model = curr.source_model,
    source_column = curr.source_column,
    is_qof = curr.is_qof,
    qof_indicator = curr.qof_indicator,
    sort_order = curr.sort_order,
    metadata_extracted_at = curr.metadata_extracted_at,
    archived_at = CURRENT_TIMESTAMP(),
    archived_by = '{{ target.user }}',
    dbt_run_id = '{{ invocation_id }}'
  WHEN NOT MATCHED THEN INSERT (
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
    version_number,
    valid_from,
    valid_to,
    is_current,
    archived_at,
    archived_by,
    dbt_run_id
  ) VALUES (
    curr.indicator_id,
    curr.indicator_type,
    curr.category,
    curr.clinical_domain,
    curr.name_short,
    curr.description_short,
    curr.description_long,
    curr.source_model,
    curr.source_column,
    curr.is_qof,
    curr.qof_indicator,
    curr.sort_order,
    curr.metadata_extracted_at,
    COALESCE(
      (SELECT MAX(version_number) + 1 
       FROM {{ history_table }} h 
       WHERE h.indicator_id = curr.indicator_id),
      1
    ),
    CURRENT_DATE(),
    NULL,
    TRUE,
    CURRENT_TIMESTAMP(),
    '{{ target.user }}',
    '{{ invocation_id }}'
  );

  -- Step 3: Close out records for indicators that no longer exist
  UPDATE {{ history_table }}
  SET 
    valid_to = DATEADD(DAY, -1, CURRENT_DATE()),
    is_current = FALSE
  WHERE is_current = TRUE
    AND valid_to IS NULL
    AND indicator_id NOT IN (
      SELECT indicator_id FROM {{ this }}
    );

{% endmacro %}

{% macro archive_usage() %}
  {#- SCD Type 2 with daily granularity for usage contexts -#}
  
  {% set history_table = this.schema ~ '.def_indicator_usage_history' %}
  
  CREATE TABLE IF NOT EXISTS {{ history_table }} (
    indicator_id STRING,
    usage_context STRING,
    metadata_extracted_at TIMESTAMP,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    archived_at TIMESTAMP,
    dbt_run_id STRING
  );

  -- Add table comment using existing macro pattern
  COMMENT ON TABLE {{ history_table }} IS '{{ generate_history_table_comment("def_indicator_usage", "Historical tracking of indicator usage contexts with daily granularity. Tracks where indicators have been used over time. Multiple runs on the same day update the existing record.") }}';

  -- Close out removed usage contexts
  UPDATE {{ history_table }}
  SET 
    valid_to = DATEADD(DAY, -1, CURRENT_DATE()),
    is_current = FALSE
  WHERE is_current = TRUE
    AND valid_to IS NULL
    AND (indicator_id, usage_context) NOT IN (
      SELECT indicator_id, usage_context FROM {{ this }}
    );

  -- Merge - Update existing records for today OR insert new ones
  MERGE INTO {{ history_table }} AS hist
  USING (
    SELECT DISTINCT
      indicator_id,
      usage_context,
      metadata_extracted_at
    FROM {{ this }}
  ) AS curr
  ON hist.indicator_id = curr.indicator_id
    AND hist.usage_context = curr.usage_context
    AND hist.valid_from = CURRENT_DATE()
    AND hist.is_current = TRUE
  WHEN MATCHED THEN UPDATE SET
    metadata_extracted_at = curr.metadata_extracted_at,
    archived_at = CURRENT_TIMESTAMP(),
    dbt_run_id = '{{ invocation_id }}'
  WHEN NOT MATCHED THEN INSERT (
    indicator_id,
    usage_context,
    metadata_extracted_at,
    valid_from,
    valid_to,
    is_current,
    archived_at,
    dbt_run_id
  ) VALUES (
    curr.indicator_id,
    curr.usage_context,
    curr.metadata_extracted_at,
    CURRENT_DATE(),
    NULL,
    TRUE,
    CURRENT_TIMESTAMP(),
    '{{ invocation_id }}'
  );

{% endmacro %}

{% macro archive_codes() %}
  {#- SCD Type 2 with daily granularity - only capture genuine changes -#}
  
  {% set history_table = this.schema ~ '.def_indicator_codes_history' %}
  
  CREATE TABLE IF NOT EXISTS {{ history_table }} (
    indicator_id STRING,
    cluster_id STRING,
    code_category STRING,
    code_system STRING,
    code STRING,
    code_description STRING,
    metadata_extracted_at TIMESTAMP,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    archived_at TIMESTAMP,
    dbt_run_id STRING
  );

  -- Close out records for codes that no longer exist
  UPDATE {{ history_table }}
  SET 
    valid_to = DATEADD(DAY, -1, CURRENT_DATE()),
    is_current = FALSE
  WHERE is_current = TRUE
    AND valid_to IS NULL
    AND (indicator_id, cluster_id, code) NOT IN (
      SELECT indicator_id, cluster_id, code FROM {{ this }}
    );

  -- Insert records that don't have a current match in history
  INSERT INTO {{ history_table }}
  SELECT 
    curr.indicator_id,
    curr.cluster_id,
    curr.code_category,
    curr.code_system,
    curr.code,
    curr.code_description,
    curr.metadata_extracted_at,
    CURRENT_DATE() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS archived_at,
    '{{ invocation_id }}' AS dbt_run_id
  FROM {{ this }} curr
  WHERE NOT EXISTS (
    SELECT 1 FROM {{ history_table }} hist
    WHERE hist.indicator_id = curr.indicator_id
      AND hist.cluster_id = curr.cluster_id
      AND hist.code = curr.code
      AND hist.is_current = TRUE
  );

  -- Close out records that have changed
  UPDATE {{ history_table }}
  SET 
    valid_to = DATEADD(DAY, -1, CURRENT_DATE()),
    is_current = FALSE
  WHERE is_current = TRUE
    AND valid_to IS NULL
    AND EXISTS (
      SELECT 1 FROM {{ this }} curr
      WHERE curr.indicator_id = {{ history_table }}.indicator_id
        AND curr.cluster_id = {{ history_table }}.cluster_id
        AND curr.code = {{ history_table }}.code
        AND (
          curr.code_category != {{ history_table }}.code_category  
          OR curr.code_system != {{ history_table }}.code_system
          OR COALESCE(curr.code_description, '') != COALESCE({{ history_table }}.code_description, '')
        )
    );

{% endmacro %}

{% macro archive_thresholds() %}
  {#- SCD Type 2 with daily granularity - only capture genuine changes -#}
  
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
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    archived_at TIMESTAMP,
    dbt_run_id STRING
  );

  -- Close out records for thresholds that no longer exist
  UPDATE {{ history_table }}
  SET 
    valid_to = DATEADD(DAY, -1, CURRENT_DATE()),
    is_current = FALSE
  WHERE is_current = TRUE
    AND valid_to IS NULL
    AND (indicator_id, population_group, threshold_type) NOT IN (
      SELECT indicator_id, population_group, threshold_type FROM {{ this }}
    );

  -- Insert new records for thresholds that have changed or are new
  INSERT INTO {{ history_table }}
  SELECT DISTINCT
    curr.indicator_id,
    curr.population_group,
    curr.threshold_type,
    curr.threshold_value,
    curr.threshold_operator,
    curr.threshold_unit,
    curr.description,
    curr.sort_order,
    curr.metadata_extracted_at,
    CURRENT_DATE() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS archived_at,
    '{{ invocation_id }}' AS dbt_run_id
  FROM {{ this }} curr
  WHERE NOT EXISTS (
    SELECT 1 FROM {{ history_table }} hist
    WHERE hist.indicator_id = curr.indicator_id
      AND hist.population_group = curr.population_group
      AND hist.threshold_type = curr.threshold_type
      AND hist.is_current = TRUE
      AND hist.threshold_value = curr.threshold_value
      AND hist.threshold_operator = curr.threshold_operator
      AND hist.threshold_unit = curr.threshold_unit
      AND COALESCE(hist.description, '') = COALESCE(curr.description, '')
      AND hist.sort_order = curr.sort_order
  );

  -- Close out records that have changed
  UPDATE {{ history_table }}
  SET 
    valid_to = DATEADD(DAY, -1, CURRENT_DATE()),
    is_current = FALSE
  WHERE is_current = TRUE
    AND valid_to IS NULL
    AND EXISTS (
      SELECT 1 FROM {{ this }} curr
      WHERE curr.indicator_id = {{ history_table }}.indicator_id
        AND curr.population_group = {{ history_table }}.population_group
        AND curr.threshold_type = {{ history_table }}.threshold_type
        AND (
          curr.threshold_value != {{ history_table }}.threshold_value
          OR curr.threshold_operator != {{ history_table }}.threshold_operator
          OR curr.threshold_unit != {{ history_table }}.threshold_unit
          OR COALESCE(curr.description, '') != COALESCE({{ history_table }}.description, '')
          OR curr.sort_order != {{ history_table }}.sort_order
        )
    );

{% endmacro %}