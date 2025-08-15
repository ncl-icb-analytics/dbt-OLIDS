-- Dynamically generated metadata query from source_mappings.yml
-- Query all databases and schemas defined in your source configuration
--
-- Usage:
--   1. Copy this entire query
--   2. Paste into Snowflake UI
--   3. Execute query
--   4. Export results as CSV to table_metadata.csv

WITH schema_metadata AS (
    -- reference: Reference data including terminologies, rulesets, and population health lookups
  SELECT 
    'DATA_LAB_OLIDS_UAT' as database_name,
    'REFERENCE' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "DATA_LAB_OLIDS_UAT".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'REFERENCE'
  
  UNION ALL
  
    -- dictionary: Reference data including lookups and terminology mappings
  SELECT 
    'Dictionary' as database_name,
    'dbo' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'dbo'
  
  UNION ALL
  
    -- olids_core: Core OLIDS patient and clinical data
  SELECT 
    'Data_Store_OLIDS_UAT' as database_name,
    'OLIDS_MASKED' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Data_Store_OLIDS_UAT".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'OLIDS_MASKED'
  
  UNION ALL
  
    -- olids_terminology: OLIDS-specific terminology and code mappings
  SELECT 
    'Data_Store_OLIDS_UAT' as database_name,
    'OLIDS_TERMINOLOGY' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Data_Store_OLIDS_UAT".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'OLIDS_TERMINOLOGY'
)

SELECT 
  database_name as "DATABASE_NAME",
  schema_name as "SCHEMA_NAME", 
  table_name as "TABLE_NAME",
  column_name as "COLUMN_NAME",
  data_type as "DATA_TYPE",
  ordinal_position as "ORDINAL_POSITION"
FROM schema_metadata
ORDER BY database_name, schema_name, table_name, ordinal_position;