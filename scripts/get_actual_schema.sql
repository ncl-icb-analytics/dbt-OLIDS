-- Script to get actual column names from Data_Store_OLIDS_UAT database
-- Run these queries in your Snowflake console to get actual schema info

-- 1. OLIDS_MASKED schema columns
USE DATABASE "Data_Store_OLIDS_UAT";

SELECT 
    'OLIDS_MASKED' AS source_schema,
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_MASKED'
ORDER BY table_name, ordinal_position;

-- 2. OLIDS_TERMINOLOGY schema columns  
SELECT 
    'OLIDS_TERMINOLOGY' AS source_schema,
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_TERMINOLOGY'
ORDER BY table_name, ordinal_position;

-- 3. REFERENCE schema columns (from target database)
USE DATABASE "DATA_LAB_OLIDS_UAT";

SELECT 
    'REFERENCE' AS source_schema,
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'REFERENCE'
ORDER BY table_name, ordinal_position;

-- 4. Check for specific tables we're having issues with
USE DATABASE "Data_Store_OLIDS_UAT";

-- Check LOCATION_CONTACT table specifically
SELECT column_name, data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_MASKED' 
  AND table_name = 'LOCATION_CONTACT'
ORDER BY ordinal_position;

-- Check MEDICATION_STATEMENT table specifically  
SELECT column_name, data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_MASKED' 
  AND table_name = 'MEDICATION_STATEMENT'
ORDER BY ordinal_position;

-- Check OBSERVATION table specifically
SELECT column_name, data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_MASKED' 
  AND table_name = 'OBSERVATION'
ORDER BY ordinal_position;

-- Check PERSON table specifically
SELECT column_name, data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_MASKED' 
  AND table_name = 'PERSON'
ORDER BY ordinal_position;