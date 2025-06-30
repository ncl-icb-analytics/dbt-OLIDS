
USE DATABASE "DATA_LAB_OLIDS_UAT";
SELECT 
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'REFERENCE'
ORDER BY table_name, ordinal_position;
        