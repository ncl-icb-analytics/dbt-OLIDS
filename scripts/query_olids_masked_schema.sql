
USE DATABASE "Data_Store_OLIDS_UAT";
SELECT 
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_MASKED'
ORDER BY table_name, ordinal_position;
        