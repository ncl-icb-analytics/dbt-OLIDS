-- Check last DML (INSERT/UPDATE/DELETE) timestamps by schema
-- Uses QUERY_HISTORY from ACCOUNT_USAGE to track data modifications
-- Note: ACCOUNT_USAGE has latency of 45 minutes to 3 hours
-- Requires BI role which has access to ACCOUNT_USAGE QUERY_HISTORY

USE ROLE BI;

SELECT
    database_name,
    schema_name,
    query_type,
    MAX(end_time) AS last_dml_timestamp,
    DATEDIFF('hour', MAX(end_time), CURRENT_TIMESTAMP()) AS hours_since_last_dml,
    COUNT(*) AS dml_operation_count,
    SUM(rows_inserted) AS total_rows_inserted,
    SUM(rows_updated) AS total_rows_updated,
    SUM(rows_deleted) AS total_rows_deleted
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name IN (
        'DATA_LAB_OLIDS_NCL',
        'Data_Store_OLIDS_Alpha',
        'Data_Store_OLIDS_Alpha_Clone_06102025',
        'Data_Store_OLIDS_Alpha_Clone_15102025',
        'Data_Store_OLIDS_Alpha_Clone_19092025',
        'Data_Store_OLIDS_Clinical_Validation',
        'Data_Store_OLIDS_Dummy',
        'Data_Store_OLIDS_UAT'
    )
    AND query_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'COPY', 'CREATE', 'CREATE_TABLE')
    AND execution_status = 'SUCCESS'
    AND start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY ALL
ORDER BY database_name, schema_name, query_type;