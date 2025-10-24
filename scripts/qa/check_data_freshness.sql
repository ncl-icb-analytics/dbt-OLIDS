-- Check data freshness by finding max lds_start_date_time for each table
-- Identifies the most recent data loaded into OLIDS_MASKED tables

USE DATABASE "Data_Store_OLIDS_Alpha";

EXECUTE IMMEDIATE $$
DECLARE
    union_query VARCHAR;
    res RESULTSET;
BEGIN
    -- Build query to get max lds_start_date_time for each table
    SELECT LISTAGG(query_text, ' UNION ALL ')
    INTO :union_query
    FROM (
        SELECT
            'SELECT ''' || table_name || ''' AS TABLE_NAME, MAX(lds_start_date_time) AS MAX_DATE FROM '
            || table_schema || '.' || table_name AS query_text
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema LIKE ('OLIDS%')
            AND table_type = 'BASE TABLE'
        ORDER BY table_name
    );

    -- Execute and return results
    res := (EXECUTE IMMEDIATE :union_query);
    RETURN TABLE(res);
END;
$$;
