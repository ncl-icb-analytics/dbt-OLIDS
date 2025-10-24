-- Check last DDL timestamps for tables in OLIDS_COMMON schema
-- Identifies when tables were last modified

USE DATABASE "Data_Store_OLIDS_Alpha";
USE ROLE "ISL-USERGROUP-SECONDEES-NCL";

EXECUTE IMMEDIATE $$
DECLARE
    union_query VARCHAR;
    res RESULTSET;
BEGIN
    -- Build query to get table names and last DDL timestamps
    SELECT LISTAGG(query_text, ' UNION ALL ')
    INTO :union_query
    FROM (
        SELECT
            'SELECT ''' || table_name || ''' AS TABLE_NAME, ''' || last_ddl || ''' AS LAST_DDL FROM '
            || 'INFORMATION_SCHEMA.TABLES WHERE table_schema = ''OLIDS_COMMON'' AND table_name = ''' || table_name || '''' AS query_text
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'OLIDS_COMMON'
            AND table_type = 'BASE TABLE'
        ORDER BY table_name
    );

    -- Execute and return results
    res := (EXECUTE IMMEDIATE :union_query);
    RETURN TABLE(res);
END;
$$;
