-- Compare row counts across all OLIDS database versions
-- Returns formatted row counts (K/M notation) for easy comparison
USE ROLE "ISL-USERGROUP-SECONDEES-NCL";
WITH all_rowcounts AS (
    SELECT
        'Stable' AS database_name,
        table_name,
        row_count
    FROM "DATA_LAB_OLIDS_NCL".INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_schema = 'OLIDS'
        AND table_name NOT LIKE '%_BACKUP'

    UNION ALL

    SELECT
        'Data_Store_OLIDS_Alpha' AS database_name,
        table_name,
        row_count
    FROM "Data_Store_OLIDS_Alpha".INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_BACKUP'

    UNION ALL

    SELECT
        'Data_Store_OLIDS_Alpha_Clone_06102025' AS database_name,
        table_name,
        row_count
    FROM "Data_Store_OLIDS_Alpha_Clone_06102025".INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_BACKUP'

    UNION ALL

    SELECT
        'Data_Store_OLIDS_Alpha_Clone_15102025' AS database_name,
        table_name,
        row_count
    FROM "Data_Store_OLIDS_Alpha_Clone_15102025".INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_BACKUP'

    UNION ALL

    SELECT
        'Data_Store_OLIDS_Alpha_Clone_19092025' AS database_name,
        table_name,
        row_count
    FROM "Data_Store_OLIDS_Alpha_Clone_19092025".INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_BACKUP'

    UNION ALL

    SELECT
        'Data_Store_OLIDS_Clinical_Validation' AS database_name,
        table_name,
        row_count
    FROM "Data_Store_OLIDS_Clinical_Validation".INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_BACKUP'

    UNION ALL

    SELECT
        'Data_Store_OLIDS_Dummy' AS database_name,
        table_name,
        row_count
    FROM "Data_Store_OLIDS_Dummy".INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_BACKUP'

    UNION ALL

    SELECT
        'Data_Store_OLIDS_UAT' AS database_name,
        table_name,
        row_count
    FROM "Data_Store_OLIDS_UAT".INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_BACKUP'
),
formatted_counts AS (
    SELECT
        table_name,
        database_name,
        CASE
            WHEN row_count >= 1000000 THEN ROUND(row_count / 1000000.0, 1) || 'M'
            WHEN row_count >= 1000 THEN ROUND(row_count / 1000.0, 1) || 'K'
            ELSE row_count::VARCHAR
        END AS formatted_count
    FROM all_rowcounts
)
SELECT
    table_name,
    MAX(CASE WHEN database_name = 'Stable' THEN formatted_count END) AS "Stable",
    MAX(CASE WHEN database_name = 'Data_Store_OLIDS_Alpha' THEN formatted_count END) AS "Alpha",
    MAX(CASE WHEN database_name = 'Data_Store_OLIDS_Alpha_Clone_15102025' THEN formatted_count END) AS "Clone_15102025",
    MAX(CASE WHEN database_name = 'Data_Store_OLIDS_Alpha_Clone_06102025' THEN formatted_count END) AS "Clone_06102025",
    MAX(CASE WHEN database_name = 'Data_Store_OLIDS_Alpha_Clone_19092025' THEN formatted_count END) AS "Clone_19092025",
    MAX(CASE WHEN database_name = 'Data_Store_OLIDS_Clinical_Validation' THEN formatted_count END) AS "Clinical_Validation",
    MAX(CASE WHEN database_name = 'Data_Store_OLIDS_UAT' THEN formatted_count END) AS "UAT",
    MAX(CASE WHEN database_name = 'Data_Store_OLIDS_Dummy' THEN formatted_count END) AS "Dummy"
FROM formatted_counts
GROUP BY table_name
ORDER BY table_name;
