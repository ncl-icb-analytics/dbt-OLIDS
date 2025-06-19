-- Replacement for legacy/codesets/mapped_concepts.sql with deduplication
-- This eliminates concept codes having multiple descriptions which cause duplicates
-- Uses COALESCE with priority order to pick consistent description per concept code
-- Run this to replace the existing MAPPED_CONCEPTS dynamic table

CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS (
    SOURCE_CODE_ID VARCHAR,
    ORIGINATING_SOURCE_TABLE VARCHAR(255), -- From SOURCE_CONCEPT_ORIGINS
    CONCEPT_ID VARCHAR,                    -- Assuming numeric ID from CONCEPT table
    CONCEPT_SYSTEM VARCHAR,
    CONCEPT_CODE VARCHAR,
    CONCEPT_DISPLAY VARCHAR,
    CLUSTER_ID VARCHAR,
    CLUSTER_DESCRIPTION VARCHAR,
    CODE_DESCRIPTION VARCHAR,
    SOURCE VARCHAR                        -- From COMBINED_CODESETS (e.g., PCD, UKHSA_COVID)
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH base_data AS (
    SELECT
        MAP."source_code_id"        AS SOURCE_CODE_ID,
        SCO.ORIGINATING_SOURCE_TABLE AS ORIGINATING_SOURCE_TABLE,
        CON."id"                    AS CONCEPT_ID,
        CON."system"                AS CONCEPT_SYSTEM,
        CON."code"                  AS CONCEPT_CODE,
        CON."display"               AS CONCEPT_DISPLAY,
        CCS.CLUSTER_ID              AS CLUSTER_ID,
        CCS.CLUSTER_DESCRIPTION     AS CLUSTER_DESCRIPTION,
        CCS.CODE_DESCRIPTION        AS CODE_DESCRIPTION,
        CCS.SOURCE                  AS SOURCE
    FROM
        "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT_MAP AS MAP
    -- Left join to the new intermediate table to find the originating table(s)
    LEFT JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.SOURCE_CONCEPT_ORIGINS AS SCO
        ON MAP."source_code_id" = SCO.SOURCE_CODE_ID_VALUE
    -- Join to get the target concept details
    JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS CON
        ON MAP."target_code_id" = CON."id"
    -- Left join to enrich with combined codeset details
    LEFT JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.COMBINED_CODESETS AS CCS
        ON CAST(CON."code" AS VARCHAR) = CAST(CCS.CODE AS VARCHAR)
),
deduplicated_descriptions AS (
    SELECT
        SOURCE_CODE_ID,
        ORIGINATING_SOURCE_TABLE,
        CONCEPT_ID,
        CONCEPT_SYSTEM,
        CONCEPT_CODE,
        CONCEPT_DISPLAY,
        CLUSTER_ID,
        CLUSTER_DESCRIPTION,
        SOURCE,
        -- Pick one consistent description per concept code within each cluster
        -- Priority: PCD > UKHSA_COVID > UKHSA_FLU > others, then alphabetically
        FIRST_VALUE(CODE_DESCRIPTION) OVER (
            PARTITION BY SOURCE_CODE_ID, CONCEPT_CODE, CLUSTER_ID 
            ORDER BY 
                CASE 
                    WHEN SOURCE = 'PCD' THEN 1
                    WHEN SOURCE = 'UKHSA_COVID' THEN 2
                    WHEN SOURCE = 'UKHSA_FLU' THEN 3
                    ELSE 4
                END,
                CODE_DESCRIPTION
        ) AS CODE_DESCRIPTION
    FROM base_data
)
SELECT DISTINCT
    SOURCE_CODE_ID,
    ORIGINATING_SOURCE_TABLE,
    CONCEPT_ID,
    CONCEPT_SYSTEM,
    CONCEPT_CODE,
    CONCEPT_DISPLAY,
    CLUSTER_ID,
    CLUSTER_DESCRIPTION,
    CODE_DESCRIPTION,
    SOURCE
FROM deduplicated_descriptions; 