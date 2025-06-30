CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS (
    SOURCE_CODE_ID VARCHAR,
    ORIGINATING_SOURCE_TABLE VARCHAR(255), -- From SOURCE_CONCEPT_ORIGINS
    CONCEPT_ID VARCHAR,                    -- Assuming numeric ID from CONCEPT table
    CONCEPT_SYSTEM VARCHAR,
    CONCEPT_CODE VARCHAR,
    CONCEPT_DISPLAY VARCHAR,
    CLUSTER_ID VARCHAR,
    CLUSTER_DESCRIPTION VARCHAR,
    CODE_DESCRIPTION VARCHAR,
    SOURCE VARCHAR                        -- From COMBINED_REFERENCE (e.g., PCD, UKHSA_COVID)
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT
    MAP."source_code_id"        AS SOURCE_CODE_ID,
    SCO.ORIGINATING_SOURCE_TABLE AS ORIGINATING_SOURCE_TABLE, -- Joined from the new intermediate table
    CON."id"                    AS CONCEPT_ID,
    CON."system"                AS CONCEPT_SYSTEM,
    CON."code"                  AS CONCEPT_CODE,
    CON."display"               AS CONCEPT_DISPLAY,
    CCS.CLUSTER_ID              AS CLUSTER_ID,
    CCS.CLUSTER_DESCRIPTION     AS CLUSTER_DESCRIPTION,
    CCS.CODE_DESCRIPTION        AS CODE_DESCRIPTION,
    CCS.SOURCE                  AS SOURCE
FROM
    "Data_Store_OLIDS_UAT".OLIDS_TERMINOLOGY.CONCEPT_MAP AS MAP
-- Left join to the new intermediate table to find the originating table(s)
LEFT JOIN
    DATA_LAB_OLIDS_UAT.REFERENCE.SOURCE_CONCEPT_ORIGINS AS SCO
    ON MAP."source_code_id" = SCO.SOURCE_CODE_ID_VALUE -- Ensure datatypes are compatible for this join
-- Join to get the target concept details
JOIN
    "Data_Store_OLIDS_UAT".OLIDS_TERMINOLOGY.CONCEPT AS CON
    ON MAP."target_code_id" = CON."id"
-- Left join to enrich with combined codeset details
LEFT JOIN
    DATA_LAB_OLIDS_UAT.REFERENCE.COMBINED_CODESETS AS CCS
    ON CAST(CON."code" AS VARCHAR) = CAST(CCS.CODE AS VARCHAR);
