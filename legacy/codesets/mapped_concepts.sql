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

create or replace view DATA_LAB_OLIDS_UAT.TESTS.AGG_TEST_CONCEPT_MAPPING_FAILURE_DETAILS(
    SOURCE_TABLE_NAME,
    SOURCE_COLUMN_NAME,
    FAILURE_CATEGORY,
    SPECIFIC_FAILURE_COUNT,
    TOTAL_RECORDS_TESTED,
    TOTAL_SUCCESS_COUNT,
    OVERALL_FAILURE_PERCENTAGE
) as
WITH
-- Step 1: Calculate the total number of records being tested for each concept.
TotalRecords AS (
    SELECT 'PROCEDURE_REQUEST' AS SourceTable, 'status_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PROCEDURE_REQUEST" WHERE "status_concept_id" IS NOT NULL UNION ALL
    SELECT 'PATIENT_ADDRESS' AS SourceTable, 'address_type_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_ADDRESS" WHERE "address_type_concept_id" IS NOT NULL UNION ALL
    SELECT 'OBSERVATION' AS SourceTable, 'result_value_unit_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."OBSERVATION" WHERE "result_value_unit_concept_id" IS NOT NULL UNION ALL
    SELECT 'DIAGNOSTIC_ORDER' AS SourceTable, 'date_precision_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."DIAGNOSTIC_ORDER" WHERE "date_precision_concept_id" IS NOT NULL UNION ALL
    SELECT 'MEDICATION_STATEMENT' AS SourceTable, 'medication_statement_core_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."MEDICATION_STATEMENT" WHERE "medication_statement_core_concept_id" IS NOT NULL UNION ALL
    SELECT 'ALLERGY_INTOLERANCE' AS SourceTable, 'allergy_intolerance_core_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ALLERGY_INTOLERANCE" WHERE "allergy_intolerance_core_concept_id" IS NOT NULL UNION ALL
    SELECT 'APPOINTMENT' AS SourceTable, 'contact_mode_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."APPOINTMENT" WHERE "contact_mode_concept_id" IS NOT NULL UNION ALL
    SELECT 'PATIENT' AS SourceTable, 'gender_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" WHERE "gender_concept_id" IS NOT NULL UNION ALL
    SELECT 'MEDICATION_ORDER' AS SourceTable, 'medication_order_core_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."MEDICATION_ORDER" WHERE "medication_order_core_concept_id" IS NOT NULL UNION ALL
    SELECT 'PROCEDURE_REQUEST' AS SourceTable, 'date_precision_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PROCEDURE_REQUEST" WHERE "date_precision_concept_id" IS NOT NULL UNION ALL
    SELECT 'APPOINTMENT' AS SourceTable, 'booking_method_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."APPOINTMENT" WHERE "booking_method_concept_id" IS NOT NULL UNION ALL
    SELECT 'REFERRAL_REQUEST' AS SourceTable, 'referal_request_type_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."REFERRAL_REQUEST" WHERE "referal_request_type_concept_id" IS NOT NULL UNION ALL
    SELECT 'ENCOUNTER' AS SourceTable, 'date_precision_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ENCOUNTER" WHERE "date_precision_concept_id" IS NOT NULL UNION ALL
    SELECT 'ENCOUNTER' AS SourceTable, 'encounter_core_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ENCOUNTER" WHERE "encounter_core_concept_id" IS NOT NULL UNION ALL
    SELECT 'LOCATION_CONTACT' AS SourceTable, 'contact_type_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."LOCATION_CONTACT" WHERE "contact_type_concept_id" IS NOT NULL UNION ALL
    SELECT 'MEDICATION_ORDER' AS SourceTable, 'date_precision_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."MEDICATION_ORDER" WHERE "date_precision_concept_id" IS NOT NULL UNION ALL
    SELECT 'PROCEDURE_REQUEST' AS SourceTable, 'procedure_core_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PROCEDURE_REQUEST" WHERE "procedure_core_concept_id" IS NOT NULL UNION ALL
    SELECT 'PATIENT_CONTACT' AS SourceTable, 'contact_type_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_CONTACT" WHERE "contact_type_concept_id" IS NOT NULL UNION ALL
    SELECT 'OBSERVATION' AS SourceTable, 'observation_core_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."OBSERVATION" WHERE "observation_core_concept_id" IS NOT NULL UNION ALL
    SELECT 'OBSERVATION' AS SourceTable, 'episodicity_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."OBSERVATION" WHERE "episodicity_concept_id" IS NOT NULL UNION ALL
    SELECT 'DIAGNOSTIC_ORDER' AS SourceTable, 'episodicity_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."DIAGNOSTIC_ORDER" WHERE "episodicity_concept_id" IS NOT NULL UNION ALL
    SELECT 'REFERRAL_REQUEST' AS SourceTable, 'date_precision_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."REFERRAL_REQUEST" WHERE "date_precision_concept_id" IS NOT NULL UNION ALL
    SELECT 'OBSERVATION' AS SourceTable, 'date_precision_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."OBSERVATION" WHERE "date_precision_concept_id" IS NOT NULL UNION ALL
    SELECT 'REFERRAL_REQUEST' AS SourceTable, 'referral_request_priority_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."REFERRAL_REQUEST" WHERE "referral_request_priority_concept_id" IS NOT NULL UNION ALL
    SELECT 'ALLERGY_INTOLERANCE' AS SourceTable, 'date_precision_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ALLERGY_INTOLERANCE" WHERE "date_precision_concept_id" IS NOT NULL UNION ALL
    SELECT 'DIAGNOSTIC_ORDER' AS SourceTable, 'diagnostic_order_core_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."DIAGNOSTIC_ORDER" WHERE "diagnostic_order_core_concept_id" IS NOT NULL UNION ALL
    SELECT 'APPOINTMENT' AS SourceTable, 'appointment_status_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."APPOINTMENT" WHERE "appointment_status_concept_id" IS NOT NULL UNION ALL
    SELECT 'REFERRAL_REQUEST' AS SourceTable, 'referral_request_core_concept_id' AS SourceColumn, COUNT(*) AS TotalCount FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."REFERRAL_REQUEST" WHERE "referral_request_core_concept_id" IS NOT NULL
),
-- Step 2: Aggregate failures by type.
FailuresByType AS (
    SELECT
        SourceTable,
        SourceColumn,
        CASE
            WHEN TestFailureReason LIKE 'Join Failure: No match in CONCEPT_MAP%' THEN 'Join Failure: No match in CONCEPT_MAP'
            WHEN TestFailureReason LIKE 'Join Failure: No match in CONCEPT%' THEN 'Join Failure: No match in CONCEPT'
            WHEN TestFailureReason LIKE 'Data Issue: CONCEPT.code is NULL%' THEN 'Data Issue: CONCEPT.code is NULL'
            WHEN TestFailureReason LIKE 'Data Issue: CONCEPT.display is NULL%' THEN 'Data Issue: CONCEPT.display is NULL'
            ELSE TestFailureReason
        END AS FailureType,
        COUNT(*) AS SpecificFailureCount
    FROM
        DATA_LAB_OLIDS_UAT.TESTS.TEST_CONCEPT_MAPPING_FAILURES
    GROUP BY
        SourceTable,
        SourceColumn,
        FailureType
),
-- Step 3: Calculate the TOTAL number of failures for each test.
OverallFailureCounts AS (
    SELECT
        SourceTable,
        SourceColumn,
        SUM(SpecificFailureCount) as TotalFailureCount
    FROM FailuresByType
    GROUP BY SourceTable, SourceColumn
)
-- Final Step: Join everything together for a complete report.
SELECT
    fbt.SourceTable AS SOURCE_TABLE_NAME,
    fbt.SourceColumn AS SOURCE_COLUMN_NAME,
    fbt.FailureType AS FAILURE_CATEGORY,
    fbt.SpecificFailureCount AS SPECIFIC_FAILURE_COUNT,
    tr.TotalCount AS TOTAL_RECORDS_TESTED,
    (tr.TotalCount - ofc.TotalFailureCount) AS TOTAL_SUCCESS_COUNT,
    -- Safely calculate the OVERALL failure percentage for the entire test and round it
    ROUND(
        CASE
            WHEN tr.TotalCount = 0 THEN 0
            ELSE (ofc.TotalFailureCount / tr.TotalCount) * 100
        END, 2
    ) AS OVERALL_FAILURE_PERCENTAGE
FROM
    FailuresByType fbt
JOIN
    TotalRecords tr ON fbt.SourceTable = tr.SourceTable AND fbt.SourceColumn = tr.SourceColumn
JOIN
    OverallFailureCounts ofc ON fbt.SourceTable = ofc.SourceTable AND fbt.SourceColumn = ofc.SourceColumn
ORDER BY
    SOURCE_TABLE_NAME,
    SOURCE_COLUMN_NAME,
    OVERALL_FAILURE_PERCENTAGE DESC,
    FAILURE_CATEGORY;
