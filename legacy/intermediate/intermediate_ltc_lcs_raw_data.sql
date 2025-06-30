CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA (
    -- Core identifiers
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    SOURCE_RECORD_ID VARCHAR, -- ID of the source record (observation_id or medication_order_id)
    SOURCE_TABLE VARCHAR, -- Source table ('OBSERVATION' or 'MEDICATION_ORDER')
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the record

    -- Concept details
    CONCEPT_ID VARCHAR, -- The mapped concept ID
    CONCEPT_CODE VARCHAR, -- The mapped concept code
    CONCEPT_DISPLAY VARCHAR, -- The display term for the concept
    CLUSTER_ID VARCHAR, -- The cluster ID from LTC_LCS_CODES
    CLUSTER_DESCRIPTION VARCHAR, -- Description of the cluster

    -- Observation-specific fields (NULL for medications)
    RESULT_VALUE NUMBER, -- Numeric result value for observations
    RESULT_UNIT VARCHAR, -- Unit of measurement for observations
    RESULT_UNIT_DISPLAY VARCHAR, -- Display term for the unit
    RESULT_TEXT VARCHAR, -- Text result for observations

    -- Medication-specific fields (NULL for observations)
    MEDICATION_NAME VARCHAR, -- Name of the medication
    DOSE VARCHAR, -- Dose of the medication
    QUANTITY_VALUE NUMBER, -- Quantity value
    QUANTITY_UNIT VARCHAR, -- Unit of quantity
    DURATION_DAYS NUMBER, -- Duration in days

    -- Additional context
    ORIGINATING_SOURCE_TABLE VARCHAR, -- The specific table where the concept originated (e.g., 'OBSERVATION', 'MEDICATION_STATEMENT')
    SOURCE_CODE_ID VARCHAR, -- The original source code ID
    SOURCE_CODE_DISPLAY VARCHAR, -- The original source code display term

    -- Traceability arrays
    ALL_CONCEPT_CODES ARRAY, -- All concept codes contributing to this record
    ALL_CONCEPT_DISPLAYS ARRAY, -- All concept display terms contributing to this record
    ALL_SOURCE_CLUSTER_IDS ARRAY -- All source cluster IDs contributing to this record
)
COMMENT = 'Intermediate table consolidating all raw data related to LTC LCS codes, including both observations and medications. This table serves as a foundation for downstream analysis of LTC LCS-related data, capturing all relevant details while maintaining traceability to source records.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH LTC_LCS_Observations AS (
    -- Get all observations mapped to LTC LCS codes
    SELECT
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."id" AS SOURCE_RECORD_ID,
        'OBSERVATION' AS SOURCE_TABLE,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_ID,
        MC.CONCEPT_CODE,
        MC.CONCEPT_DISPLAY,
        MC.CLUSTER_ID,
        MC.CLUSTER_DESCRIPTION,
        O."result_value" AS RESULT_VALUE,
        UNIT_CON."code" AS RESULT_UNIT,
        UNIT_CON."display" AS RESULT_UNIT_DISPLAY,
        O."result_text" AS RESULT_TEXT,
        NULL AS MEDICATION_NAME,
        NULL AS DOSE,
        NULL AS QUANTITY_VALUE,
        NULL AS QUANTITY_UNIT,
        NULL AS DURATION_DAYS,
        SCO.ORIGINATING_SOURCE_TABLE,
        MC.SOURCE_CODE_ID,
        MC.CODE_DESCRIPTION AS SOURCE_CODE_DISPLAY,
        ARRAY_AGG(DISTINCT MC.CONCEPT_CODE) WITHIN GROUP (ORDER BY MC.CONCEPT_CODE) AS ALL_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT MC.CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY MC.CONCEPT_DISPLAY) AS ALL_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT MC.CLUSTER_ID) WITHIN GROUP (ORDER BY MC.CLUSTER_ID) AS ALL_SOURCE_CLUSTER_IDS
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    LEFT JOIN DATA_LAB_OLIDS_UAT.REFERENCE.SOURCE_CONCEPT_ORIGINS AS SCO
        ON MC.SOURCE_CODE_ID = SCO.SOURCE_CODE_ID_VALUE
    LEFT JOIN "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS UNIT_CON
        ON O."result_value_unit_concept_id" = UNIT_CON."id"
    WHERE MC.SOURCE = 'LTC_LCS'
    GROUP BY
        PP."person_id",
        P."sk_patient_id",
        O."id",
        O."clinical_effective_date",
        MC.CONCEPT_ID,
        MC.CONCEPT_CODE,
        MC.CONCEPT_DISPLAY,
        MC.CLUSTER_ID,
        MC.CLUSTER_DESCRIPTION,
        O."result_value",
        UNIT_CON."code",
        UNIT_CON."display",
        O."result_text",
        SCO.ORIGINATING_SOURCE_TABLE,
        MC.SOURCE_CODE_ID,
        MC.CODE_DESCRIPTION
),
LTC_LCS_Medications AS (
    -- Get all medications mapped to LTC LCS codes
    SELECT
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        MO."id" AS SOURCE_RECORD_ID,
        'MEDICATION_ORDER' AS SOURCE_TABLE,
        MO."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_ID,
        MC.CONCEPT_CODE,
        MC.CONCEPT_DISPLAY,
        MC.CLUSTER_ID,
        MC.CLUSTER_DESCRIPTION,
        NULL AS RESULT_VALUE,
        NULL AS RESULT_UNIT,
        NULL AS RESULT_UNIT_DISPLAY,
        NULL AS RESULT_TEXT,
        MO."medication_name" AS MEDICATION_NAME,
        MO."dose" AS DOSE,
        MO."quantity_value" AS QUANTITY_VALUE,
        MO."quantity_unit" AS QUANTITY_UNIT,
        MO."duration_days" AS DURATION_DAYS,
        SCO.ORIGINATING_SOURCE_TABLE,
        MC.SOURCE_CODE_ID,
        MC.CODE_DESCRIPTION AS SOURCE_CODE_DISPLAY,
        ARRAY_AGG(DISTINCT MC.CONCEPT_CODE) WITHIN GROUP (ORDER BY MC.CONCEPT_CODE) AS ALL_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT MC.CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY MC.CONCEPT_DISPLAY) AS ALL_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT MC.CLUSTER_ID) WITHIN GROUP (ORDER BY MC.CLUSTER_ID) AS ALL_SOURCE_CLUSTER_IDS
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS
        ON MO."medication_statement_id" = MS."id"
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON MS."medication_statement_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON MO."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON MO."patient_id" = P."id"
    LEFT JOIN DATA_LAB_OLIDS_UAT.REFERENCE.SOURCE_CONCEPT_ORIGINS AS SCO
        ON MC.SOURCE_CODE_ID = SCO.SOURCE_CODE_ID_VALUE
    WHERE MC.SOURCE = 'LTC_LCS'
    GROUP BY
        PP."person_id",
        P."sk_patient_id",
        MO."id",
        MO."clinical_effective_date",
        MC.CONCEPT_ID,
        MC.CONCEPT_CODE,
        MC.CONCEPT_DISPLAY,
        MC.CLUSTER_ID,
        MC.CLUSTER_DESCRIPTION,
        MO."medication_name",
        MO."dose",
        MO."quantity_value",
        MO."quantity_unit",
        MO."duration_days",
        SCO.ORIGINATING_SOURCE_TABLE,
        MC.SOURCE_CODE_ID,
        MC.CODE_DESCRIPTION
)
-- Combine observations and medications
SELECT * FROM LTC_LCS_Observations
UNION ALL
SELECT * FROM LTC_LCS_Medications;
