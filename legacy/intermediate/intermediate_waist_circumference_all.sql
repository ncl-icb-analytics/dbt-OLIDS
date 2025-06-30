CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_WAIST_CIRCUMFERENCE_ALL (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    OBSERVATION_DATE DATE, -- Date the measurement was taken
    OBSERVATION_VALUE NUMBER, -- The waist circumference measurement value
    OBSERVATION_UNIT VARCHAR, -- Unit of measurement (cm)
    OBSERVATION_CONCEPT_CODE VARCHAR, -- The concept code for the observation
    OBSERVATION_CONCEPT_DISPLAY VARCHAR, -- The display term for the concept code
    RECENT_MEASUREMENT_COUNT NUMBER -- Count of measurements in the last 12 months
)
COMMENT = 'Intermediate table containing all waist circumference measurements. Includes all measurements from the WAIST_COD cluster.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BaseWaistMeasurements AS (
    -- Get all waist circumference measurements
    SELECT
        o."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        o."clinical_effective_date"::DATE AS OBSERVATION_DATE,
        CAST(o."result_value" AS NUMBER(10,2)) AS OBSERVATION_VALUE,
        UNIT_CON."display" AS OBSERVATION_UNIT,
        MC.CONCEPT_CODE AS OBSERVATION_CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS OBSERVATION_CONCEPT_DISPLAY
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" o
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS MC
        ON o."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" PP
        ON o."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" P
        ON o."patient_id" = P."id"
    LEFT JOIN "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT UNIT_CON
        ON o."result_value_unit_concept_id" = UNIT_CON."id"
    WHERE MC.CLUSTER_ID = 'WAIST_COD'
    AND o."result_value" IS NOT NULL
    AND REGEXP_LIKE(o."result_value"::VARCHAR, '^[+-]?([0-9]*[.])?[0-9]+$') -- Ensure value is numeric
),
MeasurementCounts AS (
    -- Counts the number of measurements per person in the last 12 months
    SELECT
        PERSON_ID,
        COUNT(*) as RECENT_MEASUREMENT_COUNT
    FROM BaseWaistMeasurements
    WHERE OBSERVATION_DATE >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY PERSON_ID
)
-- Final selection combining measurement details with the count
SELECT
    bwm.*,
    COALESCE(mc.RECENT_MEASUREMENT_COUNT, 0) as RECENT_MEASUREMENT_COUNT
FROM BaseWaistMeasurements bwm
LEFT JOIN MeasurementCounts mc
    ON bwm.PERSON_ID = mc.PERSON_ID;
