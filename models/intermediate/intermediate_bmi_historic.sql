CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BMI_HISTORIC (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the BMI measurement
    BMI_VALUE NUMBER -- The numeric BMI value (filtered to valid range 5-400)
)
COMMENT = 'Historic record of all valid numeric BMI values over time. Includes only BMIVAL_COD measurements within valid range (5-400), ordered by patient and date.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Get all numeric BMI values (BMIVAL_COD only)
    SELECT 
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        CAST(O."result_value"::FLOAT AS NUMBER(10,2)) AS BMI_VALUE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID = 'BMIVAL_COD'
      AND O."clinical_effective_date" IS NOT NULL
      AND O."result_value" IS NOT NULL
)
-- Final selection with valid BMI values only
SELECT 
    PERSON_ID,
    SK_PATIENT_ID,
    CLINICAL_EFFECTIVE_DATE,
    BMI_VALUE
FROM BaseObservations
WHERE BMI_VALUE BETWEEN 5 AND 400
ORDER BY PERSON_ID, CLINICAL_EFFECTIVE_DATE; 