CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_COPD_UNABLE_SPIROMETRY (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    LATEST_UNABLE_SPIROMETRY_DATE DATE -- Latest date when patient was recorded as unable to have spirometry
)
COMMENT = 'Intermediate table containing records of patients who are unable to have spirometry, based on the UNABLESPI_COD cluster.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Fetches all unable-to-have-spirometry observations
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID = 'UNABLESPI_COD'
),
PersonLevelAggregation AS (
    -- Gets the latest unable-to-have-spirometry date for each person
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_UNABLE_SPIROMETRY_DATE
    FROM BaseObservations
    GROUP BY PERSON_ID
)
-- Final selection combining base observations with latest date
SELECT
    bo.*,
    pla.LATEST_UNABLE_SPIROMETRY_DATE
FROM BaseObservations bo
JOIN PersonLevelAggregation pla
    ON bo.PERSON_ID = pla.PERSON_ID; 