CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_COPD_DIAGNOSES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either COPD_COD or COPDRES_COD
    IS_DIAGNOSIS BOOLEAN, -- Flag indicating if this is a diagnosis code
    IS_RESOLUTION BOOLEAN, -- Flag indicating if this is a resolution code
    EARLIEST_DIAGNOSIS_DATE DATE, -- Earliest COPD diagnosis date for this person
    LATEST_DIAGNOSIS_DATE DATE, -- Latest COPD diagnosis date for this person
    LATEST_RESOLUTION_DATE DATE, -- Latest COPD resolution date for this person
    EARLIEST_UNRESOLVED_DIAGNOSIS_DATE DATE -- Earliest unresolved diagnosis date (EUNRESCOPD_DAT)
)
COMMENT = 'Intermediate table containing COPD diagnoses and resolutions, with calculated earliest unresolved diagnosis date (EUNRESCOPD_DAT).'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID,
        CASE WHEN MC.CLUSTER_ID = 'COPD_COD' THEN O."clinical_effective_date"::DATE END AS DIAGNOSIS_DATE,
        CASE WHEN MC.CLUSTER_ID = 'COPDRES_COD' THEN O."clinical_effective_date"::DATE END AS RESOLUTION_DATE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('COPD_COD', 'COPDRES_COD')
),
PersonDates AS (
    SELECT
        bo.*,
        MIN(DIAGNOSIS_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_DIAGNOSIS_DATE,
        MAX(DIAGNOSIS_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_DIAGNOSIS_DATE,
        MAX(RESOLUTION_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_RESOLUTION_DATE,
        CASE
            WHEN SOURCE_CLUSTER_ID = 'COPD_COD' AND (
                MAX(RESOLUTION_DATE) OVER (PARTITION BY PERSON_ID) IS NULL
                OR CLINICAL_EFFECTIVE_DATE > MAX(RESOLUTION_DATE) OVER (PARTITION BY PERSON_ID)
            ) THEN CLINICAL_EFFECTIVE_DATE
            ELSE NULL
        END AS POTENTIAL_UNRESOLVED_DATE
    FROM BaseObservations bo
)
SELECT
    pd.PERSON_ID,
    pd.SK_PATIENT_ID,
    pd.OBSERVATION_ID,
    pd.CLINICAL_EFFECTIVE_DATE,
    pd.CONCEPT_CODE,
    pd.CODE_DESCRIPTION,
    pd.SOURCE_CLUSTER_ID,
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'COPD_COD' THEN TRUE ELSE FALSE END AS IS_DIAGNOSIS,
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'COPDRES_COD' THEN TRUE ELSE FALSE END AS IS_RESOLUTION,
    pd.EARLIEST_DIAGNOSIS_DATE,
    pd.LATEST_DIAGNOSIS_DATE,
    pd.LATEST_RESOLUTION_DATE,
    MIN(pd.POTENTIAL_UNRESOLVED_DATE) OVER (PARTITION BY pd.PERSON_ID) AS EARLIEST_UNRESOLVED_DIAGNOSIS_DATE
FROM PersonDates pd
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
