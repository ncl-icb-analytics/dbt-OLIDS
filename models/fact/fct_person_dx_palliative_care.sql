CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_PALLIATIVE_CARE (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_PALLIATIVE_CARE_REGISTER BOOLEAN, -- Flag indicating if person is on the palliative care register
    EARLIEST_PALLIATIVE_CARE_DATE DATE, -- First palliative care code after April 2008
    LATEST_PALLIATIVE_CARE_DATE DATE, -- Most recent palliative care code
    EARLIEST_NO_LONGER_INDICATED_DATE DATE, -- First code indicating palliative care no longer needed
    ALL_PALLIATIVE_CARE_CONCEPT_CODES ARRAY, -- All palliative care concept codes
    ALL_PALLIATIVE_CARE_CONCEPT_DISPLAYS ARRAY, -- All palliative care concept display terms
    ALL_NO_LONGER_INDICATED_CONCEPT_CODES ARRAY, -- All 'no longer indicated' concept codes
    ALL_NO_LONGER_INDICATED_CONCEPT_DISPLAYS ARRAY -- All 'no longer indicated' concept display terms
)
COMMENT = 'Fact table identifying individuals on the palliative care register. Includes patients with a palliative care code after April 2008 who have not been subsequently marked as no longer requiring palliative care.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Get all palliative care and 'no longer indicated' codes
    SELECT 
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        AGE.AGE,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        MC.CLUSTER_ID,
        -- Flag different types of observations
        CASE WHEN MC.CLUSTER_ID = 'PALCARE_COD' THEN O."clinical_effective_date"::DATE END AS PALLIATIVE_CARE_DATE,
        CASE WHEN MC.CLUSTER_ID = 'PALCARENI_COD' THEN O."clinical_effective_date"::DATE END AS NO_LONGER_INDICATED_DATE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE AS AGE
        ON PP."person_id" = AGE.PERSON_ID
    WHERE MC.CLUSTER_ID IN ('PALCARE_COD', 'PALCARENI_COD')
),
PalliativeCareDates AS (
    -- Get all palliative care dates after April 2008
    SELECT 
        PERSON_ID,
        SK_PATIENT_ID,
        AGE,
        PALLIATIVE_CARE_DATE,
        NO_LONGER_INDICATED_DATE,
        -- Calculate latest palliative care date per person
        MAX(PALLIATIVE_CARE_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_PALLIATIVE_CARE_DATE
    FROM BaseObservations
    WHERE PALLIATIVE_CARE_DATE >= '2008-04-01'
),
PersonLevelAggregation AS (
    -- Aggregate to one row per person with all necessary dates and codes
    SELECT
        pcd.PERSON_ID,
        pcd.SK_PATIENT_ID,
        pcd.AGE,
        MIN(pcd.PALLIATIVE_CARE_DATE) AS EARLIEST_PALLIATIVE_CARE_DATE,
        MAX(pcd.PALLIATIVE_CARE_DATE) AS LATEST_PALLIATIVE_CARE_DATE,
        -- Get earliest 'no longer indicated' date that's after the latest palliative care date
        MIN(CASE 
            WHEN pcd.NO_LONGER_INDICATED_DATE > pcd.LATEST_PALLIATIVE_CARE_DATE 
            THEN pcd.NO_LONGER_INDICATED_DATE 
        END) AS EARLIEST_NO_LONGER_INDICATED_DATE,
        -- Aggregate concept codes and displays
        ARRAY_AGG(DISTINCT CASE WHEN bo.CLUSTER_ID = 'PALCARE_COD' THEN bo.CONCEPT_CODE END) AS ALL_PALLIATIVE_CARE_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN bo.CLUSTER_ID = 'PALCARE_COD' THEN bo.CODE_DESCRIPTION END) AS ALL_PALLIATIVE_CARE_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT CASE WHEN bo.CLUSTER_ID = 'PALCARENI_COD' THEN bo.CONCEPT_CODE END) AS ALL_NO_LONGER_INDICATED_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN bo.CLUSTER_ID = 'PALCARENI_COD' THEN bo.CODE_DESCRIPTION END) AS ALL_NO_LONGER_INDICATED_CONCEPT_DISPLAYS
    FROM PalliativeCareDates pcd
    LEFT JOIN BaseObservations bo
        ON pcd.PERSON_ID = bo.PERSON_ID
    GROUP BY pcd.PERSON_ID, pcd.SK_PATIENT_ID, pcd.AGE
)
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    -- Rule: Include if has palliative care code after April 2008 and no 'no longer indicated' code after latest palliative care
    CASE 
        WHEN EARLIEST_PALLIATIVE_CARE_DATE IS NOT NULL 
        AND EARLIEST_NO_LONGER_INDICATED_DATE IS NULL 
        THEN TRUE
        ELSE FALSE
    END AS IS_ON_PALLIATIVE_CARE_REGISTER,
    EARLIEST_PALLIATIVE_CARE_DATE,
    LATEST_PALLIATIVE_CARE_DATE,
    EARLIEST_NO_LONGER_INDICATED_DATE,
    ALL_PALLIATIVE_CARE_CONCEPT_CODES,
    ALL_PALLIATIVE_CARE_CONCEPT_DISPLAYS,
    ALL_NO_LONGER_INDICATED_CONCEPT_CODES,
    ALL_NO_LONGER_INDICATED_CONCEPT_DISPLAYS
FROM PersonLevelAggregation; 