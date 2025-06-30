CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_NAFLD (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_NAFLD_REGISTER BOOLEAN, -- Flag indicating if the person is currently on the NAFLD register
    -- Diagnosis dates
    EARLIEST_NAFLD_DIAGNOSIS_DATE DATE, -- Earliest recorded date of a NAFLD diagnosis
    LATEST_NAFLD_DIAGNOSIS_DATE DATE, -- Latest recorded date of a NAFLD diagnosis
    -- Aggregated details for traceability
    ALL_NAFLD_CONCEPT_CODES ARRAY, -- Array of all NAFLD concept codes recorded for the person
    ALL_NAFLD_CONCEPT_DISPLAYS ARRAY -- Array of display terms for the NAFLD concept codes
)
COMMENT = 'Fact table identifying individuals with a diagnosis of Non-Alcoholic Fatty Liver Disease (NAFLD). Currently uses hardcoded concept codes as no NAFLD cluster is defined in the terminology mapping. This should be reviewed and updated once an appropriate cluster is available.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Fetches observation records related to NAFLD diagnosis using hardcoded concept codes
    -- These codes will be replaced with a proper cluster definition once available
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS CONCEPT_DISPLAY
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CONCEPT_CODE IN (
        '197315008',
        '1197739005',
        '1231824009',
        '442685003',
        '722866000',
        '503681000000108'
    )
),
PersonLevelNAFLDAggregation AS (
    -- Aggregates NAFLD diagnosis information for each person
    -- Calculates earliest/latest diagnosis dates
    -- Collects all associated concept details into arrays
    SELECT
        bo.PERSON_ID,
        ANY_VALUE(bo.SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(age.AGE) as AGE,
        MIN(bo.CLINICAL_EFFECTIVE_DATE) AS EARLIEST_NAFLD_DIAGNOSIS_DATE,
        MAX(bo.CLINICAL_EFFECTIVE_DATE) AS LATEST_NAFLD_DIAGNOSIS_DATE,
        ARRAY_AGG(DISTINCT bo.CONCEPT_CODE) WITHIN GROUP (ORDER BY bo.CONCEPT_CODE) AS ALL_NAFLD_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT bo.CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY bo.CONCEPT_DISPLAY) AS ALL_NAFLD_CONCEPT_DISPLAYS,
        -- Person is considered on register if they have any NAFLD diagnosis
        TRUE AS IS_ON_NAFLD_REGISTER
    FROM BaseObservations bo
    LEFT JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE age
        ON bo.PERSON_ID = age.PERSON_ID
    GROUP BY bo.PERSON_ID
)
-- Final selection: Includes all individuals with a NAFLD diagnosis
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    IS_ON_NAFLD_REGISTER,
    EARLIEST_NAFLD_DIAGNOSIS_DATE,
    LATEST_NAFLD_DIAGNOSIS_DATE,
    ALL_NAFLD_CONCEPT_CODES,
    ALL_NAFLD_CONCEPT_DISPLAYS
FROM PersonLevelNAFLDAggregation;
