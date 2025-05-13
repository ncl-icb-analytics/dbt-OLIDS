CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_LD (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person (will be >= 14)
    HAS_LD_DIAGNOSIS BOOLEAN, -- Flag indicating an LD diagnosis (always TRUE for rows in this table)
    EARLIEST_LD_DIAGNOSIS_DATE DATE, -- Earliest recorded date of an LD diagnosis for the person
    LATEST_LD_DIAGNOSIS_DATE DATE, -- Latest recorded date of an LD diagnosis for the person
    ALL_LD_OBSERVATION_IDS ARRAY, -- Array of all observation IDs related to LD for the person
    ALL_LD_CONCEPT_CODES ARRAY, -- Array of all LD concept codes recorded for the person
    ALL_LD_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for the LD concept codes
    ALL_LD_SOURCE_CLUSTER_IDS ARRAY -- Array of source cluster IDs (will contain 'LD_COD')
)
COMMENT = 'Fact table identifying individuals aged 14 and over with a current Learning Disability (LD) diagnosis. Sourced from INTERMEDIATE_LD_DIAGNOSES_ALL.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH FilteredByAge AS (
    -- Selects individuals from the intermediate LD diagnoses table who are aged 14 or older.
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        AGE,
        OBSERVATION_ID,
        CLINICAL_EFFECTIVE_DATE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY,
        SOURCE_CLUSTER_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LD_DIAGNOSES_ALL
    WHERE AGE >= 14
),
PersonLevelLDAggregation AS (
    -- Aggregates learning disability information for each person aged 14+,
    -- determining earliest/latest diagnosis dates and collecting all associated observation details.
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(AGE) as AGE,
        MIN(CLINICAL_EFFECTIVE_DATE) AS EARLIEST_LD_DIAGNOSIS_DATE,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_LD_DIAGNOSIS_DATE,
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_LD_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_LD_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_LD_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_LD_SOURCE_CLUSTER_IDS
    FROM FilteredByAge
    GROUP BY PERSON_ID
)
-- Final selection to populate the fact table with individuals aged 14+
-- who have a learning disability diagnosis, along with aggregated details.
SELECT
    pla.PERSON_ID,
    pla.SK_PATIENT_ID,
    pla.AGE,
    TRUE AS HAS_LD_DIAGNOSIS, -- All individuals in this table have an LD diagnosis by definition
    pla.EARLIEST_LD_DIAGNOSIS_DATE,
    pla.LATEST_LD_DIAGNOSIS_DATE,
    pla.ALL_LD_OBSERVATION_IDS,
    pla.ALL_LD_CONCEPT_CODES,
    pla.ALL_LD_CONCEPT_DISPLAYS,
    pla.ALL_LD_SOURCE_CLUSTER_IDS
FROM PersonLevelLDAggregation pla; 