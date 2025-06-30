CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_PPP_STATUS_ALL (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for the person',
    PPP_EVENT_DATE DATE COMMENT 'Date of the PPP-related observation',
    PPP_OBSERVATION_ID VARCHAR COMMENT 'Identifier for the PPP observation',
    PPP_CONCEPT_CODE VARCHAR COMMENT 'The medical concept code for the PPP event',
    PPP_CONCEPT_DISPLAY VARCHAR COMMENT 'Display term for the PPP concept code',
    PPP_STATUS_DESCRIPTION VARCHAR COMMENT 'Human-readable description of PPP status',
    PPP_CATEGORIES ARRAY(VARCHAR) COMMENT 'Array of all categories applicable to this PPP code'
)
COMMENT = 'Intermediate table containing all Pregnancy Prevention Programme (PPP) events from source systems. Raw data collection layer that feeds the PPP dimension table.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT
    PP."person_id" AS PERSON_ID,
    O."clinical_effective_date"::DATE AS PPP_EVENT_DATE,
    O."id" AS PPP_OBSERVATION_ID,
    MC.CONCEPT_CODE AS PPP_CONCEPT_CODE,
    MC.CODE_DESCRIPTION AS PPP_CONCEPT_DISPLAY,

    -- Map categories to descriptive text
    CASE
        WHEN VPC.CODE_CATEGORY = 'PPP_ENROLLED' THEN 'Yes - PPP enrolled'
        WHEN VPC.CODE_CATEGORY = 'PPP_DISCONTINUED' THEN 'No - PPP discontinued'
        WHEN VPC.CODE_CATEGORY = 'PPP_NOT_NEEDED' THEN 'No - PPP not needed'
        WHEN VPC.CODE_CATEGORY = 'PPP_DECLINED' THEN 'No - PPP declined'
        ELSE 'Unknown PPP status'
    END AS PPP_STATUS_DESCRIPTION,

    ARRAY_CONSTRUCT(VPC.CODE_CATEGORY) AS PPP_CATEGORIES

FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" O
JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS MC
    ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.VALPROATE_PROG_CODES VPC
    ON MC.CONCEPT_CODE = VPC.CODE
JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" PP
    ON O."patient_id" = PP."patient_id"
WHERE VPC.CODE_CATEGORY LIKE 'PPP%';
