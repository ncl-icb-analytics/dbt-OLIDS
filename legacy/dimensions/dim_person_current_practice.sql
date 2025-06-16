CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_CURRENT_PRACTICE(
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    PRACTICE_ID VARCHAR COMMENT 'ID of the person\'s current registered practice',
    PRACTICE_CODE VARCHAR COMMENT 'Organisation code of the current practice',
    PRACTICE_NAME VARCHAR COMMENT 'Name of the current practice',
    PRACTICE_TYPE_CODE VARCHAR COMMENT 'Type code of the current practice',
    PRACTICE_TYPE_DESC VARCHAR COMMENT 'Type description of the current practice',
    PRACTICE_POSTCODE VARCHAR COMMENT 'Postcode of the current practice',
    PRACTICE_PARENT_ORG_ID VARCHAR COMMENT 'Parent organisation ID of the current practice',
    PRACTICE_OPEN_DATE DATE COMMENT 'Date when the current practice opened',
    PRACTICE_CLOSE_DATE DATE COMMENT 'Date when the current practice closed/will close (if applicable)',
    PRACTICE_IS_OBSOLETE BOOLEAN COMMENT 'Flag indicating if the current practice is marked as obsolete',
    REGISTRATION_START_DATE TIMESTAMP_NTZ COMMENT 'Start date of the current practice registration',
    REGISTRATION_END_DATE TIMESTAMP_NTZ COMMENT 'End date of the current practice registration (NULL if active)'
)
COMMENT = 'Dimension table showing the current practice registration for each person, derived from dim_person_historical_practice. The registration period is determined by aggregating all GP relationships at the same practice - using the earliest GP assignment as the registration start date and the latest GP assignment end date as the registration end date.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT 
    PERSON_ID,
    SK_PATIENT_ID,
    PRACTICE_ID,
    PRACTICE_CODE,
    PRACTICE_NAME,
    PRACTICE_TYPE_CODE,
    PRACTICE_TYPE_DESC,
    PRACTICE_POSTCODE,
    PRACTICE_PARENT_ORG_ID,
    PRACTICE_OPEN_DATE,
    PRACTICE_CLOSE_DATE,
    PRACTICE_IS_OBSOLETE,
    REGISTRATION_START_DATE,
    REGISTRATION_END_DATE
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_HISTORICAL_PRACTICE
WHERE IS_CURRENT_PRACTICE = TRUE; 