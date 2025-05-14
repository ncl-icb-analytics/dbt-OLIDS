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
COMMENT = 'Dimension table tracking current practice registration for each person, including comprehensive practice details. Links PATIENT_REGISTERED_PRACTITIONER_IN_ROLE with ORGANISATION to provide current practice information.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH current_registrations AS (
    -- Identifies the current practice registration for each person
    SELECT 
        pp."person_id" AS PERSON_ID,
        p."sk_patient_id" AS SK_PATIENT_ID,
        prp."organisation_id" AS PRACTICE_ID,
        prp."start_date" AS REGISTRATION_START_DATE,
        prp."end_date" AS REGISTRATION_END_DATE,
        o."organisation_code" AS PRACTICE_CODE,
        o."name" AS PRACTICE_NAME,
        o."type_code" AS PRACTICE_TYPE_CODE,
        o."type_desc" AS PRACTICE_TYPE_DESC,
        o."postcode" AS PRACTICE_POSTCODE,
        o."parent_organisation_id" AS PARENT_ORG_ID,
        o."open_date" AS PRACTICE_OPEN_DATE,
        o."close_date" AS PRACTICE_CLOSE_DATE,
        o."is_obsolete" AS PRACTICE_IS_OBSOLETE,
        -- Rank registrations to get the most current one
        ROW_NUMBER() OVER (
            PARTITION BY pp."person_id" 
            ORDER BY 
                prp."start_date" DESC,
                COALESCE(prp."end_date", TIMESTAMP '9999-12-31 23:59:59') DESC
        ) AS registration_rank
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p 
        ON pp."patient_id" = p."id"
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_REGISTERED_PRACTITIONER_IN_ROLE prp 
        ON pp."person_id" = prp."person_id"
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.ORGANISATION o 
        ON prp."organisation_id" = o."id"
)
-- Select only the current registration (rank = 1) for each person
SELECT 
    PERSON_ID,
    SK_PATIENT_ID,
    PRACTICE_ID,
    PRACTICE_CODE,
    PRACTICE_NAME,
    PRACTICE_TYPE_CODE,
    PRACTICE_TYPE_DESC,
    PRACTICE_POSTCODE,
    PARENT_ORG_ID AS PRACTICE_PARENT_ORG_ID,
    PRACTICE_OPEN_DATE,
    PRACTICE_CLOSE_DATE,
    PRACTICE_IS_OBSOLETE,
    REGISTRATION_START_DATE,
    REGISTRATION_END_DATE
FROM current_registrations
WHERE registration_rank = 1; 