CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_HISTORICAL_PRACTICE(
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    PRACTICE_ID VARCHAR COMMENT 'ID of the practice for this registration',
    PRACTICE_CODE VARCHAR COMMENT 'Organisation code of the practice',
    PRACTICE_NAME VARCHAR COMMENT 'Name of the practice',
    PRACTICE_TYPE_CODE VARCHAR COMMENT 'Type code of the practice',
    PRACTICE_TYPE_DESC VARCHAR COMMENT 'Type description of the practice',
    PRACTICE_POSTCODE VARCHAR COMMENT 'Postcode of the practice',
    PRACTICE_PARENT_ORG_ID VARCHAR COMMENT 'Parent organisation ID of the practice',
    PRACTICE_OPEN_DATE DATE COMMENT 'Date when the practice opened',
    PRACTICE_CLOSE_DATE DATE COMMENT 'Date when the practice closed/will close (if applicable)',
    PRACTICE_IS_OBSOLETE BOOLEAN COMMENT 'Flag indicating if the practice is marked as obsolete',
    REGISTRATION_START_DATE TIMESTAMP_NTZ COMMENT 'Start date of this practice registration',
    REGISTRATION_END_DATE TIMESTAMP_NTZ COMMENT 'End date of this practice registration',
    REGISTRATION_SEQUENCE NUMBER COMMENT 'Sequential number of this registration (1 is oldest)',
    TOTAL_REGISTRATIONS NUMBER COMMENT 'Total number of practice registrations for this person',
    IS_CURRENT_PRACTICE BOOLEAN COMMENT 'Flag indicating if this is the current practice registration'
)
COMMENT = 'Dimension table tracking all practice registrations (current and historical) for each person. Each row represents a unique practice registration period, determined by aggregating GP relationships - using the earliest GP assignment as the registration start date and the latest GP assignment end date as the registration end date for each practice. This ensures continuous registration periods even when GPs change within the same practice.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH practice_registration_periods AS (
    -- Aggregate practice registrations by person and practice, finding earliest start and latest end dates
    SELECT 
        prp."person_id",
        prp."organisation_id",
        MIN(prp."start_date") AS registration_start_date,
        MAX(prp."end_date") AS registration_end_date
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_REGISTERED_PRACTITIONER_IN_ROLE prp
    GROUP BY 
        prp."person_id",
        prp."organisation_id"
),
all_registrations AS (
    -- Gets all practice registrations for each person with sequencing
    SELECT 
        pp."person_id" AS PERSON_ID,
        p."sk_patient_id" AS SK_PATIENT_ID,
        prp."organisation_id" AS PRACTICE_ID,
        prp.registration_start_date AS REGISTRATION_START_DATE,
        prp.registration_end_date AS REGISTRATION_END_DATE,
        o."organisation_code" AS PRACTICE_CODE,
        o."name" AS PRACTICE_NAME,
        o."type_code" AS PRACTICE_TYPE_CODE,
        o."type_desc" AS PRACTICE_TYPE_DESC,
        o."postcode" AS PRACTICE_POSTCODE,
        o."parent_organisation_id" AS PARENT_ORG_ID,
        o."open_date" AS PRACTICE_OPEN_DATE,
        o."close_date" AS PRACTICE_CLOSE_DATE,
        o."is_obsolete" AS PRACTICE_IS_OBSOLETE,
        -- Sequence number (1 is oldest registration)
        ROW_NUMBER() OVER (
            PARTITION BY pp."person_id" 
            ORDER BY 
                prp.registration_start_date ASC,
                COALESCE(prp.registration_end_date, TIMESTAMP '9999-12-31 23:59:59') ASC
        ) AS registration_sequence,
        -- Reverse sequence to identify current registration (1 is newest)
        ROW_NUMBER() OVER (
            PARTITION BY pp."person_id" 
            ORDER BY 
                prp.registration_start_date DESC,
                COALESCE(prp.registration_end_date, TIMESTAMP '9999-12-31 23:59:59') DESC
        ) AS reverse_sequence,
        -- Count total registrations per person
        COUNT(*) OVER (PARTITION BY pp."person_id") AS total_registrations
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p 
        ON pp."patient_id" = p."id"
    JOIN practice_registration_periods prp 
        ON pp."person_id" = prp."person_id"
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.ORGANISATION o 
        ON prp."organisation_id" = o."id"
)
-- Select all registrations with additional flags
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
    REGISTRATION_END_DATE,
    registration_sequence AS REGISTRATION_SEQUENCE,
    total_registrations AS TOTAL_REGISTRATIONS,
    -- Flag for current practice (reverse_sequence = 1)
    reverse_sequence = 1 AS IS_CURRENT_PRACTICE
FROM all_registrations
ORDER BY PERSON_ID, registration_sequence; 