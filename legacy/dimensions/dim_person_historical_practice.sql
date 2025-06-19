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
    REGISTRATION_START_DATE TIMESTAMP_NTZ COMMENT 'Start date of this practice registration period from Episode of Care',
    REGISTRATION_END_DATE TIMESTAMP_NTZ COMMENT 'End date of this practice registration period from Episode of Care',
    REGISTRATION_SEQUENCE NUMBER COMMENT 'Sequential number of this registration (1 is oldest)',
    TOTAL_REGISTRATIONS NUMBER COMMENT 'Total number of practice registrations for this person',
    IS_CURRENT_PRACTICE BOOLEAN COMMENT 'Flag indicating if this is the current practice registration (end date is NULL)'
)
COMMENT = 'Dimension table tracking all practice registrations (current and historical) for each person using Episode of Care data. Each row represents a unique practice registration period as recorded in the Episode of Care table, which contains patient registration periods at practices rather than traditional episode of care data.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH episode_registration_periods AS (
    -- Get practice registration periods from Episode of Care table
    -- Filter to only include registration-type episodes if episode_type identifies registration episodes
    SELECT 
        eoc."person_id",
        eoc."organisation_id",
        eoc."episode_of_care_start_date" AS registration_start_date,
        eoc."episode_of_care_end_date" AS registration_end_date
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.EPISODE_OF_CARE eoc
    WHERE eoc."person_id" IS NOT NULL 
        AND eoc."organisation_id" IS NOT NULL
        AND eoc."episode_of_care_start_date" IS NOT NULL
        -- Add episode type filter if needed to identify registration episodes
        -- AND eoc."episode_type_raw_concept_id" = 'REGISTRATION_TYPE_ID'
),
all_registrations AS (
    -- Gets all practice registrations for each person with sequencing
    SELECT 
        pp."person_id" AS PERSON_ID,
        p."sk_patient_id" AS SK_PATIENT_ID,
        erp."organisation_id" AS PRACTICE_ID,
        erp.registration_start_date AS REGISTRATION_START_DATE,
        erp.registration_end_date AS REGISTRATION_END_DATE,
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
                erp.registration_start_date ASC,
                COALESCE(erp.registration_end_date, TIMESTAMP '9999-12-31 23:59:59') ASC
        ) AS registration_sequence,
        -- Reverse sequence to identify current registration (1 is newest)
        ROW_NUMBER() OVER (
            PARTITION BY pp."person_id" 
            ORDER BY 
                erp.registration_start_date DESC,
                COALESCE(erp.registration_end_date, TIMESTAMP '9999-12-31 23:59:59') DESC
        ) AS reverse_sequence,
        -- Count total registrations per person
        COUNT(*) OVER (PARTITION BY pp."person_id") AS total_registrations
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p 
        ON pp."patient_id" = p."id"
    JOIN episode_registration_periods erp 
        ON pp."person_id" = erp."person_id"
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.ORGANISATION o 
        ON erp."organisation_id" = o."id"
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
    -- Flag for current practice (reverse_sequence = 1 and end_date is NULL)
    reverse_sequence = 1 AND REGISTRATION_END_DATE IS NULL AS IS_CURRENT_PRACTICE
FROM all_registrations
ORDER BY PERSON_ID, registration_sequence; 