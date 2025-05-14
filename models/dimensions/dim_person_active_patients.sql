-- ==========================================================================
-- Dimension Dynamic Table holding active patient status at person level.
-- Filters out deceased, dummy, and deregistered patients.
-- Links PATIENT_PERSON, PATIENT, and PERSON tables to provide a clean view of active patients.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_ACTIVE_PATIENTS (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_ID NUMBER COMMENT 'Surrogate key for the patient',
    PRIMARY_PATIENT_ID VARCHAR COMMENT 'ID of the primary patient record for this person',
    LATEST_PATIENT_ID VARCHAR COMMENT 'ID of the most recent patient record for this person',
    IS_ACTIVE BOOLEAN COMMENT 'Whether the person is currently an active patient',
    IS_DECEASED BOOLEAN COMMENT 'Whether the person is recorded as deceased',
    IS_DUMMY_PATIENT BOOLEAN COMMENT 'Whether the person is a dummy patient record',
    IS_CONFIDENTIAL BOOLEAN COMMENT 'Whether the person has confidential status',
    IS_SPINE_SENSITIVE BOOLEAN COMMENT 'Whether the person has spine sensitive status',
    BIRTH_YEAR NUMBER COMMENT 'Year of birth',
    BIRTH_MONTH NUMBER COMMENT 'Month of birth',
    DEATH_YEAR NUMBER COMMENT 'Year of death (NULL if alive)',
    DEATH_MONTH NUMBER COMMENT 'Month of death (NULL if alive)',
    REGISTERED_PRACTICE_ID VARCHAR COMMENT 'ID of the practice where the person is currently registered',
    RECORD_OWNER_ORG_CODE VARCHAR COMMENT 'Organisation code of the record owner',
    LATEST_RECORD_DATE TIMESTAMP_NTZ COMMENT 'Date of the most recent patient record update'
)
COMMENT = 'Dimension table providing active patient status at person level. Excludes deceased, dummy, and deregistered patients. Links PATIENT_PERSON, PATIENT, and PERSON tables to provide a clean view of active patients.'
TARGET_LAG = '4 hours'
REFRESH_MODE = auto
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH LatestPatientRecordPerPerson AS (
    -- Get the latest patient record for each person
    SELECT
        pp."person_id" AS PERSON_ID,
        p."sk_patient_id" AS SK_PATIENT_ID,
        per."primary_patient_id" AS PRIMARY_PATIENT_ID,
        p."id" AS LATEST_PATIENT_ID,
        -- Determine if patient is active based on various criteria
        CASE
            WHEN p."death_year" IS NOT NULL THEN FALSE -- Deceased
            WHEN p."is_dummy_patient" THEN FALSE -- Dummy patient
            WHEN p."lds_end_date_time" IS NOT NULL THEN FALSE -- Deregistered
            ELSE TRUE
        END AS IS_ACTIVE,
        p."death_year" IS NOT NULL AS IS_DECEASED,
        p."is_dummy_patient" AS IS_DUMMY_PATIENT,
        p."is_confidential" AS IS_CONFIDENTIAL,
        p."is_spine_sensitive" AS IS_SPINE_SENSITIVE,
        p."birth_year" AS BIRTH_YEAR,
        p."birth_month" AS BIRTH_MONTH,
        p."death_year" AS DEATH_YEAR,
        p."death_month" AS DEATH_MONTH,
        p."registered_practice_id" AS REGISTERED_PRACTICE_ID,
        p."record_owner_organisation_code" AS RECORD_OWNER_ORG_CODE,
        p."lds_datetime_data_acquired" AS LATEST_RECORD_DATE,
        -- Rank to get the latest record
        ROW_NUMBER() OVER (
            PARTITION BY pp."person_id"
            ORDER BY 
                p."lds_datetime_data_acquired" DESC,
                p."id" DESC
        ) AS record_rank
    FROM
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
        ON pp."patient_id" = p."id"
    JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PERSON per
        ON pp."person_id" = per."id"
    WHERE
        -- Filter out records that are no longer valid
        pp."lds_end_date_time" IS NULL
        AND p."lds_end_date_time" IS NULL
        AND per."lds_end_date_time" IS NULL
)
-- Select only the latest record per person
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    PRIMARY_PATIENT_ID,
    LATEST_PATIENT_ID,
    IS_ACTIVE,
    IS_DECEASED,
    IS_DUMMY_PATIENT,
    IS_CONFIDENTIAL,
    IS_SPINE_SENSITIVE,
    BIRTH_YEAR,
    BIRTH_MONTH,
    DEATH_YEAR,
    DEATH_MONTH,
    REGISTERED_PRACTICE_ID,
    RECORD_OWNER_ORG_CODE,
    LATEST_RECORD_DATE
FROM
    LatestPatientRecordPerPerson
WHERE
    record_rank = 1
    AND IS_ACTIVE = TRUE; -- Only include active patients 