-- ==========================================================================
-- Dimension Dynamic Table holding active patient status at person level.
-- Uses Episode of Care table for registration periods instead of practitioner roles.
-- Filters out deceased patients and dummy patients.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_ACTIVE_PATIENTS (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_ID NUMBER COMMENT 'Surrogate key for the patient',
    PRIMARY_PATIENT_ID VARCHAR COMMENT 'ID of the primary patient record for this person',
    PATIENT_IDS ARRAY COMMENT 'Array of all patient IDs associated with this person',
    IS_ACTIVE BOOLEAN COMMENT 'Whether the person is currently an active patient',
    IS_DECEASED BOOLEAN COMMENT 'Whether the person is recorded as deceased',
    IS_DUMMY_PATIENT BOOLEAN COMMENT 'Whether the person is a dummy patient record',
    IS_CONFIDENTIAL BOOLEAN COMMENT 'Whether the person has confidential status',
    IS_SPINE_SENSITIVE BOOLEAN COMMENT 'Whether the person has spine sensitive status',
    BIRTH_YEAR NUMBER COMMENT 'Year of birth',
    BIRTH_MONTH NUMBER COMMENT 'Month of birth',
    DEATH_YEAR NUMBER COMMENT 'Year of death (NULL if alive)',
    DEATH_MONTH NUMBER COMMENT 'Month of death (NULL if alive)',
    -- Practice details from Episode of Care and Organisation tables
    REGISTERED_PRACTICE_ID VARCHAR COMMENT 'ID of the practice where the person is currently registered',
    PRACTICE_CODE VARCHAR COMMENT 'Organisation code of the current practice',
    PRACTICE_NAME VARCHAR COMMENT 'Name of the current practice',
    PRACTICE_TYPE_CODE VARCHAR COMMENT 'Type code of the current practice',
    PRACTICE_TYPE_DESC VARCHAR COMMENT 'Type description of the current practice',
    PRACTICE_POSTCODE VARCHAR COMMENT 'Postcode of the current practice',
    PRACTICE_PARENT_ORG_ID VARCHAR COMMENT 'Parent organisation ID of the current practice',
    PRACTICE_OPEN_DATE DATE COMMENT 'Date when the current practice opened',
    PRACTICE_CLOSE_DATE DATE COMMENT 'Date when the current practice closed/will close (if applicable)',
    PRACTICE_IS_OBSOLETE BOOLEAN COMMENT 'Flag indicating if the current practice is marked as obsolete',
    REGISTRATION_START_DATE TIMESTAMP_NTZ COMMENT 'Start date of current registration period',
    REGISTRATION_END_DATE TIMESTAMP_NTZ COMMENT 'End date of current registration period (NULL for active registrations)',
    RECORD_OWNER_ORG_CODE VARCHAR COMMENT 'Organisation code of the record owner',
    LATEST_RECORD_DATE TIMESTAMP_NTZ COMMENT 'Date of the most recent patient record update'
)
COMMENT = 'Dimension table providing active patient status at person level using Episode of Care registration periods. Excludes deceased patients and dummy patients. Only includes patients with current active registrations (end date is NULL).'
TARGET_LAG = '4 hours'
REFRESH_MODE = auto
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH PatientIdsPerPerson AS (
    -- First collect all patient IDs for each person
    SELECT
        pp."person_id",
        ARRAY_AGG(DISTINCT pp."patient_id") WITHIN GROUP (ORDER BY pp."patient_id") AS patient_ids
    FROM
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    GROUP BY
        pp."person_id"
),
CurrentRegistrations AS (
    -- Get current active registrations from Episode of Care
    SELECT
        eoc."person_id",
        eoc."organisation_id",
        eoc."episode_of_care_start_date",
        eoc."episode_of_care_end_date",
        -- Rank to get the most recent registration for each person
        ROW_NUMBER() OVER (
            PARTITION BY eoc."person_id"
            ORDER BY eoc."episode_of_care_start_date" DESC
        ) AS registration_rank
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.EPISODE_OF_CARE eoc
    WHERE eoc."person_id" IS NOT NULL
        AND eoc."organisation_id" IS NOT NULL
        AND eoc."episode_of_care_start_date" IS NOT NULL
        AND eoc."episode_of_care_end_date" IS NULL  -- Only current active registrations
        -- Add episode type filter if needed to identify registration episodes
        -- AND eoc."episode_type_raw_concept_id" = 'REGISTRATION_TYPE_ID'
),
LatestPatientRecordPerPerson AS (
    -- Get the latest patient record for each person with current registration
    SELECT
        pp."person_id" AS PERSON_ID,
        p."sk_patient_id" AS SK_PATIENT_ID,
        per."primary_patient_id" AS PRIMARY_PATIENT_ID,
        pip.patient_ids AS PATIENT_IDS,
        -- Determine if patient is active based on various criteria
        CASE
            WHEN p."death_year" IS NOT NULL THEN FALSE -- Deceased
            WHEN p."is_dummy_patient" THEN FALSE -- Dummy patient
            WHEN cr."person_id" IS NULL THEN FALSE -- No current registration
            WHEN o."close_date" IS NOT NULL THEN FALSE -- Practice closed
            WHEN o."is_obsolete" THEN FALSE -- Practice obsolete
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
        -- Practice details from Episode of Care and Organisation
        cr."organisation_id" AS REGISTERED_PRACTICE_ID,
        o."organisation_code" AS PRACTICE_CODE,
        o."name" AS PRACTICE_NAME,
        o."type_code" AS PRACTICE_TYPE_CODE,
        o."type_desc" AS PRACTICE_TYPE_DESC,
        o."postcode" AS PRACTICE_POSTCODE,
        o."parent_organisation_id" AS PRACTICE_PARENT_ORG_ID,
        o."open_date" AS PRACTICE_OPEN_DATE,
        o."close_date" AS PRACTICE_CLOSE_DATE,
        o."is_obsolete" AS PRACTICE_IS_OBSOLETE,
        cr."episode_of_care_start_date" AS REGISTRATION_START_DATE,
        cr."episode_of_care_end_date" AS REGISTRATION_END_DATE,
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
    JOIN
        PatientIdsPerPerson pip
        ON pp."person_id" = pip."person_id"
    LEFT JOIN
        CurrentRegistrations cr
        ON pp."person_id" = cr."person_id"
        AND cr.registration_rank = 1  -- Only most recent registration
    LEFT JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.ORGANISATION o
        ON cr."organisation_id" = o."id"
)
-- Select only the latest record per person
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    PRIMARY_PATIENT_ID,
    PATIENT_IDS,
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
    REGISTRATION_END_DATE,
    RECORD_OWNER_ORG_CODE,
    LATEST_RECORD_DATE
FROM
    LatestPatientRecordPerPerson
WHERE
    record_rank = 1
    AND IS_ACTIVE = TRUE; -- Only include active patients
