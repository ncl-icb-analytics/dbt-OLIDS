-- ==========================================================================
-- Dimension Dynamic Table holding active patient status at person level.
-- Filters out deceased patients (using death_year) and dummy patients.
-- Links PATIENT_PERSON, PATIENT, PERSON, and ORGANISATION tables.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_ACTIVE_PATIENTS (
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
    -- Practice details from ORGANISATION
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
    RECORD_OWNER_ORG_CODE VARCHAR COMMENT 'Organisation code of the record owner',
    LATEST_RECORD_DATE TIMESTAMP_NTZ COMMENT 'Date of the most recent patient record update'
)
COMMENT = 'Dimension table providing active patient status at person level. Excludes deceased patients (using death_year) and dummy patients. Links PATIENT_PERSON, PATIENT, PERSON, and ORGANISATION tables.'
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
LatestPatientRecordPerPerson AS (
    -- Get the latest patient record for each person
    SELECT
        pp."person_id" AS PERSON_ID,
        p."sk_patient_id" AS SK_PATIENT_ID,
        per."primary_patient_id" AS PRIMARY_PATIENT_ID,
        pip.patient_ids AS PATIENT_IDS,
        -- Determine if patient is active based on various criteria
        CASE
            WHEN p."death_year" IS NOT NULL THEN FALSE -- Deceased
            WHEN p."is_dummy_patient" THEN FALSE -- Dummy patient
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
        -- Practice details from ORGANISATION
        p."registered_practice_id" AS REGISTERED_PRACTICE_ID,
        o."organisation_code" AS PRACTICE_CODE,
        o."name" AS PRACTICE_NAME,
        o."type_code" AS PRACTICE_TYPE_CODE,
        o."type_desc" AS PRACTICE_TYPE_DESC,
        o."postcode" AS PRACTICE_POSTCODE,
        o."parent_organisation_id" AS PRACTICE_PARENT_ORG_ID,
        o."open_date" AS PRACTICE_OPEN_DATE,
        o."close_date" AS PRACTICE_CLOSE_DATE,
        o."is_obsolete" AS PRACTICE_IS_OBSOLETE,
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
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.ORGANISATION o
        ON p."registered_practice_id" = o."id"
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
    RECORD_OWNER_ORG_CODE,
    LATEST_RECORD_DATE
FROM
    LatestPatientRecordPerPerson
WHERE
    record_rank = 1
    AND IS_ACTIVE = TRUE; -- Only include active patients 