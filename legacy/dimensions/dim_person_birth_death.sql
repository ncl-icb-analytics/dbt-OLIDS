-- ==========================================================================
-- Person Birth and Death Dimension Dynamic Table
-- Core birth and death information for each person
-- Designed to be reused by other dimension tables for age calculations
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_BIRTH_DEATH (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    BIRTH_YEAR NUMBER COMMENT 'Year of birth as recorded for the patient',
    BIRTH_MONTH NUMBER COMMENT 'Month of birth as recorded for the patient',
    BIRTH_DATE_APPROX DATE COMMENT 'Approximate date of birth, calculated as the exact midpoint of the birth month/year',
    DEATH_YEAR NUMBER COMMENT 'Year of death (NULL if alive)',
    DEATH_MONTH NUMBER COMMENT 'Month of death (NULL if alive)',
    DEATH_DATE_APPROX DATE COMMENT 'Approximate date of death, calculated as the exact midpoint of the death month/year (NULL if alive)',
    IS_DECEASED BOOLEAN COMMENT 'Whether the person is recorded as deceased',
    IS_DUMMY_PATIENT BOOLEAN COMMENT 'Whether the person is a dummy patient record'
)
COMMENT = 'Core birth and death information for each person. Provides foundation data for age calculations and demographic analysis. Calculates birth/death dates as the exact midpoint of the recorded month for optimal statistical precision.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT DISTINCT
    pp."person_id" AS PERSON_ID,
    p."sk_patient_id" AS SK_PATIENT_ID,
    p."birth_year" AS BIRTH_YEAR,
    p."birth_month" AS BIRTH_MONTH,
    -- Calculate approximate birth date using exact midpoint of the month
    CASE
        WHEN p."birth_year" IS NOT NULL AND p."birth_month" IS NOT NULL
        THEN DATEADD(day,
            FLOOR(DAY(LAST_DAY(TO_DATE(p."birth_year" || '-' || p."birth_month" || '-01'))) / 2),
            TO_DATE(p."birth_year" || '-' || p."birth_month" || '-01')
        )
        ELSE NULL
    END AS BIRTH_DATE_APPROX,
    p."death_year" AS DEATH_YEAR,
    p."death_month" AS DEATH_MONTH,
    -- Calculate approximate death date using exact midpoint of the month
    CASE
        WHEN p."death_year" IS NOT NULL AND p."death_month" IS NOT NULL
        THEN DATEADD(day,
            FLOOR(DAY(LAST_DAY(TO_DATE(p."death_year" || '-' || p."death_month" || '-01'))) / 2),
            TO_DATE(p."death_year" || '-' || p."death_month" || '-01')
        )
        ELSE NULL
    END AS DEATH_DATE_APPROX,
    p."death_year" IS NOT NULL AS IS_DECEASED,
    COALESCE(p."is_dummy_patient", FALSE) AS IS_DUMMY_PATIENT
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
INNER JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON p."id" = pp."patient_id"
WHERE p."birth_year" IS NOT NULL AND p."birth_month" IS NOT NULL
