CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_DEMOGRAPHICS (
    -- Core Identifiers
    PERSON_ID VARCHAR COMMENT 'Unique identifier for the person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',

    -- Demographics
    SEX VARCHAR COMMENT 'Sex of the person (Female, Male, Unknown)',

    -- Age Information
    AGE NUMBER COMMENT 'Current age in years (or age at death if deceased)',
    AGE_BAND_5Y VARCHAR COMMENT '5-year age band (e.g., 0-4, 5-9)',
    AGE_BAND_NHS VARCHAR COMMENT 'NHS standard age bands for healthcare reporting',
    AGE_LIFE_STAGE VARCHAR COMMENT 'Life stage categorisation (e.g., Child, Adult, Elderly)',
    IS_DECEASED BOOLEAN COMMENT 'Whether the person is recorded as deceased',
    BIRTH_DATE_APPROX DATE COMMENT 'Approximate date of birth (midpoint of birth month/year)',
    BIRTH_YEAR NUMBER COMMENT 'Year of birth',
    DEATH_DATE_APPROX DATE COMMENT 'Approximate date of death (midpoint of death month/year)',
    DEATH_YEAR NUMBER COMMENT 'Year of death (NULL if alive)',

    -- Ethnicity
    ETHNICITY_CATEGORY VARCHAR COMMENT 'Broad ethnicity category (e.g., White, Asian, Not Recorded)',
    ETHNICITY_SUBCATEGORY VARCHAR COMMENT 'More specific ethnicity subcategory',
    ETHNICITY_GRANULAR VARCHAR COMMENT 'Most granular ethnicity detail available',

    -- Language and Communication
    MAIN_LANGUAGE VARCHAR COMMENT 'Main spoken language',
    LANGUAGE_CATEGORY VARCHAR COMMENT 'Language category (e.g., English, Other)',
    INTERPRETER_NEEDED BOOLEAN COMMENT 'Whether an interpreter is needed',
    INTERPRETER_TYPE VARCHAR COMMENT 'Type of interpreter needed',

    -- Current Practice
    CURRENT_PRACTICE_CODE VARCHAR COMMENT 'Organisation code of current registered practice',
    CURRENT_PRACTICE_NAME VARCHAR COMMENT 'Name of current registered practice',
    CURRENT_PRACTICE_POSTCODE VARCHAR COMMENT 'Postcode of current practice',

    -- Practice Neighbourhood and Organisational Hierarchy
    PCN_CODE VARCHAR COMMENT 'Primary Care Network code',
    PCN_NAME VARCHAR COMMENT 'Primary Care Network name',
    LOCAL_AUTHORITY VARCHAR COMMENT 'Local authority area based on practice',
    PRACTICE_NEIGHBOURHOOD VARCHAR COMMENT 'Practice neighbourhood classification',

    -- Geographic Information (Placeholders for future data)
    LSOA_CODE VARCHAR COMMENT 'Lower Super Output Area code (placeholder)',
    LSOA_NAME VARCHAR COMMENT 'Lower Super Output Area name (placeholder)',
    WARD_CODE VARCHAR COMMENT 'Electoral ward code (placeholder)',
    WARD_NAME VARCHAR COMMENT 'Electoral ward name (placeholder)',
    IMD_DECILE NUMBER COMMENT 'Index of Multiple Deprivation decile (placeholder)',
    IMD_QUINTILE NUMBER COMMENT 'Index of Multiple Deprivation quintile (placeholder)',

    -- Registration Status
    REGISTRATION_START_DATE TIMESTAMP_NTZ COMMENT 'Start date of current practice registration'
)
COMMENT = 'Comprehensive demographics dimension table providing a single source of truth for person demographics.

Consolidates information from multiple dimension tables:
- Age and birth/death information
- Sex demographics
- Ethnicity details
- Language and communication needs
- Current practice registration
- Practice neighbourhood and organisational hierarchy
- Geographic placeholders for future LSOA, ward, and deprivation data

Serves as the primary demographics reference for analytics and reporting.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT
    -- Core Identifiers
    age.PERSON_ID,
    age.SK_PATIENT_ID,

    -- Demographics
    COALESCE(sex.SEX, 'Unknown') AS SEX,

    -- Age Information
    age.AGE,
    age.AGE_BAND_5Y,
    age.AGE_BAND_NHS,
    age.AGE_LIFE_STAGE,
    age.IS_DECEASED,
    age.BIRTH_DATE_APPROX,
    age.BIRTH_YEAR,
    age.DEATH_DATE_APPROX,
    age.DEATH_YEAR,

    -- Ethnicity
    eth.ETHNICITY_CATEGORY,
    eth.ETHNICITY_SUBCATEGORY,
    eth.ETHNICITY_GRANULAR,

    -- Language and Communication
    lang.LANGUAGE AS MAIN_LANGUAGE,
    lang.LANGUAGE_CATEGORY,
    COALESCE(lang.INTERPRETER_NEEDED, FALSE) AS INTERPRETER_NEEDED,
    lang.INTERPRETER_TYPE,

    -- Current Practice
    prac.PRACTICE_CODE AS CURRENT_PRACTICE_CODE,
    prac.PRACTICE_NAME AS CURRENT_PRACTICE_NAME,
    prac.PRACTICE_POSTCODE AS CURRENT_PRACTICE_POSTCODE,

    -- Practice Neighbourhood and Organisational Hierarchy
    pcn.PCN_CODE,
    pcn.PCN_NAME,
    nbhd.LOCAL_AUTHORITY,
    nbhd.PRACTICE_NEIGHBOURHOOD,

    -- Geographic Information (Placeholders for future data)
    NULL AS LSOA_CODE,
    NULL AS LSOA_NAME,
    NULL AS WARD_CODE,
    NULL AS WARD_NAME,
    NULL AS IMD_DECILE,
    NULL AS IMD_QUINTILE,

    -- Registration Status
    prac.REGISTRATION_START_DATE

FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age

-- Join demographics
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_SEX sex
    ON age.PERSON_ID = sex.PERSON_ID

LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_ETHNICITY eth
    ON age.PERSON_ID = eth.PERSON_ID

LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_MAIN_LANGUAGE lang
    ON age.PERSON_ID = lang.PERSON_ID

-- Join practice information
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_CURRENT_PRACTICE prac
    ON age.PERSON_ID = prac.PERSON_ID

-- Join practice neighbourhood information
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PRACTICE_NEIGHBOURHOOD nbhd
    ON prac.PRACTICE_CODE = nbhd.PRACTICE_CODE

-- Join practice PCN and commissioning information
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PRACTICE_PCN pcn
    ON prac.PRACTICE_CODE = pcn.PRACTICE_CODE;
