CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_FOOT_CHECK_LATEST (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the foot check or related observation
    IS_UNSUITABLE BOOLEAN, -- Flag indicating if foot check was deemed unsuitable
    IS_DECLINED BOOLEAN, -- Flag indicating if patient declined foot check
    LEFT_FOOT_CHECKED BOOLEAN, -- Flag indicating if left foot was checked
    RIGHT_FOOT_CHECKED BOOLEAN, -- Flag indicating if right foot was checked
    BOTH_FEET_CHECKED BOOLEAN, -- Flag indicating if both feet were checked (e.g. for Townson scale)
    LEFT_FOOT_ABSENT BOOLEAN, -- Flag indicating congenital absence of left foot
    RIGHT_FOOT_ABSENT BOOLEAN, -- Flag indicating congenital absence of right foot
    LEFT_FOOT_AMPUTATED BOOLEAN, -- Flag indicating left foot amputation
    RIGHT_FOOT_AMPUTATED BOOLEAN, -- Flag indicating right foot amputation
    LEFT_FOOT_RISK_LEVEL VARCHAR, -- Risk level for left foot (Low, Moderate, High, Ulcerated)
    RIGHT_FOOT_RISK_LEVEL VARCHAR, -- Risk level for right foot (Low, Moderate, High, Ulcerated)
    TOWNSON_SCALE_LEVEL VARCHAR, -- Young Townson footskin scale level if used
    ALL_CONCEPT_CODES ARRAY, -- Array of all unique concept codes contributing to this event
    ALL_CONCEPT_DISPLAYS ARRAY, -- Array of all unique concept display terms contributing to this event
    ALL_SOURCE_CLUSTER_IDS ARRAY -- Array of all unique source cluster IDs contributing to this event
)
COMMENT = 'Latest foot check record per person, sourced from INTERMEDIATE_FOOT_CHECK_ALL.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

SELECT 
    PERSON_ID,
    SK_PATIENT_ID,
    CLINICAL_EFFECTIVE_DATE,
    IS_UNSUITABLE,
    IS_DECLINED,
    LEFT_FOOT_CHECKED,
    RIGHT_FOOT_CHECKED,
    BOTH_FEET_CHECKED,
    LEFT_FOOT_ABSENT,
    RIGHT_FOOT_ABSENT,
    LEFT_FOOT_AMPUTATED,
    RIGHT_FOOT_AMPUTATED,
    LEFT_FOOT_RISK_LEVEL,
    RIGHT_FOOT_RISK_LEVEL,
    TOWNSON_SCALE_LEVEL,
    ALL_CONCEPT_CODES,
    ALL_CONCEPT_DISPLAYS,
    ALL_SOURCE_CLUSTER_IDS
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_FOOT_CHECK_ALL
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1; 