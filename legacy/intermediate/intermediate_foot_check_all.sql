CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_FOOT_CHECK_ALL (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the foot check or related observation
    IS_UNSUITABLE BOOLEAN, -- Flag indicating if foot check was deemed unsuitable
    IS_DECLINED BOOLEAN, -- Flag indicating if patient declined foot check
    LEFT_FOOT_CHECKED BOOLEAN, -- Flag indicating if left foot was checked (either explicitly or via Townson scale)
    RIGHT_FOOT_CHECKED BOOLEAN, -- Flag indicating if right foot was checked (either explicitly or via Townson scale)
    BOTH_FEET_CHECKED BOOLEAN, -- Flag indicating if both feet were checked (either explicitly or via Townson scale)
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
COMMENT = 'Intermediate table containing all foot check records and related observations (unsuitable, declined, amputations, etc). Includes flags for foot status and check completion. One row per person per date.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH FootObservations AS (
    -- Get all relevant foot-related observations
    SELECT
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CLUSTER_ID,
        MC.CONCEPT_CODE AS CODE,
        MC.CODE_DESCRIPTION AS TERM,
        -- Check if code term contains 'left' or 'right' (case insensitive)
        REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*left.*') AS HAS_LEFT,
        REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*right.*') AS HAS_RIGHT,
        -- Check if code is a Townson scale and extract level
        REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level.*') AS IS_TOWNSON,
        CASE
            WHEN REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level 1.*') THEN 'Level 1'
            WHEN REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level 2.*') THEN 'Level 2'
            WHEN REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level 3.*') THEN 'Level 3'
            WHEN REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level 4.*') THEN 'Level 4'
            ELSE NULL
        END AS TOWNSON_LEVEL,
        -- Extract risk level from description
        CASE
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%low risk%' THEN 'Low'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%moderate risk%' THEN 'Moderate'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%increased risk%' THEN 'Moderate'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%high risk%' THEN 'High'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%ulcerated%' THEN 'Ulcerated'
            -- Map Townson scale levels to risk levels
            WHEN REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level 1.*') THEN 'Low'
            WHEN REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level 2.*') THEN 'Moderate'
            WHEN REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level 3.*') THEN 'High'
            WHEN REGEXP_LIKE(LOWER(MC.CODE_DESCRIPTION), '.*townson.*scale.*level 4.*') THEN 'High'
            ELSE NULL
        END AS RISK_LEVEL
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN (
        'FEPU_COD',    -- Foot check unsuitable
        'FEDEC_COD',   -- Foot check declined
        'FRC_COD',     -- Foot risk classification
        'CONABL_COD',  -- Congenital absence left foot
        'CONABR_COD',  -- Congenital absence right foot
        'AMPL_COD',    -- Left foot amputation
        'AMPR_COD'     -- Right foot amputation
    )
),
-- First aggregate foot status (amputations/absences) across all time
FootStatus AS (
    SELECT
        PERSON_ID,
        MAX(CASE WHEN CLUSTER_ID = 'CONABL_COD' THEN TRUE ELSE FALSE END) AS LEFT_FOOT_ABSENT,
        MAX(CASE WHEN CLUSTER_ID = 'CONABR_COD' THEN TRUE ELSE FALSE END) AS RIGHT_FOOT_ABSENT,
        MAX(CASE WHEN CLUSTER_ID = 'AMPL_COD' THEN TRUE ELSE FALSE END) AS LEFT_FOOT_AMPUTATED,
        MAX(CASE WHEN CLUSTER_ID = 'AMPR_COD' THEN TRUE ELSE FALSE END) AS RIGHT_FOOT_AMPUTATED
    FROM FootObservations
    GROUP BY PERSON_ID
),
-- Then get the check details for each date
CheckDetails AS (
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        -- Check status
        MAX(CASE WHEN CLUSTER_ID = 'FEPU_COD' THEN TRUE ELSE FALSE END) AS IS_UNSUITABLE,
        MAX(CASE WHEN CLUSTER_ID = 'FEDEC_COD' THEN TRUE ELSE FALSE END) AS IS_DECLINED,
        -- For foot checks (FRC_COD), set checked flags based on code description
        -- Left foot is checked if either:
        -- 1. We have an explicit left foot check, OR
        -- 2. A Townson scale was used (implying both feet)
        MAX(CASE
            WHEN CLUSTER_ID = 'FRC_COD' AND (HAS_LEFT OR IS_TOWNSON) THEN TRUE
            ELSE FALSE
        END) AS LEFT_FOOT_CHECKED,
        -- Right foot is checked if either:
        -- 1. We have an explicit right foot check, OR
        -- 2. A Townson scale was used (implying both feet)
        MAX(CASE
            WHEN CLUSTER_ID = 'FRC_COD' AND (HAS_RIGHT OR IS_TOWNSON) THEN TRUE
            ELSE FALSE
        END) AS RIGHT_FOOT_CHECKED,
        -- Both feet checked if either:
        -- 1. A Townson scale was used (implying both feet), OR
        -- 2. We have explicit checks for both left and right feet
        MAX(CASE
            WHEN CLUSTER_ID = 'FRC_COD' AND IS_TOWNSON THEN TRUE
            WHEN CLUSTER_ID = 'FRC_COD' AND HAS_LEFT AND EXISTS (
                SELECT 1 FROM FootObservations f2
                WHERE f2.PERSON_ID = FootObservations.PERSON_ID
                AND f2.CLINICAL_EFFECTIVE_DATE = FootObservations.CLINICAL_EFFECTIVE_DATE
                AND f2.CLUSTER_ID = 'FRC_COD'
                AND f2.HAS_RIGHT
            ) THEN TRUE
            ELSE FALSE
        END) AS BOTH_FEET_CHECKED,
        -- Get risk levels for each foot
        MAX(CASE
            WHEN CLUSTER_ID = 'FRC_COD' AND (HAS_LEFT OR IS_TOWNSON) THEN RISK_LEVEL
            ELSE NULL
        END) AS LEFT_FOOT_RISK_LEVEL,
        MAX(CASE
            WHEN CLUSTER_ID = 'FRC_COD' AND (HAS_RIGHT OR IS_TOWNSON) THEN RISK_LEVEL
            ELSE NULL
        END) AS RIGHT_FOOT_RISK_LEVEL,
        -- Get Townson scale level if used
        MAX(CASE
            WHEN IS_TOWNSON THEN TOWNSON_LEVEL
            ELSE NULL
        END) AS TOWNSON_SCALE_LEVEL,
        -- Collect all codes and terms for traceability
        ARRAY_AGG(DISTINCT CODE) WITHIN GROUP (ORDER BY CODE) AS ALL_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT TERM) WITHIN GROUP (ORDER BY TERM) AS ALL_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT CLUSTER_ID) WITHIN GROUP (ORDER BY CLUSTER_ID) AS ALL_SOURCE_CLUSTER_IDS
    FROM FootObservations
    GROUP BY
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE
)
-- Final selection combining check details with foot status
SELECT
    cd.PERSON_ID,
    cd.SK_PATIENT_ID,
    cd.CLINICAL_EFFECTIVE_DATE,
    cd.IS_UNSUITABLE,
    cd.IS_DECLINED,
    cd.LEFT_FOOT_CHECKED,
    cd.RIGHT_FOOT_CHECKED,
    cd.BOTH_FEET_CHECKED,
    fs.LEFT_FOOT_ABSENT,
    fs.RIGHT_FOOT_ABSENT,
    fs.LEFT_FOOT_AMPUTATED,
    fs.RIGHT_FOOT_AMPUTATED,
    cd.LEFT_FOOT_RISK_LEVEL,
    cd.RIGHT_FOOT_RISK_LEVEL,
    cd.TOWNSON_SCALE_LEVEL,
    cd.ALL_CONCEPT_CODES,
    cd.ALL_CONCEPT_DISPLAYS,
    cd.ALL_SOURCE_CLUSTER_IDS
FROM CheckDetails cd
LEFT JOIN FootStatus fs ON cd.PERSON_ID = fs.PERSON_ID;
