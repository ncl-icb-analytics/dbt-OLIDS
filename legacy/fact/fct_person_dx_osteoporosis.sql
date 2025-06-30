CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_OSTEOPOROSIS (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_OSTEOPOROSIS_REGISTER BOOLEAN, -- Flag indicating if person is on the osteoporosis register
    HAS_OSTEOPOROSIS_DIAGNOSIS BOOLEAN, -- Flag indicating if person has an osteoporosis diagnosis
    HAS_DXA_SCAN BOOLEAN, -- Flag indicating if person has a DXA scan
    HAS_DXA_T_SCORE BOOLEAN, -- Flag indicating if person has a DXA T-score
    HAS_FRAGILITY_FRACTURE BOOLEAN, -- Flag indicating if person has a fragility fracture after April 2012
    HAS_VALID_DXA_CONFIRMATION BOOLEAN, -- Flag indicating if person has either DXA scan or T-score <= -2.5
    EARLIEST_OSTEOPOROSIS_DATE DATE, -- Earliest osteoporosis diagnosis date
    EARLIEST_DXA_DATE DATE, -- Earliest DXA scan date
    EARLIEST_DXA_T_SCORE_DATE DATE, -- Earliest DXA T-score date
    EARLIEST_FRAGILITY_FRACTURE_DATE DATE, -- Earliest fragility fracture date after April 2012
    LATEST_OSTEOPOROSIS_DATE DATE, -- Latest osteoporosis diagnosis date
    LATEST_DXA_DATE DATE, -- Latest DXA scan date
    LATEST_DXA_T_SCORE_DATE DATE, -- Latest DXA T-score date
    LATEST_FRAGILITY_FRACTURE_DATE DATE, -- Latest fragility fracture date after April 2012
    LATEST_DXA_T_SCORE NUMBER, -- Latest DXA T-score value
    ALL_OSTEOPOROSIS_CONCEPT_CODES ARRAY, -- All osteoporosis concept codes
    ALL_OSTEOPOROSIS_CONCEPT_DISPLAYS ARRAY, -- All osteoporosis concept display terms
    ALL_DXA_CONCEPT_CODES ARRAY, -- All DXA scan concept codes
    ALL_DXA_CONCEPT_DISPLAYS ARRAY, -- All DXA scan concept display terms
    ALL_FRAGILITY_FRACTURE_CONCEPT_CODES ARRAY, -- All fragility fracture concept codes
    ALL_FRAGILITY_FRACTURE_CONCEPT_DISPLAYS ARRAY, -- All fragility fracture concept display terms
    ALL_FRACTURE_SITES ARRAY -- All fracture sites
)
COMMENT = 'Fact table for osteoporosis register. Includes patients aged 50-74 who meet all of: 1) have a fragility fracture after April 2012, 2) have an osteoporosis diagnosis, and 3) have either a DXA scan or a DXA T-score <= -2.5. Tracks diagnosis dates, DXA results, and fracture history.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH FilteredByAge AS (
    -- Get all relevant patients with their age
    SELECT
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        AGE.AGE,
        O.IS_OSTEOPOROSIS_DIAGNOSIS,
        O.IS_DXA_SCAN,
        O.IS_DXA_T_SCORE,
        O.DXA_T_SCORE,
        O.EARLIEST_OSTEOPOROSIS_DATE,
        O.EARLIEST_DXA_DATE,
        O.EARLIEST_DXA_T_SCORE_DATE,
        O.LATEST_OSTEOPOROSIS_DATE,
        O.LATEST_DXA_DATE,
        O.LATEST_DXA_T_SCORE_DATE,
        O.ALL_OSTEOPOROSIS_CONCEPT_CODES,
        O.ALL_OSTEOPOROSIS_CONCEPT_DISPLAYS,
        O.ALL_DXA_CONCEPT_CODES,
        O.ALL_DXA_CONCEPT_DISPLAYS,
        F.IS_FRAGILITY_FRACTURE,
        F.EARLIEST_FRACTURE_DATE AS EARLIEST_FRAGILITY_FRACTURE_DATE,
        F.LATEST_FRACTURE_DATE AS LATEST_FRAGILITY_FRACTURE_DATE,
        F.ALL_FRACTURE_CONCEPT_CODES,
        F.ALL_FRACTURE_CONCEPT_DISPLAYS,
        F.ALL_FRACTURE_SITES
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON PP."patient_id" = P."id"
    JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE AS AGE
        ON PP."person_id" = AGE.PERSON_ID
    LEFT JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_OSTEOPOROSIS_DIAGNOSES AS O
        ON PP."person_id" = O.PERSON_ID
    LEFT JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_FRAGILITY_FRACTURES AS F
        ON PP."person_id" = F.PERSON_ID
    WHERE AGE.AGE BETWEEN 50 AND 74
),
BusinessRules AS (
    SELECT
        f.*,
        -- Implement business rules for register inclusion
        CASE
            -- Rule 1: Age 50-74 (already filtered in CTE)
            -- Rule 2: Has fragility fracture after April 2012
            -- Rule 3: Has osteoporosis diagnosis
            -- Rule 4: Has DXA confirmation (either DXA scan or T-score <= -2.5)
            WHEN f.EARLIEST_FRAGILITY_FRACTURE_DATE IS NOT NULL
                AND f.EARLIEST_OSTEOPOROSIS_DATE IS NOT NULL
                AND (
                    f.EARLIEST_DXA_DATE IS NOT NULL
                    OR (f.EARLIEST_DXA_T_SCORE_DATE IS NOT NULL AND f.DXA_T_SCORE <= -2.5)
                ) THEN TRUE
            ELSE FALSE
        END AS IS_ON_OSTEOPOROSIS_REGISTER,
        f.EARLIEST_OSTEOPOROSIS_DATE IS NOT NULL AS HAS_OSTEOPOROSIS_DIAGNOSIS,
        f.EARLIEST_DXA_DATE IS NOT NULL AS HAS_DXA_SCAN,
        f.EARLIEST_DXA_T_SCORE_DATE IS NOT NULL AS HAS_DXA_T_SCORE,
        f.EARLIEST_FRAGILITY_FRACTURE_DATE IS NOT NULL AS HAS_FRAGILITY_FRACTURE,
        (
            f.EARLIEST_DXA_DATE IS NOT NULL
            OR (f.EARLIEST_DXA_T_SCORE_DATE IS NOT NULL AND f.DXA_T_SCORE <= -2.5)
        ) AS HAS_VALID_DXA_CONFIRMATION
    FROM FilteredByAge f
)
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    IS_ON_OSTEOPOROSIS_REGISTER,
    HAS_OSTEOPOROSIS_DIAGNOSIS,
    HAS_DXA_SCAN,
    HAS_DXA_T_SCORE,
    HAS_FRAGILITY_FRACTURE,
    HAS_VALID_DXA_CONFIRMATION,
    EARLIEST_OSTEOPOROSIS_DATE,
    EARLIEST_DXA_DATE,
    EARLIEST_DXA_T_SCORE_DATE,
    EARLIEST_FRAGILITY_FRACTURE_DATE,
    LATEST_OSTEOPOROSIS_DATE,
    LATEST_DXA_DATE,
    LATEST_DXA_T_SCORE_DATE,
    LATEST_FRAGILITY_FRACTURE_DATE,
    DXA_T_SCORE AS LATEST_DXA_T_SCORE,
    ALL_OSTEOPOROSIS_CONCEPT_CODES,
    ALL_OSTEOPOROSIS_CONCEPT_DISPLAYS,
    ALL_DXA_CONCEPT_CODES,
    ALL_DXA_CONCEPT_DISPLAYS,
    ALL_FRACTURE_CONCEPT_CODES AS ALL_FRAGILITY_FRACTURE_CONCEPT_CODES,
    ALL_FRACTURE_CONCEPT_DISPLAYS AS ALL_FRAGILITY_FRACTURE_CONCEPT_DISPLAYS,
    ALL_FRACTURE_SITES
FROM BusinessRules
WHERE IS_ON_OSTEOPOROSIS_REGISTER = TRUE; -- Only include patients on the osteoporosis register
