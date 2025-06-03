CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CYP_AST_61 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient (18 months to 17 years 364 days)
    HAS_HAD_REVIEW BOOLEAN, -- Flag indicating if person has had an asthma review in last 12 months
    LATEST_REVIEW_DATE DATE, -- Date of most recent asthma review
    -- Metadata
    LAST_REFRESH_DATE TIMESTAMP,
    INDICATOR_VERSION VARCHAR
)
COMMENT = 'Dimension table for LTC LCS case finding indicator CYP_AST_61: Children and young people (18 months to 17 years) with asthma who have had an asthma review in the last 12 months.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population of patients aged 18 months to 17 years 364 days
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        age.AGE,
        age.AGE_DAYS_APPROX
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        USING (PERSON_ID)
    WHERE age.AGE_DAYS_APPROX BETWEEN 547 AND 6574  -- 18 months to 17 years 364 days
),
Exclusions AS (
    -- Get patients to exclude
    SELECT DISTINCT PERSON_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_EXCLUSIONS
    WHERE HAS_EXCLUDING_CONDITION = TRUE
        OR HAS_TYPE2_DIABETES = TRUE
),
ResolvedAsthma AS (
    -- Get patients with resolved asthma using ASTRES_COD from mapped concepts
    SELECT DISTINCT
        PP."person_id" AS PERSON_ID,
        MAX(O."clinical_effective_date") AS LATEST_RESOLVED_DATE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    WHERE MC.CLUSTER_ID = 'ASTRES_COD'
    GROUP BY PP."person_id"
),
InclusionCriteria AS (
    -- Get patients meeting any of the inclusion criteria
    SELECT DISTINCT PERSON_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE (
        -- Asthma medications in last 12 months
        (CLUSTER_ID = 'ASTHMA_MEDICATIONS' AND CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -12, CURRENT_DATE()))
        OR
        -- Prednisolone/Montelukast in last 12 months
        (CLUSTER_ID IN ('ASTHMA_PREDNISOLONE', 'MONTELUKAST_MEDICATIONS') AND CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -12, CURRENT_DATE()))
        OR
        -- Suspected asthma or viral wheeze in last 12 months
        (CLUSTER_ID IN ('SUSPECTED_ASTHMA', 'VIRAL_WHEEZE') AND CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -12, CURRENT_DATE()))
    )
),
AsthmaReviews AS (
    -- Get asthma reviews from last 12 months
    SELECT
        PERSON_ID,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_REVIEW_DATE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'ASTHMA_REVIEW'
        AND CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY PERSON_ID
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE 
        WHEN ar.LATEST_REVIEW_DATE IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS HAS_HAD_REVIEW,
    ar.LATEST_REVIEW_DATE,
    -- Metadata
    CURRENT_TIMESTAMP() AS LAST_REFRESH_DATE,
    '1.0' AS INDICATOR_VERSION
FROM BasePopulation bp
JOIN InclusionCriteria ic
    USING (PERSON_ID)
LEFT JOIN AsthmaReviews ar
    USING (PERSON_ID)
WHERE NOT EXISTS (
    SELECT 1 FROM Exclusions e 
    WHERE e.PERSON_ID = bp.PERSON_ID
)
AND NOT EXISTS (
    SELECT 1 FROM ResolvedAsthma ra 
    WHERE ra.PERSON_ID = bp.PERSON_ID
); 