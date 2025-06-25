CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CYP_AST_61 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient (18 months to under 18 years)
    HAS_ASTHMA_SYMPTOMS BOOLEAN, -- Flag indicating if person has asthma symptoms (medications or suspected asthma)
    LATEST_SYMPTOM_DATE DATE, -- Date of most recent asthma symptom (medication or suspected asthma)
    -- Metadata
    LAST_REFRESH_DATE TIMESTAMP,
    INDICATOR_VERSION VARCHAR
)
COMMENT = 'Dimension table for LTC LCS case finding indicator CYP_AST_61: Children and young people (18 months to under 18 years) with asthma symptoms (medications or suspected asthma) who do not have a formal asthma diagnosis. This indicator aims to identify children who may need formal asthma diagnosis and care.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population of patients aged 18 months to under 18 years
    -- Note: Base population already excludes those on LTC registers and with diabetes
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        age.AGE,
        age.AGE_DAYS_APPROX
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        USING (PERSON_ID)
    WHERE age.AGE_DAYS_APPROX >= 547  -- 18 months
        AND age.AGE < 18  -- under 18 years
),
AsthmaDiagnosis AS (
    -- Get patients with asthma diagnosis (excluding resolved asthma)
    SELECT DISTINCT
        PERSON_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID IN ('ASTHMA_DIAGNOSIS', 'ASTHMA_RESOLVED')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
    AND CLUSTER_ID != 'ASTHMA_RESOLVED'  -- Exclude resolved asthma
),
AsthmaSymptoms AS (
    -- Get patients with asthma symptoms in last 12 months
    SELECT
        PERSON_ID,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_SYMPTOM_DATE
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
    GROUP BY PERSON_ID
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE
        WHEN symptoms.LATEST_SYMPTOM_DATE IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS HAS_ASTHMA_SYMPTOMS,
    symptoms.LATEST_SYMPTOM_DATE,
    -- Metadata
    CURRENT_TIMESTAMP() AS LAST_REFRESH_DATE,
    '1.0' AS INDICATOR_VERSION
FROM BasePopulation bp
JOIN AsthmaSymptoms symptoms
    USING (PERSON_ID)
WHERE NOT EXISTS (
    SELECT 1 FROM AsthmaDiagnosis ad
    WHERE ad.PERSON_ID = bp.PERSON_ID
);
