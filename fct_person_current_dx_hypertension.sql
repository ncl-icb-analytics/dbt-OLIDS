-- Creates a dynamic table identifying patients currently coded as having Hypertension (HTN).
-- Includes latest BP reading, its context (Home/ABPM), and an inferred HTN stage
-- applying NICE-aligned thresholds based on the reading's context.
-- Inclusion criteria: Patients aged >= 18 with a HYP_COD date later than their latest HYPRES_COD date (or no HYPRES_COD).
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_DX_HYPERTENSION (
    -- Identifiers
    PERSON_ID VARCHAR,
    SK_PATIENT_ID NUMBER(38,0),
    -- Demographics
    AGE NUMBER,
    -- Hypertension Coding Dates & Status
    EARLIEST_HTN_DIAGNOSIS_DATE DATE,
    LATEST_HTN_DIAGNOSIS_DATE DATE,
    LATEST_HTN_RESOLVED_DATE DATE,
    IS_ON_HTN_REGISTER_CALC BOOLEAN,
    -- Latest Blood Pressure Info & Staging
    LATEST_BP_DATE DATE,
    LATEST_BP_SYSTOLIC_VALUE NUMBER,
    LATEST_BP_DIASTOLIC_VALUE NUMBER,
    LATEST_BP_IS_HOME BOOLEAN,        -- Was the latest BP reading event flagged as Home?
    LATEST_BP_IS_ABPM BOOLEAN,        -- Was the latest BP reading event flagged as ABPM?
    LATEST_BP_HTN_STAGE VARCHAR,       -- HTN Stage inferred from latest BP, using context-specific NICE thresholds
    -- Coding Traceability
    ALL_HTN_OBSERVATION_IDS ARRAY,
    ALL_HTN_CONCEPT_CODES ARRAY,
    ALL_HTN_CONCEPT_DISPLAYS ARRAY,
    ALL_HTN_SOURCE_CLUSTER_IDS ARRAY
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Identifies patients aged >= 18 currently coded as having Hypertension (latest HYP_COD > latest HYPRES_COD). Joins latest BP reading and its context (Home/ABPM) from BLOOD_PRESSURE_LATEST. Calculates a simplified HTN stage (Normal, Stage 1, Stage 2, Severe) using context-specific NICE-aligned thresholds applied to the single latest reading.'
AS
WITH BaseObservationsAndClusters AS (
    -- Select base observation details for HTN Diagnosis and Resolved clusters ONLY
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS CONCEPT_DISPLAY,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('HYP_COD', 'HYPRES_COD')
),
FilteredByAge AS (
    -- Join with age dimension and filter for age >= 18
    SELECT
        boc.*,
        age.AGE
    FROM BaseObservationsAndClusters boc
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age ON boc.PERSON_ID = age.PERSON_ID
    WHERE age.AGE >= 18
),
PersonLevelHTNCodingAggregation AS (
    -- Aggregate HTN diagnosis/resolved code information per person
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(AGE) as AGE,
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'HYP_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_HTN_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'HYP_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_HTN_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'HYPRES_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_HTN_RESOLVED_DATE,
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_HTN_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_HTN_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_HTN_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_HTN_SOURCE_CLUSTER_IDS,
        CASE
            WHEN LATEST_HTN_DIAGNOSIS_DATE IS NOT NULL AND
                 (LATEST_HTN_RESOLVED_DATE IS NULL OR LATEST_HTN_DIAGNOSIS_DATE > LATEST_HTN_RESOLVED_DATE)
            THEN TRUE
            ELSE FALSE
        END AS IS_ON_HTN_REGISTER_CALC
    FROM FilteredByAge
    GROUP BY PERSON_ID
)
-- Final selection: Filter for those on HTN register and join latest BP info + context
SELECT
    htn_agg.PERSON_ID,
    htn_agg.SK_PATIENT_ID,
    htn_agg.AGE,
    -- Coding Dates & Status
    htn_agg.EARLIEST_HTN_DIAGNOSIS_DATE,
    htn_agg.LATEST_HTN_DIAGNOSIS_DATE,
    htn_agg.LATEST_HTN_RESOLVED_DATE,
    htn_agg.IS_ON_HTN_REGISTER_CALC,
    -- Latest BP Details (Joined)
    bp.CLINICAL_EFFECTIVE_DATE AS LATEST_BP_DATE,
    bp.SYSTOLIC_VALUE AS LATEST_BP_SYSTOLIC_VALUE,
    bp.DIASTOLIC_VALUE AS LATEST_BP_DIASTOLIC_VALUE,
    bp.IS_HOME_BP_EVENT AS LATEST_BP_IS_HOME, -- Include context flag
    bp.IS_ABPM_BP_EVENT AS LATEST_BP_IS_ABPM, -- Include context flag
    -- Calculate HTN Stage based on the LATEST BP reading using context-specific NICE thresholds
    CASE
        WHEN bp.SYSTOLIC_VALUE IS NULL OR bp.DIASTOLIC_VALUE IS NULL THEN NULL -- Cannot stage if values are missing
        -- Apply Severe threshold first (applies regardless of context usually)
        WHEN bp.SYSTOLIC_VALUE >= 180 OR bp.DIASTOLIC_VALUE >= 120 THEN 'Severe HTN'
        -- Check if Home or ABPM context applies for lower thresholds
        WHEN LATEST_BP_IS_HOME OR LATEST_BP_IS_ABPM THEN
            CASE
                WHEN bp.SYSTOLIC_VALUE >= 150 OR bp.DIASTOLIC_VALUE >= 95 THEN 'Stage 2 HTN (Home/ABPM Threshold)'
                WHEN bp.SYSTOLIC_VALUE >= 135 OR bp.DIASTOLIC_VALUE >= 85 THEN 'Stage 1 HTN (Home/ABPM Threshold)'
                ELSE 'Normal (Home/ABPM Threshold)'
            END
        -- Otherwise, assume Clinic context and apply clinic thresholds
        ELSE
            CASE
                WHEN bp.SYSTOLIC_VALUE >= 160 OR bp.DIASTOLIC_VALUE >= 100 THEN 'Stage 2 HTN (Clinic Threshold)'
                WHEN bp.SYSTOLIC_VALUE >= 140 OR bp.DIASTOLIC_VALUE >= 90  THEN 'Stage 1 HTN (Clinic Threshold)'
                ELSE 'Normal / High Normal (Clinic Threshold)'
            END
    END AS LATEST_BP_HTN_STAGE,
    -- Coding Traceability
    htn_agg.ALL_HTN_OBSERVATION_IDS,
    htn_agg.ALL_HTN_CONCEPT_CODES,
    htn_agg.ALL_HTN_CONCEPT_DISPLAYS,
    htn_agg.ALL_HTN_SOURCE_CLUSTER_IDS
FROM PersonLevelHTNCodingAggregation htn_agg
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_LATEST bp
    ON htn_agg.PERSON_ID = bp.PERSON_ID
WHERE htn_agg.IS_ON_HTN_REGISTER_CALC = TRUE;



