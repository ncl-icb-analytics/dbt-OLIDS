CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_SUMMARY (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    -- AF indicators
    IN_AF_61 BOOLEAN, -- Patients on digoxin, flecainide, propafenone or anticoagulants
    IN_AF_62 BOOLEAN, -- Patients over 65 missing pulse check in last 36 months
    -- CKD indicators
    IN_CKD_61 BOOLEAN, -- Patients with two consecutive eGFR readings < 60
    IN_CKD_62 BOOLEAN, -- Patients with gestational diabetes or pregnancy risk
    IN_CKD_63 BOOLEAN, -- Patients with HbA1c 46-48 mmol/mol and no HbA1c in last year
    IN_CKD_64 BOOLEAN, -- Patients with HbA1c 48-50 mmol/mol and no HbA1c in last year
    -- CVD indicators
    IN_CVD_61 BOOLEAN, -- Patients aged 40-83 with QRISK2 ≥ 10%
    IN_CVD_62 BOOLEAN, -- Patients aged 40-83 with QRISK2 ≥ 20%
    IN_CVD_63 BOOLEAN, -- Patients aged 40-83 with QRISK2 ≥ 10% and no statin
    IN_CVD_64 BOOLEAN, -- Patients aged 40-83 with QRISK2 ≥ 20% and no statin
    IN_CVD_65 BOOLEAN, -- Patients aged 40-83 with QRISK2 ≥ 10% and no statin (excluding high dose)
    IN_CVD_66 BOOLEAN, -- Patients aged 40-83 with QRISK2 ≥ 20% and no statin (excluding high dose)
    -- CYP Asthma indicator
    IN_CYP_AST_61 BOOLEAN, -- Children (18 months to under 18 years) with asthma symptoms who need formal diagnosis
    -- Diabetes indicators
    IN_DM_61 BOOLEAN, -- Patients with HbA1c 42-46 mmol/mol and no HbA1c in last year
    IN_DM_62 BOOLEAN, -- Patients with gestational diabetes or pregnancy risk
    IN_DM_63 BOOLEAN, -- Patients with HbA1c 46-48 mmol/mol and no HbA1c in last year
    IN_DM_64 BOOLEAN, -- Patients with HbA1c 48-50 mmol/mol and no HbA1c in last year
    IN_DM_65 BOOLEAN, -- Patients with HbA1c 50-58 mmol/mol and no HbA1c in last year
    IN_DM_66 BOOLEAN, -- Patients with HbA1c 58-64 mmol/mol and no HbA1c in last year
    -- Hypertension indicators
    IN_HTN_61 BOOLEAN, -- Patients with severe hypertension (Clinic: ≥180/120, Home: ≥170/115)
    IN_HTN_62 BOOLEAN, -- Patients with stage 2 hypertension (Clinic: ≥160/100, Home: ≥150/95)
    IN_HTN_63 BOOLEAN, -- Patients with stage 2 hypertension who are BSA with risk factors
    IN_HTN_65 BOOLEAN, -- Patients with stage 1 hypertension with risk factors (Clinic: ≥140/90, Home: ≥135/85)
    IN_HTN_66 BOOLEAN, -- Patients with stage 1 hypertension without risk factors (Clinic: ≥140/90, Home: ≥135/85)
    -- Metadata
    LAST_REFRESH_DATE TIMESTAMP,
    INDICATOR_VERSION VARCHAR
)
COMMENT = 'Summary table for all LTC LCS case finding indicators. Provides a single view of which indicators each person is included in. Note that this table focuses on indicator flags rather than detailed dimension data. For detailed information about each indicator, refer to the individual dimension tables.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        age.AGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        USING (PERSON_ID)
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    -- AF indicators
    af61.PERSON_ID IS NOT NULL AS IN_AF_61,
    af62.PERSON_ID IS NOT NULL AS IN_AF_62,
    -- CKD indicators
    ckd61.PERSON_ID IS NOT NULL AS IN_CKD_61,
    ckd62.PERSON_ID IS NOT NULL AS IN_CKD_62,
    ckd63.PERSON_ID IS NOT NULL AS IN_CKD_63,
    ckd64.PERSON_ID IS NOT NULL AS IN_CKD_64,
    -- CVD indicators
    cvd61.PERSON_ID IS NOT NULL AS IN_CVD_61,
    cvd62.PERSON_ID IS NOT NULL AS IN_CVD_62,
    cvd63.PERSON_ID IS NOT NULL AS IN_CVD_63,
    cvd64.PERSON_ID IS NOT NULL AS IN_CVD_64,
    cvd65.PERSON_ID IS NOT NULL AS IN_CVD_65,
    cvd66.PERSON_ID IS NOT NULL AS IN_CVD_66,
    -- CYP Asthma indicator
    cypast61.PERSON_ID IS NOT NULL AND cypast61.HAS_ASTHMA_SYMPTOMS AS IN_CYP_AST_61,
    -- Diabetes indicators
    dm61.PERSON_ID IS NOT NULL AS IN_DM_61,
    dm62.PERSON_ID IS NOT NULL AS IN_DM_62,
    dm63.PERSON_ID IS NOT NULL AS IN_DM_63,
    dm64.PERSON_ID IS NOT NULL AS IN_DM_64,
    dm65.PERSON_ID IS NOT NULL AS IN_DM_65,
    dm66.PERSON_ID IS NOT NULL AS IN_DM_66,
    -- Hypertension indicators
    htn61.PERSON_ID IS NOT NULL AND htn61.HAS_SEVERE_HYPERTENSION AS IN_HTN_61,
    htn62.PERSON_ID IS NOT NULL AND htn62.HAS_STAGE_2_HYPERTENSION AS IN_HTN_62,
    htn63.PERSON_ID IS NOT NULL AND htn63.HAS_STAGE_2_HYPERTENSION_BSA AS IN_HTN_63,
    htn65.PERSON_ID IS NOT NULL AND htn65.HAS_STAGE_1_HYPERTENSION_RISK AS IN_HTN_65,
    htn66.PERSON_ID IS NOT NULL AND htn66.HAS_STAGE_1_HYPERTENSION AS IN_HTN_66,
    -- Metadata
    CURRENT_TIMESTAMP() AS LAST_REFRESH_DATE,
    '1.0' AS INDICATOR_VERSION
FROM BasePopulation bp
-- AF indicators
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_AF_61 af61
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_AF_62 af62
    USING (PERSON_ID)
-- CKD indicators
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CKD_61 ckd61
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CKD_62 ckd62
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CKD_63 ckd63
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CKD_64 ckd64
    USING (PERSON_ID)
-- CVD indicators
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CVD_61 cvd61
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CVD_62 cvd62
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CVD_63 cvd63
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CVD_64 cvd64
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CVD_65 cvd65
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CVD_66 cvd66
    USING (PERSON_ID)
-- CYP Asthma indicator
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CYP_AST_61 cypast61
    USING (PERSON_ID)
-- Diabetes indicators
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_61 dm61
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_62 dm62
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_63 dm63
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_64 dm64
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_65 dm65
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_66 dm66
    USING (PERSON_ID)
-- Hypertension indicators
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_61 htn61
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_62 htn62
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_63 htn63
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_65 htn65
    USING (PERSON_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_66 htn66
    USING (PERSON_ID); 