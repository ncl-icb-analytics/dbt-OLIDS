-- ==========================================================================
-- Wide View of LTC Summary - Person Condition Register Flags
-- Pivots FCT_PERSON_LTC_SUMMARY from long format (one row per condition) 
-- to wide format (one row per person with boolean flags for each condition)
-- Provides easy boolean lookup for condition register status per person
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_CONDITION_REGISTERS (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    -- Boolean condition flags (pivoted from FCT_PERSON_LTC_SUMMARY.CONDITION_CODE)
    HAS_AF BOOLEAN COMMENT 'TRUE if person has Atrial Fibrillation (CONDITION_CODE = AF)',
    HAS_AST BOOLEAN COMMENT 'TRUE if person has Asthma (CONDITION_CODE = AST)',
    HAS_CA BOOLEAN COMMENT 'TRUE if person has Cancer (CONDITION_CODE = CA)',
    HAS_CHD BOOLEAN COMMENT 'TRUE if person has Coronary Heart Disease (CONDITION_CODE = CHD)',
    HAS_CKD BOOLEAN COMMENT 'TRUE if person has Chronic Kidney Disease (CONDITION_CODE = CKD)',
    HAS_COPD BOOLEAN COMMENT 'TRUE if person has Chronic Obstructive Pulmonary Disease (CONDITION_CODE = COPD)',
    HAS_CYP_AST BOOLEAN COMMENT 'TRUE if person has Children and Young People Asthma (CONDITION_CODE = CYP_AST)',
    HAS_DEM BOOLEAN COMMENT 'TRUE if person has Dementia (CONDITION_CODE = DEM)',
    HAS_DEP BOOLEAN COMMENT 'TRUE if person has Depression (CONDITION_CODE = DEP)',
    HAS_DM BOOLEAN COMMENT 'TRUE if person has Diabetes Mellitus (CONDITION_CODE = DM)',
    HAS_EPIL BOOLEAN COMMENT 'TRUE if person has Epilepsy (CONDITION_CODE = EPIL)',
    HAS_FHYP BOOLEAN COMMENT 'TRUE if person has Familial Hypercholesterolaemia (CONDITION_CODE = FHYP)',
    HAS_GDM BOOLEAN COMMENT 'TRUE if person has Gestational Diabetes Mellitus (CONDITION_CODE = GDM)',
    HAS_HF BOOLEAN COMMENT 'TRUE if person has Heart Failure (CONDITION_CODE = HF)',
    HAS_HTN BOOLEAN COMMENT 'TRUE if person has Hypertension (CONDITION_CODE = HTN)',
    HAS_LD BOOLEAN COMMENT 'TRUE if person has Learning Disability (CONDITION_CODE = LD)',
    HAS_NAF BOOLEAN COMMENT 'TRUE if person has Non-Alcoholic Fatty Liver Disease (CONDITION_CODE = NAF)',
    HAS_NDH BOOLEAN COMMENT 'TRUE if person has Non-Diabetic Hyperglycaemia (CONDITION_CODE = NDH)',
    HAS_OB BOOLEAN COMMENT 'TRUE if person has Obesity (CONDITION_CODE = OB)',
    HAS_OST BOOLEAN COMMENT 'TRUE if person has Osteoporosis (CONDITION_CODE = OST)',
    HAS_PAD BOOLEAN COMMENT 'TRUE if person has Peripheral Arterial Disease (CONDITION_CODE = PAD)',
    HAS_PC BOOLEAN COMMENT 'TRUE if person has Palliative Care (CONDITION_CODE = PC)',
    HAS_RA BOOLEAN COMMENT 'TRUE if person has Rheumatoid Arthritis (CONDITION_CODE = RA)',
    HAS_SMI BOOLEAN COMMENT 'TRUE if person has Severe Mental Illness (CONDITION_CODE = SMI)',
    HAS_STIA BOOLEAN COMMENT 'TRUE if person has Stroke/Transient Ischaemic Attack (CONDITION_CODE = STIA)'
)
COMMENT = 'Wide view of FCT_PERSON_LTC_SUMMARY - pivots condition codes into boolean flags per person. Transforms long format (one row per person-condition) into wide format (one row per person with boolean columns for each condition). This provides easier consumption for dashboards, feature stores, and analytics requiring condition flags.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
-- Pivot FCT_PERSON_LTC_SUMMARY from long to wide format
-- Each CONDITION_CODE becomes a boolean HAS_{CONDITION} column
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    -- Create boolean flags for each condition (pivoting CONDITION_CODE values)
    MAX(CASE WHEN CONDITION_CODE = 'AF' THEN TRUE ELSE FALSE END) AS HAS_AF,
    MAX(CASE WHEN CONDITION_CODE = 'AST' THEN TRUE ELSE FALSE END) AS HAS_AST,
    MAX(CASE WHEN CONDITION_CODE = 'CA' THEN TRUE ELSE FALSE END) AS HAS_CA,
    MAX(CASE WHEN CONDITION_CODE = 'CHD' THEN TRUE ELSE FALSE END) AS HAS_CHD,
    MAX(CASE WHEN CONDITION_CODE = 'CKD' THEN TRUE ELSE FALSE END) AS HAS_CKD,
    MAX(CASE WHEN CONDITION_CODE = 'COPD' THEN TRUE ELSE FALSE END) AS HAS_COPD,
    MAX(CASE WHEN CONDITION_CODE = 'CYP_AST' THEN TRUE ELSE FALSE END) AS HAS_CYP_AST,
    MAX(CASE WHEN CONDITION_CODE = 'DEM' THEN TRUE ELSE FALSE END) AS HAS_DEM,
    MAX(CASE WHEN CONDITION_CODE = 'DEP' THEN TRUE ELSE FALSE END) AS HAS_DEP,
    MAX(CASE WHEN CONDITION_CODE = 'DM' THEN TRUE ELSE FALSE END) AS HAS_DM,
    MAX(CASE WHEN CONDITION_CODE = 'EPIL' THEN TRUE ELSE FALSE END) AS HAS_EPIL,
    MAX(CASE WHEN CONDITION_CODE = 'FHYP' THEN TRUE ELSE FALSE END) AS HAS_FHYP,
    MAX(CASE WHEN CONDITION_CODE = 'GDM' THEN TRUE ELSE FALSE END) AS HAS_GDM,
    MAX(CASE WHEN CONDITION_CODE = 'HF' THEN TRUE ELSE FALSE END) AS HAS_HF,
    MAX(CASE WHEN CONDITION_CODE = 'HTN' THEN TRUE ELSE FALSE END) AS HAS_HTN,
    MAX(CASE WHEN CONDITION_CODE = 'LD' THEN TRUE ELSE FALSE END) AS HAS_LD,
    MAX(CASE WHEN CONDITION_CODE = 'NAF' THEN TRUE ELSE FALSE END) AS HAS_NAF,
    MAX(CASE WHEN CONDITION_CODE = 'NDH' THEN TRUE ELSE FALSE END) AS HAS_NDH,
    MAX(CASE WHEN CONDITION_CODE = 'OB' THEN TRUE ELSE FALSE END) AS HAS_OB,
    MAX(CASE WHEN CONDITION_CODE = 'OST' THEN TRUE ELSE FALSE END) AS HAS_OST,
    MAX(CASE WHEN CONDITION_CODE = 'PAD' THEN TRUE ELSE FALSE END) AS HAS_PAD,
    MAX(CASE WHEN CONDITION_CODE = 'PC' THEN TRUE ELSE FALSE END) AS HAS_PC,
    MAX(CASE WHEN CONDITION_CODE = 'RA' THEN TRUE ELSE FALSE END) AS HAS_RA,
    MAX(CASE WHEN CONDITION_CODE = 'SMI' THEN TRUE ELSE FALSE END) AS HAS_SMI,
    MAX(CASE WHEN CONDITION_CODE = 'STIA' THEN TRUE ELSE FALSE END) AS HAS_STIA
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_LTC_SUMMARY
GROUP BY 
    PERSON_ID,
    SK_PATIENT_ID 