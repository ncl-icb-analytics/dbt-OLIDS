-- ==========================================================================
-- Dynamic Table providing condition register flags for all persons
-- Powers the feature store views with complete population coverage
-- Each person gets TRUE/FALSE for each condition based on LTC summary
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_CONDITION_REGISTERS (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    -- Individual condition flags
    HAS_AF BOOLEAN COMMENT 'TRUE if person has Atrial Fibrillation',
    HAS_AST BOOLEAN COMMENT 'TRUE if person has Asthma',
    HAS_CA BOOLEAN COMMENT 'TRUE if person has Cancer',
    HAS_CHD BOOLEAN COMMENT 'TRUE if person has Coronary Heart Disease',
    HAS_CKD BOOLEAN COMMENT 'TRUE if person has Chronic Kidney Disease',
    HAS_COPD BOOLEAN COMMENT 'TRUE if person has Chronic Obstructive Pulmonary Disease',
    HAS_CYP_AST BOOLEAN COMMENT 'TRUE if person has Children and Young People Asthma',
    HAS_DEM BOOLEAN COMMENT 'TRUE if person has Dementia',
    HAS_DEP BOOLEAN COMMENT 'TRUE if person has Depression',
    HAS_DM BOOLEAN COMMENT 'TRUE if person has Diabetes Mellitus',
    HAS_EPIL BOOLEAN COMMENT 'TRUE if person has Epilepsy',
    HAS_FHYP BOOLEAN COMMENT 'TRUE if person has Familial Hypercholesterolaemia',
    HAS_GDM BOOLEAN COMMENT 'TRUE if person has Gestational Diabetes Mellitus',
    HAS_HF BOOLEAN COMMENT 'TRUE if person has Heart Failure',
    HAS_HTN BOOLEAN COMMENT 'TRUE if person has Hypertension',
    HAS_LD BOOLEAN COMMENT 'TRUE if person has Learning Disability',
    HAS_NAF BOOLEAN COMMENT 'TRUE if person has Non-Alcoholic Fatty Liver Disease',
    HAS_NDH BOOLEAN COMMENT 'TRUE if person has Non-Diabetic Hyperglycaemia',
    HAS_OB BOOLEAN COMMENT 'TRUE if person has Obesity',
    HAS_OST BOOLEAN COMMENT 'TRUE if person has Osteoporosis',
    HAS_PAD BOOLEAN COMMENT 'TRUE if person has Peripheral Arterial Disease',
    HAS_PC BOOLEAN COMMENT 'TRUE if person has Palliative Care',
    HAS_RA BOOLEAN COMMENT 'TRUE if person has Rheumatoid Arthritis',
    HAS_SMI BOOLEAN COMMENT 'TRUE if person has Severe Mental Illness',
    HAS_STIA BOOLEAN COMMENT 'TRUE if person has Stroke/Transient Ischaemic Attack'
)
COMMENT = 'Condition register flags for all persons - powers feature store views with complete population coverage'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH AllPersons AS (
    -- Get all persons from DIM_PERSON (comprehensive population)
    SELECT DISTINCT 
        PERSON_ID,
        SK_PATIENT_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON
),
PersonConditions AS (
    -- Get all person-condition combinations from LTC summary
    SELECT 
        PERSON_ID,
        CONDITION_CODE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_LTC_SUMMARY
)
SELECT
    ap.PERSON_ID,
    ap.SK_PATIENT_ID,
    -- Create boolean flags for each condition
    MAX(CASE WHEN pc.CONDITION_CODE = 'AF' THEN TRUE ELSE FALSE END) AS HAS_AF,
    MAX(CASE WHEN pc.CONDITION_CODE = 'AST' THEN TRUE ELSE FALSE END) AS HAS_AST,
    MAX(CASE WHEN pc.CONDITION_CODE = 'CA' THEN TRUE ELSE FALSE END) AS HAS_CA,
    MAX(CASE WHEN pc.CONDITION_CODE = 'CHD' THEN TRUE ELSE FALSE END) AS HAS_CHD,
    MAX(CASE WHEN pc.CONDITION_CODE = 'CKD' THEN TRUE ELSE FALSE END) AS HAS_CKD,
    MAX(CASE WHEN pc.CONDITION_CODE = 'COPD' THEN TRUE ELSE FALSE END) AS HAS_COPD,
    MAX(CASE WHEN pc.CONDITION_CODE = 'CYP_AST' THEN TRUE ELSE FALSE END) AS HAS_CYP_AST,
    MAX(CASE WHEN pc.CONDITION_CODE = 'DEM' THEN TRUE ELSE FALSE END) AS HAS_DEM,
    MAX(CASE WHEN pc.CONDITION_CODE = 'DEP' THEN TRUE ELSE FALSE END) AS HAS_DEP,
    MAX(CASE WHEN pc.CONDITION_CODE = 'DM' THEN TRUE ELSE FALSE END) AS HAS_DM,
    MAX(CASE WHEN pc.CONDITION_CODE = 'EPIL' THEN TRUE ELSE FALSE END) AS HAS_EPIL,
    MAX(CASE WHEN pc.CONDITION_CODE = 'FHYP' THEN TRUE ELSE FALSE END) AS HAS_FHYP,
    MAX(CASE WHEN pc.CONDITION_CODE = 'GDM' THEN TRUE ELSE FALSE END) AS HAS_GDM,
    MAX(CASE WHEN pc.CONDITION_CODE = 'HF' THEN TRUE ELSE FALSE END) AS HAS_HF,
    MAX(CASE WHEN pc.CONDITION_CODE = 'HTN' THEN TRUE ELSE FALSE END) AS HAS_HTN,
    MAX(CASE WHEN pc.CONDITION_CODE = 'LD' THEN TRUE ELSE FALSE END) AS HAS_LD,
    MAX(CASE WHEN pc.CONDITION_CODE = 'NAF' THEN TRUE ELSE FALSE END) AS HAS_NAF,
    MAX(CASE WHEN pc.CONDITION_CODE = 'NDH' THEN TRUE ELSE FALSE END) AS HAS_NDH,
    MAX(CASE WHEN pc.CONDITION_CODE = 'OB' THEN TRUE ELSE FALSE END) AS HAS_OB,
    MAX(CASE WHEN pc.CONDITION_CODE = 'OST' THEN TRUE ELSE FALSE END) AS HAS_OST,
    MAX(CASE WHEN pc.CONDITION_CODE = 'PAD' THEN TRUE ELSE FALSE END) AS HAS_PAD,
    MAX(CASE WHEN pc.CONDITION_CODE = 'PC' THEN TRUE ELSE FALSE END) AS HAS_PC,
    MAX(CASE WHEN pc.CONDITION_CODE = 'RA' THEN TRUE ELSE FALSE END) AS HAS_RA,
    MAX(CASE WHEN pc.CONDITION_CODE = 'SMI' THEN TRUE ELSE FALSE END) AS HAS_SMI,
    MAX(CASE WHEN pc.CONDITION_CODE = 'STIA' THEN TRUE ELSE FALSE END) AS HAS_STIA
FROM AllPersons ap
LEFT JOIN PersonConditions pc
    ON ap.PERSON_ID = pc.PERSON_ID
GROUP BY 
    ap.PERSON_ID,
    ap.SK_PATIENT_ID 