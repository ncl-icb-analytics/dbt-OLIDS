CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_EXCLUSIONS (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    HAS_EXCLUDING_CONDITION BOOLEAN, -- Flag indicating if person has any of the excluding conditions
    -- Individual condition flags
    HAS_CKD BOOLEAN, -- Chronic Kidney Disease
    HAS_AF BOOLEAN, -- Atrial Fibrillation
    HAS_COPD BOOLEAN, -- Chronic Obstructive Pulmonary Disease
    HAS_HYPERTENSION BOOLEAN, -- Hypertension
    HAS_CHD BOOLEAN, -- Coronary Heart Disease
    HAS_STIA BOOLEAN, -- Stroke/TIA
    HAS_PAD BOOLEAN, -- Peripheral Arterial Disease
    HAS_HF BOOLEAN, -- Heart Failure
    HAS_TYPE2_DIABETES BOOLEAN, -- Type 2 Diabetes
    HAS_HYPERLIPIDAEMIA BOOLEAN, -- Hyperlipidaemia (FHYP)
    HAS_NAFLD BOOLEAN, -- Non-Alcoholic Fatty Liver Disease
    HAS_ASTHMA BOOLEAN, -- Asthma (adult)
    HAS_CYP_ASTHMA BOOLEAN, -- Children and Young People Asthma
    -- Earliest diagnosis date across all conditions
    EARLIEST_EXCLUDING_CONDITION_DATE DATE -- Earliest date of any excluding condition
)
COMMENT = 'Dimension table identifying patients with any of the specified conditions that exclude them from the LTC LCS Case Finding programme. Includes CKD, AF, COPD, Hypertension, CHD, Stroke/TIA, PAD, Heart Failure, Type 2 Diabetes, Hyperlipidaemia, NAFLD, and Asthma (both adult and CYP).'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH LTCSummaryConditions AS (
    -- Get all relevant conditions from LTC summary
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CONDITION_CODE,
        EARLIEST_DIAGNOSIS_DATE,
        LATEST_DIAGNOSIS_DATE,
        IS_ON_REGISTER
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_LTC_SUMMARY
    WHERE CONDITION_CODE IN ('CKD', 'AF', 'COPD', 'HTN', 'CHD', 'STIA', 'PAD', 'HF', 'FHYP', 'NAF', 'AST', 'CYP_AST')
    AND IS_ON_REGISTER = TRUE
),
Type2Diabetes AS (
    -- Get Type 2 Diabetes specifically from diabetes fact table
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        EARLIEST_DMTYPE2_DIAGNOSIS_DATE AS EARLIEST_DIAGNOSIS_DATE,
        LATEST_DMTYPE2_DIAGNOSIS_DATE AS LATEST_DIAGNOSIS_DATE
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_DIABETES
    WHERE DIABETES_TYPE = 'Type 2'
    AND IS_ON_DM_REGISTER = TRUE
),
AllConditions AS (
    -- Combine LTC summary conditions with Type 2 Diabetes
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CONDITION_CODE,
        EARLIEST_DIAGNOSIS_DATE,
        LATEST_DIAGNOSIS_DATE
    FROM LTCSummaryConditions

    UNION ALL

    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        'DM2' AS CONDITION_CODE, -- Using DM2 to represent Type 2 Diabetes
        EARLIEST_DIAGNOSIS_DATE,
        LATEST_DIAGNOSIS_DATE
    FROM Type2Diabetes
),
PersonLevelAggregation AS (
    -- Aggregate to one row per person with flags for each condition
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        MIN(EARLIEST_DIAGNOSIS_DATE) as EARLIEST_EXCLUDING_CONDITION_DATE,
        BOOLOR_AGG(CONDITION_CODE = 'CKD') as HAS_CKD,
        BOOLOR_AGG(CONDITION_CODE = 'AF') as HAS_AF,
        BOOLOR_AGG(CONDITION_CODE = 'COPD') as HAS_COPD,
        BOOLOR_AGG(CONDITION_CODE = 'HTN') as HAS_HYPERTENSION,
        BOOLOR_AGG(CONDITION_CODE = 'CHD') as HAS_CHD,
        BOOLOR_AGG(CONDITION_CODE = 'STIA') as HAS_STIA,
        BOOLOR_AGG(CONDITION_CODE = 'PAD') as HAS_PAD,
        BOOLOR_AGG(CONDITION_CODE = 'HF') as HAS_HF,
        BOOLOR_AGG(CONDITION_CODE = 'DM2') as HAS_TYPE2_DIABETES,
        BOOLOR_AGG(CONDITION_CODE = 'FHYP') as HAS_HYPERLIPIDAEMIA,
        BOOLOR_AGG(CONDITION_CODE = 'NAF') as HAS_NAFLD,
        BOOLOR_AGG(CONDITION_CODE = 'AST') as HAS_ASTHMA,
        BOOLOR_AGG(CONDITION_CODE = 'CYP_AST') as HAS_CYP_ASTHMA
    FROM AllConditions
    GROUP BY PERSON_ID
)
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    (HAS_CKD OR HAS_AF OR HAS_COPD OR HAS_HYPERTENSION OR HAS_CHD OR
     HAS_STIA OR HAS_PAD OR HAS_HF OR HAS_TYPE2_DIABETES OR
     HAS_HYPERLIPIDAEMIA OR HAS_NAFLD OR HAS_ASTHMA OR HAS_CYP_ASTHMA) as HAS_EXCLUDING_CONDITION,
    HAS_CKD,
    HAS_AF,
    HAS_COPD,
    HAS_HYPERTENSION,
    HAS_CHD,
    HAS_STIA,
    HAS_PAD,
    HAS_HF,
    HAS_TYPE2_DIABETES,
    HAS_HYPERLIPIDAEMIA,
    HAS_NAFLD,
    HAS_ASTHMA,
    HAS_CYP_ASTHMA,
    EARLIEST_EXCLUDING_CONDITION_DATE
FROM PersonLevelAggregation;
