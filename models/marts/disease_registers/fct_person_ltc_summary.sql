{{
    config(
        materialized='table',
        cluster_by=['person_id', 'condition_code']
    )
}}

-- LTC Summary Fact Table
-- Comprehensive summary of all long-term condition registers
-- Union of all QOF disease registers for analytical and reporting purposes

WITH condition_union AS (
    -- Atrial Fibrillation
    SELECT 
        person_id,
        'AF' AS condition_code,
        'Atrial Fibrillation' AS condition_name,
        is_on_af_register AS is_on_register,
        earliest_af_diagnosis_date AS earliest_diagnosis_date,
        latest_af_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_atrial_fibrillation_register') }}
    WHERE is_on_af_register = TRUE

    UNION ALL

    -- Asthma
    SELECT 
        person_id,
        'AST' AS condition_code,
        'Asthma' AS condition_name,
        is_on_asthma_register AS is_on_register,
        earliest_asthma_diagnosis_date AS earliest_diagnosis_date,
        latest_asthma_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_asthma_register') }}
    WHERE is_on_asthma_register = TRUE

    UNION ALL

    -- Children & Young People Asthma
    SELECT 
        person_id,
        'CYP_AST' AS condition_code,
        'Children and Young People Asthma' AS condition_name,
        is_on_asthma_register AS is_on_register,
        earliest_asthma_diagnosis_date AS earliest_diagnosis_date,
        latest_asthma_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_cyp_asthma_register') }}
    WHERE is_on_asthma_register = TRUE

    UNION ALL

    -- Cancer
    SELECT 
        person_id,
        'CA' AS condition_code,
        'Cancer' AS condition_name,
        is_on_cancer_register AS is_on_register,
        earliest_cancer_diagnosis_date AS earliest_diagnosis_date,
        latest_cancer_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_cancer_register') }}
    WHERE is_on_cancer_register = TRUE

    UNION ALL

    -- Coronary Heart Disease
    SELECT 
        person_id,
        'CHD' AS condition_code,
        'Coronary Heart Disease' AS condition_name,
        is_on_chd_register AS is_on_register,
        earliest_chd_date AS earliest_diagnosis_date,
        latest_chd_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_chd_register = TRUE

    UNION ALL

    -- Chronic Kidney Disease
    SELECT 
        person_id,
        'CKD' AS condition_code,
        'Chronic Kidney Disease' AS condition_name,
        is_on_ckd_register AS is_on_register,
        earliest_ckd_diagnosis_date AS earliest_diagnosis_date,
        latest_ckd_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_ckd_register') }}
    WHERE is_on_ckd_register = TRUE

    UNION ALL

    -- COPD
    SELECT 
        person_id,
        'COPD' AS condition_code,
        'Chronic Obstructive Pulmonary Disease' AS condition_name,
        is_on_copd_register AS is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_copd_register') }}
    WHERE is_on_copd_register = TRUE

    UNION ALL

    -- Dementia
    SELECT 
        person_id,
        'DEM' AS condition_code,
        'Dementia' AS condition_name,
        is_on_dementia_register AS is_on_register,
        earliest_dementia_diagnosis_date AS earliest_diagnosis_date,
        latest_dementia_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_dementia_register') }}
    WHERE is_on_dementia_register = TRUE

    UNION ALL

    -- Depression
    SELECT 
        person_id,
        'DEP' AS condition_code,
        'Depression' AS condition_name,
        is_on_depression_register AS is_on_register,
        earliest_depression_diagnosis_date AS earliest_diagnosis_date,
        latest_depression_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_depression_register') }}
    WHERE is_on_depression_register = TRUE

    UNION ALL

    -- Diabetes
    SELECT 
        person_id,
        'DM' AS condition_code,
        'Diabetes' AS condition_name,
        is_on_diabetes_register AS is_on_register,
        earliest_diabetes_date AS earliest_diagnosis_date,
        latest_diabetes_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE is_on_diabetes_register = TRUE

    UNION ALL

    -- Epilepsy
    SELECT 
        person_id,
        'EPIL' AS condition_code,
        'Epilepsy' AS condition_name,
        is_on_epilepsy_register AS is_on_register,
        earliest_epilepsy_diagnosis_date AS earliest_diagnosis_date,
        latest_epilepsy_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_epilepsy_register') }}
    WHERE is_on_epilepsy_register = TRUE

    UNION ALL

    -- Familial Hypercholesterolaemia
    SELECT 
        person_id,
        'FHYP' AS condition_code,
        'Familial Hypercholesterolaemia' AS condition_name,
        is_on_fh_register AS is_on_register,
        earliest_fh_date AS earliest_diagnosis_date,
        latest_fh_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_familial_hypercholesterolaemia_register') }}
    WHERE is_on_fh_register = TRUE

    UNION ALL

    -- Gestational Diabetes
    SELECT 
        person_id,
        'GESTDIAB' AS condition_code,
        'Gestational Diabetes' AS condition_name,
        is_on_gestational_diabetes_register AS is_on_register,
        earliest_gestational_diabetes_date AS earliest_diagnosis_date,
        latest_gestational_diabetes_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_gestational_diabetes_register') }}
    WHERE is_on_gestational_diabetes_register = TRUE

    UNION ALL

    -- Heart Failure
    SELECT 
        person_id,
        'HF' AS condition_code,
        'Heart Failure' AS condition_name,
        is_on_hf_register AS is_on_register,
        earliest_hf_diagnosis_date AS earliest_diagnosis_date,
        latest_hf_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_heart_failure_register') }}
    WHERE is_on_hf_register = TRUE

    UNION ALL

    -- Hypertension
    SELECT 
        person_id,
        'HTN' AS condition_code,
        'Hypertension' AS condition_name,
        is_on_htn_register AS is_on_register,
        earliest_htn_diagnosis_date AS earliest_diagnosis_date,
        latest_htn_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_hypertension_register') }}
    WHERE is_on_htn_register = TRUE

    UNION ALL

    -- Learning Disability
    SELECT 
        person_id,
        'LD' AS condition_code,
        'Learning Disability' AS condition_name,
        is_on_ld_register AS is_on_register,
        earliest_ld_diagnosis_date AS earliest_diagnosis_date,
        latest_ld_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_learning_disability_register') }}
    WHERE is_on_ld_register = TRUE

    UNION ALL

    -- Non-Alcoholic Fatty Liver Disease
    SELECT 
        person_id,
        'NAF' AS condition_code,
        'Non-Alcoholic Fatty Liver Disease' AS condition_name,
        is_on_nafld_register AS is_on_register,
        earliest_nafld_date AS earliest_diagnosis_date,
        latest_nafld_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_nafld_register') }}
    WHERE is_on_nafld_register = TRUE

    UNION ALL

    -- Non-Diabetic Hyperglycaemia
    SELECT 
        person_id,
        'NDH' AS condition_code,
        'Non-Diabetic Hyperglycaemia' AS condition_name,
        is_on_ndh_register AS is_on_register,
        earliest_any_ndh_date AS earliest_diagnosis_date,
        latest_any_ndh_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_ndh_register') }}
    WHERE is_on_ndh_register = TRUE

    UNION ALL

    -- Obesity
    SELECT 
        person_id,
        'OB' AS condition_code,
        'Obesity' AS condition_name,
        is_on_obesity_register AS is_on_register,
        latest_valid_bmi_date AS earliest_diagnosis_date,
        latest_bmi_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_obesity_register') }}
    WHERE is_on_obesity_register = TRUE

    UNION ALL

    -- Osteoporosis
    SELECT 
        person_id,
        'OP' AS condition_code,
        'Osteoporosis' AS condition_name,
        is_on_osteoporosis_register AS is_on_register,
        earliest_osteoporosis_date AS earliest_diagnosis_date,
        latest_osteoporosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_osteoporosis_register') }}
    WHERE is_on_osteoporosis_register = TRUE

    UNION ALL

    -- Peripheral Arterial Disease
    SELECT 
        person_id,
        'PAD' AS condition_code,
        'Peripheral Arterial Disease' AS condition_name,
        is_on_pad_register AS is_on_register,
        earliest_pad_date AS earliest_diagnosis_date,
        latest_pad_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_pad_register') }}
    WHERE is_on_pad_register = TRUE

    UNION ALL

    -- Palliative Care
    SELECT 
        person_id,
        'PC' AS condition_code,
        'Palliative Care' AS condition_name,
        is_on_palliative_care_register AS is_on_register,
        earliest_palliative_care_date AS earliest_diagnosis_date,
        latest_palliative_care_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_palliative_care_register') }}
    WHERE is_on_palliative_care_register = TRUE

    UNION ALL

    -- Rheumatoid Arthritis
    SELECT 
        person_id,
        'RA' AS condition_code,
        'Rheumatoid Arthritis' AS condition_name,
        is_on_ra_register AS is_on_register,
        earliest_ra_date AS earliest_diagnosis_date,
        latest_ra_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_rheumatoid_arthritis_register') }}
    WHERE is_on_ra_register = TRUE

    UNION ALL

    -- Serious Mental Illness
    SELECT 
        person_id,
        'SMI' AS condition_code,
        'Serious Mental Illness' AS condition_name,
        is_on_smi_register AS is_on_register,
        earliest_smi_diagnosis_date AS earliest_diagnosis_date,
        latest_smi_diagnosis_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_smi_register') }}
    WHERE is_on_smi_register = TRUE

    UNION ALL

    -- Stroke/TIA
    SELECT 
        person_id,
        'STIA' AS condition_code,
        'Stroke or Transient Ischaemic Attack' AS condition_name,
        is_on_stroke_tia_register AS is_on_register,
        earliest_stroke_tia_date AS earliest_diagnosis_date,
        latest_stroke_tia_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_stroke_tia_register = TRUE
)

SELECT 
    person_id,
    condition_code,
    condition_name,
    is_on_register,
    earliest_diagnosis_date,
    latest_diagnosis_date
FROM condition_union
ORDER BY person_id, condition_code 