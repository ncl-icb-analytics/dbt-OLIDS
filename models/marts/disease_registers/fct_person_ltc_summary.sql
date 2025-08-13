{{
    config(
        materialized='table',
        cluster_by=['person_id', 'condition_code'])
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
        'Cardiovascular' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_atrial_fibrillation_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Asthma
    SELECT
        person_id,
        'AST' AS condition_code,
        'Asthma' AS condition_name,
        'Respiratory' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_asthma_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Cancer
    SELECT
        person_id,
        'CAN' AS condition_code,
        'Cancer' AS condition_name,
        'Oncology' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_cancer_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Coronary Heart Disease
    SELECT
        person_id,
        'CHD' AS condition_code,
        'Coronary Heart Disease' AS condition_name,
        'Cardiovascular' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Chronic Kidney Disease
    SELECT
        person_id,
        'CKD' AS condition_code,
        'Chronic Kidney Disease' AS condition_name,
        'Renal' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_ckd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- COPD
    SELECT
        person_id,
        'COPD' AS condition_code,
        'Chronic Obstructive Pulmonary Disease' AS condition_name,
        'Respiratory' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_copd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Children and Young People Asthma
    SELECT
        person_id,
        'CYP_AST' AS condition_code,
        'Children and Young People Asthma' AS condition_name,
        'Respiratory' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        FALSE AS is_qof
    FROM {{ ref('fct_person_cyp_asthma_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Dementia
    SELECT
        person_id,
        'DEM' AS condition_code,
        'Dementia' AS condition_name,
        'Mental Health' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_dementia_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Depression
    SELECT
        person_id,
        'DEP' AS condition_code,
        'Depression' AS condition_name,
        'Mental Health' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_depression_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Diabetes
    SELECT
        person_id,
        'DM' AS condition_code,
        'Diabetes Mellitus' AS condition_name,
        'Metabolic' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Epilepsy
    SELECT
        person_id,
        'EP' AS condition_code,
        'Epilepsy' AS condition_name,
        'Neurology' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_epilepsy_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Familial Hypercholesterolaemia
    SELECT
        person_id,
        'FH' AS condition_code,
        'Familial Hypercholesterolaemia' AS condition_name,
        'Genetics' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        FALSE AS is_qof
    FROM {{ ref('fct_person_familial_hypercholesterolaemia_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Heart Failure
    SELECT
        person_id,
        'HF' AS condition_code,
        'Heart Failure' AS condition_name,
        'Cardiovascular' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_heart_failure_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Hypertension
    SELECT
        person_id,
        'HTN' AS condition_code,
        'Hypertension' AS condition_name,
        'Cardiovascular' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_hypertension_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Learning Disability
    SELECT
        person_id,
        'LD' AS condition_code,
        'Learning Disability' AS condition_name,
        'Neurodevelopmental' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_learning_disability_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Learning Disability (All Ages)
    SELECT
        person_id,
        'LD_ALL' AS condition_code,
        'Learning Disability (All Ages)' AS condition_name,
        'Neurodevelopmental' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        FALSE AS is_qof
    FROM {{ ref('fct_person_learning_disability_register_all_ages') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- NAFLD
    SELECT
        person_id,
        'NAFLD' AS condition_code,
        'Non-Alcoholic Fatty Liver Disease' AS condition_name,
        'Hepatology' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        FALSE AS is_qof
    FROM {{ ref('fct_person_nafld_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Non-Diabetic Hyperglycaemia
    SELECT
        person_id,
        'NDH' AS condition_code,
        'Non-Diabetic Hyperglycaemia' AS condition_name,
        'Metabolic' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_ndh_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Obesity
    SELECT
        person_id,
        'OB' AS condition_code,
        'Obesity' AS condition_name,
        'Metabolic' AS clinical_domain,
        is_on_register,
        latest_valid_bmi_date AS earliest_diagnosis_date,
        latest_bmi_date AS latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_obesity_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Osteoporosis
    SELECT
        person_id,
        'OST' AS condition_code,
        'Osteoporosis' AS condition_name,
        'Musculoskeletal' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_osteoporosis_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Peripheral Arterial Disease
    SELECT
        person_id,
        'PAD' AS condition_code,
        'Peripheral Arterial Disease' AS condition_name,
        'Cardiovascular' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_pad_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Palliative Care
    SELECT
        person_id,
        'PC' AS condition_code,
        'Palliative Care' AS condition_name,
        'Palliative Care' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_palliative_care_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Rheumatoid Arthritis
    SELECT
        person_id,
        'RA' AS condition_code,
        'Rheumatoid Arthritis' AS condition_name,
        'Musculoskeletal' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_rheumatoid_arthritis_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Severe Mental Illness
    SELECT
        person_id,
        'SMI' AS condition_code,
        'Severe Mental Illness' AS condition_name,
        'Mental Health' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_smi_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Stroke and TIA
    SELECT
        person_id,
        'STIA' AS condition_code,
        'Stroke and TIA' AS condition_name,
        'Neurology' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        TRUE AS is_qof
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Gestational Diabetes
    SELECT
        person_id,
        'GESTDIAB' AS condition_code,
        'Gestational Diabetes' AS condition_name,
        'Maternity' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        FALSE AS is_qof
    FROM {{ ref('fct_person_gestational_diabetes_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    -- Frailty
    SELECT
        person_id,
        'FRAIL' AS condition_code,
        'Frailty' AS condition_name,
        'Geriatric' AS clinical_domain,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        FALSE AS is_qof
    FROM {{ ref('fct_person_frailty_register') }}
    WHERE is_on_register = TRUE
)

SELECT
    person_id,
    condition_code,
    condition_name,
    clinical_domain,
    is_on_register,
    is_qof,
    earliest_diagnosis_date,
    latest_diagnosis_date,

    -- Derived metrics for easier analysis

FROM condition_union
ORDER BY person_id, condition_code
