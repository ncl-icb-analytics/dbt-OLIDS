{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Person Conditions Dimension
-- Wide format pivot of fct_person_ltc_summary providing boolean flags for each condition
-- One row per person with condition presence flags for easier analytical consumption

SELECT
    person_id,
    
    -- Boolean condition flags (pivoted from condition_code)
    MAX(CASE WHEN condition_code = 'AF' THEN TRUE ELSE FALSE END) AS has_atrial_fibrillation,
    MAX(CASE WHEN condition_code = 'AST' THEN TRUE ELSE FALSE END) AS has_asthma,
    MAX(CASE WHEN condition_code = 'CAN' THEN TRUE ELSE FALSE END) AS has_cancer,
    MAX(CASE WHEN condition_code = 'CHD' THEN TRUE ELSE FALSE END) AS has_coronary_heart_disease,
    MAX(CASE WHEN condition_code = 'CKD' THEN TRUE ELSE FALSE END) AS has_chronic_kidney_disease,
    MAX(CASE WHEN condition_code = 'COPD' THEN TRUE ELSE FALSE END) AS has_copd,
    MAX(CASE WHEN condition_code = 'CYP_AST' THEN TRUE ELSE FALSE END) AS has_cyp_asthma,
    MAX(CASE WHEN condition_code = 'DEM' THEN TRUE ELSE FALSE END) AS has_dementia,
    MAX(CASE WHEN condition_code = 'DEP' THEN TRUE ELSE FALSE END) AS has_depression,
    MAX(CASE WHEN condition_code = 'DM' THEN TRUE ELSE FALSE END) AS has_diabetes,
    MAX(CASE WHEN condition_code = 'EP' THEN TRUE ELSE FALSE END) AS has_epilepsy,
    MAX(CASE WHEN condition_code = 'FH' THEN TRUE ELSE FALSE END) AS has_familial_hypercholesterolaemia,
    MAX(CASE WHEN condition_code = 'GESTDIAB' THEN TRUE ELSE FALSE END) AS has_gestational_diabetes,
    MAX(CASE WHEN condition_code = 'HF' THEN TRUE ELSE FALSE END) AS has_heart_failure,
    MAX(CASE WHEN condition_code = 'HTN' THEN TRUE ELSE FALSE END) AS has_hypertension,
    MAX(CASE WHEN condition_code = 'LD' THEN TRUE ELSE FALSE END) AS has_learning_disability,
    MAX(CASE WHEN condition_code = 'LD_ALL' THEN TRUE ELSE FALSE END) AS has_learning_disability_all_ages,
    MAX(CASE WHEN condition_code = 'NAFLD' THEN TRUE ELSE FALSE END) AS has_nafld,
    MAX(CASE WHEN condition_code = 'NDH' THEN TRUE ELSE FALSE END) AS has_non_diabetic_hyperglycaemia,
    MAX(CASE WHEN condition_code = 'OB' THEN TRUE ELSE FALSE END) AS has_obesity,
    MAX(CASE WHEN condition_code = 'OST' THEN TRUE ELSE FALSE END) AS has_osteoporosis,
    MAX(CASE WHEN condition_code = 'PAD' THEN TRUE ELSE FALSE END) AS has_peripheral_arterial_disease,
    MAX(CASE WHEN condition_code = 'PC' THEN TRUE ELSE FALSE END) AS has_palliative_care,
    MAX(CASE WHEN condition_code = 'RA' THEN TRUE ELSE FALSE END) AS has_rheumatoid_arthritis,
    MAX(CASE WHEN condition_code = 'SMI' THEN TRUE ELSE FALSE END) AS has_severe_mental_illness,
    MAX(CASE WHEN condition_code = 'STIA' THEN TRUE ELSE FALSE END) AS has_stroke_tia,
    
    -- Summary counts
    COUNT(DISTINCT condition_code) AS total_conditions,
    COUNT(DISTINCT CASE WHEN is_qof = TRUE THEN condition_code END) AS total_qof_conditions,
    COUNT(DISTINCT CASE WHEN is_qof = FALSE THEN condition_code END) AS total_non_qof_conditions,
    
    -- Clinical domain counts
    COUNT(DISTINCT CASE WHEN clinical_domain = 'Cardiovascular' THEN condition_code END) AS cardiovascular_conditions,
    COUNT(DISTINCT CASE WHEN clinical_domain = 'Respiratory' THEN condition_code END) AS respiratory_conditions,
    COUNT(DISTINCT CASE WHEN clinical_domain = 'Mental Health' THEN condition_code END) AS mental_health_conditions,
    COUNT(DISTINCT CASE WHEN clinical_domain = 'Metabolic' THEN condition_code END) AS metabolic_conditions,
    COUNT(DISTINCT CASE WHEN clinical_domain = 'Musculoskeletal' THEN condition_code END) AS musculoskeletal_conditions,
    COUNT(DISTINCT CASE WHEN clinical_domain = 'Neurology' THEN condition_code END) AS neurology_conditions,
    
    -- Earliest and latest diagnosis dates across all conditions
    MIN(earliest_diagnosis_date) AS earliest_condition_diagnosis,
    MAX(latest_diagnosis_date) AS latest_condition_diagnosis

FROM {{ ref('fct_person_ltc_summary') }}
GROUP BY person_id