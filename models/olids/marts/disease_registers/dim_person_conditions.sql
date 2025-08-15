{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Person Conditions Dimension
-- Wide format with explicit boolean flags for ALL persons from dim_person_demographics
-- Includes persons with no conditions (all flags = FALSE) for complete population view
-- One row per person matching dim_person_demographics grain for 1:1:1 joins

WITH all_persons AS (
    SELECT person_id 
    FROM {{ ref('dim_person_demographics') }}
),

person_conditions AS (
    SELECT
        p.person_id,
        
        -- Boolean condition flags (pivoted from condition_code) - explicit TRUE/FALSE for all
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'AF' THEN TRUE END), FALSE) AS has_atrial_fibrillation,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'AST' THEN TRUE END), FALSE) AS has_asthma,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CAN' THEN TRUE END), FALSE) AS has_cancer,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CHD' THEN TRUE END), FALSE) AS has_coronary_heart_disease,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CKD' THEN TRUE END), FALSE) AS has_chronic_kidney_disease,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'COPD' THEN TRUE END), FALSE) AS has_copd,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'CYP_AST' THEN TRUE END), FALSE) AS has_cyp_asthma,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'DEM' THEN TRUE END), FALSE) AS has_dementia,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'DEP' THEN TRUE END), FALSE) AS has_depression,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'DM' THEN TRUE END), FALSE) AS has_diabetes,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'EP' THEN TRUE END), FALSE) AS has_epilepsy,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'FH' THEN TRUE END), FALSE) AS has_familial_hypercholesterolaemia,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'GESTDIAB' THEN TRUE END), FALSE) AS has_gestational_diabetes,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'FRAIL' THEN TRUE END), FALSE) AS has_frailty,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'HF' THEN TRUE END), FALSE) AS has_heart_failure,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'HTN' THEN TRUE END), FALSE) AS has_hypertension,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'LD' THEN TRUE END), FALSE) AS has_learning_disability,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'LD_ALL' THEN TRUE END), FALSE) AS has_learning_disability_all_ages,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'NAFLD' THEN TRUE END), FALSE) AS has_nafld,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'NDH' THEN TRUE END), FALSE) AS has_non_diabetic_hyperglycaemia,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'OB' THEN TRUE END), FALSE) AS has_obesity,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'OST' THEN TRUE END), FALSE) AS has_osteoporosis,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'PAD' THEN TRUE END), FALSE) AS has_peripheral_arterial_disease,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'PC' THEN TRUE END), FALSE) AS has_palliative_care,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'RA' THEN TRUE END), FALSE) AS has_rheumatoid_arthritis,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'SMI' THEN TRUE END), FALSE) AS has_severe_mental_illness,
        COALESCE(MAX(CASE WHEN ltc.condition_code = 'STIA' THEN TRUE END), FALSE) AS has_stroke_tia,
        
        -- Summary counts (0 for persons with no conditions)
        COALESCE(COUNT(DISTINCT ltc.condition_code), 0) AS total_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.is_qof = TRUE THEN ltc.condition_code END), 0) AS total_qof_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.is_qof = FALSE THEN ltc.condition_code END), 0) AS total_non_qof_conditions,
        
        -- Clinical domain counts (0 for persons with no conditions)
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Cardiovascular' THEN ltc.condition_code END), 0) AS cardiovascular_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Respiratory' THEN ltc.condition_code END), 0) AS respiratory_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Mental Health' THEN ltc.condition_code END), 0) AS mental_health_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Metabolic' THEN ltc.condition_code END), 0) AS metabolic_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Musculoskeletal' THEN ltc.condition_code END), 0) AS musculoskeletal_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Neurology' THEN ltc.condition_code END), 0) AS neurology_conditions,
        COALESCE(COUNT(DISTINCT CASE WHEN ltc.clinical_domain = 'Geriatric' THEN ltc.condition_code END), 0) AS geriatric_conditions,
        
        -- Earliest and latest diagnosis dates across all conditions (NULL for no conditions)
        MIN(ltc.earliest_diagnosis_date) AS earliest_condition_diagnosis,
        MAX(ltc.latest_diagnosis_date) AS latest_condition_diagnosis

    FROM all_persons p
    LEFT JOIN {{ ref('fct_person_ltc_summary') }} ltc
        ON p.person_id = ltc.person_id
    GROUP BY p.person_id
)

SELECT * FROM person_conditions