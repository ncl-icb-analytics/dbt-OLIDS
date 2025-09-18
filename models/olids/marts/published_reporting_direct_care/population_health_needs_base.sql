{{
    config(
        materialized='view')
}}

/*
Complete population health foundation combining demographics, behavioural risk factors, and conditions.
Provides comprehensive analytical dataset for population health dashboards and reporting.

Business Logic:
- All three source tables designed with identical grain (one row per person from dim_person)
- LEFT JOINs from demographics ensure complete population coverage (defensive approach)
- Demographics: Complete population with full demographic profiles (base table)
- Behavioural risk factors: All persons with explicit data availability flags (NULL where no assessments)
- Conditions: All persons with explicit TRUE/FALSE flags (FALSE where no conditions)

TEMPORARY WORKAROUND: 
- Patient LSOA fields populated with practice LSOA data as we don't have patient postcode to LSOA lookup yet
- Deprivation data derived from practice LSOA rather than patient LSOA
- This will be replaced when patient postcode to IMD lookup table becomes available
*/

SELECT
    -- Core identifiers from demographics
    d.person_id,
    d.sk_patient_id,
    
    -- Demographics: Status and identifiers
    d.is_active,
    d.is_deceased,
    d.sex,
    d.birth_date_approx,
    d.birth_date_approx_end_of_month,
    d.age,
    d.age_at_least,
    d.death_date_approx,
    
    -- Demographics: Age bands
    d.age_band_5y,
    d.age_band_10y,
    d.age_band_nhs,
    d.age_band_ons,
    d.age_life_stage,
    
    -- Demographics: Ethnicity
    d.ethnicity_category,
    d.ethnicity_subcategory,
    d.ethnicity_granular,
    
    -- Demographics: Language
    d.main_language,
    d.interpreter_needed,
    
    -- Demographics: Practice and geography
    d.practice_code,
    d.practice_name,
    d.pcn_code,
    d.pcn_name,
    d.pcn_name_with_borough,
    d.neighbourhood_registered,
    d.borough_registered,
    d.practice_postcode,
    
    -- TEMPORARY: Using practice LSOA for patient LSOA until patient postcode lookup available
    d.practice_lsoa AS patient_lsoa,
    
    -- Original practice LSOA field
    d.practice_lsoa,
    
    d.ward_code,
    d.ward_name,
    
    -- TEMPORARY: Using practice-based deprivation until patient postcode lookup available
    imd.imddecile AS patient_imd_decile_19,
    CASE 
        WHEN imd.imddecile IN (1, 2) THEN 'Most Deprived'
        WHEN imd.imddecile IN (3, 4) THEN 'Second Most Deprived'
        WHEN imd.imddecile IN (5, 6) THEN 'Third Most Deprived'
        WHEN imd.imddecile IN (7, 8) THEN 'Second Least Deprived'
        WHEN imd.imddecile IN (9, 10) THEN 'Least Deprived'
        ELSE NULL
    END AS patient_imd_quintile_19,
    
    -- Original practice deprivation fields preserved  
    imd.imddecile AS practice_imd_decile_19,
    CASE 
        WHEN imd.imddecile IN (1, 2) THEN 'Most Deprived'
        WHEN imd.imddecile IN (3, 4) THEN 'Second Most Deprived'
        WHEN imd.imddecile IN (5, 6) THEN 'Third Most Deprived'
        WHEN imd.imddecile IN (7, 8) THEN 'Second Least Deprived'
        WHEN imd.imddecile IN (9, 10) THEN 'Least Deprived'
        ELSE NULL
    END AS practice_imd_quintile_19,
    
    d.neighbourhood_resident,
    d.postcode_hash,
    d.uprn_hash,
    d.registration_start_date,
    
    -- Behavioural Risk Factors: BMI
    b.bmi_category,
    b.bmi_value,
    b.bmi_risk_sort_key,
    
    -- Behavioural Risk Factors: Smoking
    b.smoking_status,
    b.smoking_risk_sort_key,
    
    -- Behavioural Risk Factors: Alcohol
    b.alcohol_status,
    b.alcohol_risk_sort_key,
    
    -- Behavioural Risk Factors: Combined metrics
    b.risk_factor_count,
    b.has_bmi_data,
    b.has_smoking_data,
    b.has_alcohol_data,
    
    -- Conditions: Cardiovascular
    c.has_atrial_fibrillation,
    c.has_coronary_heart_disease,
    c.has_heart_failure,
    c.has_hypertension,
    c.has_peripheral_arterial_disease,
    c.has_stroke_tia,
    
    -- Conditions: Respiratory
    c.has_asthma,
    c.has_copd,
    c.has_cyp_asthma,
    
    -- Conditions: Metabolic
    c.has_diabetes,
    c.has_gestational_diabetes,
    c.has_non_diabetic_hyperglycaemia,
    c.has_obesity,
    c.has_nafld,
    
    -- Conditions: Mental Health
    c.has_dementia,
    c.has_depression,
    c.has_severe_mental_illness,
    
    -- Conditions: Other
    c.has_cancer,
    c.has_chronic_kidney_disease,
    c.has_epilepsy,
    c.has_familial_hypercholesterolaemia,
    c.has_frailty,
    c.has_learning_disability,
    c.has_learning_disability_all_ages,
    c.has_osteoporosis,
    c.has_palliative_care,
    c.has_rheumatoid_arthritis,
    
    -- Conditions: Summary counts
    c.total_conditions,
    c.total_qof_conditions,
    c.total_non_qof_conditions,
    
    -- Conditions: Clinical domain counts
    c.cardiovascular_conditions,
    c.respiratory_conditions,
    c.mental_health_conditions,
    c.metabolic_conditions,
    c.musculoskeletal_conditions,
    c.neurology_conditions,
    c.geriatric_conditions,
    
    -- Conditions: Temporal
    c.earliest_condition_diagnosis,
    c.latest_condition_diagnosis

FROM {{ ref('dim_person_demographics') }} d
LEFT JOIN {{ ref('fct_person_behavioural_risk_factors') }} b
    ON d.person_id = b.person_id
LEFT JOIN {{ ref('dim_person_conditions') }} c
    ON d.person_id = c.person_id
-- TEMPORARY: Join to IMD reference using practice LSOA until patient postcode lookup available
LEFT JOIN {{ ref('stg_reference_imd2019') }} imd
    ON d.practice_lsoa = imd.lsoacode