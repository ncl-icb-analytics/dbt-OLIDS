{{
    config(
        materialized='table')
}}

/*
Aggregate patient counts by age, ethnicity, PCN, neighbourhood, IMD quintile, LTCs for population health needs dashboard.

Next features:
resident neighbourhood
*/

SELECT
  is_active,
  is_deceased,
  sex,
  age_band_5y,
  age_band_10y,
  age_band_nhs,
  age_band_ons,
  age_life_stage,
  ethnicity_category,
  ethnicity_subcategory,
  ethnicity_granular,
  main_language,
  pcn_name,
  practice_neighbourhood,
  practice_borough,
  imd_quintile_19,
  has_atrial_fibrillation,
  has_coronary_heart_disease,
  has_heart_failure,
  has_hypertension,
  has_peripheral_arterial_disease,
  has_stroke_tia,
  has_asthma,
  has_copd,
  has_cyp_asthma,
  has_diabetes,
  has_gestational_diabetes,
  has_non_diabetic_hyperglycaemia,
  has_obesity,
  has_nafld,
  has_dementia,
  has_depression,
  has_severe_mental_illness,
  has_cancer,
  has_epilepsy,
  has_familial_hypercholesterolaemia,
  has_frailty,
  has_learning_disability,
  has_osteoporosis,
  has_palliative_care,
  has_rheumatoid_arthritis,
  COUNT(*) AS patient_count
FROM data_lab_olids_uat.dbt_dev.population_health_needs_base
GROUP BY ALL