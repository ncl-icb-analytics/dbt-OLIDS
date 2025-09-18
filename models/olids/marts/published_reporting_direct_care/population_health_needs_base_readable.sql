{{
    config(
        materialized='view')
}}

/*
Turn base table into readable names for fields that will be directly in Power BI.
*/

SELECT
 person_id,
 sk_patient_id,
 CASE 
    WHEN is_active = TRUE 
    THEN 'Active'
    ELSE 'Inactive'
    END AS "Active Flag",
  CASE 
    WHEN is_deceased = TRUE 
    THEN 'Active' 
    ELSE 'Inactive' 
    END AS "Deceased Flag",
  sex AS "Gender",
  age AS "Age",
  age_at_least AS "Age (at least)",
  death_date_approx AS "Death Date (est.)",
  age_band_5y AS "Age Band (5 Years)",
  age_band_10y AS "Age Band (10 Years)",
  age_band_nhs AS "Age Band (NHS)", 
  --age_band_ons AS "Age Band (ONS)", -- duplicative of 5 years
  age_life_stage AS "Age (Stage of Life)",
  ethnicity_category AS "Ethnicity Category",
  ethnicity_subcategory AS "Ethnicity Subcategory",
  ethnicity_granular AS "Ethnicity",
  main_language AS "Main Language",
  CASE 
    WHEN interpreter_needed = TRUE 
    THEN 'Interpreter Needed'
    ELSE 'No Interpreter Needed'
    END AS "Interpreter Needed Flag",
  practice_code AS "Practice Code",
  practice_name AS "GP Practice",
  pcn_name AS "PCN",
  pcn_name_with_borough AS "PCN (Borough)",
  neighbourhood_registered AS "Neighbourhood (Registered)",
  borough_registered AS "Borough (Registered)",
  patient_lsoa AS "LSOA Code",
  ward_code,
  ward_name AS "Ward",
  patient_imd_decile_19 AS "IMD Decile",
  patient_imd_quintile_19 AS "IMD Quintile",
  neighbourhood_resident AS "Neighbourhood (Resident)",
  postcode_hash,
  uprn_hash,
  registration_start_date,
  bmi_category AS "BMI Category",
  bmi_value AS "BMI",
  bmi_risk_sort_key,
  smoking_status AS "Smoking Status",
  smoking_risk_sort_key,
  alcohol_status AS "Alcohol Status",
  alcohol_risk_sort_key,
  risk_factor_count,
  has_bmi_data,
  has_alcohol_data,
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
  total_conditions,
  total_qof_conditions,
  total_non_qof_conditions,
  cardiovascular_conditions,
  respiratory_conditions,
  mental_health_conditions,
  metabolic_conditions,
  musculoskeletal_conditions,
  neurology_conditions,
  geriatric_conditions,
  earliest_condition_diagnosis,
  latest_condition_diagnosis
FROM {{ ref('population_health_needs_base') }}