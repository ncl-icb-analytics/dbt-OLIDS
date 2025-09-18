{{ config(
    materialized='table',
    tags=['fact', 'ltc', 'lcs', 'case_finding', 'person', 'powerbi', 'dashboard'],
    cluster_by=['person_id', 'practice_code']) }}

-- Enhanced person-level case finding data with demographic and practice context
-- Optimised for PowerBI person-level analytics and drill-through capabilities

SELECT
    -- Core identifiers
    cf.person_id,
    demo.sk_patient_id,
    
    -- Practice context
    prac.practice_code,
    prac.practice_name,
    prac.practice_postcode,
    demo.pcn_code,
    demo.pcn_name,
    demo.borough_registered,
    demo.neighbourhood_registered,
    
    -- Demographics
    demo.age,
    cf.age AS cf_eligibility_age,  -- Age used for case finding eligibility
    demo.age_band_5y,
    demo.age_band_10y,
    demo.age_band_nhs,
    demo.age_life_stage,
    demo.ethnicity_category,
    demo.ethnicity_subcategory,
    demo.ethnicity_granular,
    demo.main_language,
    demo.language_type,
    demo.interpreter_type,
    
    -- Geographic and deprivation context
    demo.postcode_hash,
    demo.uprn_hash,
    demo.lsoa_code_21,
    demo.lsoa_name_21,
    demo.imd_decile_19,
    demo.imd_quintile_19,
    CASE 
        WHEN demo.imd_quintile_19 IS NULL THEN 'Unknown'
        WHEN demo.imd_quintile_19 = 1 THEN 'Quintile 1 (Most Deprived)'
        WHEN demo.imd_quintile_19 = 2 THEN 'Quintile 2'
        WHEN demo.imd_quintile_19 = 3 THEN 'Quintile 3'
        WHEN demo.imd_quintile_19 = 4 THEN 'Quintile 4'
        WHEN demo.imd_quintile_19 = 5 THEN 'Quintile 5 (Least Deprived)'
    END AS imd_quintile_label,
    
    -- Case finding summary flags
    cf.in_any_case_finding,
    cf.case_finding_count,
    
    -- AF indicators
    cf.in_af_61,
    cf.in_af_62,
    
    -- CKD indicators
    cf.in_ckd_61,
    cf.in_ckd_62,
    cf.in_ckd_63,
    cf.in_ckd_64,
    
    -- CVD indicators
    cf.in_cvd_61,
    cf.in_cvd_62,
    cf.in_cvd_63,
    cf.in_cvd_64,
    cf.in_cvd_65,
    cf.in_cvd_66,
    
    -- Diabetes indicators
    cf.in_dm_61,
    cf.in_dm_62,
    cf.in_dm_63,
    cf.in_dm_64,
    cf.in_dm_65,
    cf.in_dm_66,
    
    -- Hypertension indicators
    cf.in_htn_61,
    cf.in_htn_62,
    cf.in_htn_63,
    cf.in_htn_65,
    cf.in_htn_66,
    
    -- CYP Asthma indicator
    cf.in_cyp_ast_61,
    
    -- Condition groupings for easier filtering
    CASE WHEN (cf.in_af_61 OR cf.in_af_62) THEN TRUE ELSE FALSE END AS has_af_indicators,
    CASE WHEN (cf.in_ckd_61 OR cf.in_ckd_62 OR cf.in_ckd_63 OR cf.in_ckd_64) THEN TRUE ELSE FALSE END AS has_ckd_indicators,
    CASE WHEN (cf.in_cvd_61 OR cf.in_cvd_62 OR cf.in_cvd_63 OR cf.in_cvd_64 OR cf.in_cvd_65 OR cf.in_cvd_66) THEN TRUE ELSE FALSE END AS has_cvd_indicators,
    CASE WHEN (cf.in_dm_61 OR cf.in_dm_62 OR cf.in_dm_63 OR cf.in_dm_64 OR cf.in_dm_65 OR cf.in_dm_66) THEN TRUE ELSE FALSE END AS has_dm_indicators,
    CASE WHEN (cf.in_htn_61 OR cf.in_htn_62 OR cf.in_htn_63 OR cf.in_htn_65 OR cf.in_htn_66) THEN TRUE ELSE FALSE END AS has_htn_indicators,
    
    -- Risk categorisation
    CASE 
        WHEN cf.case_finding_count >= 5 THEN 'High Risk (5+ indicators)'
        WHEN cf.case_finding_count >= 3 THEN 'Medium Risk (3-4 indicators)'
        WHEN cf.case_finding_count >= 1 THEN 'Low Risk (1-2 indicators)'
        ELSE 'No Case Finding'
    END AS risk_category,
    
    -- Priority flags for dashboard filtering
    CASE WHEN (cf.in_cvd_61 OR cf.in_cvd_62) THEN TRUE ELSE FALSE END AS high_priority_cvd,
    CASE WHEN (cf.in_dm_61 OR cf.in_dm_63) THEN TRUE ELSE FALSE END AS high_priority_diabetes,
    CASE WHEN cf.in_htn_61 THEN TRUE ELSE FALSE END AS high_priority_hypertension,
    
    -- Age-specific flags
    CASE WHEN demo.age >= 75 THEN TRUE ELSE FALSE END AS is_elderly,
    CASE WHEN demo.age BETWEEN 40 AND 64 THEN TRUE ELSE FALSE END AS is_working_age,
    CASE WHEN demo.age < 18 THEN TRUE ELSE FALSE END AS is_child_young_person,
    
    -- Registration context
    prac.registration_start_date,
    
    -- Metadata
    CURRENT_DATE() AS data_refresh_date

FROM {{ ref('dim_ltc_lcs_cf_summary') }} AS cf
INNER JOIN {{ ref('dim_person_demographics') }} AS demo
    ON cf.person_id = demo.person_id
INNER JOIN {{ ref('dim_person_current_practice') }} AS prac
    ON cf.person_id = prac.person_id
WHERE 
    cf.in_any_case_finding = TRUE  -- Only include people with case finding eligibility
    AND prac.registration_end_date IS NULL  -- Current registrations only

ORDER BY 
    cf.case_finding_count DESC,  -- Highest risk first
    cf.person_id