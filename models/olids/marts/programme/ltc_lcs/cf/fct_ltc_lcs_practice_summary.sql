{{ config(
    materialized='table',
    tags=['fact', 'ltc', 'lcs', 'case_finding', 'practice', 'powerbi', 'dashboard'],
    cluster_by=['practice_code']) }}

-- Practice-level aggregation of LTC LCS case finding indicators
-- Optimised for PowerBI dashboard performance and practice comparison analytics

WITH practice_demographics AS (
    -- Get practice population demographics from person demographics
    SELECT
        prac.practice_code,
        prac.practice_name,
        prac.practice_postcode,
        demo.pcn_code,
        demo.pcn_name,
        demo.borough_registered,
        demo.neighbourhood_registered,
        
        -- Count total registered population by demographics
        COUNT(*) AS total_population,
        
        -- Age band breakdowns
        COUNT(CASE WHEN demo.age_band_10y = '0-9' THEN 1 END) AS pop_age_0_9,
        COUNT(CASE WHEN demo.age_band_10y = '10-19' THEN 1 END) AS pop_age_10_19,
        COUNT(CASE WHEN demo.age_band_10y = '20-29' THEN 1 END) AS pop_age_20_29,
        COUNT(CASE WHEN demo.age_band_10y = '30-39' THEN 1 END) AS pop_age_30_39,
        COUNT(CASE WHEN demo.age_band_10y = '40-49' THEN 1 END) AS pop_age_40_49,
        COUNT(CASE WHEN demo.age_band_10y = '50-59' THEN 1 END) AS pop_age_50_59,
        COUNT(CASE WHEN demo.age_band_10y = '60-69' THEN 1 END) AS pop_age_60_69,
        COUNT(CASE WHEN demo.age_band_10y = '70-79' THEN 1 END) AS pop_age_70_79,
        COUNT(CASE WHEN demo.age_band_10y = '80+' THEN 1 END) AS pop_age_80_plus,
        
        -- Ethnicity breakdowns
        COUNT(CASE WHEN demo.ethnicity_category = 'White' THEN 1 END) AS pop_ethnicity_white,
        COUNT(CASE WHEN demo.ethnicity_category = 'Mixed' THEN 1 END) AS pop_ethnicity_mixed,
        COUNT(CASE WHEN demo.ethnicity_category = 'Asian' THEN 1 END) AS pop_ethnicity_asian,
        COUNT(CASE WHEN demo.ethnicity_category = 'Black' THEN 1 END) AS pop_ethnicity_black,
        COUNT(CASE WHEN demo.ethnicity_category = 'Other' THEN 1 END) AS pop_ethnicity_other,
        COUNT(CASE WHEN demo.ethnicity_category IS NULL OR demo.ethnicity_category = 'Unknown' THEN 1 END) AS pop_ethnicity_unknown,
        
        -- Deprivation quintiles (when available)
        COUNT(CASE WHEN demo.imd_quintile_19 = 1 THEN 1 END) AS pop_imd_quintile_1_most_deprived,
        COUNT(CASE WHEN demo.imd_quintile_19 = 2 THEN 1 END) AS pop_imd_quintile_2,
        COUNT(CASE WHEN demo.imd_quintile_19 = 3 THEN 1 END) AS pop_imd_quintile_3,
        COUNT(CASE WHEN demo.imd_quintile_19 = 4 THEN 1 END) AS pop_imd_quintile_4,
        COUNT(CASE WHEN demo.imd_quintile_19 = 5 THEN 1 END) AS pop_imd_quintile_5_least_deprived,
        COUNT(CASE WHEN demo.imd_quintile_19 IS NULL THEN 1 END) AS pop_imd_unknown
        
    FROM {{ ref('dim_person_current_practice') }} AS prac
    INNER JOIN {{ ref('dim_person_demographics') }} AS demo
        ON prac.person_id = demo.person_id
    WHERE prac.registration_end_date IS NULL  -- Current registrations only
    GROUP BY 
        prac.practice_code,
        prac.practice_name,
        prac.practice_postcode,
        demo.pcn_code,
        demo.pcn_name,
        demo.borough_registered,
        demo.neighbourhood_registered
),

case_finding_aggregates AS (
    -- Aggregate case finding indicators by practice
    SELECT
        prac.practice_code,
        
        -- Total case finding eligible population
        COUNT(*) AS case_finding_eligible_population,
        COUNT(CASE WHEN cf.in_any_case_finding THEN 1 END) AS total_case_finding_count,
        
        -- AF indicators
        COUNT(CASE WHEN cf.in_af_61 THEN 1 END) AS cf_af_61_count,
        COUNT(CASE WHEN cf.in_af_62 THEN 1 END) AS cf_af_62_count,
        
        -- CKD indicators
        COUNT(CASE WHEN cf.in_ckd_61 THEN 1 END) AS cf_ckd_61_count,
        COUNT(CASE WHEN cf.in_ckd_62 THEN 1 END) AS cf_ckd_62_count,
        COUNT(CASE WHEN cf.in_ckd_63 THEN 1 END) AS cf_ckd_63_count,
        COUNT(CASE WHEN cf.in_ckd_64 THEN 1 END) AS cf_ckd_64_count,
        
        -- CVD indicators
        COUNT(CASE WHEN cf.in_cvd_61 THEN 1 END) AS cf_cvd_61_count,
        COUNT(CASE WHEN cf.in_cvd_62 THEN 1 END) AS cf_cvd_62_count,
        COUNT(CASE WHEN cf.in_cvd_63 THEN 1 END) AS cf_cvd_63_count,
        COUNT(CASE WHEN cf.in_cvd_64 THEN 1 END) AS cf_cvd_64_count,
        COUNT(CASE WHEN cf.in_cvd_65 THEN 1 END) AS cf_cvd_65_count,
        COUNT(CASE WHEN cf.in_cvd_66 THEN 1 END) AS cf_cvd_66_count,
        
        -- Diabetes indicators
        COUNT(CASE WHEN cf.in_dm_61 THEN 1 END) AS cf_dm_61_count,
        COUNT(CASE WHEN cf.in_dm_62 THEN 1 END) AS cf_dm_62_count,
        COUNT(CASE WHEN cf.in_dm_63 THEN 1 END) AS cf_dm_63_count,
        COUNT(CASE WHEN cf.in_dm_64 THEN 1 END) AS cf_dm_64_count,
        COUNT(CASE WHEN cf.in_dm_65 THEN 1 END) AS cf_dm_65_count,
        COUNT(CASE WHEN cf.in_dm_66 THEN 1 END) AS cf_dm_66_count,
        
        -- Hypertension indicators
        COUNT(CASE WHEN cf.in_htn_61 THEN 1 END) AS cf_htn_61_count,
        COUNT(CASE WHEN cf.in_htn_62 THEN 1 END) AS cf_htn_62_count,
        COUNT(CASE WHEN cf.in_htn_63 THEN 1 END) AS cf_htn_63_count,
        COUNT(CASE WHEN cf.in_htn_65 THEN 1 END) AS cf_htn_65_count,
        COUNT(CASE WHEN cf.in_htn_66 THEN 1 END) AS cf_htn_66_count,
        
        -- CYP Asthma indicator
        COUNT(CASE WHEN cf.in_cyp_ast_61 THEN 1 END) AS cf_cyp_ast_61_count
        
    FROM {{ ref('dim_ltc_lcs_cf_summary') }} AS cf
    INNER JOIN {{ ref('dim_person_current_practice') }} AS prac
        ON cf.person_id = prac.person_id
    WHERE prac.registration_end_date IS NULL  -- Current registrations only
    GROUP BY prac.practice_code
)

-- Final aggregation with calculated rates
SELECT
    -- Practice identifiers and context
    demo.practice_code,
    demo.practice_name,
    demo.practice_postcode,
    demo.pcn_code,
    demo.pcn_name,
    demo.borough_registered,
    demo.neighbourhood_registered,
    
    -- Population denominators
    demo.total_population,
    COALESCE(cf.case_finding_eligible_population, 0) AS case_finding_eligible_population,
    COALESCE(cf.total_case_finding_count, 0) AS total_case_finding_count,
    
    -- Age band populations
    demo.pop_age_0_9,
    demo.pop_age_10_19,
    demo.pop_age_20_29,
    demo.pop_age_30_39,
    demo.pop_age_40_49,
    demo.pop_age_50_59,
    demo.pop_age_60_69,
    demo.pop_age_70_79,
    demo.pop_age_80_plus,
    
    -- Ethnicity populations
    demo.pop_ethnicity_white,
    demo.pop_ethnicity_mixed,
    demo.pop_ethnicity_asian,
    demo.pop_ethnicity_black,
    demo.pop_ethnicity_other,
    demo.pop_ethnicity_unknown,
    
    -- Deprivation quintile populations
    demo.pop_imd_quintile_1_most_deprived,
    demo.pop_imd_quintile_2,
    demo.pop_imd_quintile_3,
    demo.pop_imd_quintile_4,
    demo.pop_imd_quintile_5_least_deprived,
    demo.pop_imd_unknown,
    
    -- Case finding counts
    COALESCE(cf.cf_af_61_count, 0) AS cf_af_61_count,
    COALESCE(cf.cf_af_62_count, 0) AS cf_af_62_count,
    COALESCE(cf.cf_ckd_61_count, 0) AS cf_ckd_61_count,
    COALESCE(cf.cf_ckd_62_count, 0) AS cf_ckd_62_count,
    COALESCE(cf.cf_ckd_63_count, 0) AS cf_ckd_63_count,
    COALESCE(cf.cf_ckd_64_count, 0) AS cf_ckd_64_count,
    COALESCE(cf.cf_cvd_61_count, 0) AS cf_cvd_61_count,
    COALESCE(cf.cf_cvd_62_count, 0) AS cf_cvd_62_count,
    COALESCE(cf.cf_cvd_63_count, 0) AS cf_cvd_63_count,
    COALESCE(cf.cf_cvd_64_count, 0) AS cf_cvd_64_count,
    COALESCE(cf.cf_cvd_65_count, 0) AS cf_cvd_65_count,
    COALESCE(cf.cf_cvd_66_count, 0) AS cf_cvd_66_count,
    COALESCE(cf.cf_dm_61_count, 0) AS cf_dm_61_count,
    COALESCE(cf.cf_dm_62_count, 0) AS cf_dm_62_count,
    COALESCE(cf.cf_dm_63_count, 0) AS cf_dm_63_count,
    COALESCE(cf.cf_dm_64_count, 0) AS cf_dm_64_count,
    COALESCE(cf.cf_dm_65_count, 0) AS cf_dm_65_count,
    COALESCE(cf.cf_dm_66_count, 0) AS cf_dm_66_count,
    COALESCE(cf.cf_htn_61_count, 0) AS cf_htn_61_count,
    COALESCE(cf.cf_htn_62_count, 0) AS cf_htn_62_count,
    COALESCE(cf.cf_htn_63_count, 0) AS cf_htn_63_count,
    COALESCE(cf.cf_htn_65_count, 0) AS cf_htn_65_count,
    COALESCE(cf.cf_htn_66_count, 0) AS cf_htn_66_count,
    COALESCE(cf.cf_cyp_ast_61_count, 0) AS cf_cyp_ast_61_count,
    
    -- Calculated rates (per 1000 population)
    CASE 
        WHEN demo.total_population > 0 
        THEN ROUND((COALESCE(cf.total_case_finding_count, 0) * 1000.0) / demo.total_population, 2)
        ELSE 0 
    END AS case_finding_rate_per_1000,
    
    -- Calculated percentages
    CASE 
        WHEN demo.total_population > 0 
        THEN ROUND((COALESCE(cf.total_case_finding_count, 0) * 100.0) / demo.total_population, 2)
        ELSE 0 
    END AS case_finding_percentage,
    
    -- Practice size category for benchmarking
    CASE 
        WHEN demo.total_population < 3000 THEN 'Small (<3k)'
        WHEN demo.total_population < 7000 THEN 'Medium (3-7k)'
        WHEN demo.total_population < 12000 THEN 'Large (7-12k)'
        ELSE 'Very Large (12k+)'
    END AS practice_size_category,
    
    -- Metadata
    CURRENT_DATE() AS data_refresh_date

FROM practice_demographics AS demo
LEFT JOIN case_finding_aggregates AS cf
    ON demo.practice_code = cf.practice_code
ORDER BY demo.practice_code