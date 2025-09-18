{{ config(
    materialized='table',
    tags=['fact', 'ltc', 'lcs', 'case_finding', 'population', 'demographics', 'powerbi', 'dashboard'],
    cluster_by=['age_band_10y', 'ethnicity_category', 'imd_quintile']) }}

-- Population health cross-tabulation of LTC LCS case finding indicators
-- Optimised for demographic analysis and health inequality monitoring

WITH demographic_population AS (
    -- Get total population denominators by demographic characteristics
    SELECT
        demo.age_band_10y,
        demo.ethnicity_category,
        COALESCE(demo.imd_quintile_19, 0) AS imd_quintile,  -- 0 = Unknown
        demo.borough_registered,
        demo.neighbourhood_registered,
        
        -- Population counts
        COUNT(*) AS total_population,
        
        -- Sex breakdown
        COUNT(CASE WHEN demo.sex = 'Male' THEN 1 END) AS population_male,
        COUNT(CASE WHEN demo.sex = 'Female' THEN 1 END) AS population_female,
        COUNT(CASE WHEN demo.sex NOT IN ('Male', 'Female') OR demo.sex IS NULL THEN 1 END) AS population_other_sex
        
    FROM {{ ref('dim_person_demographics') }} AS demo
    INNER JOIN {{ ref('dim_person_current_practice') }} AS prac
        ON demo.person_id = prac.person_id
    WHERE prac.registration_end_date IS NULL  -- Current registrations only
        AND demo.age_band_10y IS NOT NULL     -- Exclude records without age
    GROUP BY 
        demo.age_band_10y,
        demo.ethnicity_category,
        COALESCE(demo.imd_quintile_19, 0),
        demo.borough_registered,
        demo.neighbourhood_registered
),

case_finding_demographics AS (
    -- Get case finding indicators cross-tabulated by demographics
    SELECT
        demo.age_band_10y,
        demo.ethnicity_category,
        COALESCE(demo.imd_quintile_19, 0) AS imd_quintile,
        demo.borough_registered,
        demo.neighbourhood_registered,
        
        -- Total case finding counts
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
        COUNT(CASE WHEN cf.in_cyp_ast_61 THEN 1 END) AS cf_cyp_ast_61_count,
        
        -- Sex-specific case finding counts
        COUNT(CASE WHEN cf.in_any_case_finding AND demo.sex = 'Male' THEN 1 END) AS case_finding_male,
        COUNT(CASE WHEN cf.in_any_case_finding AND demo.sex = 'Female' THEN 1 END) AS case_finding_female,
        COUNT(CASE WHEN cf.in_any_case_finding AND (demo.sex NOT IN ('Male', 'Female') OR demo.sex IS NULL) THEN 1 END) AS case_finding_other_sex
        
    FROM {{ ref('dim_ltc_lcs_cf_summary') }} AS cf
    INNER JOIN {{ ref('dim_person_demographics') }} AS demo
        ON cf.person_id = demo.person_id
    INNER JOIN {{ ref('dim_person_current_practice') }} AS prac
        ON cf.person_id = prac.person_id
    WHERE prac.registration_end_date IS NULL  -- Current registrations only
        AND demo.age_band_10y IS NOT NULL     -- Exclude records without age
    GROUP BY 
        demo.age_band_10y,
        demo.ethnicity_category,
        COALESCE(demo.imd_quintile_19, 0),
        demo.borough_registered,
        demo.neighbourhood_registered
)

-- Final cross-tabulated population health summary
SELECT
    -- Demographic dimensions
    demo.age_band_10y,
    demo.ethnicity_category,
    demo.imd_quintile,
    CASE 
        WHEN demo.imd_quintile = 0 THEN 'Unknown'
        WHEN demo.imd_quintile = 1 THEN 'Quintile 1 (Most Deprived)'
        WHEN demo.imd_quintile = 2 THEN 'Quintile 2'
        WHEN demo.imd_quintile = 3 THEN 'Quintile 3'
        WHEN demo.imd_quintile = 4 THEN 'Quintile 4'
        WHEN demo.imd_quintile = 5 THEN 'Quintile 5 (Least Deprived)'
    END AS imd_quintile_label,
    demo.borough_registered,
    demo.neighbourhood_registered,
    
    -- Population denominators
    demo.total_population,
    demo.population_male,
    demo.population_female,
    demo.population_other_sex,
    COALESCE(cf.case_finding_eligible_population, 0) AS case_finding_eligible_population,
    COALESCE(cf.total_case_finding_count, 0) AS total_case_finding_count,
    
    -- Case finding counts by indicator
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
    
    -- Sex-specific case finding
    COALESCE(cf.case_finding_male, 0) AS case_finding_male,
    COALESCE(cf.case_finding_female, 0) AS case_finding_female,
    COALESCE(cf.case_finding_other_sex, 0) AS case_finding_other_sex,
    
    -- Calculated rates and percentages
    CASE 
        WHEN demo.total_population > 0 
        THEN ROUND((COALESCE(cf.total_case_finding_count, 0) * 1000.0) / demo.total_population, 2)
        ELSE 0 
    END AS case_finding_rate_per_1000,
    
    CASE 
        WHEN demo.total_population > 0 
        THEN ROUND((COALESCE(cf.total_case_finding_count, 0) * 100.0) / demo.total_population, 2)
        ELSE 0 
    END AS case_finding_percentage,
    
    -- Age band numeric for sorting
    CASE demo.age_band_10y
        WHEN '0-9' THEN 1
        WHEN '10-19' THEN 2
        WHEN '20-29' THEN 3
        WHEN '30-39' THEN 4
        WHEN '40-49' THEN 5
        WHEN '50-59' THEN 6
        WHEN '60-69' THEN 7
        WHEN '70-79' THEN 8
        WHEN '80+' THEN 9
        ELSE 10
    END AS age_band_sort_order,
    
    -- Metadata
    CURRENT_DATE() AS data_refresh_date

FROM demographic_population AS demo
LEFT JOIN case_finding_demographics AS cf
    ON demo.age_band_10y = cf.age_band_10y
    AND demo.ethnicity_category = cf.ethnicity_category
    AND demo.imd_quintile = cf.imd_quintile
    AND demo.borough_registered = cf.borough_registered
    AND demo.neighbourhood_registered = cf.neighbourhood_registered

ORDER BY 
    demo.age_band_10y,
    demo.ethnicity_category,
    demo.imd_quintile,
    demo.borough_registered,
    demo.neighbourhood_registered