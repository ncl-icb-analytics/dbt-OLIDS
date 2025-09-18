{{
    config(
        materialized='table',
        cluster_by=['indicator_category', 'indicator_id']
    )
}}

/*
LTC/LCS Case Finding Dashboard Base Table

Long format table providing dashboard-ready data by combining triggered case finding 
indicators with demographics and indicator metadata.

Key features:
- One row per person-indicator combination (only triggered indicators)
- Joins dim_ltc_lcs_cf_summary with dim_person_demographics
- Enriches with indicator metadata from def_indicator
- Optimised for PowerBI/Tableau dashboard consumption with dynamic filtering
- Single source of truth for case finding programme monitoring

Usage:
- Primary table for LTC/LCS Case Finding Dashboard
- Filter by indicator categories (AF, CKD, CVD, DM, HTN, CYP_AST)
- Demographic breakdowns for equity analysis
- Practice-level performance monitoring
*/

WITH indicator_definitions AS (
    -- Get all LTC/LCS case finding indicator definitions
    -- This ensures we're using the authoritative indicator metadata
    SELECT 
        i.indicator_id,
        i.indicator_type,
        i.category as indicator_category,
        i.clinical_domain,
        i.name_short,
        i.description_short,
        i.description_long,
        i.source_model,
        i.sort_order
    FROM {{ ref('def_indicator') }} i
    INNER JOIN {{ ref('def_indicator_usage') }} u
        ON i.indicator_id = u.indicator_id
    WHERE u.usage_context = 'LTC_LCS_DASHBOARD'
      AND i.indicator_type = 'CASE_FINDING'  -- Only case finding indicators, not the dashboard itself
),

base_data AS (
    SELECT 
        cf.person_id,
        cf.age as cf_eligibility_age,
        cf.in_any_case_finding,
        cf.case_finding_count,
        -- All individual indicator flags
        cf.in_af_61, cf.in_af_62,
        cf.in_ckd_61, cf.in_ckd_62, cf.in_ckd_63, cf.in_ckd_64,
        cf.in_cvd_61, cf.in_cvd_62, cf.in_cvd_63, cf.in_cvd_64, cf.in_cvd_65, cf.in_cvd_66,
        cf.in_dm_61, cf.in_dm_62, cf.in_dm_63, cf.in_dm_64, cf.in_dm_65, cf.in_dm_66,
        cf.in_htn_61, cf.in_htn_62, cf.in_htn_63, cf.in_htn_65, cf.in_htn_66,
        cf.in_cyp_ast_61,
        -- Demographics
        d.sk_patient_id,
        d.is_active,
        d.sex,
        d.age,
        d.age_band_5y,
        d.age_band_10y,
        d.age_band_nhs,
        d.age_life_stage,
        d.ethnicity_category,
        d.ethnicity_subcategory,
        d.ethnicity_granular,
        d.main_language,
        d.language_type,
        d.interpreter_needed,
        d.interpreter_type,
        d.imd_quintile_19,
        d.imd_decile_19,
        d.lsoa_code_21,
        d.practice_code,
        d.practice_name,
        d.pcn_code,
        d.pcn_name,
        d.borough_registered,
        d.neighbourhood_registered
    FROM {{ ref('dim_ltc_lcs_cf_summary') }} cf
    LEFT JOIN {{ ref('dim_person_demographics') }} d ON cf.person_id = d.person_id
),

final_dashboard AS (
    -- Long format: unpivot triggered indicators only
    SELECT 
        -- Person identifiers
        person_id,
        sk_patient_id, 
        is_active,
        
        -- LTC/LCS case finding indicators  
        in_any_case_finding, 
        case_finding_count,
        i.indicator_category, 
        unpivoted.indicator_id,
        i.clinical_domain,
        i.name_short as indicator_name,
        i.description_short as indicator_description,
        i.description_long as indicator_description_long,
        i.sort_order,
        TRUE as indicator_flag,
        
        -- Demographics
        sex, 
        age,
        age_band_5y, 
        age_band_10y, 
        age_band_nhs, 
        age_life_stage,
        ethnicity_category, 
        ethnicity_subcategory, 
        ethnicity_granular,
        main_language, 
        language_type, 
        interpreter_needed, 
        interpreter_type,
        
        -- Geographic and deprivation
        imd_quintile_19, 
        imd_decile_19, 
        lsoa_code_21 as lsoa_code,
        CASE 
            WHEN imd_quintile_19 IS NULL THEN 'Unknown'
            WHEN imd_quintile_19 = 1 THEN 'Quintile 1 (Most Deprived)'
            WHEN imd_quintile_19 = 2 THEN 'Quintile 2'
            WHEN imd_quintile_19 = 3 THEN 'Quintile 3'
            WHEN imd_quintile_19 = 4 THEN 'Quintile 4'
            WHEN imd_quintile_19 = 5 THEN 'Quintile 5 (Least Deprived)'
        END AS imd_quintile_label,
        
        -- Practice information
        practice_code, 
        practice_name, 
        pcn_code, 
        pcn_name, 
        borough_registered, 
        neighbourhood_registered,
        
        CURRENT_TIMESTAMP() AS created_at
    FROM (
        SELECT *, 'AF_61' as indicator_id FROM base_data WHERE in_af_61
        UNION ALL SELECT *, 'AF_62' FROM base_data WHERE in_af_62
        UNION ALL SELECT *, 'CKD_61' FROM base_data WHERE in_ckd_61
        UNION ALL SELECT *, 'CKD_62' FROM base_data WHERE in_ckd_62
        UNION ALL SELECT *, 'CKD_63' FROM base_data WHERE in_ckd_63
        UNION ALL SELECT *, 'CKD_64' FROM base_data WHERE in_ckd_64
        UNION ALL SELECT *, 'CVD_61' FROM base_data WHERE in_cvd_61
        UNION ALL SELECT *, 'CVD_62' FROM base_data WHERE in_cvd_62
        UNION ALL SELECT *, 'CVD_63' FROM base_data WHERE in_cvd_63
        UNION ALL SELECT *, 'CVD_64' FROM base_data WHERE in_cvd_64
        UNION ALL SELECT *, 'CVD_65' FROM base_data WHERE in_cvd_65
        UNION ALL SELECT *, 'CVD_66' FROM base_data WHERE in_cvd_66
        UNION ALL SELECT *, 'DM_61' FROM base_data WHERE in_dm_61
        UNION ALL SELECT *, 'DM_62' FROM base_data WHERE in_dm_62
        UNION ALL SELECT *, 'DM_63' FROM base_data WHERE in_dm_63
        UNION ALL SELECT *, 'DM_64' FROM base_data WHERE in_dm_64
        UNION ALL SELECT *, 'DM_65' FROM base_data WHERE in_dm_65
        UNION ALL SELECT *, 'DM_66' FROM base_data WHERE in_dm_66
        UNION ALL SELECT *, 'HTN_61' FROM base_data WHERE in_htn_61
        UNION ALL SELECT *, 'HTN_62' FROM base_data WHERE in_htn_62
        UNION ALL SELECT *, 'HTN_63' FROM base_data WHERE in_htn_63
        UNION ALL SELECT *, 'HTN_65' FROM base_data WHERE in_htn_65
        UNION ALL SELECT *, 'HTN_66' FROM base_data WHERE in_htn_66
        UNION ALL SELECT *, 'CYP_AST_61' FROM base_data WHERE in_cyp_ast_61
    ) AS unpivoted
    LEFT JOIN indicator_definitions i ON i.indicator_id = 'LTC_LCS_CF_' || unpivoted.indicator_id
)

SELECT * FROM final_dashboard
ORDER BY indicator_category, indicator_id, person_id