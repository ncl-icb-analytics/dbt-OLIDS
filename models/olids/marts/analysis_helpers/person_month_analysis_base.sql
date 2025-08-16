{{
    config(
        materialized='incremental',
        unique_key=['person_id', 'analysis_month'],
        on_schema_change='fail',
        cluster_by=['analysis_month', 'practice_borough', 'person_id']
    )
}}

-- Person Month Analysis Base
-- Pre-joined incremental table combining active person-months with demographics and conditions
-- Eliminates repetitive temporal join logic and provides a fast table for analysis
-- 
-- Incremental Strategy:
-- - Only processes new months since last run (truly incremental)
-- - Use `dbt run --full-refresh` to rebuild entire table when needed
-- - For late-arriving data updates, use full refresh periodically (weekly/monthly)

WITH active_person_months AS (
    -- Generate person-months only where patients were actually registered
    -- This prevents empty months from appearing in the data
    SELECT DISTINCT
        ds.month_start_date as analysis_month,
        hr.person_id,
        hr.practice_id,
        hr.practice_name
    FROM {{ ref('dim_person_historical_practice') }} hr
    INNER JOIN {{ ref('int_date_spine') }} ds
        ON hr.registration_start_date <= ds.month_end_date
        AND (hr.registration_end_date IS NULL OR hr.registration_end_date >= ds.month_start_date)
        AND ds.month_start_date >= DATEADD('month', -60, CURRENT_DATE)  -- Limit to last 5 years for performance
        AND ds.month_start_date <= DATE_TRUNC('month', CURRENT_DATE)    -- Don't create future months
    WHERE hr.registration_status = 'Active'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ds.month_start_date, hr.person_id 
        ORDER BY hr.registration_start_date DESC, hr.is_current_registration DESC
    ) = 1
)

SELECT 
    -- Core identifiers
    apm.analysis_month,
    apm.person_id,
    apm.practice_id,
    apm.practice_name,
    
    -- Core date components for filtering
    ds.year_number,
    ds.month_number,
    ds.quarter_number,
    
    -- Display labels
    ds.month_year_label,
    
    -- Financial year (essential for NHS reporting)
    ds.financial_year_label as financial_year,
    ds.financial_year_start,
    ds.financial_quarter_label as financial_quarter,
    ds.financial_quarter_number,
    
    -- Complete demographics (temporal join applied)
    d.birth_year,
    d.birth_date_approx,
    d.birth_date_approx_end_of_month,
    d.age_at_least,
    d.age,
    d.sex,
    d.ethnicity_category,
    d.ethnicity_subcategory,
    d.ethnicity_granular,
    d.ethnicity_category_sort,
    d.ethnicity_display_sort_key,
    d.age_band_5y,
    d.age_band_10y,
    d.age_band_nhs,
    d.age_band_ons,
    d.age_life_stage,
    d.main_language,
    d.language_type,
    d.interpreter_type,
    d.interpreter_needed,
    d.is_active,
    d.inactive_reason,
    d.death_year,
    d.death_date_approx,
    d.is_deceased,
    
    -- Practice and geography (complete)
    d.practice_code,
    d.practice_borough,
    d.practice_postcode,
    d.practice_lsoa,
    d.practice_msoa,
    d.practice_latitude,
    d.practice_longitude,
    d.practice_neighbourhood,
    d.pcn_code,
    d.pcn_name,
    d.pcn_name_with_borough,
    
    -- Address and household
    d.post_code_hash,
    d.uprn_hash,
    d.household_id,
    d.lsoa_code_21,
    d.lsoa_name_21,
    d.ward_code,
    d.ward_name,
    d.imd_decile_19,
    d.imd_quintile_19,
    d.patient_neighbourhood,
    
    -- SCD2 metadata
    d.effective_start_date,
    d.effective_end_date,
    d.period_sequence,
    d.is_current_period,
    d.age_changes_in_period,
    
    -- Condition flags (all major conditions)
    c.has_ast,
    c.has_copd,
    c.has_htn,
    c.has_chd,
    c.has_af,
    c.has_hf,
    c.has_pad,
    c.has_dm,
    c.has_gestdiab,
    c.has_ndh,
    c.has_dep,
    c.has_smi,
    c.has_ckd,
    c.has_dem,
    c.has_ep,
    c.has_stia,
    c.has_can,
    c.has_pc,
    c.has_ld,
    c.has_frail,
    c.has_ra,
    c.has_ost,
    c.has_nafld,
    c.has_fh,
    
    -- New episode flags (all conditions)
    c.new_ast,
    c.new_copd,
    c.new_htn,
    c.new_chd,
    c.new_af,
    c.new_hf,
    c.new_pad,
    c.new_dm,
    c.new_gestdiab,
    c.new_ndh,
    c.new_dep,
    c.new_smi,
    c.new_ckd,
    c.new_dem,
    c.new_ep,
    c.new_stia,
    c.new_can,
    c.new_pc,
    c.new_ld,
    c.new_frail,
    c.new_ra,
    c.new_ost,
    c.new_nafld,
    c.new_fh,
    
    -- Summary metrics
    c.total_active_conditions,
    c.total_new_episodes_this_month,
    c.has_any_condition,
    c.has_any_new_episode

FROM active_person_months apm

-- Join date spine for all date dimensions
INNER JOIN {{ ref('int_date_spine') }} ds
    ON apm.analysis_month = ds.month_start_date

-- Join demographics with proper temporal logic
INNER JOIN {{ ref('dim_person_demographics_historical') }} d
    ON apm.person_id = d.person_id
    AND apm.analysis_month >= d.effective_start_date
    AND (d.effective_end_date IS NULL OR apm.analysis_month < d.effective_end_date)

-- Calculate condition flags directly from episodes table
LEFT JOIN (
    SELECT 
        person_id,
        analysis_month,
        
        -- Active condition flags (has_*)
        MAX(CASE WHEN condition_code = 'AST' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_ast,
        MAX(CASE WHEN condition_code = 'COPD' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_copd,
        MAX(CASE WHEN condition_code = 'HTN' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_htn,
        MAX(CASE WHEN condition_code = 'CHD' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_chd,
        MAX(CASE WHEN condition_code = 'AF' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_af,
        MAX(CASE WHEN condition_code = 'HF' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_hf,
        MAX(CASE WHEN condition_code = 'PAD' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_pad,
        MAX(CASE WHEN condition_code = 'DM' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_dm,
        MAX(CASE WHEN condition_code = 'GESTDIAB' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_gestdiab,
        MAX(CASE WHEN condition_code = 'NDH' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_ndh,
        MAX(CASE WHEN condition_code = 'DEP' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_dep,
        MAX(CASE WHEN condition_code = 'SMI' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_smi,
        MAX(CASE WHEN condition_code = 'CKD' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_ckd,
        MAX(CASE WHEN condition_code = 'DEM' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_dem,
        MAX(CASE WHEN condition_code = 'EP' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_ep,
        MAX(CASE WHEN condition_code = 'STIA' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_stia,
        MAX(CASE WHEN condition_code = 'CAN' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_can,
        MAX(CASE WHEN condition_code = 'PC' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_pc,
        MAX(CASE WHEN condition_code = 'LD' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_ld,
        MAX(CASE WHEN condition_code = 'FRAIL' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_frail,
        MAX(CASE WHEN condition_code = 'RA' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_ra,
        MAX(CASE WHEN condition_code = 'OST' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_ost,
        MAX(CASE WHEN condition_code = 'NAFLD' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_nafld,
        MAX(CASE WHEN condition_code = 'FH' AND has_active_episode = 1 THEN 1 ELSE 0 END) as has_fh,
        
        -- New episode flags (episode started during this month) 
        MAX(CASE WHEN condition_code = 'AST' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_ast,
        MAX(CASE WHEN condition_code = 'COPD' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_copd,
        MAX(CASE WHEN condition_code = 'HTN' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_htn,
        MAX(CASE WHEN condition_code = 'CHD' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_chd,
        MAX(CASE WHEN condition_code = 'AF' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_af,
        MAX(CASE WHEN condition_code = 'HF' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_hf,
        MAX(CASE WHEN condition_code = 'PAD' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_pad,
        MAX(CASE WHEN condition_code = 'DM' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_dm,
        MAX(CASE WHEN condition_code = 'GESTDIAB' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_gestdiab,
        MAX(CASE WHEN condition_code = 'NDH' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_ndh,
        MAX(CASE WHEN condition_code = 'DEP' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_dep,
        MAX(CASE WHEN condition_code = 'SMI' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_smi,
        MAX(CASE WHEN condition_code = 'CKD' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_ckd,
        MAX(CASE WHEN condition_code = 'DEM' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_dem,
        MAX(CASE WHEN condition_code = 'EP' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_ep,
        MAX(CASE WHEN condition_code = 'STIA' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_stia,
        MAX(CASE WHEN condition_code = 'CAN' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_can,
        MAX(CASE WHEN condition_code = 'PC' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_pc,
        MAX(CASE WHEN condition_code = 'LD' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_ld,
        MAX(CASE WHEN condition_code = 'FRAIL' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_frail,
        MAX(CASE WHEN condition_code = 'RA' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_ra,
        MAX(CASE WHEN condition_code = 'OST' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_ost,
        MAX(CASE WHEN condition_code = 'NAFLD' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_nafld,
        MAX(CASE WHEN condition_code = 'FH' AND has_new_episode = 1 THEN 1 ELSE 0 END) as new_fh,
        
        -- Summary metrics
        COUNT(DISTINCT CASE WHEN has_active_episode = 1 THEN condition_code END) as total_active_conditions,
        COUNT(DISTINCT CASE WHEN has_new_episode = 1 THEN condition_code END) as total_new_episodes_this_month,
        CASE WHEN COUNT(DISTINCT CASE WHEN has_active_episode = 1 THEN condition_code END) > 0 THEN 1 ELSE 0 END as has_any_condition,
        CASE WHEN COUNT(DISTINCT CASE WHEN has_new_episode = 1 THEN condition_code END) > 0 THEN 1 ELSE 0 END as has_any_new_episode
        
    FROM (
        SELECT 
            person_id,
            condition_code,
            ds.month_start_date as analysis_month,
            -- Active episode: episode is ongoing during this month
            CASE WHEN episode_start_date <= ds.month_end_date 
                AND (episode_end_date IS NULL OR episode_end_date >= ds.month_start_date)
                THEN 1 ELSE 0 END as has_active_episode,
            -- New episode: episode started during this month  
            CASE WHEN episode_start_date >= ds.month_start_date 
                AND episode_start_date <= ds.month_end_date
                THEN 1 ELSE 0 END as has_new_episode
        FROM {{ ref('fct_person_condition_episodes') }} ep
        CROSS JOIN {{ ref('int_date_spine') }} ds
        WHERE ds.month_start_date >= DATEADD('month', -60, CURRENT_DATE)  -- Last 5 years
            AND ds.month_start_date <= DATE_TRUNC('month', CURRENT_DATE)
    ) episode_flags
    GROUP BY person_id, analysis_month
) c ON apm.person_id = c.person_id AND apm.analysis_month = c.analysis_month

{% if is_incremental() %}
    -- Only process new months since last run
    WHERE apm.analysis_month > (SELECT COALESCE(MAX(analysis_month), '1900-01-01') FROM {{ this }})
{% endif %}