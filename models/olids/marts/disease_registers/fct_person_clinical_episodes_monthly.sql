{{
    config(
        materialized='table',
        cluster_by=['person_id', 'analysis_month'])
}}

-- Clinical Episodes Monthly Fact Table
-- Person-month level aggregation of clinical condition episodes for trend analysis
-- Simplifies queries for population health monitoring and condition prevalence tracking

WITH monthly_spine AS (
    -- Create monthly date spine for the last 5 years
    SELECT 
        DATE_TRUNC('month', dateadd('month', seq4() - 60, CURRENT_DATE)) as analysis_month
    FROM table(generator(rowcount => 60))  -- 5 years of monthly data
    WHERE analysis_month <= CURRENT_DATE
),

active_patients_monthly AS (
    -- Get patients who were actively registered in each month
    SELECT DISTINCT
        ms.analysis_month,
        hr.person_id,
        hr.practice_id,
        hr.practice_name
    FROM monthly_spine ms
    INNER JOIN {{ ref('dim_person_historical_practice') }} hr
        ON hr.registration_start_date <= LAST_DAY(ms.analysis_month)
        AND (hr.registration_end_date IS NULL OR hr.registration_end_date >= ms.analysis_month)
    WHERE hr.registration_status = 'Active'
),

monthly_condition_status AS (
    -- For each person-month, determine status for each condition
    SELECT 
        apm.person_id,
        apm.analysis_month,
        apm.practice_id,
        apm.practice_name,
        ce.condition_name,
        ce.condition_code,
        ce.clinical_domain,
        
        -- Episode status this month
        CASE 
            WHEN ce.episode_start_date <= LAST_DAY(apm.analysis_month) 
                AND (ce.episode_end_date IS NULL OR ce.episode_end_date >= apm.analysis_month)
                THEN 1 
            ELSE 0 
        END as has_active_episode,
        
        -- New episode this month
        CASE 
            WHEN DATE_TRUNC('month', ce.episode_start_date) = apm.analysis_month 
                THEN 1 
            ELSE 0 
        END as new_episode_this_month,
        
        -- Episode details
        ce.episode_number,
        ce.episode_status,
        ce.total_episodes_for_condition
        
    FROM active_patients_monthly apm
    LEFT JOIN {{ ref('fct_person_clinical_condition_episodes') }} ce
        ON apm.person_id = ce.person_id
),

condition_flags AS (
    -- Pivot conditions into boolean flags for easier querying
    SELECT 
        person_id,
        analysis_month,
        practice_id,
        practice_name,
        
        -- Use condition codes for shorter, more memorable column names
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
        
        -- New episode flags using codes
        MAX(CASE WHEN condition_code = 'HTN' AND new_episode_this_month = 1 THEN 1 ELSE 0 END) as new_htn,
        MAX(CASE WHEN condition_code = 'DM' AND new_episode_this_month = 1 THEN 1 ELSE 0 END) as new_dm,
        MAX(CASE WHEN condition_code = 'DEP' AND new_episode_this_month = 1 THEN 1 ELSE 0 END) as new_dep,
        MAX(CASE WHEN condition_code = 'AST' AND new_episode_this_month = 1 THEN 1 ELSE 0 END) as new_ast,
        MAX(CASE WHEN condition_code = 'COPD' AND new_episode_this_month = 1 THEN 1 ELSE 0 END) as new_copd,
        
        -- Summary metrics (count distinct conditions, not episodes)
        COUNT(DISTINCT CASE WHEN has_active_episode = 1 THEN condition_name END) as total_active_conditions,
        COUNT(DISTINCT CASE WHEN new_episode_this_month = 1 THEN condition_name END) as total_new_episodes_this_month,
        
        -- Any condition flags for quick filtering
        CASE WHEN SUM(has_active_episode) > 0 THEN 1 ELSE 0 END as has_any_condition,
        CASE WHEN SUM(new_episode_this_month) > 0 THEN 1 ELSE 0 END as has_any_new_episode
        
    FROM monthly_condition_status
    GROUP BY person_id, analysis_month, practice_id, practice_name
)

-- Final output
SELECT 
    person_id,
    analysis_month,
    practice_id,
    practice_name,
    
    -- Individual condition flags (using memorable codes)
    has_ast,     -- Asthma
    has_copd,    -- COPD  
    has_htn,     -- Hypertension
    has_chd,     -- Coronary Heart Disease
    has_af,      -- Atrial Fibrillation
    has_hf,      -- Heart Failure
    has_pad,     -- Peripheral Arterial Disease
    has_dm,      -- Diabetes
    has_gestdiab, -- Gestational Diabetes
    has_ndh,     -- Non-Diabetic Hyperglycaemia
    has_dep,     -- Depression
    has_smi,     -- Severe Mental Illness
    has_ckd,     -- Chronic Kidney Disease
    has_dem,     -- Dementia
    has_ep,      -- Epilepsy
    has_stia,    -- Stroke and TIA
    has_can,     -- Cancer
    has_pc,      -- Palliative Care
    has_ld,      -- Learning Disability
    has_frail,   -- Frailty
    has_ra,      -- Rheumatoid Arthritis
    has_ost,     -- Osteoporosis
    has_nafld,   -- NAFLD
    has_fh,      -- Familial Hypercholesterolaemia
    
    -- New episode flags
    new_htn,     -- New hypertension
    new_dm,      -- New diabetes
    new_dep,     -- New depression
    new_ast,     -- New asthma
    new_copd,    -- New COPD
    
    -- Summary metrics
    total_active_conditions,
    total_new_episodes_this_month,
    has_any_condition,
    has_any_new_episode
    
FROM condition_flags
ORDER BY person_id, analysis_month