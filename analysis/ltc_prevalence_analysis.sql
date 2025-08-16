-- LTC Prevalence Analysis
-- Focused patterns for long-term condition prevalence over time and by demographics

-- =============================================================================
-- PATTERN 1: Current LTC Prevalence Rates
-- =============================================================================
-- Overall prevalence for major long-term conditions
SELECT 
    COUNT(DISTINCT person_id) as total_population,
    COUNT(DISTINCT CASE WHEN has_htn THEN person_id END) as hypertension_cases,
    COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) as diabetes_cases,
    COUNT(DISTINCT CASE WHEN has_smi THEN person_id END) as smi_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_htn THEN person_id END) / COUNT(DISTINCT person_id), 1) as htn_prevalence_pct,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as dm_prevalence_pct,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_smi THEN person_id END) / COUNT(DISTINCT person_id), 1) as smi_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }});

-- =============================================================================
-- PATTERN 2: LTC Prevalence by Age and Sex
-- =============================================================================
-- Diabetes prevalence breakdown by demographics
SELECT 
    age_band_nhs,
    sex,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) as diabetes_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND sex IN ('Male', 'Female')
    AND age_band_nhs IS NOT NULL
GROUP BY ALL
HAVING COUNT(DISTINCT person_id) >= 100  -- Adequate sample size
ORDER BY MIN(age), sex;

-- =============================================================================
-- PATTERN 3: LTC Prevalence Trends Over Years
-- =============================================================================
-- Track diabetes prevalence changes across all available years
SELECT 
    year_number,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) as diabetes_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct,
    LAG(ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1)) OVER (ORDER BY year_number) as previous_year_pct,
    ROUND(
        ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) - 
        LAG(ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1)) OVER (ORDER BY year_number), 1
    ) as yoy_change_pct_points
FROM {{ ref('person_month_analysis_base') }}
GROUP BY ALL
ORDER BY year_number;

-- =============================================================================
-- PATTERN 4: LTC Prevalence Trends Over Months
-- =============================================================================
-- Monthly diabetes prevalence over last 12 months
SELECT 
    analysis_month,
    month_year_label,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) as diabetes_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month >= DATEADD('month', -12, (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }}))
GROUP BY ALL
ORDER BY analysis_month;

-- =============================================================================
-- PATTERN 5: Condition Overlap Analysis
-- =============================================================================
-- Analyse overlap between diabetes, hypertension, and SMI
SELECT 
    CASE 
        WHEN has_dm = 1 AND has_htn = 1 AND has_smi = 1 THEN 'DM + HTN + SMI'
        WHEN has_dm = 1 AND has_htn = 1 AND has_smi = 0 THEN 'DM + HTN only'
        WHEN has_dm = 1 AND has_smi = 1 AND has_htn = 0 THEN 'DM + SMI only'
        WHEN has_htn = 1 AND has_smi = 1 AND has_dm = 0 THEN 'HTN + SMI only'
        WHEN has_dm = 1 AND has_htn = 0 AND has_smi = 0 THEN 'DM only'
        WHEN has_htn = 1 AND has_dm = 0 AND has_smi = 0 THEN 'HTN only'
        WHEN has_smi = 1 AND has_dm = 0 AND has_htn = 0 THEN 'SMI only'
        ELSE 'None of these conditions'
    END as condition_combination,
    COUNT(DISTINCT person_id) as patients,
    ROUND(100 * COUNT(DISTINCT person_id) / SUM(COUNT(DISTINCT person_id)) OVER (), 1) as percentage,
    ROUND(AVG(age), 1) as mean_age
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
GROUP BY ALL
ORDER BY patients DESC;