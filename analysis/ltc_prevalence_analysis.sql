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
HAVING COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) > 5  -- Small number suppression
ORDER BY MIN(age), sex;

-- =============================================================================
-- PATTERN 3: LTC Prevalence by Financial Year
-- =============================================================================
-- Track diabetes prevalence across UK financial years (April-March)
SELECT 
    financial_year,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) as diabetes_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct,
    LAG(ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1)) 
        OVER (ORDER BY financial_year) as previous_fy_pct,
    ROUND(
        ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) - 
        LAG(ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1)) 
            OVER (ORDER BY financial_year), 1
    ) as fy_change_pct_points
FROM {{ ref('person_month_analysis_base') }}
GROUP BY ALL
ORDER BY financial_year;

-- =============================================================================
-- PATTERN 4: LTC Prevalence by Financial Quarter
-- =============================================================================
-- Quarterly diabetes prevalence within current financial year
SELECT 
    financial_year,
    financial_quarter,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) as diabetes_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE financial_year = (
    SELECT MAX(financial_year) 
    FROM {{ ref('person_month_analysis_base') }}
)
GROUP BY ALL
ORDER BY financial_year, financial_quarter;

-- =============================================================================
-- PATTERN 5: CKD Prevalence by Neighbourhood and Ethnicity
-- =============================================================================
-- CKD prevalence rates by practice neighbourhood and ethnicity category
SELECT 
    neighbourhood_registered,
    ethnicity_category,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) as ckd_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) / COUNT(DISTINCT person_id), 1) as ckd_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND neighbourhood_registered IS NOT NULL
    AND ethnicity_category IS NOT NULL
GROUP BY ALL
ORDER BY neighbourhood_registered, MIN(ethnicity_category_sort);

-- =============================================================================
-- PATTERN 6: CKD Prevalence by Neighbourhood Over Time (FY End Position)
-- =============================================================================
-- Track CKD prevalence by practice neighbourhood at end of each financial year
-- Uses March data (31st March) for financial year-end position
SELECT 
    financial_year,
    neighbourhood_registered,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) as ckd_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) / COUNT(DISTINCT person_id), 1) as ckd_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE neighbourhood_registered IS NOT NULL
    AND month_number = 3  -- March (31st March - end of financial year)
GROUP BY ALL
HAVING COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) > 5  -- Small number suppression
ORDER BY neighbourhood_registered, financial_year;

-- =============================================================================
-- PATTERN 7: Condition Overlap Analysis
-- =============================================================================
-- Analyse overlap between diabetes, hypertension, and SMI
SELECT 
    CASE 
        WHEN has_dm AND has_htn AND has_smi THEN 'DM + HTN + SMI'
        WHEN has_dm AND has_htn AND NOT has_smi THEN 'DM + HTN only'
        WHEN has_dm AND has_smi AND NOT has_htn THEN 'DM + SMI only'
        WHEN has_htn AND has_smi AND NOT has_dm THEN 'HTN + SMI only'
        WHEN has_dm AND NOT has_htn AND NOT has_smi THEN 'DM only'
        WHEN has_htn AND NOT has_dm AND NOT has_smi THEN 'HTN only'
        WHEN has_smi AND NOT has_dm AND NOT has_htn THEN 'SMI only'
        ELSE 'None of these conditions'
    END as condition_combination,
    COUNT(DISTINCT person_id) as patients,
    ROUND(100 * COUNT(DISTINCT person_id) / SUM(COUNT(DISTINCT person_id)) OVER (), 1) as percentage
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
GROUP BY ALL
ORDER BY patients DESC;