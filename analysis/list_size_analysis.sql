-- List Size Analysis
-- Focused patterns for calculating practice list sizes and demographics

-- =============================================================================
-- PATTERN 1: Current List Size by Practice
-- =============================================================================
-- Get current patient counts by practice with key identifiers
SELECT 
    practice_name,
    practice_code,
    pcn_name,
    practice_borough,
    COUNT(DISTINCT person_id) as list_size
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
GROUP BY ALL
ORDER BY list_size DESC;

-- =============================================================================
-- PATTERN 2: List Size by Demographics
-- =============================================================================
-- Population breakdown by age band and sex across all practices
SELECT 
    age_band_nhs,
    sex,
    COUNT(DISTINCT person_id) as population,
    ROUND(100 * COUNT(DISTINCT person_id) / SUM(COUNT(DISTINCT person_id)) OVER (), 1) as percentage_of_total
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND sex IN ('Male', 'Female')
    AND age_band_nhs IS NOT NULL
GROUP BY ALL
ORDER BY MIN(age), sex;

-- =============================================================================
-- PATTERN 3: List Size by Ethnicity
-- =============================================================================
-- Population distribution by ethnicity category
SELECT 
    ethnicity_category,
    COUNT(DISTINCT person_id) as population,
    ROUND(100 * COUNT(DISTINCT person_id) / SUM(COUNT(DISTINCT person_id)) OVER (), 1) as percentage
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND ethnicity_category NOT IN ('Unknown', 'Not Recorded')
GROUP BY ALL
ORDER BY MIN(ethnicity_category_sort);

-- =============================================================================
-- PATTERN 4: List Size Trends Over Time
-- =============================================================================
-- Track total population changes over last 12 months
SELECT 
    analysis_month,
    month_year_label,
    COUNT(DISTINCT person_id) as total_population,
    COUNT(DISTINCT practice_code) as active_practices
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month >= DATEADD('month', -12, (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }}))
GROUP BY ALL
ORDER BY analysis_month;

-- =============================================================================
-- PATTERN 5: Borough Population Summary
-- =============================================================================
-- Compare population size and demographics across boroughs
SELECT 
    practice_borough,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT practice_code) as practices_count,
    ROUND(AVG(age), 1) as mean_age,
    ROUND(100 * COUNT(DISTINCT CASE WHEN sex = 'Female' THEN person_id END) / COUNT(DISTINCT person_id), 1) as female_pct,
    ROUND(100 * COUNT(DISTINCT CASE WHEN age >= 65 THEN person_id END) / COUNT(DISTINCT person_id), 1) as elderly_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND practice_borough IS NOT NULL
GROUP BY ALL
ORDER BY population DESC;