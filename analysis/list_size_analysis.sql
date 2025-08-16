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
-- PATTERN 4: List Size Trends (Last 12 Months)
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
-- PATTERN 5: Practice Size Distribution
-- =============================================================================
-- Distribution of practices by list size bands
SELECT 
    CASE 
        WHEN list_size < 5000 THEN 'Small'
        WHEN list_size < 10000 THEN 'Medium'
        WHEN list_size < 20000 THEN 'Large'
        WHEN list_size < 50000 THEN 'Very Large'
        ELSE 'Super Large'
    END as practice_size_band,
    COUNT(*) as practices_count,
    ROUND(AVG(list_size)) as avg_list_size,
    SUM(list_size) as total_population
FROM (
    SELECT 
        practice_code,
        practice_name,
        COUNT(DISTINCT person_id) as list_size
    FROM {{ ref('person_month_analysis_base') }}
    WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    GROUP BY practice_code, practice_name
) practice_sizes
GROUP BY ALL
ORDER BY MIN(CASE 
    WHEN practice_size_band = 'Small' THEN 1
    WHEN practice_size_band = 'Medium' THEN 2
    WHEN practice_size_band = 'Large' THEN 3
    WHEN practice_size_band = 'Very Large' THEN 4
    ELSE 5
END);