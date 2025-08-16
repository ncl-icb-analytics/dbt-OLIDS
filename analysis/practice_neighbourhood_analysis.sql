-- Practice Neighbourhood Analysis
-- Focused patterns for analysing practices by neighbourhood and geographic area

-- =============================================================================
-- PATTERN 1: Practice Population by Neighbourhood
-- =============================================================================
-- Current patient counts by practice neighbourhood
SELECT 
    practice_neighbourhood,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT practice_code) as practices_count,
    ROUND(AVG(age), 1) as mean_age
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND practice_neighbourhood IS NOT NULL
GROUP BY ALL
ORDER BY population DESC;

-- =============================================================================
-- PATTERN 2: LTC Prevalence by Neighbourhood
-- =============================================================================
-- Diabetes prevalence comparison across neighbourhoods
SELECT 
    practice_neighbourhood,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) as diabetes_cases,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_htn THEN person_id END) / COUNT(DISTINCT person_id), 1) as htn_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND practice_neighbourhood IS NOT NULL
GROUP BY ALL
HAVING COUNT(DISTINCT person_id) >= 1000  -- Adequate sample size
ORDER BY diabetes_prevalence_pct DESC;

-- =============================================================================
-- PATTERN 3: Individual Practice Performance
-- =============================================================================
-- Practice-level metrics within neighbourhoods
SELECT 
    practice_neighbourhood,
    practice_name,
    practice_borough,
    COUNT(DISTINCT person_id) as list_size,
    ROUND(AVG(age), 1) as mean_age,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND practice_neighbourhood IS NOT NULL
GROUP BY ALL
HAVING COUNT(DISTINCT person_id) >= 500  -- Meaningful practice size
ORDER BY practice_neighbourhood, list_size DESC;

-- =============================================================================
-- PATTERN 4: Borough vs Neighbourhood Comparison
-- =============================================================================
-- Compare neighbourhood patterns within boroughs
SELECT 
    practice_borough,
    practice_neighbourhood,
    COUNT(DISTINCT person_id) as population,
    ROUND(100 * COUNT(DISTINCT CASE WHEN age >= 65 THEN person_id END) / COUNT(DISTINCT person_id), 1) as elderly_pct,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct,
    RANK() OVER (PARTITION BY practice_borough ORDER BY COUNT(DISTINCT person_id) DESC) as population_rank_in_borough
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
    AND practice_neighbourhood IS NOT NULL
    AND practice_borough IS NOT NULL
GROUP BY ALL
ORDER BY practice_borough, population DESC;

-- =============================================================================
-- PATTERN 5: Financial Year Comparison by Neighbourhood
-- =============================================================================
-- Compare neighbourhood performance across financial years
SELECT 
    practice_neighbourhood,
    financial_year,
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT practice_code) as practices_count,
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1) as diabetes_prevalence_pct,
    LAG(ROUND(100 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / COUNT(DISTINCT person_id), 1)) 
        OVER (PARTITION BY practice_neighbourhood ORDER BY financial_year) as previous_fy_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE practice_neighbourhood IS NOT NULL
GROUP BY ALL
HAVING COUNT(DISTINCT person_id) >= 1000  -- Adequate sample size
ORDER BY practice_neighbourhood, financial_year;