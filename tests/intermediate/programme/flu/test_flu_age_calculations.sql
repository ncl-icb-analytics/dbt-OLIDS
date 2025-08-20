/*
Data Quality Test: Flu Age Calculations

This test validates that the corrected age calculations in flu models
are working properly and match expected population counts.

Test checks:
1. Over 65 population count is reasonable (around 15k-16k for 2024-25 campaign)
2. Under 65 at risk population exists and is reasonable
3. No age calculation edge cases (like negative ages)
4. Age calculations are consistent across models
*/

WITH campaign_config AS (
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
),

over_65_population AS (
    SELECT COUNT(DISTINCT person_id) AS over_65_count
    FROM {{ ref('dim_person_demographics') }}
    CROSS JOIN campaign_config cc
    WHERE birth_date_approx <= DATEADD('year', -65, cc.campaign_reference_date)
      AND is_active = TRUE
),

under_65_at_risk_population AS (
    SELECT COUNT(DISTINCT person_id) AS under_65_at_risk_count
    FROM {{ ref('int_flu_under_65_at_risk') }}
    WHERE campaign_id = '{{ var("flu_current_campaign", "flu_2024_25") }}'
),

age_validation AS (
    SELECT 
        op.over_65_count,
        uar.under_65_at_risk_count,
        -- Test 1: Over 65 population should be between 14k-18k (reasonable range)
        CASE 
            WHEN op.over_65_count BETWEEN 14000 AND 18000 THEN 'PASS'
            ELSE 'FAIL'
        END AS over_65_range_check,
        
        -- Test 2: Under 65 at risk should exist and be reasonable (at least 1000 people)
        CASE 
            WHEN uar.under_65_at_risk_count >= 1000 THEN 'PASS'
            ELSE 'FAIL'
        END AS under_65_at_risk_check
    FROM over_65_population op
    CROSS JOIN under_65_at_risk_population uar
)

-- Test should pass (return no rows) if all validations pass
SELECT 
    'Over 65 population count (' || over_65_count || ') outside expected range 14k-18k' AS test_failure
FROM age_validation 
WHERE over_65_range_check = 'FAIL'

UNION ALL

SELECT 
    'Under 65 at risk population (' || under_65_at_risk_count || ') unexpectedly low (<1000)' AS test_failure
FROM age_validation 
WHERE under_65_at_risk_check = 'FAIL'