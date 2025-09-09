/*
Data Quality Test: Flu Under 65 At Risk Population

This test validates that the flu under 65 at risk population
has reasonable counts and exists in the system.

Test checks:
1. Under 65 at risk population exists and is reasonable
*/

WITH campaign_config AS (
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
),


under_65_at_risk_population AS (
    SELECT COUNT(DISTINCT person_id) AS under_65_at_risk_count
    FROM {{ ref('int_flu_under_65_at_risk') }}
    WHERE campaign_id = '{{ var("flu_current_campaign", "flu_2024_25") }}'
),

age_validation AS (
    SELECT 
        uar.under_65_at_risk_count,
        -- Test: Under 65 at risk should exist and be reasonable
        CASE 
            WHEN uar.under_65_at_risk_count >= 10 THEN 'PASS'
            ELSE 'FAIL'
        END AS under_65_at_risk_check
    FROM under_65_at_risk_population uar
)

-- Test should pass (return no rows) if all validations pass
SELECT 
    'Under 65 at risk population (' || under_65_at_risk_count || ') unexpectedly low (<10)' AS test_failure
FROM age_validation 
WHERE under_65_at_risk_check = 'FAIL'