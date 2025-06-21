-- Intermediate model for LTC LCS CF Base Population
-- Reusable base population for LTC LCS case finding indicators.
-- Excludes patients already in LTC programmes and those with NHS health checks in the last 24 months.

WITH health_checks AS (
    -- Get patients with health checks in last 24 months
    SELECT DISTINCT
        person_id
    FROM {{ ref('int_ltc_lcs_af_observations') }}
    WHERE cluster_id = 'HEALTH_CHECK_COMP'
      AND clinical_effective_date >= DATEADD(month, -24, CURRENT_DATE())
)
SELECT DISTINCT
    person_id,
    condition_code,
    is_on_register
FROM {{ ref('fct_person_ltc_summary') }}
WHERE person_id NOT IN (
    -- Exclude patients already in LTC programmes
    SELECT person_id 
    FROM {{ ref('int_ltc_lcs_cf_exclusions') }}
)
AND person_id NOT IN (
    -- Exclude patients with health checks in last 24 months
    SELECT person_id
    FROM health_checks
)
