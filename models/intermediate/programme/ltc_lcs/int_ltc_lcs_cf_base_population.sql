{{ config(materialized='table') }}

-- Intermediate model for LTC LCS CF Base Population
-- Reusable base population for LTC LCS case finding indicators.
-- Excludes patients already in LTC programmes and those with NHS health checks in the last 24 months.

WITH health_checks AS (
    -- Get patients with health checks in last 24 months
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_nhs_health_checks') }}
    WHERE clinical_effective_date >= DATEADD(MONTH, -24, CURRENT_DATE())
)

SELECT DISTINCT
    ltc.person_id,
    age.age
FROM {{ ref('fct_person_ltc_summary') }} AS ltc
INNER JOIN {{ ref('dim_person_age') }} AS age ON ltc.person_id = age.person_id
WHERE ltc.person_id NOT IN (
    -- Exclude patients already in LTC programmes
    SELECT person_id
    FROM {{ ref('int_ltc_lcs_cf_exclusions') }}
)
AND ltc.person_id NOT IN (
    -- Exclude patients with health checks in last 24 months
    SELECT person_id
    FROM health_checks
)
