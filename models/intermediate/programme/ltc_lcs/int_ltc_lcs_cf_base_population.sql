{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS CF Base Population - Defines reusable base population for Long Term Conditions case finding indicators with appropriate exclusions.

Clinical Purpose:
• Establishes standardised base population for LTC case finding measures
• Applies systematic exclusions for patients already in LTC programmes
• Excludes patients with recent NHS health checks to avoid duplication
• Provides consistent population denominator for case finding indicators

Data Granularity:
• One row per eligible person for LTC case finding programmes
• Excludes patients already in LTC programmes (from fct_person_ltc_summary)
• Excludes patients with NHS health checks in last 24 months
• Includes current age information for demographic stratification

Key Features:
• Reusable base population for multiple LTC case finding indicators
• Systematic exclusion logic preventing duplicate programme targeting
• NHS health check lookback period of 24 months
• Integration with person age and LTC summary data for comprehensive population definition'"
    ]
) }}

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
