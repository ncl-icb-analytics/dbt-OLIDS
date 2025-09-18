{{ config(
    materialized='table') }}

-- Intermediate model for LTC LCS CF Base Population
-- Reusable base population for LTC LCS case finding indicators.
-- Starts with all patients and excludes those already in LTC programmes and those with NHS health checks in the last 24 months.

WITH all_patients AS (
    -- Start with all patients (not just those with LTC conditions)
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('dim_person_age') }}
),

health_checks AS (
    -- Get patients with health checks in last 24 months
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_nhs_health_checks') }}
    WHERE clinical_effective_date >= DATEADD(MONTH, -24, CURRENT_DATE())
),

ltc_exclusions AS (
    -- Get patients already in LTC programmes (with excluding conditions)
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_cf_exclusions') }}
    WHERE has_excluding_condition = TRUE
)

SELECT DISTINCT
    all_patients.person_id,
    all_patients.age
FROM all_patients
WHERE all_patients.person_id NOT IN (
    -- Exclude patients already in LTC programmes
    SELECT person_id FROM ltc_exclusions
)
AND all_patients.person_id NOT IN (
    -- Exclude patients with health checks in last 24 months
    SELECT person_id FROM health_checks
)
