{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: LTC LCS Case Finding AF_62 - Identifies patients over 65 with missing pulse checks at NHS Health Check appointments.

Business Purpose:
• Support systematic case finding for atrial fibrillation through NHS Health Check programme compliance
• Enable quality improvement monitoring for pulse check completion in high-risk age groups
• Provide clinical decision support for AF screening in patients over 65 with incomplete assessments
• Support NHS Health Check programme effectiveness and cardiovascular risk management

Data Granularity:
• One row per person aged 65+ in the LTC case finding base population
• Includes pulse check history within 36-month assessment window
• Tracks NHS Health Check attendance and pulse assessment completion

Key Features:
• Age-based eligibility (65+ years) aligned with AF screening guidelines
• 36-month assessment window for pulse check compliance monitoring
• Integration with NHS Health Check programme data for comprehensive assessment
• Clinical exclusion criteria applied for appropriate case finding targeting'"
    ]
) }}

-- Intermediate model for LTC LCS Case Finding AF_62: Patients over 65 missing pulse check in last 36 months
-- Uses modular approach: leverages base population, observations intermediate, and exclusions

WITH base_population AS (
    SELECT
        bp.person_id,
        age.age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON bp.person_id = age.person_id
    WHERE age.age >= 65
),

pulse_checks AS (
    SELECT
        person_id,
        clinical_effective_date,
        mapped_concept_code,
        mapped_concept_display
    FROM {{ ref('int_ltc_lcs_af_observations') }}
    WHERE
        cluster_id IN ('PULSE_RATE', 'PULSE_RHYTHM')
        AND clinical_effective_date >= dateadd(MONTH, -36, current_date())
),

pulse_check_summary AS (
    SELECT
        person_id,
        max(clinical_effective_date) AS latest_pulse_check_date,
        boolor_agg(TRUE) AS has_pulse_check,
        array_agg(DISTINCT mapped_concept_code) AS all_pulse_check_codes,
        array_agg(DISTINCT mapped_concept_display) AS all_pulse_check_displays
    FROM pulse_checks
    GROUP BY person_id
),

health_checks AS (
    SELECT
        person_id,
        max(clinical_effective_date) AS latest_health_check_date
    FROM {{ ref('int_nhs_health_check_latest') }}
    GROUP BY person_id
),

exclusions AS (
    SELECT
        person_id,
        has_excluding_condition
    FROM {{ ref('int_ltc_lcs_cf_exclusions') }}
)

SELECT
    bp.person_id,
    bp.age,
    pcs.latest_pulse_check_date,
    hc.latest_health_check_date,
    ex.has_excluding_condition,
    pcs.all_pulse_check_codes,
    pcs.all_pulse_check_displays,
    coalesce(pcs.has_pulse_check, FALSE) AS has_pulse_check
FROM base_population AS bp
LEFT JOIN pulse_check_summary AS pcs ON bp.person_id = pcs.person_id
LEFT JOIN health_checks AS hc ON bp.person_id = hc.person_id
LEFT JOIN exclusions AS ex ON bp.person_id = ex.person_id
