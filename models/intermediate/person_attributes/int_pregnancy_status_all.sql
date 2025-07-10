{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All pregnancy-related observations with enhanced analytics features.
Uses QOF pregnancy cluster IDs: PREG_COD (pregnancy status), PREGDEL_COD (delivery events).

Enhanced Analytics Features:
- Pregnancy status categorisation and clinical context
- Enhanced timeframe analysis for maternity care
- Clinical safety integration support (e.g., medication contraindications)

Single Responsibility: Pregnancy observation data collection with analytics enhancement.
Includes ALL persons following intermediate layer principles.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Pregnancy-specific flags (observation-level)
    CASE WHEN obs.cluster_id = 'PREG_COD' THEN TRUE ELSE FALSE END AS is_pregnancy_code,
    CASE WHEN obs.cluster_id = 'PREGDEL_COD' THEN TRUE ELSE FALSE END AS is_pregnancy_outcome_code,

    -- Enhanced pregnancy status categorisation
    CASE
        WHEN obs.cluster_id = 'PREG_COD' AND LOWER(obs.mapped_concept_display) LIKE '%pregnant%' THEN 'Active Pregnancy'
        WHEN obs.cluster_id = 'PREG_COD' AND LOWER(obs.mapped_concept_display) LIKE '%gravid%' THEN 'Pregnancy Confirmed'
        WHEN obs.cluster_id = 'PREGDEL_COD' AND (LOWER(obs.mapped_concept_display) LIKE '%delivery%' OR LOWER(obs.mapped_concept_display) LIKE '%birth%') THEN 'Live Birth'
        WHEN obs.cluster_id = 'PREGDEL_COD' AND LOWER(obs.mapped_concept_display) LIKE '%caesarean%' THEN 'Caesarean Birth'
        WHEN obs.cluster_id = 'PREGDEL_COD' AND (LOWER(obs.mapped_concept_display) LIKE '%termination%' OR LOWER(obs.mapped_concept_display) LIKE '%abortion%') THEN 'Termination of Pregnancy'
        WHEN obs.cluster_id = 'PREGDEL_COD' AND LOWER(obs.mapped_concept_display) LIKE '%miscarriage%' THEN 'Miscarriage'
        WHEN obs.cluster_id = 'PREG_COD' THEN 'Pregnancy Status'
        WHEN obs.cluster_id = 'PREGDEL_COD' THEN 'Pregnancy Outcome'
        ELSE 'Pregnancy Related'
    END AS pregnancy_status_category,

    -- Clinical context for safety and care planning
    CASE
        WHEN obs.cluster_id = 'PREG_COD' THEN 'Active Pregnancy (medication safety critical)'
        WHEN obs.cluster_id = 'PREGDEL_COD' THEN 'Post-pregnancy care period'
        ELSE 'Maternity care context'
    END AS clinical_safety_context,

    -- Enhanced time calculations for maternity care
    DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) AS days_since_event,
    ROUND(DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) / 7, 1) AS weeks_since_event,

    -- Maternity care timeframes
    CASE
        WHEN obs.cluster_id = 'PREG_COD' AND DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 280 THEN TRUE
        ELSE FALSE
    END AS within_pregnancy_timeframe,

    CASE
        WHEN obs.cluster_id = 'PREGDEL_COD' AND DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS within_post_pregnancy_year,

    -- Clinical interpretation for care planning
    CASE
        WHEN obs.cluster_id = 'PREG_COD' AND DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 280
        THEN 'Current/Recent Pregnancy'
        WHEN obs.cluster_id = 'PREGDEL_COD' AND DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365
        THEN 'Recent Pregnancy Outcome (within 1 year)'
        WHEN obs.cluster_id = 'PREGDEL_COD' AND DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 1825
        THEN 'Previous Pregnancy Outcome (within 5 years)'
        ELSE 'Historical Pregnancy/Outcome'
    END AS maternity_care_status_interpretation

FROM ({{ get_observations("'PREG_COD', 'PREGDEL_COD'") }}) obs
LEFT JOIN {{ ref('dim_person_active_patients') }} ap
    ON obs.person_id = ap.person_id
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC
