{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Intermediate: CKD Lab Classification
This table classifies each person’s most recent eGFR and ACR lab results according to CKD thresholds and staging criteria. It supports case finding and audit by identifying people whose labs are consistent with CKD, regardless of clinical diagnosis.
- Uses int_egfr_all, int_egfr_latest, and (if present) int_acr_all/int_acr_latest as sources.
- Applies CKD clinical rules: G-stage (G1-G5), A-stage (A1-A3), persistent abnormal results (>90 days apart), and combined inferred stage.
- Mirrors and improves on the legacy INTERMEDIATE_CKD_LAB_INFERENCE logic.
*/

WITH latest_egfr AS (
    SELECT
        person_id,
        egfr_value AS latest_egfr_value,
        clinical_effective_date AS latest_egfr_date,
        ckd_stage AS latest_egfr_stage
    FROM {{ ref('int_egfr_latest') }}
),
latest_acr AS (
    -- Replace with actual ACR latest logic/model if available
    SELECT
        person_id,
        acr_value AS latest_acr_value,
        clinical_effective_date AS latest_acr_date,
        acr_stage AS latest_acr_stage
    FROM {{ ref('int_acr_latest') }}
),
egfr_all AS (
    SELECT person_id, clinical_effective_date, egfr_value
    FROM {{ ref('int_egfr_all') }}
    WHERE egfr_value IS NOT NULL
),
acr_all AS (
    SELECT person_id, clinical_effective_date, acr_value
    FROM {{ ref('int_acr_all') }}
    WHERE acr_value IS NOT NULL
),
low_egfr_events AS (
    SELECT *,
        LAG(clinical_effective_date) OVER (PARTITION BY person_id ORDER BY clinical_effective_date) AS prev_low_egfr_date
    FROM egfr_all
    WHERE egfr_value < 60
),
high_acr_events AS (
    SELECT *,
        LAG(clinical_effective_date) OVER (PARTITION BY person_id ORDER BY clinical_effective_date) AS prev_high_acr_date
    FROM acr_all
    WHERE acr_value >= 3
),
egfr_confirmation AS (
    SELECT person_id,
        MAX(CASE WHEN prev_low_egfr_date IS NOT NULL AND DATEDIFF('day', prev_low_egfr_date, clinical_effective_date) >= 90 AND DATEDIFF('day', prev_low_egfr_date, clinical_effective_date) <= 730 THEN 1 ELSE 0 END) AS has_confirmed_low_egfr
    FROM low_egfr_events
    GROUP BY person_id
),
acr_confirmation AS (
    SELECT person_id,
        MAX(CASE WHEN prev_high_acr_date IS NOT NULL AND DATEDIFF('day', prev_high_acr_date, clinical_effective_date) >= 90 AND DATEDIFF('day', prev_high_acr_date, clinical_effective_date) <= 730 THEN 1 ELSE 0 END) AS has_confirmed_high_acr
    FROM high_acr_events
    GROUP BY person_id
),
ckd_lab_classification AS (
    SELECT
        COALESCE(e.person_id, a.person_id) AS person_id,
        le.latest_egfr_value,
        le.latest_egfr_date,
        le.latest_egfr_stage,
        la.latest_acr_value,
        la.latest_acr_date,
        la.latest_acr_stage,
        -- Combined inferred stage
        CASE
            WHEN le.latest_egfr_stage IS NOT NULL AND la.latest_acr_stage IS NOT NULL THEN le.latest_egfr_stage || ' ' || la.latest_acr_stage
            WHEN le.latest_egfr_stage IS NOT NULL THEN le.latest_egfr_stage
            WHEN la.latest_acr_stage IS NOT NULL THEN la.latest_acr_stage
            ELSE NULL
        END AS latest_ckd_stage_inferred,
        -- Latest labs meet CKD criteria
        CASE
            WHEN le.latest_egfr_value < 60 THEN TRUE
            WHEN le.latest_egfr_value >= 60 AND la.latest_acr_value >= 3 THEN TRUE
            ELSE FALSE
        END AS latest_labs_meet_ckd_criteria,
        -- Confirmation flags
        COALESCE(ec.has_confirmed_low_egfr, 0)::BOOLEAN AS has_confirmed_low_egfr,
        COALESCE(ac.has_confirmed_high_acr, 0)::BOOLEAN AS has_confirmed_high_acr,
        (COALESCE(ec.has_confirmed_low_egfr, 0) = 1 OR COALESCE(ac.has_confirmed_high_acr, 0) = 1) AS has_confirmed_ckd_by_labs
    FROM latest_egfr le
    FULL OUTER JOIN latest_acr la ON le.person_id = la.person_id
    LEFT JOIN egfr_confirmation ec ON le.person_id = ec.person_id
    LEFT JOIN acr_confirmation ac ON la.person_id = ac.person_id
    LEFT JOIN egfr_all e ON le.person_id = e.person_id
    LEFT JOIN acr_all a ON la.person_id = a.person_id
)
SELECT * FROM ckd_lab_classification;
