{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: LTC LCS Case Finding AF_61 - Identifies patients on cardiac medications who may have undiagnosed atrial fibrillation.

Population Scope:
• Patients from LTC case finding base population on anticoagulants, digoxin, or cardiac glycosides
• Excludes patients with DVT, existing AF/flutter diagnoses

Key Features:
• Medication history tracking with latest prescription dates
• Clinical exclusion criteria assessment
• Evidence-based case finding for AF screening'"
    ]
) }}

-- AF_61 case finding dimension: Patients on specific cardiac medications
-- Identifies patients on medications that may indicate undiagnosed atrial fibrillation

WITH af_meds AS (
    SELECT
        person_id,
        MAX(
            CASE WHEN cluster_id = 'ORAL_ANTICOAGULANT_2_8_2' THEN 1 ELSE 0 END
        ) AS has_active_anticoagulant,
        MAX(CASE WHEN cluster_id = 'DIGOXIN_MEDICATIONS' THEN 1 ELSE 0 END)
            AS has_active_digoxin,
        MAX(CASE WHEN cluster_id = 'CARDIAC_GLYCOSIDES' THEN 1 ELSE 0 END)
            AS has_active_cardiac_glycoside,
        MAX(order_date) AS latest_af_medication_date,
        ARRAY_AGG(DISTINCT mapped_concept_code) AS all_af_medication_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) AS all_af_medication_displays
    FROM {{ ref('int_ltc_lcs_af_medications') }}
    WHERE
        cluster_id IN (
            'ORAL_ANTICOAGULANT_2_8_2',
            'DIGOXIN_MEDICATIONS',
            'CARDIAC_GLYCOSIDES'
        )
    GROUP BY person_id
),

af_exclusions AS (
    SELECT
        person_id,
        BOOLOR_AGG(
            cluster_id IN (
                'DEEP_VEIN_THROMBOSIS',
                'ATRIAL_FLUTTER',
                'ATRIAL_FIBRILLATION_61_EXCLUSIONS'
            )
        ) AS has_exclusion_condition,
        LISTAGG(DISTINCT cluster_id, ', ') AS exclusion_reason
    FROM {{ ref('int_ltc_lcs_af_observations') }}
    WHERE
        cluster_id IN (
            'DEEP_VEIN_THROMBOSIS',
            'ATRIAL_FLUTTER',
            'ATRIAL_FIBRILLATION_61_EXCLUSIONS'
        )
    GROUP BY person_id
)

SELECT DISTINCT
    bp.person_id,
    m.latest_af_medication_date,
    NULL AS latest_health_check_date,
    e.exclusion_reason,
    m.all_af_medication_codes,
    m.all_af_medication_displays, -- To be replaced if health check int is created
    COALESCE(m.has_active_anticoagulant, 0) AS has_active_anticoagulant,
    COALESCE(m.has_active_digoxin, 0) AS has_active_digoxin,
    COALESCE(m.has_active_cardiac_glycoside, 0) AS has_active_cardiac_glycoside,
    COALESCE(e.has_exclusion_condition, FALSE) AS has_exclusion_condition
FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
LEFT JOIN af_meds AS m
    ON bp.person_id = m.person_id
LEFT JOIN af_exclusions AS e
    ON bp.person_id = e.person_id
