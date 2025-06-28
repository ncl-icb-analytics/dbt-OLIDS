{{ config(
    materialized='table',
    description='Implements clinical decision logic for Valproate safety monitoring, determining recommended actions for each patient based on clinical status and dependencies.'
) }}

WITH db_scope AS (
    SELECT * FROM {{ ref('dim_valproate_db_scope') }}
),

ppp_status AS (
    SELECT * FROM {{ ref('dim_valproate_ppp_status') }}
),

araf AS (
    SELECT * FROM {{ ref('dim_valproate_araf') }}
),

araf_referral AS (
    SELECT * FROM {{ ref('dim_valproate_araf_referral') }}
),

neurology AS (
    SELECT * FROM {{ ref('dim_valproate_neurology') }}
),

psychiatry AS (
    SELECT * FROM {{ ref('dim_valproate_psychiatry') }}
),

preg AS (
    SELECT
        person_id,
        is_currently_pregnant
    FROM {{ ref('fct_person_pregnancy_status') }}
)

SELECT
    db.person_id,
    db.age,
    db.sex,
    db.is_child_bearing_age_0_55,
    preg.is_currently_pregnant AS is_pregnant,
    ppp.has_ppp_event,
    -- PPP
    ppp.is_currently_ppp_enrolled,
    ppp.current_ppp_status_description,
    araf.has_araf_event,
    -- ARAF
    araf.has_specific_araf_form_meeting_lookback,
    arref.has_araf_referral_event,
    -- ARAF Referral
    neu.has_neurology_event,
    -- Neurology
    psych.has_psych_event,
    -- Psychiatry
    db.valproate_medication_order_id IS NOT NULL
        AS has_recent_valproate_medication,
    -- Action logic (simplified for demonstration)
    CASE
        WHEN is_pregnant THEN 'Review or refer: Pregnancy detected'
        WHEN
            NOT has_recent_valproate_medication
            THEN 'No action: Not on valproate'
        WHEN
            NOT db.is_child_bearing_age_0_55
            THEN 'No action: Not woman of child-bearing age'
        WHEN
            NOT ppp.is_currently_ppp_enrolled
            THEN 'Review: Not enrolled in PPP'
        WHEN
            NOT araf.has_specific_araf_form_meeting_lookback
            THEN 'Review: ARAF not completed in lookback'
        WHEN arref.has_araf_referral_event THEN 'Monitor: Referral made'
        WHEN
            neu.has_neurology_event OR psych.has_psych_event
            THEN 'Monitor: Under specialist care'
        ELSE 'No action needed'
    END AS recommended_action
FROM db_scope AS db
LEFT JOIN ppp_status AS ppp ON db.person_id = ppp.person_id
LEFT JOIN araf AS araf ON db.person_id = araf.person_id
LEFT JOIN araf_referral AS arref ON db.person_id = arref.person_id
LEFT JOIN neurology AS neu ON db.person_id = neu.person_id
LEFT JOIN psychiatry AS psych ON db.person_id = psych.person_id
LEFT JOIN preg ON db.person_id = preg.person_id
-- Brief: Implements clinical action logic for Valproate safety monitoring, using all dependency marts. Adjust logic as needed for full business rules.
