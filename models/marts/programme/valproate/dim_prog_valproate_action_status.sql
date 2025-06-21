{{ config(
    materialized='table',
    description='Implements clinical decision logic for Valproate safety monitoring, determining recommended actions for each patient based on clinical status and dependencies.'
) }}

with db_scope as (
    select * from {{ ref('dim_prog_valproate_db_scope') }}
),
ppp_status as (
    select * from {{ ref('dim_prog_valproate_ppp_status') }}
),
araf as (
    select * from {{ ref('dim_prog_valproate_araf') }}
),
araf_referral as (
    select * from {{ ref('dim_prog_valproate_araf_referral') }}
),
neurology as (
    select * from {{ ref('dim_prog_valproate_neurology') }}
),
psychiatry as (
    select * from {{ ref('dim_prog_valproate_psychiatry') }}
),
preg as (
    select person_id, is_currently_pregnant from {{ ref('fct_person_pregnancy_status') }}
)

select
    db.person_id,
    db.age,
    db.sex,
    db.is_child_bearing_age_0_55,
    db.valproate_medication_order_id is not null as has_recent_valproate_medication,
    preg.is_currently_pregnant as is_pregnant,
    -- PPP
    ppp.has_ppp_event,
    ppp.is_currently_ppp_enrolled,
    ppp.current_ppp_status_description,
    -- ARAF
    araf.has_araf_event,
    araf.has_specific_araf_form_meeting_lookback,
    -- ARAF Referral
    arref.has_araf_referral_event,
    -- Neurology
    neu.has_neurology_event,
    -- Psychiatry
    psych.has_psych_event,
    -- Action logic (simplified for demonstration)
    case
        when is_pregnant then 'Review or refer: Pregnancy detected'
        when not has_recent_valproate_medication then 'No action: Not on valproate'
        when not db.is_child_bearing_age_0_55 then 'No action: Not woman of child-bearing age'
        when not ppp.is_currently_ppp_enrolled then 'Review: Not enrolled in PPP'
        when not araf.has_specific_araf_form_meeting_lookback then 'Review: ARAF not completed in lookback'
        when arref.has_araf_referral_event then 'Monitor: Referral made'
        when neu.has_neurology_event or psych.has_psych_event then 'Monitor: Under specialist care'
        else 'No action needed'
    end as recommended_action
from db_scope db
left join ppp_status ppp on db.person_id = ppp.person_id
left join araf araf on db.person_id = araf.person_id
left join araf_referral arref on db.person_id = arref.person_id
left join neurology neu on db.person_id = neu.person_id
left join psychiatry psych on db.person_id = psych.person_id
left join preg on db.person_id = preg.person_id
-- Brief: Implements clinical action logic for Valproate safety monitoring, using all dependency marts. Adjust logic as needed for full business rules.
