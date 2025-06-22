{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'AF_62 case finding: Patients over 65 with missing pulse check at NHS Health Check'"
) }}

-- Intermediate model for LTC LCS Case Finding AF_62: Patients over 65 missing pulse check in last 36 months
-- Uses modular approach: leverages base population, observations intermediate, and exclusions

with base_population as (
    select
        bp.person_id,
        age
    from {{ ref('int_ltc_lcs_cf_base_population') }} bp
    join {{ ref('dim_person_age') }} age
      on bp.person_id = age.person_id
    where age.age >= 65
),
pulse_checks as (
    select
        person_id,
        clinical_effective_date,
        mapped_concept_code,
        mapped_concept_display
    from {{ ref('int_ltc_lcs_af_observations') }}
    where cluster_id in ('PULSE_RATE', 'PULSE_RHYTHM')
      and clinical_effective_date >= dateadd(month, -36, current_date())
),
pulse_check_summary as (
    select
        person_id,
        max(clinical_effective_date) as latest_pulse_check_date,
        boolor_agg(true) as has_pulse_check,
        array_agg(distinct mapped_concept_code) as all_pulse_check_codes,
        array_agg(distinct mapped_concept_display) as all_pulse_check_displays
    from pulse_checks
    group by person_id
),
health_checks as (
    select
        person_id,
        max(clinical_effective_date) as latest_health_check_date
    from {{ ref('int_nhs_health_check_latest') }}
    group by person_id
),
exclusions as (
    select
        person_id,
        has_excluding_condition
    from {{ ref('int_ltc_lcs_cf_exclusions') }}
)

select
    bp.person_id,
    bp.age,
    coalesce(pcs.has_pulse_check, false) as has_pulse_check,
    pcs.latest_pulse_check_date,
    hc.latest_health_check_date,
    ex.has_excluding_condition,
    pcs.all_pulse_check_codes,
    pcs.all_pulse_check_displays
from base_population bp
left join pulse_check_summary pcs on bp.person_id = pcs.person_id
left join health_checks hc on bp.person_id = hc.person_id
left join exclusions ex on bp.person_id = ex.person_id
