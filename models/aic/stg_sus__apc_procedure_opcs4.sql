{{ config(materialized="table") }}
with
    -- deduplicate patient mapping
    patients as (
        select distinct sk_patient_id, person_id from {{ ref("stg_gp__patient_pseudo_id") }}
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            ["f.primarykey_id", "f.episodes_id", "f.rownumber_id", "f.opcs_id"]
        )
    }} as apc_procedure_id,
    f.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    f.episodes_id,
    opcs_id,
    f.rownumber_id,
    se.patient_identity_nhs_number_value_pseudo as sk_patient_id,
    pp.person_id,
    se.commissioning_service_agreement_provider_derived as organisation_id,
    null as organisation_name,  -- join to reference
    se.commissioning_service_agreement_provider as sub_organisation_id,
    null as sub_organisation_name,  -- join to reference
    system_transaction_cds_activity_date as activity_date,
    f.code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'OPCS4' as concept_vocabulary
from {{ ref("base_sus__apc_procedure_opcs4") }} f
left join
    {{ ref("base_athena__concept") }} c
    on replace(c.concept_code, '.', '') = f.code
    and c.vocabulary_id = 'OPCS4'
left join {{ ref("base_sus__apc_spell_episode") }} se on se.primarykey_id = f.primarykey_id
left join patients pp on pp.sk_patient_id = se.patient_identity_nhs_number_value_pseudo
