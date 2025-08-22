{{ config(materialized="table") }}
with
    -- remove dot from ICD codes
    dotless_icd_codes as (
        select primarykey_id, icd_id, rownumber_id, replace(code, '.', '') as dotless_code, code
        from {{ ref("base_sus__op_diagnosis_icd10") }}
    ),

    -- standardize ICD codes by removing trailing characters
    -- e.g. A/D or `-`/`*` etc which are added too inconsistently to be of value
    standardised_icd_codes as (
        select
            primarykey_id,
            icd_id,
            rownumber_id,
            trim(regexp_replace(dotless_code, '[-][A-Z]$|[X-]+$')) as concept_code_cleaned,
            code
        from dotless_icd_codes
    ),
    -- standardize the ICD codes to ensure they follow the expected format
    -- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
    final_icd_codes as (
        select
            primarykey_id,
            icd_id,
            rownumber_id,
            case
                when len(concept_code_cleaned) > 3
                then left(concept_code_cleaned, 3) || '.' || substr(concept_code_cleaned, 4, 1)
                else concept_code_cleaned
            end as concept_code,
            code
        from standardised_icd_codes
    ),
    -- deduplicate patient mapping
    patients as (
        select distinct sk_patient_id, person_id from {{ ref("stg_gp__patient_pseudo_id") }}
    )
select
    {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.rownumber_id", "f.icd_id"]) }}
    as op_diagnosis_id,
    f.primarykey_id as visit_occurrence_id,
    'OP_ATTENDANCE' as visit_occurrence_type,
    null::number as episodes_id,
    f.icd_id,
    f.rownumber_id,
    sa.appointment_patient_identity_nhs_number_value_pseudo as sk_patient_id,
    pp.person_id,
    sa.appointment_commissioning_service_agreement_provider_derived as organisation_id,
    null as organisation_name,  -- join to reference
    sa.appointment_commissioning_service_agreement_provider as sub_organisation_id,
    null as sub_organisation_name,  -- join to reference
    appointment_date as activity_date,
    f.concept_code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'ICD10' as concept_vocabulary
from final_icd_codes f
left join
    {{ ref("base_athena__concept") }} c
    on c.concept_code = f.concept_code
    and c.vocabulary_id = 'ICD10'
left join {{ ref("base_sus__op_appointment") }} sa on sa.primarykey_id = f.primarykey_id
left join patients pp on pp.sk_patient_id = sa.appointment_patient_identity_nhs_number_value_pseudo
