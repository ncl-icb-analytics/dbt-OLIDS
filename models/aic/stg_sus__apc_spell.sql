{{ config(materialized='table') }}

with
    spell_base as (
        select * from {{ ref("base_sus__apc_spell_episode") }}
        where spell_hospital_provider_spell_number is not null
    ),

    patient_mapping as (
        select * from {{ ref("stg_gp__patient_pseudo_id") }}
    ),

    spell_w_person as (
        select
            s.*,
            pm.master_person_id as mapped_person_id
        from spell_base s
        left join patient_mapping pm
            on s.patient_identity_nhs_number_value_pseudo = pm.id_value
            and pm.id_type = 'sk_patient_id'
    ),

    spell_grouped as (
        select
            spell_hospital_provider_spell_number,
            any_value(mapped_person_id) as person_id,
            any_value(patient_identity_nhs_number_value_pseudo) as sk_patient_id,
            min(admission_date) as admission_date,
            any_value(admission_admission_sub_type) as admission_type,
            max(discharge_date) as discharge_date,
            any_value(care_professional_main_specialty) as main_specialty,
            any_value(care_professional_treatment_function) as treatment_function,
            any_value(commissioning_service_agreement_provider) as provider_code,
            any_value(patient_identity_age_on_admission) as age_on_admission,
            any_value(patient_identity_gender) as gender,
            any_value(patient_identity_ethnic_category) as ethnic_category,
            any_value(system_transaction_cds_activity_date) as activity_date
        from spell_w_person
        group by spell_hospital_provider_spell_number
    )

select
    {{ dbt_utils.generate_surrogate_key(['spell_hospital_provider_spell_number']) }} as apc_spell_id,
    spell_hospital_provider_spell_number as spell_number,
    person_id,
    sk_patient_id,
    admission_date,
    admission_type,
    discharge_date,
    datediff('day', admission_date, discharge_date) as admission_length,
    main_specialty as main_specialty_code,
    {{ get_main_specialty_description('main_specialty') }} as main_specialty_description,
    treatment_function,
    provider_code,
    age_on_admission,
    gender,
    ethnic_category,
    activity_date
from spell_grouped