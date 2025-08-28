{{ config(materialized='table') }}

with
    appointment_base as (
        select * from {{ ref("base_sus__op_appointment") }}
        where appointment_identifier is not null
    ),

    patient_mapping as (
        select * from {{ ref("stg_gp__patient_pseudo_id") }}
    ),

    appointment_w_person as (
        select
            a.*,
            pm.master_person_id as mapped_person_id
        from appointment_base a
        left join patient_mapping pm
            on a.appointment_patient_identity_nhs_number_value_pseudo = pm.id_value
            and pm.id_type = 'sk_patient_id'
    )

select
    {{ dbt_utils.generate_surrogate_key(['primarykey_id', 'appointment_identifier']) }} as op_appointment_id,
    appointment_identifier,
    mapped_person_id as person_id,
    appointment_patient_identity_nhs_number_value_pseudo as sk_patient_id,
    appointment_date,
    appointment_time,
    appointment_first_attendance,
    appointment_attended_or_dna as attended_or_dna_code,
    case appointment_attended_or_dna
        when '5' then 'Attended on time or, if late, before the relevant CARE PROFESSIONAL was ready to see the PATIENT'
        when '6' then 'Arrived late, after the relevant CARE PROFESSIONAL was ready to see the PATIENT, but was seen'
        when '7' then 'PATIENT arrived late and could not be seen'
        when '2' then 'APPOINTMENT cancelled by, or on behalf of, the PATIENT'
        when '3' then 'Did not attend - no advance warning given'
        when '4' then 'APPOINTMENT cancelled or postponed by the Health Care Provider'
        when '0' then 'Not applicable - APPOINTMENT occurs in the future'
        else appointment_attended_or_dna
    end as attended_or_dna_description,
    appointment_outcome,
    appointment_care_professional_main_specialty as main_specialty_code,
    {{ get_main_specialty_description('appointment_care_professional_main_specialty') }} as main_specialty_description,
    appointment_care_professional_treatment_function as treatment_function,
    appointment_commissioning_service_agreement_provider as provider_code,
    appointment_patient_identity_age_at_cds_activity_date as age_at_appointment,
    appointment_patient_identity_gender as gender,
    appointment_patient_identity_ethnic_category as ethnic_category,
    system_transaction_cds_activity_date as activity_date
from appointment_w_person