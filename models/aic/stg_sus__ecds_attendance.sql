{{ config(materialized='table') }}

with
    attendance_base as (
        select * from {{ ref("base_sus__ecds_attendance") }}
        where attendance_arrival_attendance_identifier is not null
    ),

    patient_mapping as (
        select * from {{ ref("stg_gp__patient_pseudo_id") }}
    ),

    snomed_concepts as (
        select distinct
            concept_code,
            concept_name
        from {{ ref("stg_gp__concept") }}
        where concept_vocabulary = 'SNOMED'
    ),

    attendance_w_person as (
        select
            a.*,
            pm.master_person_id as mapped_person_id
        from attendance_base a
        left join patient_mapping pm
            on a.patient_nhs_number_value_pseudo = pm.id_value
            and pm.id_type = 'sk_patient_id'
    )

select
    {{ dbt_utils.generate_surrogate_key(['primarykey_id', 'attendance_arrival_attendance_identifier']) }} as ecds_attendance_id,
    attendance_arrival_attendance_identifier as attendance_identifier,
    mapped_person_id as person_id,
    patient_nhs_number_value_pseudo as sk_patient_id,
    attendance_arrival_date as arrival_date,
    a.attendance_arrival_arrival_mode_code as arrival_mode_code,
    sc_arrival.concept_name as arrival_mode_description,
    attendance_departure_date as departure_date,
    a.attendance_discharge_destination_code as discharge_destination_code,
    sc_discharge.concept_name as discharge_destination_description,
    a.clinical_acuity_code as acuity_code,
    sc_acuity.concept_name as acuity_description,
    attendance_location_department_type as department_type,
    commissioning_service_agreement_provider as provider_code,
    patient_age_at_arrival as age_at_arrival,
    patient_stated_gender as gender,
    patient_ethnic_category as ethnic_category,
    system_record_cds_activity_date as activity_date
from attendance_w_person a
left join snomed_concepts sc_acuity
    on a.clinical_acuity_code = sc_acuity.concept_code
left join snomed_concepts sc_arrival
    on a.attendance_arrival_arrival_mode_code = sc_arrival.concept_code
left join snomed_concepts sc_discharge
    on a.attendance_discharge_destination_code = sc_discharge.concept_code