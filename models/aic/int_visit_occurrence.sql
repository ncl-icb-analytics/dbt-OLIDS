{{ config(materialized='view') }}

-- excludes DNAs: i.e. patient turned up

with
    gp_visits as (
        select
            gp_encounter_id as visit_id,
            person_id,
            clinical_effective_date as visit_start_date,
            clinical_effective_date as visit_end_date,
            null as length_of_stay,
            'GP' as visit_type,
            null as visit_description_code,
            null as visit_description,
            organisation_id,
            'stg_gp__encounter' as source_table
        from {{ ref("stg_gp__encounter") }}
    ),

    apc_visits as (
        select
            apc_spell_id as visit_id,
            person_id,
            admission_date as visit_start_date,
            discharge_date as visit_end_date,
            admission_length as length_of_stay,
            'APC' as visit_type,
            main_specialty_code as visit_description_code,
            main_specialty_description as visit_description,
            provider_code as organisation_id,
            'stg_sus__apc_spell' as source_table
        from {{ ref("stg_sus__apc_spell") }}
        where person_id is not null
    ),

    ecds_visits as (
        select
            ecds_attendance_id as visit_id,
            person_id,
            arrival_date as visit_start_date,
            departure_date as visit_end_date,
            null as length_of_stay,
            'ECDS' as visit_type,
            acuity_code as visit_description_code,
            acuity_description as visit_description,
            provider_code as organisation_id,
            'stg_sus__ecds_attendance' as source_table
        from {{ ref("stg_sus__ecds_attendance") }}
        where person_id is not null
    ),

    op_visits as (
        select
            op_appointment_id as visit_id,
            person_id,
            appointment_date as visit_start_date,
            appointment_date as visit_end_date,
            null as length_of_stay,
            'OP' as visit_type,
            main_specialty_code as visit_description_code,
            main_specialty_description as visit_description,
            provider_code as organisation_id,
            'stg_sus__op_appointment' as source_table
        from {{ ref("stg_sus__op_appointment") }}
        where person_id is not null
            and attended_or_dna_code in ('5', '6', '7')
    ),

    all_visits as (
        select * from gp_visits
        union all
        select * from apc_visits
        union all
        select * from ecds_visits
        union all
        select * from op_visits
    )

select
    {{ dbt_utils.generate_surrogate_key(['visit_id', 'source_table']) }} as visit_occurrence_id,
    person_id,
    visit_start_date,
    visit_end_date,
    length_of_stay,
    visit_type,
    visit_description_code,
    visit_description,
    organisation_id,
    source_table
from all_visits