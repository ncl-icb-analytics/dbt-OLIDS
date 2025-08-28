{{ config(materialized='view') }}

-- note: using sk_patient_id as person_id

with
    -- source data has duplicates!
    -- DISTINCT requires full aggregation (too expensive)
    encounter_base as (
        select * from {{ ref("base_olids__encounter") }}
        qualify row_number() over (partition by id order by lds_datetime_data_acquired desc) = 1
    ),

    patient_mapping as (select * from {{ ref("stg_gp__patient_pseudo_id") }}),

    concept_map as (select * from {{ ref("stg_gp__concept_map") }} where target_concept_vocabulary = 'SNOMED'),

    encounter_w_person as (
        select
            e.*,
            pm.master_person_id as mapped_person_id
        from encounter_base e
        left join patient_mapping pm
            on e.patient_id = pm.id_value
            and pm.id_type = 'patient_id'
    ),

    encounter_w_concept as (
        select
            e.*,
            cm.target_concept_code as encounter_concept_code,
            cm.target_concept_name as encounter_concept_name,
            cm.target_concept_vocabulary as encounter_concept_vocabulary
        from encounter_w_person e
        left join concept_map cm
            on e.encounter_core_concept_id = cm.source_db_concept_id
    )

select
    id as gp_encounter_id,
    mapped_person_id as person_id,
    patient_id,
    practitioner_id,
    appointment_id,
    record_owner_organisation_code as organisation_id,
    clinical_effective_date,
    age_at_event,
    encounter_core_concept_id,
    encounter_concept_code,
    encounter_concept_name,
    encounter_concept_vocabulary,
    end_date,
    location
from encounter_w_concept