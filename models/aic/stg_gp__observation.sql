{{ config(materialized='view') }}

with
    observation_base as (select * from {{ ref("base_olids__observation") }}),

    concept_map as (select * from {{ ref("stg_gp__concept_map") }}),

    concept as (select * from {{ ref("stg_gp__concept") }}),

    observation_w_concept as (
        select
            o.*,
            c.concept_code as observation_concept_code,
            c.concept_name as observation_concept_name,
            c.concept_vocabulary as observation_concept_vocabulary
        from observation_base o
        left join concept_map cm
            on o.observation_source_concept_id = cm.source_db_concept_id
        left join concept c
            on cm.target_db_concept_id = c.db_concept_id
    ),

    observation_w_unit_concept as (
        select
            o.*,
            uc.concept_code as unit_concept_code,
            uc.concept_name as unit_concept_name,
            uc.concept_vocabulary as unit_concept_vocabulary
        from observation_w_concept o
        left join concept uc
            on o.result_value_unit_concept_id = uc.db_concept_id
    )

select
    md5(cast(id as varchar)) as gp_observation_id,
    id as observation_id,
    person_id,
    patient_id,
    practitioner_id,
    encounter_id,
    record_owner_organisation_code as organisation_id,
    age_at_event,
    clinical_effective_date,
    problem_end_date as clinical_end_date,
    observation_source_concept_id as observation_concept_id,
    observation_concept_code,
    observation_concept_name,
    observation_concept_vocabulary,
    result_value,
    unit_concept_code as result_value_unit,
    result_text,
    null as observation_type  -- intended to hold snomed tag
from observation_w_unit_concept