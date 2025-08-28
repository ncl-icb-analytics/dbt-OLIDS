{{ config(materialized='view') }}

-- note: using sk_patient_id as person_id

with
    -- Filter out measurement definitions from DEFINITIONSTORE
    definitionstore_filtered as (
        select *
        from {{ source("phenolab_dev", "DEFINITIONSTORE") }}
        where not lower(definition_name) like 'measurement_%'
    ),

    gp_observations as (
        select
            gp.gp_observation_id as master_observation_id,
            gp.person_id,
            gp.encounter_id::varchar as visit_occurrence_id,
            'GP_ENCOUNTER' as visit_occurrence_type,
            gp.organisation_id::varchar as organisation_id,
            null as organisation_name,  -- join to reference
            'PRIMARY_CARE' as organisation_type,
            gp.age_at_event,
            gp.clinical_effective_date,
            gp.clinical_end_date,
            null as problem_order,
            -- gp.observation_concept_id,
            -- 'Discovery' as observation_concept_id_type,
            gp.observation_concept_code,
            -- gp.observation_concept_name,
            -- gp.observation_concept_vocabulary,
            'OLIDS_GP' as observation_type,  -- intended to hold snomed tag
            ds.definition_id,
            ds.definition_name,
            ds.definition_source
        from {{ ref("stg_gp__observation") }} gp
        inner join
            definitionstore_filtered ds
            on gp.observation_concept_code = ds.code
            and gp.observation_concept_vocabulary = ds.vocabulary
    ),

    apc_diagnosis as (
        select
            apc.apc_diagnosis_id as master_observation_id,
            apc.person_id,
            apc.visit_occurrence_id::varchar as visit_occurrence_id,
            apc.visit_occurrence_type,
            apc.organisation_id::varchar as organisation_id,
            apc.organisation_name,
            'NON_PRIMARY_CARE' as organisation_type,
            null as age_at_event,  -- TODO: calculate from patient birth date
            apc.activity_date as clinical_effective_date,
            null as clinical_end_date,
            apc.icd_id as problem_order,
            -- null as observation_concept_id,
            -- null as observation_concept_id_type,
            apc.concept_code as observation_concept_code,
            -- apc.concept_name as observation_concept_name,
            -- apc.concept_vocabulary as observation_concept_vocabulary,
            'SUS_APC_DIAGNOSIS' as observation_type,
            ds.definition_id,
            ds.definition_name,
            ds.definition_source
        from {{ ref("stg_sus__apc_diagnosis_icd10") }} apc
        inner join
            definitionstore_filtered ds
            on apc.concept_code = ds.code
            and apc.concept_vocabulary = ds.vocabulary
    ),

    apc_procedure as (
        select
            apc.apc_procedure_id as master_observation_id,
            apc.person_id,
            apc.visit_occurrence_id::varchar as visit_occurrence_id,
            apc.visit_occurrence_type,
            apc.organisation_id::varchar as organisation_id,
            apc.organisation_name,
            'NON_PRIMARY_CARE' as organisation_type,
            null as age_at_event,  -- TODO: calculate from patient birth date
            apc.activity_date as clinical_effective_date,
            null as clinical_end_date,
            apc.opcs_id as problem_order,
            -- null as observation_concept_id,
            -- null as observation_concept_id_type,
            apc.concept_code as observation_concept_code,
            -- apc.concept_name as observation_concept_name,
            -- apc.concept_vocabulary as observation_concept_vocabulary,
            'SUS_APC_PROCEDURE' as observation_type,
            ds.definition_id,
            ds.definition_name,
            ds.definition_source
        from {{ ref("stg_sus__apc_procedure_opcs4") }} apc
        inner join
            definitionstore_filtered ds
            on apc.concept_code = ds.code
            and apc.concept_vocabulary = ds.vocabulary
    ),

    op_diagnosis as (
        select
            op.op_diagnosis_id as master_observation_id,
            op.person_id,
            op.visit_occurrence_id::varchar as visit_occurrence_id,
            op.visit_occurrence_type,
            op.organisation_id::varchar as organisation_id,
            op.organisation_name,
            'NON_PRIMARY_CARE' as organisation_type,
            null as age_at_event,  -- TODO: calculate from patient birth date
            op.activity_date as clinical_effective_date,
            null as clinical_end_date,
            op.icd_id as problem_order,
            -- null as observation_concept_id,
            -- null as observation_concept_id_type,
            op.concept_code as observation_concept_code,
            -- op.concept_name as observation_concept_name,
            -- op.concept_vocabulary as observation_concept_vocabulary,
            'SUS_OP_DIAGNOSIS' as observation_type,
            ds.definition_id,
            ds.definition_name,
            ds.definition_source
        from {{ ref("stg_sus__op_diagnosis_icd10") }} op
        inner join
            definitionstore_filtered ds
            on op.concept_code = ds.code
            and op.concept_vocabulary = ds.vocabulary
    ),

    op_procedure as (
        select
            op.op_procedure_id as master_observation_id,
            op.person_id,
            op.visit_occurrence_id::varchar as visit_occurrence_id,
            op.visit_occurrence_type,
            op.organisation_id::varchar as organisation_id,
            op.organisation_name,
            'NON_PRIMARY_CARE' as organisation_type,
            null as age_at_event,  -- TODO: calculate from patient birth date
            op.activity_date as clinical_effective_date,
            null as clinical_end_date,
            op.opcs_id as problem_order,
            -- null as observation_concept_id,
            -- null as observation_concept_id_type,
            op.concept_code as observation_concept_code,
            -- op.concept_name as observation_concept_name,
            -- op.concept_vocabulary as observation_concept_vocabulary,
            'SUS_OP_PROCEDURE' as observation_type,
            ds.definition_id,
            ds.definition_name,
            ds.definition_source
        from {{ ref("stg_sus__op_procedure_opcs4") }} op
        inner join
            definitionstore_filtered ds
            on op.concept_code = ds.code
            and op.concept_vocabulary = ds.vocabulary
    ),

    all_observations as (
        select *
        from gp_observations
        union all
        select *
        from apc_diagnosis
        union all
        select *
        from apc_procedure
        union all
        select *
        from op_diagnosis
        union all
        select *
        from op_procedure
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            ["master_observation_id", "observation_concept_code", "definition_id"]
        )
    }} as master_condition_id,
    person_id,
    visit_occurrence_id,
    visit_occurrence_type,
    age_at_event,
    clinical_effective_date,
    clinical_end_date,
    problem_order,
    definition_id,
    definition_name as condition_definition_name,
    definition_source,
-- observation_concept_code,
-- observation_concept_name,
-- observation_concept_vocabulary,
    observation_type
from all_observations