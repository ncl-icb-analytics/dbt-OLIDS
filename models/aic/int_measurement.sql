{{ config(materialized='view') }}

-- note: using sk_patient_id as person_id

with
    -- Filter DEFINITIONSTORE using MEASUREMENT_CONFIGS
    definitionstore_filtered as (
        select ds.*
        from {{ source("phenolab_dev", "DEFINITIONSTORE") }} ds
        inner join
            {{ source("phenolab_dev", "MEASUREMENT_CONFIGS") }} mc
            on ds.definition_id = mc.definition_id
    ),

    gp_measurements as (
        select
            gp.gp_observation_id,
            gp.person_id,
            gp.encounter_id::varchar as visit_occurrence_id,
            'GP_ENCOUNTER' as visit_occurrence_type,
            gp.organisation_id::varchar as organisation_id,
            null as organisation_name,
            'PRIMARY_CARE' as organisation_type,
            gp.age_at_event,
            gp.clinical_effective_date,
            -- gp.observation_concept_id,
            -- 'Discovery' as observation_concept_id_type,
            gp.observation_concept_code,
            -- gp.observation_concept_name,
            -- gp.observation_concept_vocabulary,
            'OLIDS_GP' as observation_type,  -- intended to hold snomed tag
            gp.result_value,
            coalesce(gp.result_value_unit, 'No Unit') as source_unit,
            ds.definition_id,
            ds.definition_name,
            ds.definition_source
        from {{ ref("stg_gp__observation") }} gp
        inner join
            definitionstore_filtered ds
            on gp.observation_concept_code = ds.code
            and gp.observation_concept_vocabulary = ds.vocabulary
        where gp.result_value is not null
    ),

    with_unit_mappings as (
        select
            gm.*,
            um.standard_unit,
            case when um.standard_unit is null then true else false end as no_unit_mapping
        from gp_measurements gm
        left join
            {{ source("phenolab_dev", "UNIT_MAPPINGS") }} um
            on gm.definition_id = um.definition_id
            and gm.source_unit = um.source_unit
    ),

    with_conversions as (
        select
            wum.*,
            uc.convert_to_unit as final_unit,
            uc.pre_offset,
            uc.multiply_by,
            uc.post_offset,
            case
                when wum.no_unit_mapping
                then wum.result_value
                when uc.definition_id is not null
                then
                    (wum.result_value + coalesce(uc.pre_offset, 0)) * coalesce(uc.multiply_by, 1)
                    + coalesce(uc.post_offset, 0)
                else wum.result_value
            end as converted_value,
            case
                when wum.no_unit_mapping
                then wum.source_unit
                when uc.convert_to_unit is not null
                then uc.convert_to_unit
                else wum.standard_unit
            end as final_result_unit
        from with_unit_mappings wum
        left join
            {{ source("phenolab_dev", "UNIT_CONVERSIONS") }} uc
            on wum.definition_id = uc.definition_id
            and wum.standard_unit = uc.convert_from_unit
    ),

    with_bounds_checks as (
        select
            wc.*,
            vb.lower_limit,
            vb.upper_limit,
            case
                when vb.lower_limit is not null and wc.converted_value < vb.lower_limit
                then true
                else false
            end as below_bound,
            case
                when vb.upper_limit is not null and wc.converted_value > vb.upper_limit
                then true
                else false
            end as above_bound
        from with_conversions wc
        left join
            {{ source("phenolab_dev", "VALUE_BOUNDS") }} vb on wc.definition_id = vb.definition_id
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            ["gp_observation_id", "observation_concept_code", "definition_id"]
        )
    }} as master_measurement_id,
    person_id,
    visit_occurrence_id,
    visit_occurrence_type,
    age_at_event,
    clinical_effective_date,
    definition_id,
    definition_name as measurement_definition_name,
    definition_source,
    converted_value as measurement_value_as_number,
    final_result_unit as measurement_unit,
    result_value as measurement_source_value,
    source_unit as measurement_source_unit,
    -- observation_concept_code,
    -- observation_concept_name,
    -- observation_concept_vocabulary,
    -- observation_type,
    lower_limit,
    upper_limit,
    no_unit_mapping,
    below_bound,
    above_bound
from with_bounds_checks