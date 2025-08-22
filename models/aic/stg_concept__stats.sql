-- This model is required for PhenoLab
-- It contains all SNOMED, ICD10, and OPCS4 codes
-- All codes have calculated usage statistics
{{ config(materialized="table") }}

with
    -- SNOMED
    snomed_stats as (
        select
            observation_concept_code as concept_code,
            observation_concept_vocabulary as concept_vocabulary,
            count(*) as concept_code_count,
            count(distinct person_id) as unique_patient_count,
            approx_percentile(try_cast(result_value as float), 0.25) as lq_value,
            approx_percentile(try_cast(result_value as float), 0.5) as median_value,
            approx_percentile(try_cast(result_value as float), 0.75) as uq_value,
            (count(result_value) * 100.0 / nullif(count(*), 0)) as percent_has_result_value
        from {{ ref("stg_gp__observation") }}
        where observation_concept_vocabulary = 'SNOMED'
        group by observation_concept_code, observation_concept_vocabulary
    ),

    snomed_concepts as (
        select distinct concept_code, concept_name, concept_vocabulary
        from {{ ref("stg_gp__concept") }}
        where concept_vocabulary = 'SNOMED'
    ),

    snomed_full as (
        select
            coalesce(c.concept_name, 'Unknown') as concept_name,
            coalesce(s.concept_code, c.concept_code) as concept_code,
            coalesce(s.concept_vocabulary, c.concept_vocabulary) as concept_vocabulary,
            coalesce(s.concept_code_count, 0) as concept_code_count,
            coalesce(s.unique_patient_count, 0) as unique_patient_count,
            s.lq_value,
            s.median_value,
            s.uq_value,
            s.percent_has_result_value
        from snomed_stats s
        full outer join snomed_concepts c on s.concept_code = c.concept_code
    ),

    -- ICD10
    icd10_apc_stats as (
        select
            concept_code,
            concept_vocabulary,
            count(*) as concept_code_count,
            count(distinct person_id) as unique_patient_count
        from {{ ref("stg_sus__apc_diagnosis_icd10") }}
        group by concept_code, concept_vocabulary
    ),

    icd10_op_stats as (
        select
            concept_code,
            concept_vocabulary,
            count(*) as concept_code_count,
            count(distinct person_id) as unique_patient_count
        from {{ ref("stg_sus__op_diagnosis_icd10") }}
        group by concept_code, concept_vocabulary
    ),

    icd10_combined_stats as (
        select
            concept_code,
            concept_vocabulary,
            sum(concept_code_count) as concept_code_count,
            sum(unique_patient_count) as unique_patient_count
        from
            (
                select *
                from icd10_apc_stats
                union all
                select *
                from icd10_op_stats
            )
        group by concept_code, concept_vocabulary
    ),

    icd10_concepts as (
        select distinct concept_code, concept_name, vocabulary_id as concept_vocabulary
        from {{ ref("base_athena__concept") }}
        where vocabulary_id = 'ICD10'
    ),

    icd10_full as (
        select
            coalesce(c.concept_name, 'Unknown') as concept_name,
            coalesce(s.concept_code, c.concept_code) as concept_code,
            coalesce(s.concept_vocabulary, c.concept_vocabulary) as concept_vocabulary,
            coalesce(s.concept_code_count, 0) as concept_code_count,
            coalesce(s.unique_patient_count, 0) as unique_patient_count,
            null::float as lq_value,
            null::float as median_value,
            null::float as uq_value,
            null::float as percent_has_result_value
        from icd10_combined_stats s
        full outer join icd10_concepts c on s.concept_code = c.concept_code
    ),

    -- OPCS4
    opcs4_apc_stats as (
        select
            concept_code,
            concept_vocabulary,
            count(*) as concept_code_count,
            count(distinct person_id) as unique_patient_count
        from {{ ref("stg_sus__apc_procedure_opcs4") }}
        group by concept_code, concept_vocabulary
    ),

    opcs4_op_stats as (
        select
            concept_code,
            concept_vocabulary,
            count(*) as concept_code_count,
            count(distinct person_id) as unique_patient_count
        from {{ ref("stg_sus__op_procedure_opcs4") }}
        group by concept_code, concept_vocabulary
    ),

    opcs4_combined_stats as (
        select
            concept_code,
            concept_vocabulary,
            sum(concept_code_count) as concept_code_count,
            sum(unique_patient_count) as unique_patient_count
        from
            (
                select *
                from opcs4_apc_stats
                union all
                select *
                from opcs4_op_stats
            )
        group by concept_code, concept_vocabulary
    ),

    opcs4_concepts as (
        select distinct concept_code, concept_name, vocabulary_id as concept_vocabulary
        from {{ ref("base_athena__concept") }}
        where vocabulary_id = 'OPCS4'
    ),

    opcs4_full as (
        select
            coalesce(c.concept_name, 'Unknown') as concept_name,
            coalesce(s.concept_code, c.concept_code) as concept_code,
            coalesce(s.concept_vocabulary, c.concept_vocabulary) as concept_vocabulary,
            coalesce(s.concept_code_count, 0) as concept_code_count,
            coalesce(s.unique_patient_count, 0) as unique_patient_count,
            null::float as lq_value,
            null::float as median_value,
            null::float as uq_value,
            null::float as percent_has_result_value
        from opcs4_combined_stats s
        full outer join opcs4_concepts c on s.concept_code = c.concept_code
    ),

    all_concepts as (
        select *
        from snomed_full
        union all
        select *
        from icd10_full
        union all
        select *
        from opcs4_full
    )

select
    concept_name,
    concept_code,
    concept_vocabulary,
    concept_code_count,
    unique_patient_count,
    lq_value,
    median_value,
    uq_value,
    percent_has_result_value
from all_concepts
