{% macro get_observations(cluster_ids) %}
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    -- Get observations filtered by cluster ID
    -- Returns standardised fields for observations
    SELECT
        o.id AS observation_id,
        o.patient_id,
        dp.person_id,
        dp.sk_patient_id,
        dp.patient_id AS dim_patient_id,
        dp.person_sk,
        o.clinical_effective_date,
        o.result_value,
        o.result_value_unit_concept_id,
        o.result_text,
        o.is_problem,
        o.is_review,
        o.problem_end_date,
        o.observation_core_concept_id,
        o.observation_raw_concept_id,
        mc.concept_code AS mapped_concept_code,
        mc.code_description AS mapped_concept_display,
        cc.cluster_id,
        cc.cluster_description
    FROM {{ ref('stg_olids_observation') }} o
    JOIN {{ ref('dim_person') }} dp
        ON o.patient_id = dp.patient_id
    JOIN {{ ref('stg_codesets_mapped_concepts') }} mc
        ON o.observation_core_concept_id = mc.source_code_id
    JOIN {{ ref('stg_codesets_combined_codesets') }} cc
        ON mc.concept_code = cc.code
    WHERE cc.cluster_id IN ({{ cluster_ids }})
{% endmacro %} 