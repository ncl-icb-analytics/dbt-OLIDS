{% macro get_observations(cluster_ids) %}
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    -- Get observations filtered by cluster ID
    -- Returns standardised fields for observations
    -- Starts with observations and ranks concept matches to avoid duplicates
    SELECT
        o.id AS observation_id,
        o.patient_id,
        pp.person_id,
        p.sk_patient_id,
        o.clinical_effective_date,
        o.result_value,
        o.result_value_unit_concept_id,
        unit_con.display AS result_unit_display,
        o.result_text,
        o.is_problem,
        o.is_review,
        o.problem_end_date,
        o.observation_core_concept_id,
        o.observation_raw_concept_id,
        best_match.mapped_concept_id,
        best_match.mapped_concept_code,
        best_match.mapped_concept_display,
        best_match.cluster_id,
        best_match.cluster_description
    FROM {{ ref('stg_olids_observation') }} o
    JOIN {{ ref('stg_olids_patient') }} p
        ON o.patient_id = p.id
    JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON p.id = pp.patient_id
    LEFT JOIN {{ ref('stg_olids_term_concept') }} unit_con
        ON o.result_value_unit_concept_id = unit_con.id
    JOIN (
        -- Get the best concept match per observation
        SELECT 
            mc.source_code_id,
            mc.concept_id AS mapped_concept_id,
            mc.concept_code AS mapped_concept_code,
            mc.code_description AS mapped_concept_display,
            cc.cluster_id,
            cc.cluster_description,
            ROW_NUMBER() OVER (
                PARTITION BY mc.source_code_id 
                ORDER BY mc.code_description, mc.concept_code
            ) AS concept_rank
        FROM {{ ref('stg_codesets_mapped_concepts') }} mc
        JOIN {{ ref('stg_codesets_combined_codesets') }} cc
            ON mc.concept_code = cc.code
        WHERE cc.cluster_id IN ({{ cluster_ids }})
    ) best_match
        ON o.observation_core_concept_id = best_match.source_code_id
        AND best_match.concept_rank = 1
{% endmacro %} 