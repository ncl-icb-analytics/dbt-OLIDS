{% macro get_observations(cluster_ids) %}
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    -- Get observations filtered by cluster ID using cleaner approach
    -- First get codes that belong to specified clusters, then filter observations
    -- Returns multiple rows per observation if it belongs to multiple clusters (clinically correct)
    WITH cluster_codes AS (
        -- Get all codes that belong to the specified clusters
        SELECT DISTINCT
            mc.source_code_id,
            mc.concept_code,
            mc.cluster_id,
            mc.cluster_description,
            mc.concept_id,
            mc.code_description
        FROM {{ ref('stg_codesets_mapped_concepts') }} mc
        WHERE mc.cluster_id IN ({{ cluster_ids }})
    ),
    
    observations_with_concepts AS (
        -- Join observations to concepts using vanilla structure
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
            c.code AS concept_code,
            c.display AS concept_display,
            c.id AS concept_id
        FROM {{ ref('stg_olids_observation') }} o
        JOIN {{ ref('stg_olids_patient') }} p
            ON o.patient_id = p.id
        JOIN {{ ref('stg_olids_patient_person') }} pp 
            ON p.id = pp.patient_id
        LEFT JOIN {{ ref('stg_olids_term_concept') }} unit_con
            ON o.result_value_unit_concept_id = unit_con.id
        LEFT JOIN {{ ref('stg_olids_term_concept_map') }} cm
            ON o.observation_core_concept_id = cm.source_code_id
        LEFT JOIN {{ ref('stg_olids_term_concept') }} c
            ON cm.target_code_id = c.id
        WHERE c.code IS NOT NULL
    )
    
    -- Final join to get cluster information for matching codes
    SELECT
        owc.observation_id,
        owc.patient_id,
        owc.person_id,
        owc.sk_patient_id,
        owc.clinical_effective_date,
        owc.result_value,
        owc.result_value_unit_concept_id,
        owc.result_unit_display,
        owc.result_text,
        owc.is_problem,
        owc.is_review,
        owc.problem_end_date,
        owc.observation_core_concept_id,
        owc.observation_raw_concept_id,
        owc.concept_id AS mapped_concept_id,
        owc.concept_code AS mapped_concept_code,
        owc.concept_display AS mapped_concept_display,
        cc.cluster_id,
        cc.cluster_description
    FROM observations_with_concepts owc
    JOIN cluster_codes cc
        ON owc.concept_code = cc.concept_code
{% endmacro %} 