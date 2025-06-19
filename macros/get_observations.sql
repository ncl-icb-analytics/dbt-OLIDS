{% macro get_observations(cluster_ids, source=none) %}
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    -- Simplified macro using direct approach that works
    -- Returns multiple rows per observation if it belongs to multiple clusters (clinically correct)
    -- Deduplicates same observation with different concept displays, preferring populated over null
    -- Optional source parameter to filter to specific refset (e.g., 'PCD' for Primary Care Domain)
    WITH macro_base_observations AS (
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
            mc.concept_id AS mapped_concept_id,
            mc.concept_code AS mapped_concept_code,
            mc.code_description AS mapped_concept_display,
            mc.cluster_id,
            mc.cluster_description,
            -- Row number to pick best row per observation/concept/cluster combination
            ROW_NUMBER() OVER (
                PARTITION BY o.id, mc.concept_code, mc.cluster_id
                ORDER BY 
                    CASE WHEN mc.code_description IS NOT NULL THEN 1 ELSE 2 END,
                    mc.code_description
            ) AS row_rank
        FROM {{ ref('stg_olids_observation') }} o
        JOIN {{ ref('stg_olids_patient') }} p
            ON o.patient_id = p.id
        JOIN {{ ref('stg_olids_patient_person') }} pp 
            ON p.id = pp.patient_id
        LEFT JOIN {{ ref('stg_olids_term_concept') }} unit_con
            ON o.result_value_unit_concept_id = unit_con.id
        JOIN {{ ref('stg_codesets_mapped_concepts') }} mc
            ON o.observation_core_concept_id = mc.source_code_id
            AND mc.cluster_id IN ({{ cluster_ids }})
            {% if source %}
            AND mc.source = '{{ source }}'
            {% endif %}
    )
    SELECT
        observation_id,
        patient_id,
        person_id,
        sk_patient_id,
        clinical_effective_date,
        result_value,
        result_value_unit_concept_id,
        result_unit_display,
        result_text,
        is_problem,
        is_review,
        problem_end_date,
        observation_core_concept_id,
        observation_raw_concept_id,
        mapped_concept_id,
        mapped_concept_code,
        mapped_concept_display,
        cluster_id,
        cluster_description
    FROM macro_base_observations
    WHERE row_rank = 1
{% endmacro %} 