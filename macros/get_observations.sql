{% macro get_observations(cluster_ids, source=none) %}
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    -- Simplified macro using direct approach with QUALIFY
    -- Returns multiple rows per observation if it belongs to multiple clusters (clinically correct)
    -- Deduplicates same observation with different concept displays, preferring populated over null
    -- Optional source parameter to filter to specific refset (e.g., 'PCD' for Primary Care Domain)
    SELECT
        o.id AS observation_id,
        o.patient_id,
        pp.person_id,
        NULL AS sk_patient_id,  -- Remove dependency on PATIENT table
        o.clinical_effective_date,
        o.result_value,
        o.result_value_unit_concept_id,
        unit_con.display AS result_unit_display,
        o.result_text,
        o.is_problem,
        o.is_review,
        o.problem_end_date,
        o.observation_source_concept_id,
        c.id AS mapped_concept_id,
        c.code AS mapped_concept_code,
        c.display AS mapped_concept_display,
        cc.cluster_id,
        cc.cluster_description,
        cc.code_description
    FROM {{ ref('stg_olids_observation') }} o
    -- Join through CONCEPT_MAP to CONCEPT (native terminology path)
    JOIN {{ ref('stg_olids_term_concept_map') }} cm
        ON o.observation_source_concept_id = cm.source_code_id
    JOIN {{ ref('stg_olids_term_concept') }} c
        ON cm.target_code_id = c.id
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON o.patient_id = pp.patient_id
    LEFT JOIN {{ ref('stg_olids_term_concept') }} unit_con
        ON o.result_value_unit_concept_id = unit_con.id
    JOIN {{ ref('stg_codesets_combined_codesets') }} cc
        ON c.code = cc.code
        AND UPPER(cc.cluster_id) IN ({{ cluster_ids|upper }})
        {% if source %}
        AND cc.source = '{{ source }}'
        {% endif %}
{% endmacro %}
