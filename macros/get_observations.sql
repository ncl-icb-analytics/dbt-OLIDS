{% macro get_observations(cluster_ids, source=none) %}
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    -- Join pre-mapped observations with cluster definitions for flexibility
    WITH cluster_codes AS (
        SELECT DISTINCT 
            code as mapped_concept_code,
            cluster_id,
            cluster_description,
            code_description
        FROM {{ ref('stg_reference_combined_codesets') }}
        WHERE UPPER(cluster_id) IN ({{ cluster_ids|upper }})
        {% if source %}
          AND source = '{{ source }}'
        {% endif %}
    )
    SELECT 
        o.observation_id,
        o.patient_id,
        pp.person_id,
        o.clinical_effective_date,
        o.result_value,
        o.result_value_unit_concept_id,
        o.result_unit_display,
        o.result_text,
        o.is_problem,
        o.is_review,
        o.problem_end_date,
        o.observation_source_concept_id,
        o.mapped_concept_id,
        o.mapped_concept_code,
        o.mapped_concept_display,
        cc.cluster_id,
        cc.cluster_description,
        cc.code_description
    FROM {{ ref('int_observations_mapped') }} o
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON o.patient_id = pp.patient_id
    INNER JOIN cluster_codes cc
        ON o.mapped_concept_code = cc.mapped_concept_code
    -- Deduplicate: preserve legitimate cross-cluster duplicates but remove within-cluster duplicates
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.observation_id, cc.cluster_id 
        ORDER BY o.mapped_concept_code
    ) = 1
    
{% endmacro %}
