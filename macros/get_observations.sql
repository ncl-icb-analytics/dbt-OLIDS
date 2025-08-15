{% macro get_observations(cluster_ids, source=none) %}
    {% do config(static_analysis='unsafe') %}
    
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    -- Resolve codes to source concept ids first to enable early filtering on observations
    WITH source_concepts AS (
        SELECT *
        FROM {{ ref('int_mapped_concepts') }}
        WHERE UPPER(cluster_id) IN ({{ cluster_ids|upper }})
        {% if source %}
          AND source = '{{ source }}'
        {% endif %}
    ),
    macro_observations AS (
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
            sc.mapped_concept_id,
            sc.mapped_concept_code,
            sc.mapped_concept_display,
            sc.cluster_id,
            sc.cluster_description,
            sc.code_description
        {# Prefer MV variant if enabled and it exists; otherwise fall back to the view #}
        {%- set stg_rel = ref('stg_olids_observation') -%}
        {%- set mv_rel_default = adapter.get_relation(
            database=stg_rel.database,
            schema=stg_rel.schema,
            identifier=stg_rel.identifier ~ '_mv'
        ) -%}
        {%- set use_mv = var('use_mv', true) -%}
        {%- set chosen_rel = (mv_rel_default if (use_mv and mv_rel_default) else stg_rel) -%}
        FROM {{ chosen_rel }} o
        -- Attach concept metadata and early filter by source code id
        JOIN source_concepts sc
          ON o.observation_source_concept_id = sc.source_code_id
        JOIN {{ ref('int_patient_person_unique') }} pp
            ON o.patient_id = pp.patient_id
        LEFT JOIN {{ ref('stg_olids_terminology_concept') }} unit_con
            ON o.result_value_unit_concept_id = unit_con.id
    )
    -- Deduplicate: preserve legitimate cross-cluster duplicates but remove within-cluster duplicates
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
        observation_source_concept_id,
        mapped_concept_id,
        mapped_concept_code,
        mapped_concept_display,
        cluster_id,
        cluster_description,
        code_description
    FROM macro_observations
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY observation_id, cluster_id 
        ORDER BY mapped_concept_code
    ) = 1
    
{% endmacro %}
