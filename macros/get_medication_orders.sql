{% macro get_medication_orders(bnf_code=none, cluster_id=none, source=none) %}
    -- Simpler: emit a single SELECT, no CTEs, cluster_id IN (...), always includes cluster_id in output
    -- Optional source parameter to filter to specific refset (e.g., 'LTC_LCS')
    {% if bnf_code is none and cluster_id is none %}
    {{ exceptions.raise_compiler_error("Must provide either bnf_code or cluster_id parameter to get_medication_orders macro") }}
{% endif %}

{# Accept cluster_id as string or list, convert to a comma-separated quoted list #}
{% set cluster_ids_str = '' %}
{% if cluster_id is not none %}
    {% if cluster_id is string and ',' in cluster_id %}
            {% set cluster_ids = cluster_id.replace("'", "").split(",") %}
        {% elif cluster_id is string %}
            {% set cluster_ids = [cluster_id] %}
        {% else %}
        {% set cluster_ids = cluster_id %}
    {% endif %}
    {% set cluster_ids_str = cluster_ids | map('trim') | map('upper') | map('string') | map('replace', "'", "") | map('replace', '"', '') | map('string') | join("','") %}
{% endif %}

    {%- if cluster_id is not none -%}
    WITH mapped_med AS (
        SELECT *
        FROM {{ ref('int_mapped_concepts') }}
        WHERE UPPER(cluster_id) IN ('{{ cluster_ids_str }}')
        {% if source is not none %}
        AND source = '{{ source }}'
        {% endif %}
    )
    SELECT
        mo.id AS medication_order_id,
        ms.id AS medication_statement_id,
        mo.patient_id,
        pp.person_id,
        p.sk_patient_id,
        mo.clinical_effective_date::DATE AS order_date,
        mo.medication_name AS order_medication_name,
        mo.dose AS order_dose,
        mo.quantity_value AS order_quantity_value,
        mo.quantity_unit AS order_quantity_unit,
        mo.duration_days AS order_duration_days,
        ms.medication_name AS statement_medication_name,
        c.code AS mapped_concept_code,
        c.display AS mapped_concept_display,
        mm.cluster_id,
        bnf.bnf_code,
        bnf.bnf_name
    {# Prefer MV if enabled and present #}
    {%- set stg_rel = ref('stg_olids_medication_order') -%}
    {%- set mv_rel = adapter.get_relation(
        database=stg_rel.database,
        schema=stg_rel.schema,
        identifier=stg_rel.identifier ~ '_mv'
    ) -%}
    {%- set use_mv = var('use_mv', true) -%}
    FROM {{ (mv_rel if (use_mv and mv_rel) else stg_rel) }} mo
    JOIN {{ ref('stg_olids_medication_statement') }} ms
        ON mo.medication_statement_id = ms.id
    -- Early filter by mapped concepts
    JOIN mapped_med mm
        ON ms.medication_statement_core_concept_id = mm.source_code_id
    JOIN {{ ref('stg_olids_term_concept') }} c
        ON mm.mapped_concept_id = c.id
    LEFT JOIN {{ ref('stg_codesets_bnf_latest') }} bnf
        ON c.code = bnf.snomed_code
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    LEFT JOIN {{ ref('stg_olids_patient') }} p
        ON mo.patient_id = p.id
    WHERE mo.clinical_effective_date IS NOT NULL
    {% if bnf_code is not none %}
        AND bnf.bnf_code LIKE '{{ bnf_code }}%'
    {% endif %}
    {%- else -%}
    SELECT
        mo.id AS medication_order_id,
        ms.id AS medication_statement_id,
        mo.patient_id,
        pp.person_id,
        p.sk_patient_id,
        mo.clinical_effective_date::DATE AS order_date,
        mo.medication_name AS order_medication_name,
        mo.dose AS order_dose,
        mo.quantity_value AS order_quantity_value,
        mo.quantity_unit AS order_quantity_unit,
        mo.duration_days AS order_duration_days,
        ms.medication_name AS statement_medication_name,
        c.code AS mapped_concept_code,
        c.display AS mapped_concept_display,
        NULL AS cluster_id,
        bnf.bnf_code,
        bnf.bnf_name
    {# Prefer MV if enabled and present #}
    {%- set stg_rel = ref('stg_olids_medication_order') -%}
    {%- set mv_rel = adapter.get_relation(
        database=stg_rel.database,
        schema=stg_rel.schema,
        identifier=stg_rel.identifier ~ '_mv'
    ) -%}
    {%- set use_mv = var('use_mv', true) -%}
    FROM {{ (mv_rel if (use_mv and mv_rel) else stg_rel) }} mo
    JOIN {{ ref('stg_olids_medication_statement') }} ms
        ON mo.medication_statement_id = ms.id
    -- Native path without cluster filtering
    JOIN {{ ref('stg_olids_term_concept_map') }} cm
        ON ms.medication_statement_core_concept_id = cm.source_code_id
    JOIN {{ ref('stg_olids_term_concept') }} c
        ON cm.target_code_id = c.id
    LEFT JOIN {{ ref('stg_codesets_bnf_latest') }} bnf
        ON c.code = bnf.snomed_code
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    LEFT JOIN {{ ref('stg_olids_patient') }} p
        ON mo.patient_id = p.id
    WHERE mo.clinical_effective_date IS NOT NULL
    {% if bnf_code is not none %}
        AND bnf.bnf_code LIKE '{{ bnf_code }}%'
    {% endif %}
    {%- endif -%}
{% endmacro %}
