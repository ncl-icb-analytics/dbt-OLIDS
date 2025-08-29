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
    -- Join pre-mapped medication orders with cluster definitions
    WITH cluster_codes AS (
        SELECT DISTINCT 
            code as mapped_concept_code,
            cluster_id,
            cluster_description
        FROM {{ ref('stg_reference_combined_codesets') }}
        WHERE UPPER(cluster_id) IN ('{{ cluster_ids_str }}')
        {% if source is not none %}
        AND source = '{{ source }}'
        {% endif %}
    )
    SELECT
        mo.medication_order_id,
        mo.medication_statement_id,
        mo.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        mo.order_date,
        mo.order_medication_name,
        mo.order_dose,
        mo.order_quantity_value,
        mo.order_quantity_unit,
        mo.order_duration_days,
        mo.statement_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        cc.cluster_id,
        bnf.bnf_code,
        bnf.bnf_name
    FROM {{ ref('int_medication_orders_mapped') }} mo
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    INNER JOIN cluster_codes cc
        ON mo.mapped_concept_code = cc.mapped_concept_code
    LEFT JOIN {{ ref('stg_reference_bnf_latest') }} bnf
        ON mo.mapped_concept_code = bnf.snomed_code
    WHERE mo.order_date IS NOT NULL
    {% if bnf_code is not none %}
        AND bnf.bnf_code LIKE '{{ bnf_code }}%'
    {% endif %}
    {%- else -%}
    -- BNF code path without cluster filtering
    SELECT
        mo.medication_order_id,
        mo.medication_statement_id,
        mo.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        mo.order_date,
        mo.order_medication_name,
        mo.order_dose,
        mo.order_quantity_value,
        mo.order_quantity_unit,
        mo.order_duration_days,
        mo.statement_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        NULL AS cluster_id,
        bnf.bnf_code,
        bnf.bnf_name
    FROM {{ ref('int_medication_orders_mapped') }} mo
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    LEFT JOIN {{ ref('stg_reference_bnf_latest') }} bnf
        ON mo.mapped_concept_code = bnf.snomed_code
    WHERE mo.order_date IS NOT NULL
    {% if bnf_code is not none %}
        AND bnf.bnf_code LIKE '{{ bnf_code }}%'
    {% endif %}
    {%- endif -%}
{% endmacro %}
