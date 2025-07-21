{% macro get_medication_orders(bnf_code=none, cluster_id=none, ecl_cluster=none, source=none) %}
    -- Simpler: emit a single SELECT, no CTEs, cluster_id IN (...), always includes cluster_id in output
    -- Optional source parameter to filter to specific refset (e.g., 'LTC_LCS')
    -- Optional ecl_cluster parameter to pull codes from ECL cache instead of mapped_concepts
    {% if bnf_code is none and cluster_id is none and ecl_cluster is none %}
    {{ exceptions.raise_compiler_error("Must provide either bnf_code, cluster_id, or ecl_cluster parameter to get_medication_orders macro") }}
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
        mc.concept_code AS mapped_concept_code,
        mc.code_description AS mapped_concept_display,
        {% if ecl_cluster is not none %}
        '{{ ecl_cluster|upper }}' AS cluster_id,
        {% else %}
        mc.cluster_id,
        {% endif %}
        bnf.bnf_code,
        bnf.bnf_name
    FROM {{ ref('stg_olids_medication_order') }} mo
    JOIN {{ ref('stg_olids_medication_statement') }} ms
        ON mo.medication_statement_id = ms.id
    JOIN {{ ref('stg_codesets_mapped_concepts') }} mc
        ON ms.medication_statement_core_concept_id = mc.source_code_id
    {% if ecl_cluster is not none %}
    JOIN TABLE(DATA_LAB_OLIDS_UAT.REFERENCE.ECL_CACHED_DETAILS('{{ ecl_cluster|lower }}')) ecl
        ON mc.concept_code = ecl.code
    {% endif %}
    LEFT JOIN {{ ref('stg_codesets_bnf_latest') }} bnf
        ON mc.concept_code = bnf.snomed_code
    JOIN {{ ref('stg_olids_patient') }} p
        ON mo.patient_id = p.id
    JOIN {{ ref('stg_olids_patient_person') }} pp
        ON p.id = pp.patient_id
    WHERE mo.clinical_effective_date IS NOT NULL
        {% if cluster_id is not none %}
            AND mc.cluster_id IN ('{{ cluster_ids_str }}')
        {% endif %}
{% if source is not none and ecl_cluster is none %}
            AND mc.source = '{{ source }}'
        {% endif %}
{% if bnf_code is not none %}
            AND bnf.bnf_code LIKE '{{ bnf_code }}%'
        {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY mo.id, mc.concept_code, bnf.bnf_code
        ORDER BY
            CASE WHEN mc.code_description IS NOT NULL THEN 1 ELSE 2 END,
            CASE WHEN bnf.bnf_name IS NOT NULL THEN 1 ELSE 2 END,
            mc.code_description,
            bnf.bnf_name
    ) = 1
{% endmacro %}
