{% macro get_medication_orders(bnf_code=none, cluster_id=none) %}
    -- Get medication orders filtered by either BNF code or cluster ID
    -- Returns standardised fields for medication orders
    {% if bnf_code is none and cluster_id is none %}
        {{ exceptions.raise_compiler_error("Must provide either bnf_code or cluster_id parameter to get_medication_orders macro") }}
    {% endif %}
    
    WITH base_orders AS (
        SELECT 
            mo.id AS medication_order_id,
            ms.id AS medication_statement_id,
            mo.patient_id,
            dp.person_id,
            dp.sk_patient_id,
            dp.patient_id AS dim_patient_id,
            dp.person_sk,
            mo.clinical_effective_date::DATE AS order_date,
            mo.medication_name AS order_medication_name,
            mo.dose AS order_dose,
            mo.quantity_value AS order_quantity_value,
            mo.quantity_unit AS order_quantity_unit,
            mo.duration_days AS order_duration_days,
            ms.medication_name AS statement_medication_name,
            mc.concept_code AS mapped_concept_code,
            mc.code_description AS mapped_concept_display,
            bnf.bnf_code,
            bnf.bnf_name
        FROM {{ ref('stg_olids_medication_order') }} mo
        JOIN {{ ref('stg_olids_medication_statement') }} ms
            ON mo.medication_statement_id = ms.id
        JOIN {{ ref('stg_codesets_mapped_concepts') }} mc
            ON ms.medication_statement_core_concept_id = mc.source_code_id
        JOIN {{ ref('dim_person') }} dp
            ON mo.patient_id = dp.patient_id
        {% if bnf_code is not none %}
            JOIN {{ ref('stg_codesets_bnf_latest') }} bnf
                ON mc.concept_code = bnf.snomed_code
            WHERE bnf.bnf_code LIKE '{{ bnf_code }}%'
        {% elif cluster_id is not none %}
            JOIN {{ ref('stg_codesets_combined_codesets') }} cc
                ON mc.concept_code = cc.code
            WHERE cc.cluster_id = '{{ cluster_id }}'
        {% endif %}
    )
    
    SELECT * FROM base_orders

{% endmacro %} 