{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Clinical Safety Fact Table: Valproate + Pregnancy
-- CRITICAL ALERT: Identifies pregnant individuals with recent valproate prescriptions
-- HIGH RISK: Valproate has significant teratogenic effects

SELECT
    -- Core identifiers
    preg.person_id,
    preg.age,
    preg.sex,
    preg.is_child_bearing_age_0_55,

    -- Pregnancy details
    preg.latest_preg_cod_date,
    preg.latest_pregdel_cod_date,
    preg.all_preg_concept_codes,
    preg.all_preg_concept_displays,
    preg.all_preg_source_cluster_ids,

    -- Valproate order details
    valp.most_recent_order_date AS most_recent_valproate_order_date,
    valp.medication_order_id AS valproate_medication_order_id,
    valp.medication_statement_id AS valproate_medication_statement_id,
    valp.order_medication_name AS valproate_order_medication_name,
    valp.order_dose AS valproate_order_dose,
    valp.order_quantity_value AS valproate_order_quantity_value,
    valp.order_quantity_unit AS valproate_order_quantity_unit,
    valp.order_duration_days AS valproate_order_duration_days,
    valp.statement_medication_name AS valproate_statement_medication_name,
    valp.mapped_concept_code AS valproate_mapped_concept_code,
    valp.mapped_concept_display AS valproate_mapped_concept_display,
    valp.valproate_product_term,
    valp.recent_order_count AS valproate_recent_order_count

FROM {{ ref('fct_person_pregnancy_status') }} AS preg
INNER JOIN {{ ref('int_valproate_medications_6m_latest') }} AS valp
    ON preg.person_id = valp.person_id
WHERE preg.is_child_bearing_age_0_55 = TRUE -- Additional safety check
