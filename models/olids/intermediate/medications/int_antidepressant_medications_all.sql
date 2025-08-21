{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All antidepressant medication orders for mental health conditions.
Uses BNF classification (4.3) for antidepressant drugs.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    person_id,
    medication_order_id,
    medication_statement_id,
    order_date,
    order_medication_name,
    order_dose,
    order_quantity_value,
    order_quantity_unit,
    order_duration_days,
    statement_medication_name,
    mapped_concept_code,
    mapped_concept_display,
    bnf_code,
    bnf_name,

    -- Antidepressant class classification
    CASE
        WHEN bnf_code LIKE '040301%' THEN 'TRICYCLIC'                    -- Tricyclic and related antidepressants
        WHEN bnf_code LIKE '040302%' THEN 'MAOI'                        -- Monoamine-oxidase inhibitors
        WHEN bnf_code LIKE '040303%' THEN 'SSRI'                        -- Selective serotonin re-uptake inhibitors
        WHEN bnf_code LIKE '040304%' THEN 'OTHER_ANTIDEPRESSANTS'       -- Other antidepressant drugs
        ELSE 'UNKNOWN_ANTIDEPRESSANT'
    END AS antidepressant_class,


    -- Antidepressant class flags
    CASE WHEN bnf_code LIKE '040303%' THEN TRUE ELSE FALSE END AS is_ssri,
    CASE WHEN bnf_code LIKE '040301%' THEN TRUE ELSE FALSE END AS is_tricyclic,
    CASE WHEN bnf_code LIKE '040302%' THEN TRUE ELSE FALSE END AS is_maoi,
    CASE WHEN bnf_code LIKE '040304%' THEN TRUE ELSE FALSE END AS is_other_antidepressant,


    -- Order recency flags (antidepressants are typically long-term therapy)
    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,

    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m,

    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS is_recent_12m

FROM (
    {{ get_medication_orders(bnf_code='0403') }}
) base_orders
ORDER BY person_id, order_date DESC
