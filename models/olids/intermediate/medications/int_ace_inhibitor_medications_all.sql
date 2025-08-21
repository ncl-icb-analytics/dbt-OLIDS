{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All ACE inhibitor medication orders for cardiovascular and renal protection.
Uses BNF classification (2.5.5.1) for ACE inhibitors.
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


    -- Order recency flags (ACE inhibitors are typically long-term therapy)
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
    {{ get_medication_orders(bnf_code='02050501') }}
) base_orders
ORDER BY person_id, order_date DESC
