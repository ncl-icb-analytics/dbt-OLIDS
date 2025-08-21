{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All inhaled corticosteroid medication orders for respiratory conditions.
Uses BNF classification (3.2) for inhaled corticosteroids.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    base_orders.person_id,
    base_orders.medication_order_id,
    base_orders.medication_statement_id,
    base_orders.order_date,
    base_orders.order_medication_name,
    base_orders.order_dose,
    base_orders.order_quantity_value,
    base_orders.order_quantity_unit,
    base_orders.order_duration_days,
    base_orders.statement_medication_name,
    base_orders.mapped_concept_code,
    base_orders.mapped_concept_display,
    base_orders.bnf_code,
    base_orders.bnf_name,

    -- Inhaled corticosteroid type classification
    CASE
        WHEN base_orders.bnf_code LIKE '030200%' THEN 'SINGLE_AGENT'      -- Single agent corticosteroids
        WHEN base_orders.bnf_code LIKE '030201%' THEN 'COMBINATION'       -- Combination preparations
        ELSE 'OTHER_ICS'
    END AS ics_type,


    -- Preparation type flags
    CASE WHEN base_orders.bnf_code LIKE '030200%' THEN TRUE ELSE FALSE END AS is_single_agent,
    CASE WHEN base_orders.bnf_code LIKE '030201%' THEN TRUE ELSE FALSE END AS is_combination_therapy,


    -- Order recency flags (ICS are typically long-term therapy)
    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,

    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m,

    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS is_recent_12m

FROM ({{ get_medication_orders(bnf_code='0302') }}) base_orders
ORDER BY base_orders.person_id, base_orders.order_date DESC
