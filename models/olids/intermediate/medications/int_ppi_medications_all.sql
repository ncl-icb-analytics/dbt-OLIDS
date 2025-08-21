{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All PPI (Proton Pump Inhibitor) medication orders for gastric acid suppression.
Uses BNF classification (1.3.5) for proton pump inhibitors.
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


    -- H. pylori eradication flag (clinically distinct indication)
    CASE WHEN base_orders.bnf_code LIKE '0103050A%' THEN TRUE ELSE FALSE END AS is_h_pylori_eradication,




    -- Order recency flags
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

FROM ({{ get_medication_orders(bnf_code='0103050') }}) base_orders
ORDER BY base_orders.person_id, base_orders.order_date DESC
