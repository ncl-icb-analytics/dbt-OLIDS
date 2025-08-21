{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All systemic corticosteroid medication orders for inflammatory conditions.
Uses BNF classification (6.3) for corticosteroids and corticotropins.
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

    -- Corticosteroid type classification
    CASE
        WHEN base_orders.bnf_code LIKE '060301%' THEN 'REPLACEMENT'        -- Replacement therapy
        WHEN base_orders.bnf_code LIKE '060302%' THEN 'GLUCOCORTICOID'     -- Glucocorticoid therapy
        WHEN base_orders.bnf_code LIKE '060303%' THEN 'MINERALOCORTICOID'  -- Mineralocorticoid
        ELSE 'OTHER_CORTICOSTEROID'
    END AS corticosteroid_type,


    -- Usage classification flags
    CASE WHEN base_orders.bnf_code LIKE '060301%' THEN TRUE ELSE FALSE END AS is_replacement_therapy,
    CASE WHEN base_orders.bnf_code LIKE '060302%' THEN TRUE ELSE FALSE END AS is_anti_inflammatory,
    CASE WHEN base_orders.bnf_code LIKE '060303%' THEN TRUE ELSE FALSE END AS is_mineralocorticoid,



    -- Order recency flags (important for steroid monitoring)
    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 30 THEN TRUE
        ELSE FALSE
    END AS is_recent_1m,

    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,

    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m

FROM ({{ get_medication_orders(bnf_code='0603') }}) base_orders
ORDER BY base_orders.person_id, base_orders.order_date DESC
