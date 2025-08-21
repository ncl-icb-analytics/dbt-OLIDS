{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All NSAID (Non-Steroidal Anti-Inflammatory Drug) medication orders for pain and inflammation.
Uses BNF classification (10.1.1) for NSAIDs and (10.3.2) for topical NSAIDs.
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

    -- NSAID type classification
    CASE
        -- COX-2 selective
        WHEN base_orders.bnf_code LIKE '1001010A%' OR base_orders.bnf_code LIKE '1001010AJ%' OR
             base_orders.bnf_code LIKE '1001010AN%' OR base_orders.bnf_code LIKE '1001010AF%' THEN 'COX2_SELECTIVE'
        -- Topical
        WHEN base_orders.bnf_code LIKE '100302%' THEN 'TOPICAL'
        -- Non-selective (all others in 10.1.1)
        WHEN base_orders.bnf_code LIKE '100101%' THEN 'NON_SELECTIVE'
        ELSE 'OTHER_NSAID'
    END AS nsaid_type,


    -- NSAID classification flags
    CASE
        WHEN base_orders.bnf_code LIKE '1001010A%' OR base_orders.bnf_code LIKE '1001010AJ%' OR
             base_orders.bnf_code LIKE '1001010AN%' OR base_orders.bnf_code LIKE '1001010AF%' THEN TRUE
        ELSE FALSE
    END AS is_cox2_selective,

    CASE WHEN base_orders.bnf_code LIKE '100302%' THEN TRUE ELSE FALSE END AS is_topical,
    CASE WHEN base_orders.bnf_code LIKE '100101%' AND base_orders.bnf_code NOT LIKE '1001010A%' THEN TRUE ELSE FALSE END AS is_non_selective,


    -- Cardiovascular risk flag (COX-2 selective)
    CASE
        WHEN base_orders.bnf_code LIKE '1001010A%' THEN TRUE  -- COX-2 selective
        ELSE FALSE
    END AS is_high_cv_risk,


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

FROM ({{ get_medication_orders(bnf_code='1001') }}) base_orders
WHERE base_orders.bnf_code LIKE '100101%' OR base_orders.bnf_code LIKE '100302%'  -- Oral NSAIDs and topical NSAIDs
ORDER BY base_orders.person_id, base_orders.order_date DESC
