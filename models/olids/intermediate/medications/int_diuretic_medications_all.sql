{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All diuretic medication orders for cardiovascular and fluid management.
Uses BNF classification (2.2) for diuretics.
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

    -- Diuretic type classification
    CASE
        WHEN bnf_code LIKE '020201%' THEN 'THIAZIDE_RELATED'     -- Thiazides and related diuretics
        WHEN bnf_code LIKE '020202%' THEN 'LOOP'                -- Loop diuretics
        WHEN bnf_code LIKE '020203%' THEN 'POTASSIUM_SPARING'   -- Potassium-sparing diuretics and aldosterone antagonists
        WHEN bnf_code LIKE '020204%' THEN 'POTASSIUM_SPARING_WITH_THIAZIDE'  -- Potassium-sparing with thiazides
        WHEN bnf_code LIKE '020205%' THEN 'OSMOTIC'             -- Osmotic diuretics
        WHEN bnf_code LIKE '020206%' THEN 'MERCURIAL'           -- Mercurial diuretics
        WHEN bnf_code LIKE '020207%' THEN 'CARBONIC_ANHYDRASE'  -- Carbonic anhydrase inhibitors
        ELSE 'OTHER_DIURETIC'
    END AS diuretic_type,


    -- Diuretic class flags
    CASE WHEN bnf_code LIKE '020201%' THEN TRUE ELSE FALSE END AS is_thiazide,
    CASE WHEN bnf_code LIKE '020202%' THEN TRUE ELSE FALSE END AS is_loop_diuretic,
    CASE WHEN bnf_code LIKE '020203%' THEN TRUE ELSE FALSE END AS is_potassium_sparing,


    -- Order recency flags
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
    {{ get_medication_orders(bnf_code='0202') }}
) base_orders
ORDER BY person_id, order_date DESC
