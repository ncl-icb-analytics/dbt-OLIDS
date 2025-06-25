{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

/*
All statin medication orders for cholesterol management and cardiovascular risk reduction.
Uses BNF classification (2.12) for lipid-regulating drugs.
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

    -- Specific statin classification
    CASE
        WHEN bnf_code LIKE '0212000B0%' THEN 'ATORVASTATIN'
        WHEN bnf_code LIKE '0212000C0%' THEN 'CERIVASTATIN'
        WHEN bnf_code LIKE '0212000M0%' THEN 'FLUVASTATIN'
        WHEN bnf_code LIKE '0212000X0%' THEN 'PRAVASTATIN'
        WHEN bnf_code LIKE '0212000AA%' THEN 'ROSUVASTATIN'
        WHEN bnf_code LIKE '0212000Y0%' THEN 'SIMVASTATIN'
        WHEN bnf_code LIKE '0212000AC%' THEN 'SIMVASTATIN_EZETIMIBE'
        ELSE 'OTHER_STATIN'
    END AS statin_type,

    -- High intensity statin flag (for cardiovascular risk management)
    CASE
        WHEN bnf_code LIKE '0212000B0%' THEN TRUE  -- Atorvastatin
        WHEN bnf_code LIKE '0212000AA%' THEN TRUE  -- Rosuvastatin
        ELSE FALSE
    END AS is_high_intensity_statin,

    -- Common statins flags
    CASE WHEN bnf_code LIKE '0212000B0%' THEN TRUE ELSE FALSE END AS is_atorvastatin,
    CASE WHEN bnf_code LIKE '0212000Y0%' THEN TRUE ELSE FALSE END AS is_simvastatin,
    CASE WHEN bnf_code LIKE '0212000AA%' THEN TRUE ELSE FALSE END AS is_rosuvastatin,
    CASE WHEN bnf_code LIKE '0212000X0%' THEN TRUE ELSE FALSE END AS is_pravastatin,

    -- Combination therapy flag
    CASE WHEN bnf_code LIKE '0212000AC%' THEN TRUE ELSE FALSE END AS is_combination_therapy,

    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,

    -- Order recency flags (statins are typically long-term therapy)
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
    {{ get_medication_orders(bnf_code='0212') }}
) base_orders
WHERE bnf_code LIKE '0212000%'  -- HMG CoA reductase inhibitors (statins)
ORDER BY person_id, order_date DESC
