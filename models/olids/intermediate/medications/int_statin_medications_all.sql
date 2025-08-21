{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
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

    -- Statin intensity classification (based on typical therapeutic doses)
    CASE
        WHEN bnf_code LIKE '0212000B0%' THEN 'HIGH_INTENSITY'     -- Atorvastatin (≥50% LDL reduction)
        WHEN bnf_code LIKE '0212000AA%' THEN 'HIGH_INTENSITY'     -- Rosuvastatin (≥50% LDL reduction)
        WHEN bnf_code LIKE '0212000Y0%' THEN 'MODERATE_INTENSITY' -- Simvastatin (30-49% LDL reduction)
        WHEN bnf_code LIKE '0212000X0%' THEN 'MODERATE_INTENSITY' -- Pravastatin (30-49% LDL reduction)
        WHEN bnf_code LIKE '0212000M0%' THEN 'MODERATE_INTENSITY' -- Fluvastatin (30-49% LDL reduction)
        WHEN bnf_code LIKE '0212000AC%' THEN 'COMBINATION'        -- Statin + ezetimibe combination
        ELSE 'OTHER_STATIN'
    END AS statin_intensity,

    -- Combination therapy flag
    CASE WHEN bnf_code LIKE '0212000AC%' THEN TRUE ELSE FALSE END AS is_combination_therapy,

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
