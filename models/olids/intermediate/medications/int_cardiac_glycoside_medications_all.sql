{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All cardiac glycoside medication orders for heart failure and arrhythmias.
Uses BNF classification (2.1.1) for cardiac glycosides.
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

    -- Specific cardiac glycoside classification (based on BNF codes)
    CASE
        WHEN bnf_code LIKE '0201010R0%' THEN 'DIGOXIN'
        WHEN bnf_code LIKE '0201010Q0%' THEN 'DIGITOXIN'
        ELSE 'OTHER_CARDIAC_GLYCOSIDE'
    END AS cardiac_glycoside_type,

    -- Cardiac glycoside flags
    CASE WHEN bnf_code LIKE '0201010R0%' THEN TRUE ELSE FALSE END AS is_digoxin,
    CASE WHEN bnf_code LIKE '0201010Q0%' THEN TRUE ELSE FALSE END AS is_digitoxin,

    -- Order recency flags (cardiac glycosides require ongoing monitoring)
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
    {{ get_medication_orders(bnf_code='020101') }}
) base_orders
ORDER BY person_id, order_date DESC
