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

    -- Specific cardiac glycoside classification
    CASE
        WHEN statement_medication_name LIKE '%DIGOXIN%' OR bnf_code LIKE '0201010R0%' THEN 'DIGOXIN'
        WHEN statement_medication_name LIKE '%DIGITOXIN%' OR bnf_code LIKE '0201010Q0%' THEN 'DIGITOXIN'
        ELSE 'OTHER_CARDIAC_GLYCOSIDE'
    END AS cardiac_glycoside_type,

    -- Cardiac glycoside flags
    CASE WHEN statement_medication_name LIKE '%DIGOXIN%' OR bnf_code LIKE '0201010R0%' THEN TRUE ELSE FALSE END AS is_digoxin,
    CASE WHEN statement_medication_name LIKE '%DIGITOXIN%' OR bnf_code LIKE '0201010Q0%' THEN TRUE ELSE FALSE END AS is_digitoxin,

    -- Dose classification for digoxin (requires careful monitoring)
    CASE
        WHEN statement_medication_name LIKE '%DIGOXIN%' AND (
            order_dose LIKE '%125%' OR order_dose LIKE '%0.125%'
        ) THEN 'STANDARD_DOSE'
        WHEN statement_medication_name LIKE '%DIGOXIN%' AND (
            order_dose LIKE '%250%' OR order_dose LIKE '%0.25%'
        ) THEN 'HIGH_DOSE'
        WHEN statement_medication_name LIKE '%DIGOXIN%' AND (
            order_dose LIKE '%62.5%' OR order_dose LIKE '%0.0625%'
        ) THEN 'LOW_DOSE'
        ELSE 'UNKNOWN_DOSE'
    END AS digoxin_dose_category,

    -- Clinical indication flags based on dose patterns
    CASE
        WHEN statement_medication_name LIKE '%DIGOXIN%' AND (
            order_dose LIKE '%125%' OR order_dose LIKE '%0.125%' OR
            order_dose LIKE '%250%' OR order_dose LIKE '%0.25%'
        ) THEN TRUE
        ELSE FALSE
    END AS is_heart_failure_dose,

    CASE
        WHEN statement_medication_name LIKE '%DIGOXIN%' AND (
            order_dose LIKE '%250%' OR order_dose LIKE '%0.25%'
        ) THEN TRUE
        ELSE FALSE
    END AS is_arrhythmia_dose,

    -- Elderly/renal dose flag (62.5mcg typical for elderly or renal impairment)
    CASE
        WHEN statement_medication_name LIKE '%DIGOXIN%' AND (
            order_dose LIKE '%62.5%' OR order_dose LIKE '%0.0625%'
        ) THEN TRUE
        ELSE FALSE
    END AS is_elderly_renal_dose,

    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,

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
