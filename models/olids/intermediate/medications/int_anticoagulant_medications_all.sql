{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All oral anticoagulant medication orders for thrombosis prevention and atrial fibrillation management.
Uses BNF classification (2.8.2) for oral anticoagulants.
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

    -- Anticoagulant type classification (based on BNF codes)
    CASE
        -- DOACs (Direct Oral Anticoagulants)
        WHEN bnf_code LIKE '0208020Z%' THEN 'DOAC'  -- APIXABAN
        WHEN bnf_code LIKE '0208020X%' THEN 'DOAC'  -- DABIGATRAN
        WHEN bnf_code LIKE '0208020AA%' THEN 'DOAC' -- EDOXABAN
        WHEN bnf_code LIKE '0208020Y%' THEN 'DOAC'  -- RIVAROXABAN
        -- VKAs (Vitamin K Antagonists)
        WHEN bnf_code LIKE '0208020V%' THEN 'VKA'   -- WARFARIN
        WHEN bnf_code LIKE '0208020H%' THEN 'VKA'   -- ACENOCOUMAROL
        WHEN bnf_code LIKE '0208020N%' THEN 'VKA'   -- PHENINDIONE
        WHEN bnf_code LIKE '0208020S%' THEN 'VKA'   -- PHENPROCOUMON
        ELSE 'OTHER_ANTICOAGULANT'
    END AS anticoagulant_type,

    -- Anticoagulant class flags
    CASE
        WHEN bnf_code LIKE '0208020Z%' OR bnf_code LIKE '0208020X%'
             OR bnf_code LIKE '0208020Y%' OR bnf_code LIKE '0208020AA%' THEN TRUE
        ELSE FALSE
    END AS is_doac,

    CASE
        WHEN bnf_code LIKE '0208020V%' OR bnf_code LIKE '0208020H%'
             OR bnf_code LIKE '0208020N%' OR bnf_code LIKE '0208020S%' THEN TRUE
        ELSE FALSE
    END AS is_vka,

    -- Order recency flags (anticoagulants are typically long-term therapy)
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
    {{ get_medication_orders(bnf_code='020802') }}
) base_orders
ORDER BY person_id, order_date DESC
