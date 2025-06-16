{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

/*
All systemic corticosteroid medication orders for inflammatory conditions.
Uses BNF classification (6.3) for corticosteroids and corticotropins.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_orders AS (
    
    SELECT
        medication_order_id,
        medication_statement_id,
        person_id,
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
        bnf_name
        
    FROM {{ get_medication_orders(bnf_code='0603') }}
)

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
    
    -- Corticosteroid type classification
    CASE 
        WHEN bnf_code LIKE '060301%' THEN 'REPLACEMENT'        -- Replacement therapy
        WHEN bnf_code LIKE '060302%' THEN 'GLUCOCORTICOID'     -- Glucocorticoid therapy
        WHEN bnf_code LIKE '060303%' THEN 'MINERALOCORTICOID'  -- Mineralocorticoid
        ELSE 'OTHER_CORTICOSTEROID'
    END AS corticosteroid_type,
    
    -- Specific corticosteroid classification
    CASE 
        WHEN statement_medication_name LIKE '%PREDNISOLONE%' OR bnf_code LIKE '0603020T0%' THEN 'PREDNISOLONE'
        WHEN statement_medication_name LIKE '%HYDROCORTISONE%' OR bnf_code LIKE '0603010F0%' THEN 'HYDROCORTISONE'
        WHEN statement_medication_name LIKE '%DEXAMETHASONE%' OR bnf_code LIKE '0603020C0%' THEN 'DEXAMETHASONE'
        WHEN statement_medication_name LIKE '%METHYLPREDNISOLONE%' OR bnf_code LIKE '0603020M0%' THEN 'METHYLPREDNISOLONE'
        WHEN statement_medication_name LIKE '%BETAMETHASONE%' OR bnf_code LIKE '0603020A0%' THEN 'BETAMETHASONE'
        WHEN statement_medication_name LIKE '%DEFLAZACORT%' OR bnf_code LIKE '0603020D0%' THEN 'DEFLAZACORT'
        WHEN statement_medication_name LIKE '%FLUDROCORTISONE%' OR bnf_code LIKE '0603030F0%' THEN 'FLUDROCORTISONE'
        ELSE 'OTHER_CORTICOSTEROID'
    END AS specific_corticosteroid,
    
    -- Potency classification (relative to hydrocortisone = 1)
    CASE 
        WHEN statement_medication_name LIKE '%HYDROCORTISONE%' OR bnf_code LIKE '0603010F0%' THEN 'LOW_POTENCY'        -- 1x
        WHEN statement_medication_name LIKE '%PREDNISOLONE%' OR bnf_code LIKE '0603020T0%' THEN 'MEDIUM_POTENCY'      -- 4x
        WHEN statement_medication_name LIKE '%METHYLPREDNISOLONE%' OR bnf_code LIKE '0603020M0%' THEN 'MEDIUM_POTENCY' -- 5x
        WHEN statement_medication_name LIKE '%DEXAMETHASONE%' OR bnf_code LIKE '0603020C0%' THEN 'HIGH_POTENCY'       -- 25x
        WHEN statement_medication_name LIKE '%BETAMETHASONE%' OR bnf_code LIKE '0603020A0%' THEN 'HIGH_POTENCY'       -- 25x
        ELSE 'UNKNOWN_POTENCY'
    END AS potency_classification,
    
    -- Common corticosteroids flags
    CASE WHEN statement_medication_name LIKE '%PREDNISOLONE%' OR bnf_code LIKE '0603020T0%' THEN TRUE ELSE FALSE END AS is_prednisolone,
    CASE WHEN statement_medication_name LIKE '%HYDROCORTISONE%' OR bnf_code LIKE '0603010F0%' THEN TRUE ELSE FALSE END AS is_hydrocortisone,
    CASE WHEN statement_medication_name LIKE '%DEXAMETHASONE%' OR bnf_code LIKE '0603020C0%' THEN TRUE ELSE FALSE END AS is_dexamethasone,
    CASE WHEN statement_medication_name LIKE '%METHYLPREDNISOLONE%' OR bnf_code LIKE '0603020M0%' THEN TRUE ELSE FALSE END AS is_methylprednisolone,
    CASE WHEN statement_medication_name LIKE '%FLUDROCORTISONE%' OR bnf_code LIKE '0603030F0%' THEN TRUE ELSE FALSE END AS is_fludrocortisone,
    
    -- Usage classification flags
    CASE WHEN bnf_code LIKE '060301%' THEN TRUE ELSE FALSE END AS is_replacement_therapy,
    CASE WHEN bnf_code LIKE '060302%' THEN TRUE ELSE FALSE END AS is_anti_inflammatory,
    CASE WHEN bnf_code LIKE '060303%' THEN TRUE ELSE FALSE END AS is_mineralocorticoid,
    
    -- High-dose flag (important for monitoring)
    CASE 
        WHEN statement_medication_name LIKE '%PREDNISOLONE%' AND (
            order_dose LIKE '%20%' OR order_dose LIKE '%25%' OR order_dose LIKE '%30%' OR 
            order_dose LIKE '%40%' OR order_dose LIKE '%50%' OR order_dose LIKE '%60%'
        ) THEN TRUE
        ELSE FALSE
    END AS is_high_dose,
    
    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,
    
    -- Order recency flags (important for steroid monitoring)
    CASE 
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 30 THEN TRUE
        ELSE FALSE
    END AS is_recent_1m,
    
    CASE 
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,
    
    CASE 
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m

FROM base_orders
ORDER BY person_id, order_date DESC 