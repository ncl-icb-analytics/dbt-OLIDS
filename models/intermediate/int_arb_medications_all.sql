{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

/*
All ARB (Angiotensin Receptor Blocker) medication orders for cardiovascular and renal protection.
Uses BNF classification (2.5.5.2) for angiotensin-II receptor antagonists.
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
        
    FROM {{ get_medication_orders(bnf_code='02050502') }}
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
    
    -- Specific ARB classification
    CASE 
        WHEN bnf_code LIKE '0205050205%' THEN 'AZILSARTAN'
        WHEN bnf_code LIKE '0205050210%' THEN 'CANDESARTAN'
        WHEN bnf_code LIKE '0205050215%' THEN 'EPROSARTAN'
        WHEN bnf_code LIKE '0205050220%' THEN 'IRBESARTAN'
        WHEN bnf_code LIKE '0205050225%' THEN 'LOSARTAN'
        WHEN bnf_code LIKE '0205050230%' THEN 'OLMESARTAN'
        WHEN bnf_code LIKE '0205050235%' THEN 'TELMISARTAN'
        WHEN bnf_code LIKE '0205050240%' THEN 'VALSARTAN'
        ELSE 'OTHER_ARB'
    END AS arb_type,
    
    -- Evidence-based ARBs (commonly used in cardiovascular disease)
    CASE 
        WHEN bnf_code LIKE '0205050225%' THEN TRUE  -- Losartan (LIFE trial)
        WHEN bnf_code LIKE '0205050240%' THEN TRUE  -- Valsartan (Val-HeFT trial)
        WHEN bnf_code LIKE '0205050210%' THEN TRUE  -- Candesartan (CHARM trial)
        WHEN bnf_code LIKE '0205050235%' THEN TRUE  -- Telmisartan (ONTARGET trial)
        ELSE FALSE
    END AS is_evidence_based_cvd,
    
    -- Common ARBs flags
    CASE WHEN bnf_code LIKE '0205050225%' THEN TRUE ELSE FALSE END AS is_losartan,
    CASE WHEN bnf_code LIKE '0205050240%' THEN TRUE ELSE FALSE END AS is_valsartan,
    CASE WHEN bnf_code LIKE '0205050210%' THEN TRUE ELSE FALSE END AS is_candesartan,
    CASE WHEN bnf_code LIKE '0205050220%' THEN TRUE ELSE FALSE END AS is_irbesartan,
    CASE WHEN bnf_code LIKE '0205050235%' THEN TRUE ELSE FALSE END AS is_telmisartan,
    
    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,
    
    -- Order recency flags (ARBs are typically long-term therapy)
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

FROM base_orders
ORDER BY person_id, order_date DESC 