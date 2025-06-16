{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

/*
All beta blocker medication orders for cardiovascular protection and rate control.
Uses BNF classification (2.4) for beta-adrenoceptor blocking drugs.
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
        
    FROM {{ get_medication_orders(bnf_code='0204') }}
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
    
    -- Beta blocker selectivity classification
    CASE 
        WHEN bnf_code LIKE '020400%' THEN 'NON_SELECTIVE'  -- Non-selective beta blockers
        WHEN bnf_code LIKE '020401%' THEN 'SELECTIVE'      -- Selective (beta1) beta blockers
        ELSE 'OTHER_BETA_BLOCKER'
    END AS beta_blocker_selectivity,
    
    -- Specific beta blocker classification
    CASE 
        WHEN bnf_code LIKE '%ACEBUTOLOL%' OR bnf_code LIKE '0204010502%' THEN 'ACEBUTOLOL'
        WHEN bnf_code LIKE '%ATENOLOL%' OR bnf_code LIKE '0204010510%' THEN 'ATENOLOL'
        WHEN bnf_code LIKE '%BETAXOLOL%' OR bnf_code LIKE '0204010515%' THEN 'BETAXOLOL'
        WHEN bnf_code LIKE '%BISOPROLOL%' OR bnf_code LIKE '0204010520%' THEN 'BISOPROLOL'
        WHEN bnf_code LIKE '%CARVEDILOL%' OR bnf_code LIKE '0204000502%' THEN 'CARVEDILOL'
        WHEN bnf_code LIKE '%LABETALOL%' OR bnf_code LIKE '0204000510%' THEN 'LABETALOL'
        WHEN bnf_code LIKE '%METOPROLOL%' OR bnf_code LIKE '0204010530%' THEN 'METOPROLOL'
        WHEN bnf_code LIKE '%NEBIVOLOL%' OR bnf_code LIKE '0204010535%' THEN 'NEBIVOLOL'
        WHEN bnf_code LIKE '%PROPRANOLOL%' OR bnf_code LIKE '0204000520%' THEN 'PROPRANOLOL'
        WHEN bnf_code LIKE '%SOTALOL%' OR bnf_code LIKE '0204000525%' THEN 'SOTALOL'
        ELSE 'OTHER_BETA_BLOCKER'
    END AS beta_blocker_type,
    
    -- Evidence-based beta blockers for heart failure and post-MI
    CASE 
        WHEN bnf_code LIKE '%BISOPROLOL%' OR bnf_code LIKE '0204010520%' THEN TRUE  -- CIBIS trials
        WHEN bnf_code LIKE '%CARVEDILOL%' OR bnf_code LIKE '0204000502%' THEN TRUE  -- COPERNICUS trial
        WHEN bnf_code LIKE '%METOPROLOL%' OR bnf_code LIKE '0204010530%' THEN TRUE  -- MERIT-HF trial
        WHEN bnf_code LIKE '%NEBIVOLOL%' OR bnf_code LIKE '0204010535%' THEN TRUE   -- SENIORS trial
        ELSE FALSE
    END AS is_evidence_based_hf,
    
    -- Common beta blockers flags
    CASE WHEN bnf_code LIKE '%ATENOLOL%' OR bnf_code LIKE '0204010510%' THEN TRUE ELSE FALSE END AS is_atenolol,
    CASE WHEN bnf_code LIKE '%BISOPROLOL%' OR bnf_code LIKE '0204010520%' THEN TRUE ELSE FALSE END AS is_bisoprolol,
    CASE WHEN bnf_code LIKE '%CARVEDILOL%' OR bnf_code LIKE '0204000502%' THEN TRUE ELSE FALSE END AS is_carvedilol,
    CASE WHEN bnf_code LIKE '%METOPROLOL%' OR bnf_code LIKE '0204010530%' THEN TRUE ELSE FALSE END AS is_metoprolol,
    CASE WHEN bnf_code LIKE '%PROPRANOLOL%' OR bnf_code LIKE '0204000520%' THEN TRUE ELSE FALSE END AS is_propranolol,
    
    -- Cardioselective flag (beta1 selective)
    CASE WHEN bnf_code LIKE '020401%' THEN TRUE ELSE FALSE END AS is_cardioselective,
    
    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,
    
    -- Order recency flags (beta blockers are typically long-term therapy)
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