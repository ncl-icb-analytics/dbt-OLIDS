{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

/*
All inhaled corticosteroid medication orders for respiratory conditions.
Uses BNF classification (3.2) for inhaled corticosteroids.
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
        
    FROM {{ get_medication_orders(bnf_code='0302') }}
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
    
    -- Inhaled corticosteroid type classification
    CASE 
        WHEN bnf_code LIKE '030200%' THEN 'SINGLE_AGENT'      -- Single agent corticosteroids
        WHEN bnf_code LIKE '030201%' THEN 'COMBINATION'       -- Combination preparations
        ELSE 'OTHER_ICS'
    END AS ics_type,
    
    -- Specific inhaled corticosteroid classification
    CASE 
        WHEN statement_medication_name LIKE '%BECLOMETASONE%' OR bnf_code LIKE '0302000C0%' THEN 'BECLOMETASONE'
        WHEN statement_medication_name LIKE '%BUDESONIDE%' OR bnf_code LIKE '0302000K0%' THEN 'BUDESONIDE'
        WHEN statement_medication_name LIKE '%CICLESONIDE%' OR bnf_code LIKE '0302000U0%' THEN 'CICLESONIDE'
        WHEN statement_medication_name LIKE '%FLUTICASONE%' OR bnf_code LIKE '0302000N0%' THEN 'FLUTICASONE'
        WHEN statement_medication_name LIKE '%MOMETASONE%' OR bnf_code LIKE '0302000R0%' THEN 'MOMETASONE'
        ELSE 'OTHER_ICS'
    END AS specific_ics,
    
    -- Combination therapy classification
    CASE 
        WHEN statement_medication_name LIKE '%BUDESONIDE%' AND statement_medication_name LIKE '%FORMOTEROL%' THEN 'BUDESONIDE_FORMOTEROL'
        WHEN statement_medication_name LIKE '%FLUTICASONE%' AND statement_medication_name LIKE '%SALMETEROL%' THEN 'FLUTICASONE_SALMETEROL'
        WHEN statement_medication_name LIKE '%FLUTICASONE%' AND statement_medication_name LIKE '%VILANTEROL%' THEN 'FLUTICASONE_VILANTEROL'
        WHEN statement_medication_name LIKE '%BECLOMETASONE%' AND statement_medication_name LIKE '%FORMOTEROL%' THEN 'BECLOMETASONE_FORMOTEROL'
        WHEN bnf_code LIKE '030201%' THEN 'OTHER_COMBINATION'
        ELSE NULL
    END AS combination_type,
    
    -- Common ICS flags
    CASE WHEN statement_medication_name LIKE '%BECLOMETASONE%' OR bnf_code LIKE '0302000C0%' THEN TRUE ELSE FALSE END AS is_beclometasone,
    CASE WHEN statement_medication_name LIKE '%BUDESONIDE%' OR bnf_code LIKE '0302000K0%' THEN TRUE ELSE FALSE END AS is_budesonide,
    CASE WHEN statement_medication_name LIKE '%FLUTICASONE%' OR bnf_code LIKE '0302000N0%' THEN TRUE ELSE FALSE END AS is_fluticasone,
    CASE WHEN statement_medication_name LIKE '%MOMETASONE%' OR bnf_code LIKE '0302000R0%' THEN TRUE ELSE FALSE END AS is_mometasone,
    CASE WHEN statement_medication_name LIKE '%CICLESONIDE%' OR bnf_code LIKE '0302000U0%' THEN TRUE ELSE FALSE END AS is_ciclesonide,
    
    -- Preparation type flags
    CASE WHEN bnf_code LIKE '030200%' THEN TRUE ELSE FALSE END AS is_single_agent,
    CASE WHEN bnf_code LIKE '030201%' THEN TRUE ELSE FALSE END AS is_combination_therapy,
    
    -- MART (Maintenance and Reliever Therapy) potential flag
    CASE 
        WHEN statement_medication_name LIKE '%BUDESONIDE%' AND statement_medication_name LIKE '%FORMOTEROL%' THEN TRUE
        ELSE FALSE
    END AS is_mart_eligible,
    
    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,
    
    -- Order recency flags (ICS are typically long-term therapy)
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