{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

/*
All PPI (Proton Pump Inhibitor) medication orders for gastric acid suppression.
Uses BNF classification (1.3.5) for proton pump inhibitors.
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
        
    FROM {{ get_medication_orders(bnf_code='0103050') }}
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
    
    -- Specific PPI classification
    CASE 
        WHEN bnf_code LIKE '0103050E%' THEN 'ESOMEPRAZOLE'
        WHEN bnf_code LIKE '0103050A%' THEN 'H_PYLORI_ERADICATION'
        WHEN bnf_code LIKE '0103050L%' THEN 'LANSOPRAZOLE'
        WHEN bnf_code LIKE '0103050P%' THEN 'OMEPRAZOLE'
        WHEN bnf_code LIKE '0103050R%' THEN 'PANTOPRAZOLE'
        WHEN bnf_code LIKE '0103050T%' THEN 'RABEPRAZOLE'
        ELSE 'OTHER_PPI'
    END AS ppi_type,
    
    -- Common PPIs flags
    CASE WHEN bnf_code LIKE '0103050P%' THEN TRUE ELSE FALSE END AS is_omeprazole,
    CASE WHEN bnf_code LIKE '0103050L%' THEN TRUE ELSE FALSE END AS is_lansoprazole,
    CASE WHEN bnf_code LIKE '0103050E%' THEN TRUE ELSE FALSE END AS is_esomeprazole,
    CASE WHEN bnf_code LIKE '0103050R%' THEN TRUE ELSE FALSE END AS is_pantoprazole,
    CASE WHEN bnf_code LIKE '0103050T%' THEN TRUE ELSE FALSE END AS is_rabeprazole,
    
    -- H. pylori eradication flag
    CASE WHEN bnf_code LIKE '0103050A%' THEN TRUE ELSE FALSE END AS is_h_pylori_eradication,
    
    -- High dose PPI flag (for bleeding prophylaxis)
    CASE 
        WHEN (bnf_code LIKE '0103050P%' AND order_dose LIKE '%40%') OR  -- Omeprazole 40mg
             (bnf_code LIKE '0103050L%' AND order_dose LIKE '%30%') OR  -- Lansoprazole 30mg
             (bnf_code LIKE '0103050E%' AND order_dose LIKE '%40%')     -- Esomeprazole 40mg
        THEN TRUE
        ELSE FALSE
    END AS is_high_dose,
    
    -- Standard dose PPI flag
    CASE 
        WHEN (bnf_code LIKE '0103050P%' AND order_dose LIKE '%20%') OR  -- Omeprazole 20mg
             (bnf_code LIKE '0103050L%' AND order_dose LIKE '%15%') OR  -- Lansoprazole 15mg
             (bnf_code LIKE '0103050E%' AND order_dose LIKE '%20%')     -- Esomeprazole 20mg
        THEN TRUE
        ELSE FALSE
    END AS is_standard_dose,
    
    -- Long-term therapy flag (duration > 8 weeks suggests maintenance therapy)
    CASE 
        WHEN order_duration_days > 56 THEN TRUE
        ELSE FALSE
    END AS is_long_term_therapy,
    
    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,
    
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

FROM base_orders
ORDER BY person_id, order_date DESC 