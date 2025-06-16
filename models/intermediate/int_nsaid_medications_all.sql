{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

/*
All NSAID (Non-Steroidal Anti-Inflammatory Drug) medication orders for pain and inflammation.
Uses BNF classification (10.1.1) for NSAIDs and (10.3.2) for topical NSAIDs.
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
        
    FROM {{ get_medication_orders(bnf_code='1001') }}
    WHERE bnf_code LIKE '100101%' OR bnf_code LIKE '100302%'  -- Oral NSAIDs and topical NSAIDs
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
    
    -- NSAID type classification
    CASE 
        -- COX-2 selective
        WHEN bnf_code LIKE '1001010A%' OR bnf_code LIKE '1001010AJ%' OR 
             bnf_code LIKE '1001010AN%' OR bnf_code LIKE '1001010AF%' THEN 'COX2_SELECTIVE'
        -- Topical
        WHEN bnf_code LIKE '100302%' THEN 'TOPICAL'
        -- Non-selective (all others in 10.1.1)
        WHEN bnf_code LIKE '100101%' THEN 'NON_SELECTIVE'
        ELSE 'OTHER_NSAID'
    END AS nsaid_type,
    
    -- Specific NSAID classification
    CASE 
        WHEN statement_medication_name LIKE '%IBUPROFEN%' OR bnf_code LIKE '1001010J0%' THEN 'IBUPROFEN'
        WHEN statement_medication_name LIKE '%DICLOFENAC%' OR bnf_code LIKE '1001010E0%' THEN 'DICLOFENAC'
        WHEN statement_medication_name LIKE '%NAPROXEN%' OR bnf_code LIKE '1001010V0%' THEN 'NAPROXEN'
        WHEN statement_medication_name LIKE '%ASPIRIN%' OR bnf_code LIKE '1001010B0%' THEN 'ASPIRIN'
        WHEN statement_medication_name LIKE '%CELECOXIB%' OR bnf_code LIKE '1001010AF%' THEN 'CELECOXIB'
        WHEN statement_medication_name LIKE '%ETORICOXIB%' OR bnf_code LIKE '1001010AJ%' THEN 'ETORICOXIB'
        WHEN statement_medication_name LIKE '%INDOMETACIN%' OR bnf_code LIKE '1001010K0%' THEN 'INDOMETACIN'
        WHEN statement_medication_name LIKE '%MELOXICAM%' OR bnf_code LIKE '1001010T0%' THEN 'MELOXICAM'
        ELSE 'OTHER_NSAID'
    END AS specific_nsaid,
    
    -- Common NSAIDs flags
    CASE WHEN statement_medication_name LIKE '%IBUPROFEN%' OR bnf_code LIKE '1001010J0%' THEN TRUE ELSE FALSE END AS is_ibuprofen,
    CASE WHEN statement_medication_name LIKE '%DICLOFENAC%' OR bnf_code LIKE '1001010E0%' THEN TRUE ELSE FALSE END AS is_diclofenac,
    CASE WHEN statement_medication_name LIKE '%NAPROXEN%' OR bnf_code LIKE '1001010V0%' THEN TRUE ELSE FALSE END AS is_naproxen,
    CASE WHEN statement_medication_name LIKE '%ASPIRIN%' OR bnf_code LIKE '1001010B0%' THEN TRUE ELSE FALSE END AS is_aspirin,
    CASE WHEN statement_medication_name LIKE '%CELECOXIB%' OR bnf_code LIKE '1001010AF%' THEN TRUE ELSE FALSE END AS is_celecoxib,
    CASE WHEN statement_medication_name LIKE '%ETORICOXIB%' OR bnf_code LIKE '1001010AJ%' THEN TRUE ELSE FALSE END AS is_etoricoxib,
    
    -- NSAID classification flags
    CASE 
        WHEN bnf_code LIKE '1001010A%' OR bnf_code LIKE '1001010AJ%' OR 
             bnf_code LIKE '1001010AN%' OR bnf_code LIKE '1001010AF%' THEN TRUE
        ELSE FALSE
    END AS is_cox2_selective,
    
    CASE WHEN bnf_code LIKE '100302%' THEN TRUE ELSE FALSE END AS is_topical,
    CASE WHEN bnf_code LIKE '100101%' AND bnf_code NOT LIKE '1001010A%' THEN TRUE ELSE FALSE END AS is_non_selective,
    
    -- High-dose ibuprofen flag (≥2400mg daily)
    CASE 
        WHEN statement_medication_name LIKE '%IBUPROFEN%' AND (
            order_dose LIKE '%2400%' OR order_dose LIKE '%2800%' OR order_dose LIKE '%3200%'
        ) THEN TRUE
        ELSE FALSE
    END AS is_high_dose_ibuprofen,
    
    -- Cardiovascular risk flag (COX-2 selective and high-dose non-selective)
    CASE 
        WHEN bnf_code LIKE '1001010A%' OR  -- COX-2 selective
             (statement_medication_name LIKE '%IBUPROFEN%' AND order_dose LIKE '%2400%') OR  -- High-dose ibuprofen
             statement_medication_name LIKE '%DICLOFENAC%'  -- Diclofenac (high CV risk)
        THEN TRUE
        ELSE FALSE
    END AS is_high_cv_risk,
    
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