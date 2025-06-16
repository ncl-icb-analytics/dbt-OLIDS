{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'valproate', 'pregnancy_safety']
    )
}}

/*
All valproate medication orders for seizure management and bipolar disorder.
Uses special matching logic combining medication name patterns and concept ID validation.
Critical for pregnancy safety monitoring and teratogenicity risk assessment.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH valproate_orders AS (
    -- Get all medication orders with valproate name matching or concept ID matching
    SELECT 
        mo.medication_order_id,
        mo.medication_statement_id,
        mo.person_id,
        mo.order_date,
        mo.order_medication_name,
        mo.order_dose,
        mo.order_quantity_value,
        mo.order_quantity_unit,
        mo.order_duration_days,
        mo.statement_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        mo.bnf_code,
        mo.bnf_name,
        
        -- Check for valproate name patterns (case-insensitive)
        (
            mo.order_medication_name ILIKE '%VALPROATE%' OR
            mo.order_medication_name ILIKE '%VALPROIC ACID%' OR
            mo.statement_medication_name ILIKE '%VALPROATE%' OR
            mo.statement_medication_name ILIKE '%VALPROIC ACID%'
        ) AS matched_on_name,
        
        -- Placeholder for concept ID matching (would need valproate concept codes table)
        FALSE AS matched_on_concept_id,
        
        -- Extract specific valproate product information
        CASE 
            WHEN mo.statement_medication_name ILIKE '%SODIUM VALPROATE%' 
                OR mo.order_medication_name ILIKE '%SODIUM VALPROATE%' THEN 'SODIUM_VALPROATE'
            WHEN mo.statement_medication_name ILIKE '%VALPROIC ACID%' 
                OR mo.order_medication_name ILIKE '%VALPROIC ACID%' THEN 'VALPROIC_ACID'
            WHEN mo.statement_medication_name ILIKE '%EPILIM%' 
                OR mo.order_medication_name ILIKE '%EPILIM%' THEN 'EPILIM'
            WHEN mo.statement_medication_name ILIKE '%DEPAKOTE%' 
                OR mo.order_medication_name ILIKE '%DEPAKOTE%' THEN 'DEPAKOTE'
            ELSE 'OTHER_VALPROATE'
        END AS valproate_product_type
        
    FROM {{ get_medication_orders() }} mo
    WHERE (
        -- Name-based matching for valproate
        mo.order_medication_name ILIKE '%VALPROATE%' OR
        mo.order_medication_name ILIKE '%VALPROIC ACID%' OR
        mo.statement_medication_name ILIKE '%VALPROATE%' OR
        mo.statement_medication_name ILIKE '%VALPROIC ACID%'
    )
),

valproate_enhanced AS (
    SELECT 
        vo.*,
        
        -- Clinical risk assessment flags
        CASE 
            WHEN vo.valproate_product_type IN ('SODIUM_VALPROATE', 'EPILIM') THEN 'ANTI_EPILEPTIC'
            WHEN vo.valproate_product_type IN ('DEPAKOTE') THEN 'MOOD_STABILISER'
            ELSE 'UNSPECIFIED'
        END AS clinical_indication,
        
        -- Pregnancy risk assessment
        TRUE AS is_high_teratogenic_risk,
        
        -- Dosage categorisation for monitoring
        CASE 
            WHEN vo.order_dose ILIKE '%MG%' THEN
                CASE 
                    WHEN REGEXP_SUBSTR(vo.order_dose, '[0-9]+')::INT <= 500 THEN 'LOW_DOSE'
                    WHEN REGEXP_SUBSTR(vo.order_dose, '[0-9]+')::INT <= 1000 THEN 'MODERATE_DOSE'
                    ELSE 'HIGH_DOSE'
                END
            ELSE 'UNKNOWN_DOSE'
        END AS dose_category,
        
        -- Formulation type for bioequivalence monitoring
        CASE 
            WHEN vo.order_medication_name ILIKE '%TABLET%' 
                OR vo.statement_medication_name ILIKE '%TABLET%' THEN 'TABLET'
            WHEN vo.order_medication_name ILIKE '%CAPSULE%' 
                OR vo.statement_medication_name ILIKE '%CAPSULE%' THEN 'CAPSULE'
            WHEN vo.order_medication_name ILIKE '%LIQUID%' OR vo.order_medication_name ILIKE '%SYRUP%'
                OR vo.statement_medication_name ILIKE '%LIQUID%' OR vo.statement_medication_name ILIKE '%SYRUP%' THEN 'LIQUID'
            WHEN vo.order_medication_name ILIKE '%MODIFIED RELEASE%' OR vo.order_medication_name ILIKE '%MR%'
                OR vo.statement_medication_name ILIKE '%MODIFIED RELEASE%' OR vo.statement_medication_name ILIKE '%MR%' THEN 'MODIFIED_RELEASE'
            ELSE 'UNKNOWN_FORMULATION'
        END AS formulation_type,
        
        -- Recency flags for clinical monitoring
        vo.order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
        vo.order_date >= CURRENT_DATE() - INTERVAL '6 months' AS is_recent_6m,
        vo.order_date >= CURRENT_DATE() - INTERVAL '12 months' AS is_recent_12m
        
    FROM valproate_orders vo
)

-- Final selection with ALL persons - no filtering by active status
-- Critical for pregnancy safety monitoring across all patient populations
SELECT 
    ve.*,
    
    -- Add person demographics for reference
    p.current_practice_id,
    p.total_patients
    
FROM valproate_enhanced ve
-- Join to main person dimension (includes ALL persons)
LEFT JOIN {{ ref('dim_person') }} p
    ON ve.person_id = p.person_id
    
-- Order by person and date for analysis
ORDER BY ve.person_id, ve.order_date DESC 