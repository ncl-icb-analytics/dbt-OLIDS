{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'allergy', 'antihistamines']
    )
}}

/*
All allergy medication orders including antihistamines and allergy treatments.
Uses BNF classification (3.4) for antihistamines and related allergy medications.
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
        
    FROM {{ get_medication_orders(bnf_code='0304') }}
),

allergy_enhanced AS (
    SELECT 
        bo.*,
        
        -- Classify antihistamine types based on medication names
        CASE 
            WHEN bo.statement_medication_name ILIKE '%CETIRIZINE%' 
                OR bo.order_medication_name ILIKE '%CETIRIZINE%' THEN 'CETIRIZINE'
            WHEN bo.statement_medication_name ILIKE '%LORATADINE%' 
                OR bo.order_medication_name ILIKE '%LORATADINE%' THEN 'LORATADINE'
            WHEN bo.statement_medication_name ILIKE '%FEXOFENADINE%' 
                OR bo.order_medication_name ILIKE '%FEXOFENADINE%' THEN 'FEXOFENADINE'
            WHEN bo.statement_medication_name ILIKE '%CHLORPHENAMINE%' 
                OR bo.order_medication_name ILIKE '%CHLORPHENAMINE%' THEN 'CHLORPHENAMINE'
            WHEN bo.statement_medication_name ILIKE '%PROMETHAZINE%' 
                OR bo.order_medication_name ILIKE '%PROMETHAZINE%' THEN 'PROMETHAZINE'
            WHEN bo.statement_medication_name ILIKE '%HYDROXYZINE%' 
                OR bo.order_medication_name ILIKE '%HYDROXYZINE%' THEN 'HYDROXYZINE'
            WHEN bo.statement_medication_name ILIKE '%DESLORATADINE%' 
                OR bo.order_medication_name ILIKE '%DESLORATADINE%' THEN 'DESLORATADINE'
            WHEN bo.statement_medication_name ILIKE '%LEVOCETIRIZINE%' 
                OR bo.order_medication_name ILIKE '%LEVOCETIRIZINE%' THEN 'LEVOCETIRIZINE'
            WHEN bo.statement_medication_name ILIKE '%BILASTINE%' 
                OR bo.order_medication_name ILIKE '%BILASTINE%' THEN 'BILASTINE'
            WHEN bo.statement_medication_name ILIKE '%ACRIVASTINE%' 
                OR bo.order_medication_name ILIKE '%ACRIVASTINE%' THEN 'ACRIVASTINE'
            ELSE 'OTHER_ANTIHISTAMINE'
        END AS antihistamine_type,
        
        -- Classify by generation (sedating vs non-sedating)
        CASE 
            WHEN bo.statement_medication_name ILIKE '%CETIRIZINE%' 
                OR bo.statement_medication_name ILIKE '%LORATADINE%' 
                OR bo.statement_medication_name ILIKE '%FEXOFENADINE%' 
                OR bo.statement_medication_name ILIKE '%DESLORATADINE%' 
                OR bo.statement_medication_name ILIKE '%LEVOCETIRIZINE%' 
                OR bo.statement_medication_name ILIKE '%BILASTINE%' 
                OR bo.statement_medication_name ILIKE '%ACRIVASTINE%' 
                OR bo.order_medication_name ILIKE '%CETIRIZINE%' 
                OR bo.order_medication_name ILIKE '%LORATADINE%' 
                OR bo.order_medication_name ILIKE '%FEXOFENADINE%' 
                OR bo.order_medication_name ILIKE '%DESLORATADINE%' 
                OR bo.order_medication_name ILIKE '%LEVOCETIRIZINE%' 
                OR bo.order_medication_name ILIKE '%BILASTINE%' 
                OR bo.order_medication_name ILIKE '%ACRIVASTINE%' THEN 'NON_SEDATING'
            WHEN bo.statement_medication_name ILIKE '%CHLORPHENAMINE%' 
                OR bo.statement_medication_name ILIKE '%PROMETHAZINE%' 
                OR bo.statement_medication_name ILIKE '%HYDROXYZINE%' 
                OR bo.order_medication_name ILIKE '%CHLORPHENAMINE%' 
                OR bo.order_medication_name ILIKE '%PROMETHAZINE%' 
                OR bo.order_medication_name ILIKE '%HYDROXYZINE%' THEN 'SEDATING'
            ELSE 'UNKNOWN_SEDATION'
        END AS sedation_profile,
        
        -- Route of administration
        CASE 
            WHEN bo.order_medication_name ILIKE '%TABLET%' 
                OR bo.order_medication_name ILIKE '%CAPSULE%' 
                OR bo.statement_medication_name ILIKE '%TABLET%' 
                OR bo.statement_medication_name ILIKE '%CAPSULE%' THEN 'ORAL'
            WHEN bo.order_medication_name ILIKE '%SYRUP%' 
                OR bo.order_medication_name ILIKE '%LIQUID%' 
                OR bo.order_medication_name ILIKE '%SOLUTION%' 
                OR bo.statement_medication_name ILIKE '%SYRUP%' 
                OR bo.statement_medication_name ILIKE '%LIQUID%' 
                OR bo.statement_medication_name ILIKE '%SOLUTION%' THEN 'ORAL_LIQUID'
            WHEN bo.order_medication_name ILIKE '%INJECTION%' 
                OR bo.order_medication_name ILIKE '%AMPOULE%' 
                OR bo.statement_medication_name ILIKE '%INJECTION%' 
                OR bo.statement_medication_name ILIKE '%AMPOULE%' THEN 'INJECTION'
            WHEN bo.order_medication_name ILIKE '%CREAM%' 
                OR bo.order_medication_name ILIKE '%OINTMENT%' 
                OR bo.statement_medication_name ILIKE '%CREAM%' 
                OR bo.statement_medication_name ILIKE '%OINTMENT%' THEN 'TOPICAL'
            ELSE 'UNKNOWN_ROUTE'
        END AS route_of_administration,
        
        -- Clinical indication flags
        CASE 
            WHEN bo.statement_medication_name ILIKE '%ALLERGIC RHINITIS%' 
                OR bo.order_medication_name ILIKE '%ALLERGIC RHINITIS%' 
                OR bo.statement_medication_name ILIKE '%HAY FEVER%' 
                OR bo.order_medication_name ILIKE '%HAY FEVER%' THEN 'ALLERGIC_RHINITIS'
            WHEN bo.statement_medication_name ILIKE '%URTICARIA%' 
                OR bo.order_medication_name ILIKE '%URTICARIA%' 
                OR bo.statement_medication_name ILIKE '%HIVES%' 
                OR bo.order_medication_name ILIKE '%HIVES%' THEN 'URTICARIA'
            WHEN bo.statement_medication_name ILIKE '%ECZEMA%' 
                OR bo.order_medication_name ILIKE '%ECZEMA%' 
                OR bo.statement_medication_name ILIKE '%DERMATITIS%' 
                OR bo.order_medication_name ILIKE '%DERMATITIS%' THEN 'ECZEMA_DERMATITIS'
            ELSE 'GENERAL_ALLERGY'
        END AS clinical_indication,
        
        -- Age-appropriate flags
        CASE 
            WHEN bo.statement_medication_name ILIKE '%PAEDIATRIC%' 
                OR bo.order_medication_name ILIKE '%PAEDIATRIC%' 
                OR bo.statement_medication_name ILIKE '%CHILDREN%' 
                OR bo.order_medication_name ILIKE '%CHILDREN%' THEN TRUE
            ELSE FALSE
        END AS is_paediatric_formulation,
        
        -- Over-the-counter availability
        CASE 
            WHEN bo.antihistamine_type IN ('CETIRIZINE', 'LORATADINE', 'CHLORPHENAMINE', 'ACRIVASTINE') THEN TRUE
            ELSE FALSE
        END AS is_otc_available,
        
        -- Drowsiness warning required
        CASE 
            WHEN bo.sedation_profile = 'SEDATING' THEN TRUE
            ELSE FALSE
        END AS requires_drowsiness_warning,
        
        -- Recency flags for monitoring
        bo.order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
        bo.order_date >= CURRENT_DATE() - INTERVAL '6 months' AS is_recent_6m,
        bo.order_date >= CURRENT_DATE() - INTERVAL '12 months' AS is_recent_12m
        
    FROM base_orders bo
),

allergy_with_counts AS (
    SELECT 
        ae.*,
        
        -- Count of allergy medication orders per person
        COUNT(*) OVER (PARTITION BY ae.person_id) AS total_allergy_orders,
        COUNT(*) OVER (PARTITION BY ae.person_id, ae.antihistamine_type) AS orders_per_type,
        
        -- Chronic allergy management indicators
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY ae.person_id) >= 3 THEN TRUE
            ELSE FALSE
        END AS is_chronic_allergy_treatment,
        
        -- Multiple antihistamine use
        COUNT(DISTINCT ae.antihistamine_type) OVER (PARTITION BY ae.person_id) AS unique_antihistamine_types
        
    FROM allergy_enhanced ae
)

-- Final selection with ALL persons - no filtering by active status
-- Important for allergy management and medication history
SELECT 
    awc.*,
    
    -- Add person demographics for reference
    p.current_practice_id,
    p.total_patients
    
FROM allergy_with_counts awc
-- Join to main person dimension (includes ALL persons)
LEFT JOIN {{ ref('dim_person') }} p
    ON awc.person_id = p.person_id
    
-- Order by person and date for analysis
ORDER BY awc.person_id, awc.order_date DESC 