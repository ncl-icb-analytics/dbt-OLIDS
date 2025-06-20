{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'asthma', 'qof']
    )
}}

/*
Asthma medication orders from the last 12 months for QOF asthma care monitoring.
Uses cluster ID ASTTRT_COD for asthma treatment medications.
Critical for asthma register and QOF quality indicators.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH asthma_orders_base AS (
    -- Get all medication orders using ASTTRT_COD cluster for asthma treatments
    SELECT 
        mo.person_id,
        mo.sk_patient_id,
        mo.medication_order_id,
        mo.order_date,
        mo.order_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        'ASTTRT_COD' AS cluster_id
        
    FROM ({{ get_medication_orders(cluster_id='ASTTRT_COD') }}) mo
    WHERE mo.order_date >= CURRENT_DATE() - INTERVAL '12 months'
        AND mo.order_date <= CURRENT_DATE()
),

asthma_enhanced AS (
    SELECT 
        aob.*,
        
        -- Classify asthma medication types based on medication names
        CASE 
            WHEN aob.order_medication_name ILIKE '%SALBUTAMOL%' 
                OR aob.order_medication_name ILIKE '%VENTOLIN%' THEN 'SABA'
            WHEN aob.order_medication_name ILIKE '%SALMETEROL%' 
                OR aob.order_medication_name ILIKE '%FORMOTEROL%' 
                OR aob.order_medication_name ILIKE '%INDACATEROL%' THEN 'LABA'
            WHEN aob.order_medication_name ILIKE '%BECLOMETASONE%' 
                OR aob.order_medication_name ILIKE '%BUDESONIDE%' 
                OR aob.order_medication_name ILIKE '%FLUTICASONE%' THEN 'ICS'
            WHEN aob.order_medication_name ILIKE '%SYMBICORT%' 
                OR aob.order_medication_name ILIKE '%SERETIDE%' 
                OR aob.order_medication_name ILIKE '%FOSTAIR%' THEN 'ICS_LABA_COMBINATION'
            WHEN aob.order_medication_name ILIKE '%MONTELUKAST%' 
                OR aob.order_medication_name ILIKE '%ZAFIRLUKAST%' THEN 'LEUKOTRIENE_ANTAGONIST'
            WHEN aob.order_medication_name ILIKE '%THEOPHYLLINE%' 
                OR aob.order_medication_name ILIKE '%AMINOPHYLLINE%' THEN 'METHYLXANTHINE'
            WHEN aob.order_medication_name ILIKE '%PREDNISOLONE%' 
                OR aob.order_medication_name ILIKE '%HYDROCORTISONE%' THEN 'ORAL_STEROID'
            ELSE 'OTHER_ASTHMA_TREATMENT'
        END AS asthma_medication_type,
        
        -- Identify reliever vs controller medications
        CASE 
            WHEN aob.order_medication_name ILIKE '%SALBUTAMOL%' 
                OR aob.order_medication_name ILIKE '%TERBUTALINE%' THEN 'RELIEVER'
            WHEN aob.order_medication_name ILIKE '%BECLOMETASONE%' 
                OR aob.order_medication_name ILIKE '%BUDESONIDE%' 
                OR aob.order_medication_name ILIKE '%FLUTICASONE%'
                OR aob.order_medication_name ILIKE '%SYMBICORT%' 
                OR aob.order_medication_name ILIKE '%SERETIDE%' 
                OR aob.order_medication_name ILIKE '%MONTELUKAST%' THEN 'CONTROLLER'
            ELSE 'OTHER'
        END AS medication_role,
        
        -- QOF asthma care process indicators
        TRUE AS is_asthma_treatment,
        
        -- Inhaler technique assessment flags
        CASE 
            WHEN aob.order_medication_name ILIKE '%MDI%' 
                OR aob.order_medication_name ILIKE '%EVOHALER%' THEN 'MDI'
            WHEN aob.order_medication_name ILIKE '%DPI%' 
                OR aob.order_medication_name ILIKE '%TURBOHALER%' 
                OR aob.order_medication_name ILIKE '%ACCUHALER%' THEN 'DPI'
            WHEN aob.order_medication_name ILIKE '%NEBULISER%' 
                OR aob.order_medication_name ILIKE '%NEBULES%' THEN 'NEBULISER'
            ELSE 'UNKNOWN_DEVICE'
        END AS inhaler_device_type,
        
        -- MART therapy identification (Maintenance and Reliever Therapy)
        CASE 
            WHEN aob.order_medication_name ILIKE '%SYMBICORT%' 
                AND aob.order_medication_name ILIKE '%MART%' THEN TRUE
            ELSE FALSE
        END AS is_mart_therapy,
        
        -- Recency flags for monitoring
        TRUE AS is_recent_12m,
        aob.order_date >= CURRENT_DATE() - INTERVAL '6 months' AS is_recent_6m,
        aob.order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m
        
    FROM asthma_orders_base aob
),

asthma_with_counts AS (
    SELECT 
        ae.*,
        
        -- Count of asthma medication orders per person in 12 months
        COUNT(*) OVER (PARTITION BY ae.person_id) AS recent_order_count_12m,
        COUNT(*) OVER (PARTITION BY ae.person_id, ae.asthma_medication_type) AS medication_type_count,
        
        -- QOF indicators
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY ae.person_id) >= 2 THEN TRUE
            ELSE FALSE
        END AS has_repeat_prescriptions
        
    FROM asthma_enhanced ae
)

-- Final selection with ALL persons - no filtering by active status
-- Essential for QOF asthma register and care process monitoring
SELECT 
    awc.*,
    
    -- Add person demographics for reference
    p.current_practice_id,
    p.total_patients
    
FROM asthma_with_counts awc
-- Join to main person dimension (includes ALL persons)
LEFT JOIN {{ ref('dim_person') }} p
    ON awc.person_id = p.person_id
    
-- Order by person and date for analysis
ORDER BY awc.person_id, awc.order_date DESC 