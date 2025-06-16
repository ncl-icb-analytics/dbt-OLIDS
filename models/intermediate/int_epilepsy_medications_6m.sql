{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'epilepsy', 'seizure_management']
    )
}}

/*
Epilepsy medication orders from the last 6 months for seizure management monitoring.
Uses cluster ID EPILDRUG_COD for anti-epileptic drugs.
Critical for epilepsy register and therapeutic drug monitoring.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH epilepsy_orders_base AS (
    -- Get all medication orders using EPILDRUG_COD cluster for anti-epileptic drugs
    SELECT 
        mo.person_id,
        mo.sk_patient_id,
        mo.medication_order_id,
        mo.order_date,
        mo.order_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        'EPILDRUG_COD' AS cluster_id
        
    FROM {{ get_medication_orders(cluster_id='EPILDRUG_COD') }} mo
    WHERE mo.order_date >= CURRENT_DATE() - INTERVAL '6 months'
        AND mo.order_date <= CURRENT_DATE()
),

epilepsy_enhanced AS (
    SELECT 
        eob.*,
        
        -- Classify anti-epileptic drug types based on medication names
        CASE 
            WHEN eob.order_medication_name ILIKE '%CARBAMAZEPINE%' THEN 'CARBAMAZEPINE'
            WHEN eob.order_medication_name ILIKE '%PHENYTOIN%' THEN 'PHENYTOIN'
            WHEN eob.order_medication_name ILIKE '%VALPROATE%' 
                OR eob.order_medication_name ILIKE '%EPILIM%' THEN 'VALPROATE'
            WHEN eob.order_medication_name ILIKE '%LAMOTRIGINE%' THEN 'LAMOTRIGINE'
            WHEN eob.order_medication_name ILIKE '%LEVETIRACETAM%' 
                OR eob.order_medication_name ILIKE '%KEPPRA%' THEN 'LEVETIRACETAM'
            WHEN eob.order_medication_name ILIKE '%GABAPENTIN%' THEN 'GABAPENTIN'
            WHEN eob.order_medication_name ILIKE '%PREGABALIN%' 
                OR eob.order_medication_name ILIKE '%LYRICA%' THEN 'PREGABALIN'
            WHEN eob.order_medication_name ILIKE '%TOPIRAMATE%' THEN 'TOPIRAMATE'
            WHEN eob.order_medication_name ILIKE '%PHENOBARBITAL%' 
                OR eob.order_medication_name ILIKE '%PHENOBARBITONE%' THEN 'PHENOBARBITAL'
            WHEN eob.order_medication_name ILIKE '%CLONAZEPAM%' THEN 'CLONAZEPAM'
            WHEN eob.order_medication_name ILIKE '%ETHOSUXIMIDE%' THEN 'ETHOSUXIMIDE'
            ELSE 'OTHER_AED'
        END AS aed_type,
        
        -- Classify by generation
        CASE 
            WHEN eob.order_medication_name ILIKE '%CARBAMAZEPINE%' 
                OR eob.order_medication_name ILIKE '%PHENYTOIN%' 
                OR eob.order_medication_name ILIKE '%VALPROATE%' 
                OR eob.order_medication_name ILIKE '%PHENOBARBITAL%' 
                OR eob.order_medication_name ILIKE '%ETHOSUXIMIDE%' THEN 'FIRST_GENERATION'
            WHEN eob.order_medication_name ILIKE '%LAMOTRIGINE%' 
                OR eob.order_medication_name ILIKE '%LEVETIRACETAM%' 
                OR eob.order_medication_name ILIKE '%GABAPENTIN%' 
                OR eob.order_medication_name ILIKE '%PREGABALIN%' 
                OR eob.order_medication_name ILIKE '%TOPIRAMATE%' THEN 'NEWER_GENERATION'
            ELSE 'UNKNOWN_GENERATION'
        END AS aed_generation,
        
        -- Therapeutic drug monitoring requirements
        CASE 
            WHEN eob.order_medication_name ILIKE '%PHENYTOIN%' 
                OR eob.order_medication_name ILIKE '%CARBAMAZEPINE%' 
                OR eob.order_medication_name ILIKE '%VALPROATE%' 
                OR eob.order_medication_name ILIKE '%PHENOBARBITAL%' THEN TRUE
            ELSE FALSE
        END AS requires_tdm,
        
        -- Teratogenicity risk assessment
        CASE 
            WHEN eob.order_medication_name ILIKE '%VALPROATE%' THEN 'HIGH_RISK'
            WHEN eob.order_medication_name ILIKE '%CARBAMAZEPINE%' 
                OR eob.order_medication_name ILIKE '%PHENYTOIN%' 
                OR eob.order_medication_name ILIKE '%TOPIRAMATE%' THEN 'MODERATE_RISK'
            WHEN eob.order_medication_name ILIKE '%LAMOTRIGINE%' 
                OR eob.order_medication_name ILIKE '%LEVETIRACETAM%' THEN 'LOW_RISK'
            ELSE 'UNKNOWN_RISK'
        END AS teratogenicity_risk,
        
        -- Brand switching considerations (narrow therapeutic index)
        CASE 
            WHEN eob.order_medication_name ILIKE '%PHENYTOIN%' 
                OR eob.order_medication_name ILIKE '%CARBAMAZEPINE%' 
                OR eob.order_medication_name ILIKE '%LAMOTRIGINE%' THEN TRUE
            ELSE FALSE
        END AS requires_brand_consistency,
        
        -- Seizure type indication
        CASE 
            WHEN eob.order_medication_name ILIKE '%ETHOSUXIMIDE%' THEN 'ABSENCE_SEIZURES'
            WHEN eob.order_medication_name ILIKE '%VALPROATE%' THEN 'GENERALISED_SEIZURES'
            WHEN eob.order_medication_name ILIKE '%CARBAMAZEPINE%' 
                OR eob.order_medication_name ILIKE '%LAMOTRIGINE%' THEN 'FOCAL_SEIZURES'
            ELSE 'BROAD_SPECTRUM'
        END AS seizure_indication,
        
        -- Recency flags for monitoring
        TRUE AS is_recent_6m,
        eob.order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
        eob.order_date >= CURRENT_DATE() - INTERVAL '1 month' AS is_recent_1m
        
    FROM epilepsy_orders_base eob
),

epilepsy_with_counts AS (
    SELECT 
        ee.*,
        
        -- Count of epilepsy medication orders per person in 6 months
        COUNT(*) OVER (PARTITION BY ee.person_id) AS recent_order_count_6m,
        COUNT(DISTINCT ee.aed_type) OVER (PARTITION BY ee.person_id) AS unique_aed_types,
        
        -- Polytherapy identification
        CASE 
            WHEN COUNT(DISTINCT ee.aed_type) OVER (PARTITION BY ee.person_id) > 1 THEN TRUE
            ELSE FALSE
        END AS is_polytherapy,
        
        -- High-risk combination flags
        CASE 
            WHEN COUNT(*) FILTER (WHERE ee.requires_tdm = TRUE) OVER (PARTITION BY ee.person_id) > 1 THEN TRUE
            ELSE FALSE
        END AS multiple_tdm_drugs
        
    FROM epilepsy_enhanced ee
)

-- Final selection with ALL persons - no filtering by active status
-- Essential for epilepsy register and therapeutic monitoring
SELECT 
    ewc.*,
    
    -- Add person demographics for reference
    p.current_practice_id,
    p.total_patients
    
FROM epilepsy_with_counts ewc
-- Join to main person dimension (includes ALL persons)
LEFT JOIN {{ ref('dim_person') }} p
    ON ewc.person_id = p.person_id
    
-- Order by person and date for analysis
ORDER BY ewc.person_id, ewc.order_date DESC 