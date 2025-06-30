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
        mo.id AS medication_order_id,
        ms.id AS medication_statement_id,
        pp.person_id,
        mo.clinical_effective_date::DATE AS order_date,
        mo.medication_name AS order_medication_name,
        mo.dose AS order_dose,
        mo.quantity_value AS order_quantity_value,
        mo.quantity_unit AS order_quantity_unit,
        mo.duration_days AS order_duration_days,
        ms.medication_name AS statement_medication_name,
        mc.concept_code AS mapped_concept_code,
        mc.code_description AS mapped_concept_display,
        'EPILDRUG_COD' AS cluster_id

    FROM {{ ref('stg_olids_medication_statement') }} AS ms
    INNER JOIN {{ ref('stg_olids_medication_order') }} AS mo
        ON ms.id = mo.medication_statement_id
    INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
        ON ms.medication_statement_core_concept_id = mc.source_code_id
    INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
        ON mo.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON mo.patient_id = p.id
    WHERE
        mc.cluster_id = 'EPILDRUG_COD'
        AND mo.clinical_effective_date::DATE
        >= CURRENT_DATE() - INTERVAL '6 months'
        AND mo.clinical_effective_date::DATE <= CURRENT_DATE()
),

epilepsy_enhanced AS (
    SELECT
        eob.*,

        -- Classify anti-epileptic drug types based on medication names
        TRUE AS is_recent_6m,

        -- Classify by generation
        CASE
            WHEN
                eob.order_medication_name ILIKE '%CARBAMAZEPINE%'
                THEN 'CARBAMAZEPINE'
            WHEN eob.order_medication_name ILIKE '%PHENYTOIN%' THEN 'PHENYTOIN'
            WHEN
                eob.order_medication_name ILIKE '%VALPROATE%'
                OR eob.order_medication_name ILIKE '%EPILIM%' THEN 'VALPROATE'
            WHEN
                eob.order_medication_name ILIKE '%LAMOTRIGINE%'
                THEN 'LAMOTRIGINE'
            WHEN
                eob.order_medication_name ILIKE '%LEVETIRACETAM%'
                OR eob.order_medication_name ILIKE '%KEPPRA%'
                THEN 'LEVETIRACETAM'
            WHEN
                eob.order_medication_name ILIKE '%GABAPENTIN%'
                THEN 'GABAPENTIN'
            WHEN
                eob.order_medication_name ILIKE '%PREGABALIN%'
                OR eob.order_medication_name ILIKE '%LYRICA%' THEN 'PREGABALIN'
            WHEN
                eob.order_medication_name ILIKE '%TOPIRAMATE%'
                THEN 'TOPIRAMATE'
            WHEN
                eob.order_medication_name ILIKE '%PHENOBARBITAL%'
                OR eob.order_medication_name ILIKE '%PHENOBARBITONE%'
                THEN 'PHENOBARBITAL'
            WHEN
                eob.order_medication_name ILIKE '%CLONAZEPAM%'
                THEN 'CLONAZEPAM'
            WHEN
                eob.order_medication_name ILIKE '%ETHOSUXIMIDE%'
                THEN 'ETHOSUXIMIDE'
            ELSE 'OTHER_AED'
        END AS aed_type,

        -- Therapeutic drug monitoring requirements
        CASE
            WHEN
                eob.order_medication_name ILIKE '%CARBAMAZEPINE%'
                OR eob.order_medication_name ILIKE '%PHENYTOIN%'
                OR eob.order_medication_name ILIKE '%VALPROATE%'
                OR eob.order_medication_name ILIKE '%PHENOBARBITAL%'
                OR eob.order_medication_name ILIKE '%ETHOSUXIMIDE%'
                THEN 'FIRST_GENERATION'
            WHEN
                eob.order_medication_name ILIKE '%LAMOTRIGINE%'
                OR eob.order_medication_name ILIKE '%LEVETIRACETAM%'
                OR eob.order_medication_name ILIKE '%GABAPENTIN%'
                OR eob.order_medication_name ILIKE '%PREGABALIN%'
                OR eob.order_medication_name ILIKE '%TOPIRAMATE%'
                THEN 'NEWER_GENERATION'
            ELSE 'UNKNOWN_GENERATION'
        END AS aed_generation,

        -- Teratogenicity risk assessment
        COALESCE(
            eob.order_medication_name ILIKE '%PHENYTOIN%'
            OR eob.order_medication_name ILIKE '%CARBAMAZEPINE%'
            OR eob.order_medication_name ILIKE '%VALPROATE%'
            OR eob.order_medication_name ILIKE '%PHENOBARBITAL%',
            FALSE
        ) AS requires_tdm,

        -- Brand switching considerations (narrow therapeutic index)
        CASE
            WHEN eob.order_medication_name ILIKE '%VALPROATE%' THEN 'HIGH_RISK'
            WHEN
                eob.order_medication_name ILIKE '%CARBAMAZEPINE%'
                OR eob.order_medication_name ILIKE '%PHENYTOIN%'
                OR eob.order_medication_name ILIKE '%TOPIRAMATE%'
                THEN 'MODERATE_RISK'
            WHEN
                eob.order_medication_name ILIKE '%LAMOTRIGINE%'
                OR eob.order_medication_name ILIKE '%LEVETIRACETAM%'
                THEN 'LOW_RISK'
            ELSE 'UNKNOWN_RISK'
        END AS teratogenicity_risk,

        -- Seizure type indication
        COALESCE(
            eob.order_medication_name ILIKE '%PHENYTOIN%'
            OR eob.order_medication_name ILIKE '%CARBAMAZEPINE%'
            OR eob.order_medication_name ILIKE '%LAMOTRIGINE%',
            FALSE
        ) AS requires_brand_consistency,

        -- Recency flags for monitoring
        CASE
            WHEN
                eob.order_medication_name ILIKE '%ETHOSUXIMIDE%'
                THEN 'ABSENCE_SEIZURES'
            WHEN
                eob.order_medication_name ILIKE '%VALPROATE%'
                THEN 'GENERALISED_SEIZURES'
            WHEN
                eob.order_medication_name ILIKE '%CARBAMAZEPINE%'
                OR eob.order_medication_name ILIKE '%LAMOTRIGINE%'
                THEN 'FOCAL_SEIZURES'
            ELSE 'BROAD_SPECTRUM'
        END AS seizure_indication,
        eob.order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
        eob.order_date >= CURRENT_DATE() - INTERVAL '1 month' AS is_recent_1m

    FROM epilepsy_orders_base AS eob
),

epilepsy_with_counts AS (
    SELECT
        ee.*,

        -- Count of epilepsy medication orders per person in 6 months
        COUNT(*) OVER (PARTITION BY ee.person_id) AS recent_order_count_6m,
        COUNT(DISTINCT ee.aed_type)
            OVER (PARTITION BY ee.person_id)
            AS unique_aed_types,

        -- Polytherapy identification
        COALESCE(
            COUNT(DISTINCT ee.aed_type) OVER (PARTITION BY ee.person_id)
            > 1,
            FALSE
        ) AS is_polytherapy,

        -- High-risk combination flags
        COALESCE(SUM(CASE WHEN ee.requires_tdm = TRUE THEN 1 ELSE 0 END)
            OVER (PARTITION BY ee.person_id)
        > 1,
        FALSE) AS multiple_tdm_drugs

    FROM epilepsy_enhanced AS ee
)

-- Final selection with ALL persons - no filtering by active status
-- Essential for epilepsy register and therapeutic monitoring
SELECT
    ewc.*,

    -- Add person demographics for reference
    p.current_practice_id,
    p.total_patients

FROM epilepsy_with_counts AS ewc
-- Join to main person dimension (includes ALL persons)
LEFT JOIN {{ ref('dim_person') }} AS p
    ON ewc.person_id = p.person_id

-- Order by person and date for analysis
ORDER BY ewc.person_id ASC, ewc.order_date DESC
