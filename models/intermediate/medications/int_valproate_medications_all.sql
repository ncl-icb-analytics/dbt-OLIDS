{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'valproate', 'pregnancy_safety'])
}}

/*
All valproate medication orders for seizure management and bipolar disorder.
Uses special matching logic combining medication name patterns and concept ID validation.
Critical for pregnancy safety monitoring and teratogenicity risk assessment.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_medication_orders AS (
    -- Get base medication order and statement data following legacy pattern
    SELECT DISTINCT
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
        mc.concept_id AS mapped_concept_id,
        vp.valproate_product_term,
        NULL AS bnf_code,
        NULL AS bnf_name
    FROM {{ ref('stg_olids_medication_order') }} AS mo
    INNER JOIN {{ ref('stg_olids_medication_statement') }} AS ms
        ON mo.medication_statement_id = ms.id
    INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
        ON mo.patient_id = pp.patient_id
    LEFT JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
        ON ms.medication_statement_core_concept_id = mc.source_code_id
    LEFT JOIN {{ ref('stg_codesets_valproate_prog_codes') }} AS vp
        ON
            mc.concept_code = vp.code
            AND vp.code_category = 'DRUG'
    WHERE (
        -- Name-based matching for valproate (following legacy logic)
        mo.medication_name ILIKE '%VALPROATE%'
        OR mo.medication_name ILIKE '%VALPROIC ACID%'
        OR ms.medication_name ILIKE '%VALPROATE%'
        OR ms.medication_name ILIKE '%VALPROIC ACID%'
    )
    OR (
        -- Concept ID matching via VALPROATE_PROG_CODES
        vp.code IS NOT NULL
    )
),

valproate_orders AS (
    SELECT
        bmo.*,

        -- Check for valproate name patterns (case-insensitive)
        (
            bmo.order_medication_name ILIKE '%VALPROATE%'
            OR bmo.order_medication_name ILIKE '%VALPROIC ACID%'
            OR bmo.statement_medication_name ILIKE '%VALPROATE%'
            OR bmo.statement_medication_name ILIKE '%VALPROIC ACID%'
        ) AS matched_on_name,

        -- Concept ID matching flag
        (bmo.valproate_product_term IS NOT NULL) AS matched_on_concept_id,

        -- Extract specific valproate product information
        CASE
            WHEN
                bmo.valproate_product_term IS NOT NULL
                THEN bmo.valproate_product_term
            WHEN
                bmo.statement_medication_name ILIKE '%SODIUM VALPROATE%'
                OR bmo.order_medication_name ILIKE '%SODIUM VALPROATE%'
                THEN 'SODIUM_VALPROATE'
            WHEN
                bmo.statement_medication_name ILIKE '%VALPROIC ACID%'
                OR bmo.order_medication_name ILIKE '%VALPROIC ACID%'
                THEN 'VALPROIC_ACID'
            WHEN
                bmo.statement_medication_name ILIKE '%EPILIM%'
                OR bmo.order_medication_name ILIKE '%EPILIM%' THEN 'EPILIM'
            WHEN
                bmo.statement_medication_name ILIKE '%DEPAKOTE%'
                OR bmo.order_medication_name ILIKE '%DEPAKOTE%' THEN 'DEPAKOTE'
            ELSE 'OTHER_VALPROATE'
        END AS valproate_product_type

    FROM base_medication_orders AS bmo
),

valproate_enhanced AS (
    SELECT
        vo.*,

        -- Clinical risk assessment flags
        TRUE AS is_high_teratogenic_risk,

        -- Pregnancy risk assessment
        CASE
            WHEN
                vo.valproate_product_type IN ('SODIUM_VALPROATE', 'EPILIM')
                THEN 'ANTI_EPILEPTIC'
            WHEN
                vo.valproate_product_type IN ('DEPAKOTE')
                THEN 'MOOD_STABILISER'
            ELSE 'UNSPECIFIED'
        END AS clinical_indication,

        -- Dosage categorisation for monitoring
        CASE
            WHEN vo.order_dose ILIKE '%MG%'
                THEN
                    CASE
                        WHEN
                            REGEXP_SUBSTR(vo.order_dose, '[0-9]+')::INT <= 500
                            THEN 'LOW_DOSE'
                        WHEN
                            REGEXP_SUBSTR(vo.order_dose, '[0-9]+')::INT <= 1000
                            THEN 'MODERATE_DOSE'
                        ELSE 'HIGH_DOSE'
                    END
            ELSE 'UNKNOWN_DOSE'
        END AS dose_category,

        -- Formulation type for bioequivalence monitoring
        CASE
            WHEN
                vo.order_medication_name ILIKE '%TABLET%'
                OR vo.statement_medication_name ILIKE '%TABLET%' THEN 'TABLET'
            WHEN
                vo.order_medication_name ILIKE '%CAPSULE%'
                OR vo.statement_medication_name ILIKE '%CAPSULE%' THEN 'CAPSULE'
            WHEN
                vo.order_medication_name ILIKE '%LIQUID%'
                OR vo.order_medication_name ILIKE '%SYRUP%'
                OR vo.statement_medication_name ILIKE '%LIQUID%'
                OR vo.statement_medication_name ILIKE '%SYRUP%'
                THEN 'LIQUID'
            WHEN
                vo.order_medication_name ILIKE '%MODIFIED RELEASE%'
                OR vo.order_medication_name ILIKE '%MR%'
                OR vo.statement_medication_name ILIKE '%MODIFIED RELEASE%'
                OR vo.statement_medication_name ILIKE '%MR%'
                THEN 'MODIFIED_RELEASE'
            ELSE 'UNKNOWN_FORMULATION'
        END AS formulation_type,

        -- Recency flags for clinical monitoring
        vo.order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
        vo.order_date >= CURRENT_DATE() - INTERVAL '6 months' AS is_recent_6m,
        vo.order_date >= CURRENT_DATE() - INTERVAL '12 months' AS is_recent_12m

    FROM valproate_orders AS vo
)

-- Final selection with ALL persons - no filtering by active status
-- Critical for pregnancy safety monitoring across all patient populations
SELECT
    ve.*,

    -- Add person demographics for reference
    p.current_practice_id,
    p.total_patients

FROM valproate_enhanced AS ve
-- Join to main person dimension (includes ALL persons)
LEFT JOIN {{ ref('dim_person') }} AS p
    ON ve.person_id = p.person_id

-- Order by person and date for analysis
ORDER BY ve.person_id ASC, ve.order_date DESC
