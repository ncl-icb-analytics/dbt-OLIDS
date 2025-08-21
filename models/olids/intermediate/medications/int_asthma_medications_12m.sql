{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'asthma', 'qof'])
}}

/*
Asthma medication orders from the last 12 months for QOF asthma care monitoring.
Uses cluster ID ASTTRT_COD for asthma treatment medications (per QOF specification).
Critical for asthma register and QOF quality indicators.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH asthma_orders_base AS (
    -- Get all medication orders using ASTTRT_COD cluster for asthma treatments
    SELECT
        mo.person_id,
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


        -- QOF asthma care process indicators
        TRUE AS is_asthma_treatment,


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
