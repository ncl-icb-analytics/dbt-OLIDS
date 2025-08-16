{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Serious Mental Illness (SMI) Register - QOF Mental Health Quality Measures**

Business Logic:
- Mental health diagnosis (MH_COD) NOT in remission (latest_diagnosis > latest_remission OR no remission)
- OR lithium therapy in last 6 months and not stopped
- No age restrictions for SMI register
- Based on legacy fct_person_dx_smi.sql

QOF Context:
Used for serious mental illness quality measures including:
- Mental health care monitoring and review
- Lithium therapy monitoring and safety
- Specialist mental health service coordination
- Recovery and rehabilitation planning

*/

WITH smi_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
            AS latest_resolved_date,

        -- QOF register logic: active diagnosis required
        COALESCE(MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) IS NOT NULL
        AND (
            MAX(
                CASE WHEN is_resolved_code THEN clinical_effective_date END
            ) IS NULL
            OR MAX(
                CASE WHEN is_diagnosis_code THEN clinical_effective_date END
            )
            > MAX(
                CASE WHEN is_resolved_code THEN clinical_effective_date END
            )
        ), FALSE) AS has_active_smi_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_smi_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        ) AS all_smi_concept_displays,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_resolved_code THEN concept_code END
        ) AS all_resolved_concept_codes

    FROM {{ ref('int_smi_diagnoses_all') }}
    GROUP BY person_id
),

lithium_medications AS (

    SELECT
        lith.person_id,
        MAX(lith.order_date) AS latest_lithium_order_date,
        MIN(
            CASE
                WHEN
                    lith.order_date >= CURRENT_DATE - INTERVAL '6 months'
                    THEN lith.order_date
            END
        ) AS earliest_recent_lithium_date,
        COUNT(
            CASE
                WHEN
                    lith.order_date >= CURRENT_DATE - INTERVAL '6 months'
                    THEN 1
            END
        ) AS recent_lithium_orders_count,
        ARRAY_AGG(DISTINCT lith.mapped_concept_code)
            AS all_lithium_concept_codes,
        ARRAY_AGG(DISTINCT lith.mapped_concept_display)
            AS all_lithium_concept_displays

    FROM {{ ref('int_lithium_medications_all') }} AS lith
    WHERE lith.order_date >= CURRENT_DATE - INTERVAL '6 months'  -- Only recent lithium orders
    GROUP BY lith.person_id
),

combined_smi_eligibility AS (

    SELECT
        age.age,
        smi.earliest_diagnosis_date,

        -- Mental health diagnosis details
        smi.latest_diagnosis_date,
        smi.latest_resolved_date,
        smi.has_active_smi_diagnosis,
        lith.latest_lithium_order_date,

        -- Lithium therapy details
        lith.recent_lithium_orders_count,
        smi.all_smi_concept_codes,

        -- SMI Register Logic: Active SMI diagnosis OR recent lithium therapy
        smi.all_smi_concept_displays,

        -- Supporting flags
        smi.all_resolved_concept_codes,
        lith.all_lithium_concept_codes,

        -- Concept arrays
        lith.all_lithium_concept_displays,
        COALESCE(smi.person_id, lith.person_id) AS person_id,
        (
            smi.has_active_smi_diagnosis = TRUE
            OR
            (lith.recent_lithium_orders_count > 0)
        ) AS is_on_register,
        smi.latest_diagnosis_date IS NOT NULL AS has_mh_diagnosis,
        lith.recent_lithium_orders_count > 0 AS is_on_lithium

    FROM smi_diagnoses AS smi
    FULL OUTER JOIN lithium_medications AS lith
        ON smi.person_id = lith.person_id
    INNER JOIN {{ ref('dim_person') }} AS p
        ON COALESCE(smi.person_id, lith.person_id) = p.person_id
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON COALESCE(smi.person_id, lith.person_id) = age.person_id
)

-- Final selection: Only include patients on the SMI register
SELECT
    cse.person_id,
    cse.age,
    cse.is_on_register,
    cse.is_on_lithium,
    cse.has_mh_diagnosis,
    cse.has_active_smi_diagnosis,
    cse.earliest_diagnosis_date,
    cse.latest_diagnosis_date,
    cse.latest_resolved_date,
    cse.latest_lithium_order_date,
    cse.all_smi_concept_codes AS all_mh_concept_codes,
    cse.all_smi_concept_displays AS all_mh_concept_displays,
    cse.all_resolved_concept_codes,
    cse.all_lithium_concept_codes,
    cse.all_lithium_concept_displays

FROM combined_smi_eligibility AS cse
WHERE cse.is_on_register = TRUE
