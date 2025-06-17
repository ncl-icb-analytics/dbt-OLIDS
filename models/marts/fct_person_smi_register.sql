{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
**Serious Mental Illness (SMI) Register - QOF Mental Health Quality Measures**

Pattern 2: Standard QOF Register (Diagnosis + Resolution + Medication Integration)

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

Matches legacy business logic and field structure with simplification.
*/

WITH mental_health_diagnoses AS (
    
    SELECT
        smi.person_id,
        smi.earliest_mh_diagnosis_date,
        smi.latest_mh_diagnosis_date,
        smi.latest_remission_date,
        smi.is_mental_health_currently_in_remission,
        smi.all_mh_diagnosis_concept_codes,
        smi.all_mh_diagnosis_concept_displays,
        smi.all_remission_concept_codes,
        smi.all_remission_concept_displays
        
    FROM {{ ref('int_smi_diagnoses_all') }} smi
    WHERE smi.has_mental_health_diagnosis = TRUE
),

lithium_medications AS (
    
    SELECT
        lith.person_id,
        MAX(lith.order_date) AS latest_lithium_order_date,
        MIN(CASE WHEN lith.order_date >= CURRENT_DATE - INTERVAL '6 months' THEN lith.order_date END) AS earliest_recent_lithium_date,
        COUNT(CASE WHEN lith.order_date >= CURRENT_DATE - INTERVAL '6 months' THEN 1 END) AS recent_lithium_orders_count,
        ARRAY_AGG(DISTINCT lith.mapped_concept_code) AS all_lithium_concept_codes,
        ARRAY_AGG(DISTINCT lith.mapped_concept_display) AS all_lithium_concept_displays
        
    FROM {{ ref('int_lithium_medications_all') }} lith
    WHERE lith.order_date >= CURRENT_DATE - INTERVAL '6 months'  -- Only recent lithium orders
    GROUP BY lith.person_id
),

combined_smi_eligibility AS (
    
    SELECT
        COALESCE(mh.person_id, lith.person_id) AS person_id,
        age.age,
        
        -- Mental health diagnosis details
        mh.earliest_mh_diagnosis_date,
        mh.latest_mh_diagnosis_date,
        mh.latest_remission_date,
        COALESCE(mh.is_mental_health_currently_in_remission, FALSE) AS is_in_remission,
        
        -- Lithium therapy details
        lith.latest_lithium_order_date,
        lith.recent_lithium_orders_count,
        
        -- SMI Register Logic: MH diagnosis not in remission OR recent lithium therapy
        (
            (mh.latest_mh_diagnosis_date IS NOT NULL AND NOT COALESCE(mh.is_mental_health_currently_in_remission, FALSE))
            OR
            (lith.recent_lithium_orders_count > 0)
        ) AS is_on_smi_register,
        
        -- Supporting flags
        mh.latest_mh_diagnosis_date IS NOT NULL AS has_mh_diagnosis,
        lith.recent_lithium_orders_count > 0 AS is_on_lithium,
        
        -- Concept arrays
        mh.all_mh_diagnosis_concept_codes,
        mh.all_mh_diagnosis_concept_displays,
        mh.all_remission_concept_codes,
        mh.all_remission_concept_displays,
        lith.all_lithium_concept_codes,
        lith.all_lithium_concept_displays
        
    FROM mental_health_diagnoses mh
    FULL OUTER JOIN lithium_medications lith
        ON mh.person_id = lith.person_id
    INNER JOIN {{ ref('dim_person') }} p
        ON COALESCE(mh.person_id, lith.person_id) = p.person_id
    INNER JOIN {{ ref('dim_person_age') }} age
        ON COALESCE(mh.person_id, lith.person_id) = age.person_id
)

-- Final selection: Only include patients on the SMI register
SELECT
    cse.person_id,
    cse.age,
    cse.is_on_smi_register,
    cse.is_on_lithium,
    cse.has_mh_diagnosis,
    cse.is_in_remission,
    cse.earliest_mh_diagnosis_date,
    cse.latest_mh_diagnosis_date,
    cse.latest_remission_date,
    cse.latest_lithium_order_date,
    cse.all_mh_diagnosis_concept_codes AS all_mh_concept_codes,
    cse.all_mh_diagnosis_concept_displays AS all_mh_concept_displays,
    cse.all_lithium_concept_codes,
    cse.all_lithium_concept_displays

FROM combined_smi_eligibility cse
WHERE cse.is_on_smi_register = TRUE 